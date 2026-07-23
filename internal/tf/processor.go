package tf

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"

	"traceforge/internal/dedup"
)

// StreamProcessor обрабатывает спаны по одному (режим потока над spans.thin).
// Вся логика — в Process; консюминг, offset, батчинг записи, метрики — на раннере.
type StreamProcessor interface {
	Process(ctx context.Context, span Span) ([]Result, error)
}

// BatchProcessor обрабатывает трейсы одной сессии целиком (режим батча над архивом).
// Причина видна только по диалогу целиком — стриму это недоступно (см. T6).
type BatchProcessor interface {
	Process(ctx context.Context, traces []Trace) ([]Result, error)
}

// --- стрим-раннер ---

// StreamDeps — зависимости стрим-раннера (коннекты строит вызывающий cmd, как в
// cmd/dedup/archiver; раннер владеет циклом консюминга/offset/записи/метрик).
type StreamDeps struct {
	Brokers     []string
	Topic       string // обычно spans.thin
	Group       string // своя consumer group, параллельно P2
	CAS         dedup.ChunkSource
	Sink        *CHSink
	MetricsAddr string        // "" = без /metrics
	CacheBytes  int64         // кэш фрагментов; 0 -> DefaultCacheBytes
	LogEvery    time.Duration // период лога; 0 -> 3s
}

// RunStream: consumer group читает spans.thin, каждый спан -> proc.Process, результаты
// пишутся в ClickHouse, и ТОЛЬКО потом коммитится offset (данные раньше указателя,
// как P1/P2). At-least-once безопасен: витрины на ReplacingMergeTree схлопывают повтор.
func RunStream(ctx context.Context, d StreamDeps, proc StreamProcessor) error {
	cacheBytes := d.CacheBytes
	if cacheBytes == 0 {
		cacheBytes = DefaultCacheBytes
	}
	cas := newLRU(d.CAS, cacheBytes) // кэш горячих фрагментов на жизнь потока
	logEvery := d.LogEvery
	if logEvery == 0 {
		logEvery = 3 * time.Second
	}

	reg := prometheus.NewRegistry()
	m := newMetrics(reg, "stream")
	serveMetrics(d.MetricsAddr, reg)

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(d.Brokers...),
		kgo.ConsumeTopics(d.Topic),
		kgo.ConsumerGroup(d.Group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return err
	}
	defer consumer.Close()

	log.Printf("tf/stream: брокеры=%v %s группа=%s -> ClickHouse", d.Brokers, d.Topic, d.Group)
	var spans, rows int64
	lastLog := time.Now()
	for {
		fs := consumer.PollFetches(ctx)
		if fs.IsClientClosed() {
			break
		}
		if errs := fs.Errors(); len(errs) > 0 {
			if ctx.Err() != nil {
				break
			}
			for _, e := range errs {
				log.Printf("tf/stream: fetch: %v", e.Err)
			}
			break
		}

		var results []Result
		var perr error
		fs.EachRecord(func(rec *kgo.Record) {
			if perr != nil {
				return
			}
			sp, err := SpanFromThinLine(rec.Value, cas)
			if err != nil {
				log.Printf("tf/stream: decode (пропуск): %v", err)
				return
			}
			rs, err := proc.Process(ctx, sp)
			if err != nil {
				perr = err
				return
			}
			results = append(results, rs...)
			spans++
			m.spans.Inc()
		})
		if perr != nil {
			return perr
		}

		// 1. Запись результатов -> 2. commit offset (порядок «данные раньше указателя»).
		n, err := d.Sink.writeGrouped(ctx, results)
		if err != nil {
			return err
		}
		rows += int64(n)
		m.rows.Add(float64(n))
		if err := consumer.CommitUncommittedOffsets(ctx); err != nil {
			return err
		}

		if time.Since(lastLog) > logEvery {
			log.Printf("tf/stream: спанов=%d строк=%d", spans, rows)
			lastLog = time.Now()
		}
	}
	log.Printf("tf/stream: остановлен (спанов=%d строк=%d)", spans, rows)
	return nil
}

// --- батч-раннер ---

// BatchDeps — зависимости батч-раннера.
type BatchDeps struct {
	Source    Archive
	Sink      *CHSink
	FlushRows int // порог буфера результатов перед записью; 0 -> 5000
}

// RunBatch: LoadRange(окно) -> на каждую сессию proc.Process(её трейсы) -> результаты
// буферизуются и пишутся пачками. Сессия обработана — отпущена (память не растёт по
// размеру окна). Ни Kafka, ни offset — источник хранилище.
func RunBatch(ctx context.Context, d BatchDeps, from, to time.Time, sessionFilter string, proc BatchProcessor) error {
	flushRows := d.FlushRows
	if flushRows == 0 {
		flushRows = 5000
	}
	var buf []Result
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		if _, err := d.Sink.writeGrouped(ctx, buf); err != nil {
			return err
		}
		buf = buf[:0]
		return nil
	}

	var sessions, results int64
	err := d.Source.LoadRange(ctx, from, to, sessionFilter, func(sess Session) error {
		rs, err := proc.Process(ctx, sess.Traces)
		if err != nil {
			return err
		}
		buf = append(buf, rs...)
		sessions++
		results += int64(len(rs))
		if len(buf) >= flushRows {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}
	log.Printf("tf/batch: сессий=%d строк=%d [%s..%s]", sessions, results,
		from.Format(time.RFC3339), to.Format(time.RFC3339))
	return nil
}

// --- метрики ---

type metrics struct {
	spans prometheus.Counter
	rows  prometheus.Counter
}

func newMetrics(reg *prometheus.Registry, mode string) *metrics {
	m := &metrics{
		spans: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tf_" + mode + "_spans_total", Help: "спанов обработано процессором"}),
		rows: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tf_" + mode + "_rows_total", Help: "строк записано в ClickHouse"}),
	}
	reg.MustRegister(m.spans, m.rows)
	return m
}

func serveMetrics(addr string, reg *prometheus.Registry) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("tf: metrics server: %v", err)
		}
	}()
}
