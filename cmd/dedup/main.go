// Команда dedup — процессор P1: читает otlp.raw.spans, выносит тяжёлый LLM-контент
// спанов в CAS (dedup-index в Postgres + сегменты в MinIO) и пишет тонкие спаны с
// ссылками ref:sha256 в spans.thin. Обратимость доказывается Restore API/T7.
//
// Инвариант корректности (at-least-once + обратимость): на каждый poll-батч
//   process все спаны -> Processor.Commit (Flush сегмента, затем Put в индекс)
//   -> produce тонких спанов -> commit Kafka-оффсета источника.
// Порядок гарантирует, что индекс не ссылается на невыгруженный сегмент, а оффсет
// не двигается раньше, чем тонкие спаны отданы дальше.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"traceforge/internal/dbmigrate"
	"traceforge/internal/dedup"
	"traceforge/internal/otlp"
)

func main() {
	brokers := flag.String("brokers", "localhost:19092", "seed-брокеры Kafka/Redpanda через запятую")
	srcTopic := flag.String("src-topic", "otlp.raw.spans", "входной топик сырых OTLP-пачек")
	thinTopic := flag.String("thin-topic", "spans.thin", "выходной топик тонких спанов")
	group := flag.String("group", "dedup-p1", "consumer group P1")
	pgDSN := flag.String("pg-dsn", "postgres://traceforge:traceforge@localhost:5433/traceforge?sslmode=disable", "DSN Postgres (dedup-index)")
	minioEndpoint := flag.String("minio-endpoint", "localhost:9000", "endpoint MinIO/S3 (host:port)")
	minioKey := flag.String("minio-key", "minioadmin", "access key MinIO")
	minioSecret := flag.String("minio-secret", "minioadmin", "secret key MinIO")
	minioBucket := flag.String("minio-bucket", "traceforge-dedup", "бакет для сегментов")
	chunkerKind := flag.String("chunker", string(dedup.KindWholeField), "режим чанкинга: whole-field|fastcdc")
	minSize := flag.Int("min-size", 1024, "минимальный размер строки для дедупа, байт")
	flushBytes := flag.Int("flush-bytes", 96<<20, "порог флаша сегмента, байт")
	metricsAddr := flag.String("metrics-addr", ":9464", "адрес Prometheus /metrics (пусто = выкл)")
	duration := flag.Duration("duration", 0, "сколько работать (0 = до сигнала)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	// --- dedup-index (Postgres): миграции + пул ---
	if err := dbmigrate.Postgres(*pgDSN); err != nil {
		log.Fatalf("dedup: миграции postgres: %v", err)
	}
	pool, err := dedup.OpenPool(ctx, *pgDSN)
	if err != nil {
		log.Fatalf("dedup: postgres: %v", err)
	}
	defer pool.Close()
	index := dedup.NewIndex(pool)

	// --- сегменты (MinIO) + packer ---
	store, err := dedup.NewMinioBlobStore(ctx, dedup.MinioConfig{
		Endpoint:  *minioEndpoint,
		AccessKey: *minioKey,
		SecretKey: *minioSecret,
		Bucket:    *minioBucket,
	})
	if err != nil {
		log.Fatalf("dedup: minio: %v", err)
	}
	packerCfg := dedup.DefaultPackerConfig()
	packerCfg.FlushBytes = *flushBytes
	packer, err := dedup.NewSegmentPacker(store, packerCfg)
	if err != nil {
		log.Fatalf("dedup: packer: %v", err)
	}

	// --- чанкер + экстрактор ---
	chunker, err := dedup.New(dedup.Kind(*chunkerKind))
	if err != nil {
		log.Fatalf("dedup: %v", err)
	}
	exCfg := dedup.DefaultExtractConfig()
	exCfg.MinSize = *minSize
	extractor := dedup.NewExtractor(exCfg)

	proc := dedup.NewProcessor(extractor, chunker, index, packer)
	defer func() {
		if err := proc.Close(context.Background()); err != nil {
			log.Printf("dedup: закрытие packer: %v", err)
		}
	}()

	// Prometheus /metrics (в фоне).
	reg := prometheus.NewRegistry()
	mtr := newMetrics(reg)
	serveMetrics(*metricsAddr, reg)

	seeds := strings.Split(*brokers, ",")

	// Producer для тонких спанов (отдельный клиент, ключ = trace_id).
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.DefaultProduceTopic(*thinTopic),
	)
	if err != nil {
		log.Fatalf("dedup: producer: %v", err)
	}
	defer producer.Close()

	// Consumer group: autocommit ВЫКЛ — оффсет коммитим вручную после produce.
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumeTopics(*srcTopic),
		kgo.ConsumerGroup(*group),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Fatalf("dedup: consumer: %v", err)
	}
	defer consumer.Close()

	log.Printf("dedup: чанкер=%s брокеры=%s %s -> %s группа=%s",
		*chunkerKind, *brokers, *srcTopic, *thinTopic, *group)

	lastLog := time.Now()
	for {
		fs := consumer.PollFetches(ctx)
		if fs.IsClientClosed() {
			break
		}
		if errs := fs.Errors(); len(errs) > 0 {
			if ctx.Err() != nil {
				break // штатное завершение (сигнал/таймаут)
			}
			for _, e := range errs {
				log.Printf("dedup: fetch: %v", e.Err)
			}
			break
		}

		if err := processBatch(ctx, proc, producer, consumer, fs); err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Fatalf("dedup: обработка батча: %v", err)
		}

		mtr.update(&proc.Stats)
		if time.Since(lastLog) > 3*time.Second {
			logStats(&proc.Stats)
			lastLog = time.Now()
		}
	}

	mtr.update(&proc.Stats)
	logStats(&proc.Stats)
	log.Printf("dedup: остановлен")
}

// processBatch обрабатывает один poll: мутирует спаны, собирает тонкие спаны,
// затем в правильном порядке фиксирует CAS, публикует спаны и коммитит оффсет.
func processBatch(ctx context.Context, proc *dedup.Processor, producer, consumer *kgo.Client, fs kgo.Fetches) error {
	var thin []*kgo.Record

	var walkErr error
	fs.EachRecord(func(rec *kgo.Record) {
		if walkErr != nil {
			return
		}
		req, err := otlp.Unmarshal(rec.Value)
		if err != nil {
			log.Printf("dedup: unmarshal (пропуск сообщения): %v", err)
			return
		}
		otlp.Walk(req, func(f otlp.FlatSpan) {
			if walkErr != nil {
				return
			}
			val, err := proc.Process(ctx, f)
			if err != nil {
				walkErr = err
				return
			}
			thin = append(thin, &kgo.Record{
				Key:   append([]byte(nil), f.Span.GetTraceId()...),
				Value: val,
			})
		})
	})
	if walkErr != nil {
		return walkErr
	}

	// 1. Flush сегмента + Put фрагментов в индекс (ссылки становятся валидными).
	if err := proc.Commit(ctx); err != nil {
		return err
	}
	// 2. Publish тонких спанов (после Commit — ref уже резолвятся из индекса).
	if len(thin) > 0 {
		if err := producer.ProduceSync(ctx, thin...).FirstErr(); err != nil {
			return err
		}
	}
	// 3. Commit Kafka-оффсета источника (последним — источник переигрывается при сбое).
	return consumer.CommitUncommittedOffsets(ctx)
}

func logStats(s *dedup.Stats) {
	log.Printf("dedup: спанов=%d полей=%d фрагментов=%d (уник=%d, дублей=%d) контент=%s -> хранимо=%s+ссылки=%s",
		s.Spans, s.Candidates, s.Chunks, s.Misses, s.Hits,
		humanBytes(s.ContentBytes), humanBytes(s.StoredBytes), humanBytes(s.RefBytes))
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(n)/float64(div), "KMGTPE"[exp])
}
