// Команда archiver — процессор P2: читает тонкие спаны из spans.thin (отдельная
// consumer group, независимо от P1-продьюсера и будущего P3) и выносит их в
// постоянный архив — часовые бандлы в S3 (пофреймово, трейс = zstd-фрейм) +
// archive_index в ClickHouse. Это делает историю переживающей retention Kafka и
// даёт «куда откатиться» перед чисткой Langfuse.
//
// Инвариант долговечности (at-least-once): накапливаем спаны по часам (по ts
// события), периодически
//
//	Flush всех бандлов в S3 -> PutBatch в archive_index -> commit Kafka-оффсета
//
// (оффсет последним; при падении переигрываем, максимум — сирота-бандл).
package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"traceforge/internal/archive"
	"traceforge/internal/dbmigrate"
	"traceforge/internal/dedup"
)

func main() {
	brokers := flag.String("brokers", "localhost:19092", "seed-брокеры Kafka/Redpanda через запятую")
	thinTopic := flag.String("thin-topic", "spans.thin", "входной топик тонких спанов")
	group := flag.String("group", "archiver-p2", "consumer group P2")
	chDSN := flag.String("ch-dsn", "clickhouse://traceforge:traceforge@localhost:9002/traceforge", "DSN ClickHouse (archive_index)")
	minioEndpoint := flag.String("minio-endpoint", "localhost:9000", "endpoint MinIO/S3 (host:port)")
	minioKey := flag.String("minio-key", "minioadmin", "access key MinIO")
	minioSecret := flag.String("minio-secret", "minioadmin", "secret key MinIO")
	minioBucket := flag.String("minio-bucket", "traceforge-archive", "бакет для бандлов архива")
	flushBytes := flag.Int("flush-bytes", 32<<20, "порог буфера для флаша бандлов, байт")
	flushInterval := flag.Duration("flush-interval", 10*time.Second, "макс. интервал между флашами")
	duration := flag.Duration("duration", 0, "сколько работать (0 = до сигнала)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	// --- archive_index (ClickHouse): миграции + клиент ---
	if err := dbmigrate.ClickHouse(*chDSN); err != nil {
		log.Fatalf("archiver: миграции clickhouse: %v", err)
	}
	index, err := archive.Open(ctx, *chDSN)
	if err != nil {
		log.Fatalf("archiver: clickhouse: %v", err)
	}
	defer index.Close()

	// --- бандлы (MinIO) + пакер ---
	store, err := dedup.NewMinioBlobStore(ctx, dedup.MinioConfig{
		Endpoint:  *minioEndpoint,
		AccessKey: *minioKey,
		SecretKey: *minioSecret,
		Bucket:    *minioBucket,
	})
	if err != nil {
		log.Fatalf("archiver: minio: %v", err)
	}
	bundleCfg := archive.DefaultBundleConfig()
	bundleCfg.FlushBytes = *flushBytes
	packer, err := archive.NewBundlePacker(store, bundleCfg)
	if err != nil {
		log.Fatalf("archiver: packer: %v", err)
	}
	defer packer.Close()

	// Consumer group: autocommit ВЫКЛ — оффсет коммитим вручную после флаша+индекса.
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ConsumeTopics(*thinTopic),
		kgo.ConsumerGroup(*group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // первый старт — забрать всю историю
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Fatalf("archiver: consumer: %v", err)
	}
	defer consumer.Close()

	log.Printf("archiver: брокеры=%s %s группа=%s -> бакет=%s ch=archive_index",
		*brokers, *thinTopic, *group, *minioBucket)

	var st stats
	// flush: Flush бандлов -> PutBatch индекса -> commit оффсета.
	flush := func(fctx context.Context) error {
		rows, err := packer.Flush(fctx)
		if err != nil {
			return err
		}
		if err := index.PutBatch(fctx, rows); err != nil {
			return err
		}
		if err := consumer.CommitUncommittedOffsets(fctx); err != nil {
			return err
		}
		if len(rows) > 0 {
			st.rows += int64(len(rows))
			st.flushes++
		}
		return nil
	}

	lastFlush := time.Now()
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
				log.Printf("archiver: fetch: %v", e.Err)
			}
			break
		}

		fs.EachRecord(func(rec *kgo.Record) {
			traceID, sessionID, ts, err := parseThin(rec.Value)
			if err != nil {
				log.Printf("archiver: parse (пропуск): %v", err)
				return
			}
			packer.Add(traceID, sessionID, ts, rec.Value)
			st.spans++
		})

		if packer.Buffered() >= *flushBytes || time.Since(lastFlush) >= *flushInterval {
			if err := flush(ctx); err != nil {
				if ctx.Err() != nil {
					break
				}
				log.Fatalf("archiver: флаш: %v", err)
			}
			lastFlush = time.Now()
		}
		if time.Since(lastLog) > 3*time.Second {
			logStats(&st)
			lastLog = time.Now()
		}
	}

	// Финальный флаш недобранного буфера — фоновым контекстом (основной мог истечь).
	if err := flush(context.Background()); err != nil {
		log.Printf("archiver: финальный флаш: %v", err)
	}
	logStats(&st)
	log.Printf("archiver: остановлен")
}

// thinMeta — минимум полей тонкого спана, нужный для маршрутизации в бандл.
type thinMeta struct {
	Span struct {
		TraceID           string `json:"traceId"`
		StartTimeUnixNano string `json:"startTimeUnixNano"`
		Attributes        []struct {
			Key   string `json:"key"`
			Value struct {
				StringValue string `json:"stringValue"`
			} `json:"value"`
		} `json:"attributes"`
	} `json:"span"`
}

// parseThin достаёт canonical trace_id (hex), session.id и ts события из тонкого спана.
func parseThin(v []byte) (traceID, sessionID string, ts time.Time, err error) {
	var m thinMeta
	if err = json.Unmarshal(v, &m); err != nil {
		return "", "", time.Time{}, fmt.Errorf("unmarshal: %w", err)
	}
	raw, e := base64.StdEncoding.DecodeString(m.Span.TraceID)
	if e != nil || len(raw) == 0 {
		return "", "", time.Time{}, fmt.Errorf("bad traceId %q", m.Span.TraceID)
	}
	traceID = hex.EncodeToString(raw)
	for _, a := range m.Span.Attributes {
		if a.Key == "session.id" {
			sessionID = a.Value.StringValue
			break
		}
	}
	nanos, e := strconv.ParseInt(m.Span.StartTimeUnixNano, 10, 64)
	if e != nil {
		return "", "", time.Time{}, fmt.Errorf("bad ts %q", m.Span.StartTimeUnixNano)
	}
	return traceID, sessionID, time.Unix(0, nanos).UTC(), nil
}

type stats struct {
	spans   int64
	rows    int64
	flushes int64
}

func logStats(s *stats) {
	log.Printf("archiver: спанов=%d трейсов_в_индексе=%d флашей=%d", s.spans, s.rows, s.flushes)
}
