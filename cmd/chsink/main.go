// Команда chsink — процессор P3: третий консюмер spans.thin (своя consumer group,
// параллельно P2-архиватору). Наполняет агрегационный слой ClickHouse (tf_spans +
// user_sessions) — то, чего Langfuse на объёмах научрука не даёт. Написан НА SDK
// (internal/tf): вся инфраструктура (консюминг, offset, батчинг записи, метрики) —
// в раннере tf.RunStream, логика процессора — ~40 строк в processor.go.
//
// Порядок долговечности (at-least-once): записать строки в ClickHouse -> commit
// Kafka-оффсета (данные раньше указателя, как P1/P2). Повтор безопасен —
// ReplacingMergeTree схлопывает задвоенные спаны.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"

	"traceforge/internal/dbmigrate"
	"traceforge/internal/dedup"
	"traceforge/internal/tf"
)

func main() {
	brokers := flag.String("brokers", "localhost:19092", "seed-брокеры Kafka/Redpanda через запятую")
	thinTopic := flag.String("thin-topic", "spans.thin", "входной топик тонких спанов")
	group := flag.String("group", "chsink-p3", "consumer group P3 (параллельно P2)")
	chDSN := flag.String("ch-dsn", "clickhouse://traceforge:traceforge@localhost:9002/traceforge", "DSN ClickHouse (tf_spans)")
	pgDSN := flag.String("pg-dsn", "postgres://traceforge:traceforge@localhost:5433/traceforge?sslmode=disable", "DSN Postgres (dedup-index, для резолва ref)")
	minioEndpoint := flag.String("minio-endpoint", "localhost:9000", "endpoint MinIO/S3")
	minioKey := flag.String("minio-key", "minioadmin", "access key MinIO")
	minioSecret := flag.String("minio-secret", "minioadmin", "secret key MinIO")
	casBucket := flag.String("cas-bucket", "traceforge-dedup", "бакет CAS-сегментов (P1)")
	metricsAddr := flag.String("metrics-addr", ":9465", "адрес Prometheus /metrics (пусто = выкл)")
	duration := flag.Duration("duration", 0, "сколько работать (0 = до сигнала)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	// --- агрегационный слой (ClickHouse): миграции + приёмник SDK ---
	if err := dbmigrate.ClickHouse(*chDSN); err != nil {
		log.Fatalf("chsink: миграции clickhouse: %v", err)
	}
	sink, err := tf.OpenCH(ctx, *chDSN)
	if err != nil {
		log.Fatalf("chsink: clickhouse: %v", err)
	}
	defer sink.Close()

	// --- CAS для ленивого резолва ref (Postgres dedup-index + MinIO сегменты) ---
	pool, err := dedup.OpenPool(ctx, *pgDSN)
	if err != nil {
		log.Fatalf("chsink: postgres: %v", err)
	}
	defer pool.Close()
	casStore, err := dedup.NewMinioBlobStore(ctx, dedup.MinioConfig{
		Endpoint: *minioEndpoint, AccessKey: *minioKey, SecretKey: *minioSecret, Bucket: *casBucket,
	})
	if err != nil {
		log.Fatalf("chsink: minio: %v", err)
	}
	packer, err := dedup.NewSegmentPacker(casStore, dedup.DefaultPackerConfig())
	if err != nil {
		log.Fatalf("chsink: packer: %v", err)
	}
	defer packer.Close(context.Background())
	cas := dedup.IndexPacker{Index: dedup.NewIndex(pool), Packer: packer}

	deps := tf.StreamDeps{
		Brokers:     strings.Split(*brokers, ","),
		Topic:       *thinTopic,
		Group:       *group,
		CAS:         cas,
		Sink:        sink,
		MetricsAddr: *metricsAddr,
	}
	if err := tf.RunStream(ctx, deps, &p3Processor{}); err != nil {
		log.Fatalf("chsink: раннер: %v", err)
	}
}
