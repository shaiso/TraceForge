// Команда batchjob — батчевый пример на SDK: КЛАССИФИКАЦИЯ ПРИЧИН ЗАВЕРШЕНИЯ
// диалога. Почему батч, а не стрим: причина видна только по диалогу ЦЕЛИКОМ, а он
// растянут по трейсам и часам — консюмер, видящий один спан, бессилен. Источник —
// архив (spans.thin осел в бандлы S3 + archive_index), не Kafka: «переанализировать
// вчера» в raw-топике с retention 3–7 дней данных бы не нашёл.
//
// Контур (весь на SDK): LoadRange(окно) -> GroupBySession уже сделан источником ->
// BuildDialog -> Classify -> WriteResults в session_end_reason. Ни Kafka, ни offset.
// Логика — ~30 строк в Process; загрузка/резолв ссылок/сборка диалога/запись — SDK.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"traceforge/internal/archive"
	"traceforge/internal/dbmigrate"
	"traceforge/internal/dedup"
	"traceforge/internal/tf"
)

func main() {
	chDSN := flag.String("ch-dsn", "clickhouse://traceforge:traceforge@localhost:9002/traceforge", "DSN ClickHouse (archive_index + session_end_reason)")
	pgDSN := flag.String("pg-dsn", "postgres://traceforge:traceforge@localhost:5433/traceforge?sslmode=disable", "DSN Postgres (dedup-index)")
	minioEndpoint := flag.String("minio-endpoint", "localhost:9000", "endpoint MinIO/S3")
	minioKey := flag.String("minio-key", "minioadmin", "access key MinIO")
	minioSecret := flag.String("minio-secret", "minioadmin", "secret key MinIO")
	casBucket := flag.String("cas-bucket", "traceforge-dedup", "бакет CAS-сегментов (P1)")
	archiveBucket := flag.String("archive-bucket", "traceforge-archive", "бакет бандлов архива (P2)")
	fromStr := flag.String("from", "", "начало окна RFC3339 (пусто = вчера 00:00 UTC)")
	toStr := flag.String("to", "", "конец окна RFC3339 (пусто = сегодня 00:00 UTC)")
	session := flag.String("session", "", "обработать только эту сессию (пусто = всё окно)")
	flag.Parse()

	from, to, err := window(*fromStr, *toStr)
	if err != nil {
		log.Fatalf("batchjob: окно: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := dbmigrate.ClickHouse(*chDSN); err != nil {
		log.Fatalf("batchjob: миграции clickhouse: %v", err)
	}
	sink, err := tf.OpenCH(ctx, *chDSN)
	if err != nil {
		log.Fatalf("batchjob: clickhouse: %v", err)
	}
	defer sink.Close()

	// --- источник = архив: archive_index (CH) + бандлы (MinIO) + CAS (PG+MinIO) ---
	index, err := archive.Open(ctx, *chDSN)
	if err != nil {
		log.Fatalf("batchjob: archive_index: %v", err)
	}
	defer index.Close()
	bundles, err := dedup.NewMinioBlobStore(ctx, dedup.MinioConfig{
		Endpoint: *minioEndpoint, AccessKey: *minioKey, SecretKey: *minioSecret, Bucket: *archiveBucket,
	})
	if err != nil {
		log.Fatalf("batchjob: minio(archive): %v", err)
	}
	pool, err := dedup.OpenPool(ctx, *pgDSN)
	if err != nil {
		log.Fatalf("batchjob: postgres: %v", err)
	}
	defer pool.Close()
	casStore, err := dedup.NewMinioBlobStore(ctx, dedup.MinioConfig{
		Endpoint: *minioEndpoint, AccessKey: *minioKey, SecretKey: *minioSecret, Bucket: *casBucket,
	})
	if err != nil {
		log.Fatalf("batchjob: minio(cas): %v", err)
	}
	packer, err := dedup.NewSegmentPacker(casStore, dedup.DefaultPackerConfig())
	if err != nil {
		log.Fatalf("batchjob: packer: %v", err)
	}
	defer packer.Close(context.Background())
	cas := dedup.IndexPacker{Index: dedup.NewIndex(pool), Packer: packer}

	src, err := tf.NewArchiveSource(index, bundles, cas)
	if err != nil {
		log.Fatalf("batchjob: источник: %v", err)
	}
	defer src.Close()

	job := &endReasonJob{clf: heuristicClassifier{}}
	if err := tf.RunBatch(ctx, tf.BatchDeps{Source: src, Sink: sink}, from, to, *session, job); err != nil {
		log.Fatalf("batchjob: раннер: %v", err)
	}
}

// window: [from, to) из флагов RFC3339 или дефолт «вчера».
func window(fromStr, toStr string) (time.Time, time.Time, error) {
	now := time.Now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	from, to := today.AddDate(0, 0, -1), today
	var err error
	if fromStr != "" {
		if from, err = time.Parse(time.RFC3339, fromStr); err != nil {
			return time.Time{}, time.Time{}, err
		}
	}
	if toStr != "" {
		if to, err = time.Parse(time.RFC3339, toStr); err != nil {
			return time.Time{}, time.Time{}, err
		}
	}
	return from, to, nil
}
