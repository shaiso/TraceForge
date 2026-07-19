// Команда restore-api — HTTP-сервис над пакетом restore: одиночный (trace_id ->
// трейс, саппорт) и диапазонный (окно -> трейсы, «Langfuse-просмотрщик») restore из
// архива. Опция ?push=langfuse заливает восстановленное обратно в Langfuse по
// OTLP/HTTP (код-путь готов; живой тест — при поднятом profile full).
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"traceforge/internal/archive"
	"traceforge/internal/dedup"
	"traceforge/internal/restore"
)

func main() {
	addr := flag.String("addr", ":8090", "адрес HTTP-сервиса")
	chDSN := flag.String("ch-dsn", "clickhouse://traceforge:traceforge@localhost:9002/traceforge", "DSN ClickHouse (archive_index)")
	pgDSN := flag.String("pg-dsn", "postgres://traceforge:traceforge@localhost:5433/traceforge?sslmode=disable", "DSN Postgres (dedup-index)")
	minioEndpoint := flag.String("minio-endpoint", "localhost:9000", "endpoint MinIO/S3")
	minioKey := flag.String("minio-key", "minioadmin", "access key MinIO")
	minioSecret := flag.String("minio-secret", "minioadmin", "secret key MinIO")
	casBucket := flag.String("cas-bucket", "traceforge-dedup", "бакет CAS-сегментов (P1)")
	archiveBucket := flag.String("archive-bucket", "traceforge-archive", "бакет бандлов архива (P2)")
	lfEndpoint := flag.String("langfuse-endpoint", "http://localhost:3000/api/public/otel", "OTLP/HTTP endpoint Langfuse (для ?push=langfuse)")
	lfPublic := flag.String("langfuse-public-key", "", "Langfuse public key (Basic auth)")
	lfSecret := flag.String("langfuse-secret-key", "", "Langfuse secret key (Basic auth)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// --- нижние слои: archive_index (CH) + CAS (Postgres+MinIO) + бандлы (MinIO) ---
	archIdx, err := archive.Open(ctx, *chDSN)
	if err != nil {
		log.Fatalf("restore-api: clickhouse: %v", err)
	}
	defer archIdx.Close()

	pool, err := dedup.OpenPool(ctx, *pgDSN)
	if err != nil {
		log.Fatalf("restore-api: postgres: %v", err)
	}
	defer pool.Close()

	casStore, err := dedup.NewMinioBlobStore(ctx, minioConf(*minioEndpoint, *minioKey, *minioSecret, *casBucket))
	if err != nil {
		log.Fatalf("restore-api: minio(cas): %v", err)
	}
	packer, err := dedup.NewSegmentPacker(casStore, dedup.DefaultPackerConfig())
	if err != nil {
		log.Fatalf("restore-api: packer: %v", err)
	}
	defer packer.Close(context.Background())
	cas := dedup.IndexPacker{Index: dedup.NewIndex(pool), Packer: packer}

	bundles, err := dedup.NewMinioBlobStore(ctx, minioConf(*minioEndpoint, *minioKey, *minioSecret, *archiveBucket))
	if err != nil {
		log.Fatalf("restore-api: minio(archive): %v", err)
	}

	restorer, err := restore.New(archIdx, bundles, cas)
	if err != nil {
		log.Fatalf("restore-api: restorer: %v", err)
	}
	defer restorer.Close()

	reg := prometheus.NewRegistry()
	mtr := newMetrics(reg)
	srv := &server{
		restorer: restorer,
		lf:       &langfuseClient{endpoint: *lfEndpoint, public: *lfPublic, secret: *lfSecret},
		mtr:      mtr,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /traces/{trace_id}", srv.handleTrace)
	mux.HandleFunc("GET /traces", srv.handleRange)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	httpSrv := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		log.Printf("restore-api: слушаю %s (GET /traces/{id}, GET /traces?from=&to=, /metrics)", *addr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("restore-api: сервер: %v", err)
		}
	}()

	<-ctx.Done()
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutCtx)
	log.Printf("restore-api: остановлен")
}

func minioConf(endpoint, key, secret, bucket string) dedup.MinioConfig {
	return dedup.MinioConfig{Endpoint: endpoint, AccessKey: key, SecretKey: secret, Bucket: bucket}
}
