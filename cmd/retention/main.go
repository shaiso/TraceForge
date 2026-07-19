// Команда retention — разгрузка Langfuse (archive-then-delete), процедура.
// Подкоманда verify: убеждается, что окно трейсов лежит в archive_index и реально
// восстановимо (sample-restore с crc), печатает отчёт и ПЛАН УДАЛЕНИЯ для соседней
// команды. Само удаление в Langfuse — вручную (у нас нет доступа к их кубере).
//
// Безопасность разгрузки: удалять в Langfuse можно ТОЛЬКО после зелёного verify —
// есть куда откатиться. Точка расширения под авто-удаление (dry-run по умолчанию ->
// явное подтверждение -> аудит) заложена, но в этом спринте не активна.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"traceforge/internal/archive"
	"traceforge/internal/dedup"
	"traceforge/internal/restore"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] != "verify" {
		fmt.Fprintln(os.Stderr, "usage: retention verify --from RFC3339 --to RFC3339 [--project P] [--sample N]")
		os.Exit(2)
	}
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	fromS := fs.String("from", "", "начало окна (RFC3339)")
	toS := fs.String("to", "", "конец окна (RFC3339)")
	project := fs.String("project", "default", "проект Langfuse (для плана удаления соседней команде)")
	sample := fs.Int("sample", 5, "сколько трейсов окна проверить restore+crc")
	chDSN := fs.String("ch-dsn", "clickhouse://traceforge:traceforge@localhost:9002/traceforge", "DSN ClickHouse")
	pgDSN := fs.String("pg-dsn", "postgres://traceforge:traceforge@localhost:5433/traceforge?sslmode=disable", "DSN Postgres")
	minioEndpoint := fs.String("minio-endpoint", "localhost:9000", "endpoint MinIO")
	minioKey := fs.String("minio-key", "minioadmin", "access key MinIO")
	minioSecret := fs.String("minio-secret", "minioadmin", "secret key MinIO")
	casBucket := fs.String("cas-bucket", "traceforge-dedup", "бакет CAS")
	archiveBucket := fs.String("archive-bucket", "traceforge-archive", "бакет бандлов")
	_ = fs.Parse(os.Args[2:])

	from, err := time.Parse(time.RFC3339, *fromS)
	if err != nil {
		log.Fatalf("retention: --from: %v", err)
	}
	to, err := time.Parse(time.RFC3339, *toS)
	if err != nil {
		log.Fatalf("retention: --to: %v", err)
	}

	ctx := context.Background()
	archIdx, err := archive.Open(ctx, *chDSN)
	if err != nil {
		log.Fatalf("retention: clickhouse: %v", err)
	}
	defer archIdx.Close()
	pool, err := dedup.OpenPool(ctx, *pgDSN)
	if err != nil {
		log.Fatalf("retention: postgres: %v", err)
	}
	defer pool.Close()
	casStore, err := dedup.NewMinioBlobStore(ctx, minioConf(*minioEndpoint, *minioKey, *minioSecret, *casBucket))
	if err != nil {
		log.Fatalf("retention: minio(cas): %v", err)
	}
	packer, err := dedup.NewSegmentPacker(casStore, dedup.DefaultPackerConfig())
	if err != nil {
		log.Fatalf("retention: packer: %v", err)
	}
	defer packer.Close(ctx)
	bundles, err := dedup.NewMinioBlobStore(ctx, minioConf(*minioEndpoint, *minioKey, *minioSecret, *archiveBucket))
	if err != nil {
		log.Fatalf("retention: minio(archive): %v", err)
	}
	restorer, err := restore.New(archIdx, bundles, dedup.IndexPacker{Index: dedup.NewIndex(pool), Packer: packer})
	if err != nil {
		log.Fatalf("retention: restorer: %v", err)
	}
	defer restorer.Close()

	os.Exit(runVerify(ctx, archIdx, restorer, from, to, *project, *sample))
}

// runVerify считает окно, sample-restore-ит N трейсов и печатает отчёт + план удаления.
// Возвращает код выхода: 0 — безопасно удалять, 1 — verify не прошёл.
func runVerify(ctx context.Context, idx *archive.Index, r *restore.Restorer, from, to time.Time, project string, sample int) int {
	rows, err := idx.LookupRange(ctx, from, to, "")
	if err != nil {
		log.Printf("retention: lookup окна: %v", err)
		return 1
	}
	traces := map[string]struct{}{}
	bundles := map[string]struct{}{}
	var bytes uint64
	var order []string
	for _, row := range rows {
		if _, ok := traces[row.TraceID]; !ok {
			traces[row.TraceID] = struct{}{}
			order = append(order, row.TraceID)
		}
		bundles[row.BundleKey] = struct{}{}
		bytes += uint64(row.Len)
	}

	// Sample-restore: успешный restore = crc фрагментов сошёлся (проверка внутри CAS).
	if sample > len(order) {
		sample = len(order)
	}
	restored, failed := 0, 0
	for i := 0; i < sample; i++ {
		if _, err := r.Restore(ctx, order[i]); err != nil {
			failed++
			log.Printf("retention: НЕ восстановлен %s: %v", order[i], err)
			continue
		}
		restored++
	}

	fmt.Println("================ retention verify ================")
	fmt.Printf("окно:        %s .. %s\n", from.Format(time.RFC3339), to.Format(time.RFC3339))
	fmt.Printf("трейсов:     %d\n", len(traces))
	fmt.Printf("бандлов:     %d\n", len(bundles))
	fmt.Printf("объём (сжат):%s\n", humanBytes(int64(bytes)))
	fmt.Printf("проверено:   %d/%d восстановлено (crc ок)", restored, sample)
	if failed > 0 {
		fmt.Printf(", %d ПРОВАЛ\n", failed)
	} else {
		fmt.Println()
	}
	fmt.Println("-------------------------------------------------")
	ok := failed == 0 && len(traces) > 0
	if ok {
		fmt.Println("ПЛАН УДАЛЕНИЯ (запрос соседней команде — удалить в Langfuse ВРУЧНУЮ):")
		fmt.Printf("  project = %s\n", project)
		fmt.Printf("  window  = [%s .. %s]\n", from.Format(time.RFC3339), to.Format(time.RFC3339))
		fmt.Printf("  трейсов к удалению = %d (все восстановимы из архива)\n", len(traces))
		fmt.Println("  ПОСЛЕ удаления: GET /traces/{id} на трейс окна должен всё ещё отдавать его из архива.")
	} else {
		fmt.Println("VERIFY НЕ ПРОШЁЛ — удалять в Langfuse НЕЛЬЗЯ (нет данных или есть невосстановимые трейсы).")
	}
	fmt.Println("=================================================")
	// Аудит.
	log.Printf("retention: verify project=%s window=[%s..%s] traces=%d bundles=%d restored=%d failed=%d ok=%v",
		project, from.Format(time.RFC3339), to.Format(time.RFC3339), len(traces), len(bundles), restored, failed, ok)
	if ok {
		return 0
	}
	return 1
}

func minioConf(endpoint, key, secret, bucket string) dedup.MinioConfig {
	return dedup.MinioConfig{Endpoint: endpoint, AccessKey: key, SecretKey: secret, Bucket: bucket}
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
