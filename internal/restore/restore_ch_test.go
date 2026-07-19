package restore_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"traceforge/internal/archive"
	"traceforge/internal/dedup"
	"traceforge/internal/restore"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// isHex32 — canonical W3C trace_id: ровно 32 hex-символа.
func isHex32(s string) bool {
	if len(s) != 32 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func minioCfg(bucket string) dedup.MinioConfig {
	return dedup.MinioConfig{
		Endpoint:  env("MINIO_ENDPOINT", "localhost:9000"),
		AccessKey: env("MINIO_KEY", "minioadmin"),
		SecretKey: env("MINIO_SECRET", "minioadmin"),
		Bucket:    bucket,
	}
}

// TestRestoreFromLiveArchive — веха Фазы 1: полная петля gen->P1->P2->restore.
// Берёт реальный trace_id из archive_index, восстанавливает и проверяет, что все
// ref:sha256 резолвнуты из CAS. Скипается, если стенд/данные недоступны.
func TestRestoreFromLiveArchive(t *testing.T) {
	ctx := context.Background()

	archIdx, err := archive.Open(ctx, env("CH_DSN", "clickhouse://traceforge:traceforge@localhost:9002/traceforge"))
	if err != nil {
		t.Skipf("CH недоступен: %v", err)
	}
	defer archIdx.Close()
	ids, err := archIdx.SampleTraceIDs(ctx, 16)
	if err != nil {
		t.Skipf("archive_index недоступен: %v", err)
	}
	// Берём только canonical trace_id (32 hex) — реальные архиваторные, минуя
	// транзиентные тестовые артефакты параллельных пакетов.
	traceID := ""
	for _, id := range ids {
		if isHex32(id) {
			traceID = id
			break
		}
	}
	if traceID == "" {
		t.Skip("нет реальных трейсов в archive_index (сначала прогнать архиватор)")
	}

	pool, err := dedup.OpenPool(ctx, env("PG_DSN", "postgres://traceforge:traceforge@localhost:5433/traceforge?sslmode=disable"))
	if err != nil {
		t.Skipf("Postgres недоступен: %v", err)
	}
	defer pool.Close()

	casStore, err := dedup.NewMinioBlobStore(ctx, minioCfg("traceforge-dedup"))
	if err != nil {
		t.Skipf("MinIO(cas) недоступен: %v", err)
	}
	packer, err := dedup.NewSegmentPacker(casStore, dedup.DefaultPackerConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer packer.Close(ctx)
	cas := dedup.IndexPacker{Index: dedup.NewIndex(pool), Packer: packer}

	bundles, err := dedup.NewMinioBlobStore(ctx, minioCfg("traceforge-archive"))
	if err != nil {
		t.Skipf("MinIO(archive) недоступен: %v", err)
	}

	r, err := restore.New(archIdx, bundles, cas)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	tr, err := r.Restore(ctx, traceID)
	if err != nil {
		t.Fatalf("Restore(%s): %v", traceID, err)
	}
	if len(tr.Spans) == 0 {
		t.Fatalf("трейс %s: 0 спанов", traceID)
	}

	// Все ref должны быть резолвнуты — в восстановленном трейсе нет ref:sha256.
	for _, rs := range tr.Spans {
		for _, kv := range rs.Span.GetAttributes() {
			if strings.HasPrefix(kv.GetValue().GetStringValue(), dedup.RefPrefix) {
				t.Errorf("нерезолвнутый ref в %s", kv.GetKey())
			}
		}
		for _, ev := range rs.Span.GetEvents() {
			for _, kv := range ev.GetAttributes() {
				if strings.HasPrefix(kv.GetValue().GetStringValue(), dedup.RefPrefix) {
					t.Errorf("нерезолвнутый ref в событии %s", ev.GetName())
				}
			}
		}
	}

	// Детерминизм: повторный restore даёт столько же спанов.
	tr2, err := r.Restore(ctx, traceID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tr2.Spans) != len(tr.Spans) {
		t.Errorf("детерминизм: %d != %d спанов", len(tr2.Spans), len(tr.Spans))
	}
	t.Logf("восстановлен трейс %s: %d спанов, все ref резолвнуты", traceID, len(tr.Spans))
}
