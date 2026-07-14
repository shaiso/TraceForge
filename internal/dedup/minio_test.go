package dedup

import (
	"context"
	"os"
	"strings"
	"testing"
)

// testMinioConfig берёт параметры из окружения либо дефолты из compose-стенда.
func testMinioConfig() MinioConfig {
	endpoint := os.Getenv("TRACEFORGE_MINIO_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:9000"
	}
	return MinioConfig{
		Endpoint:  endpoint,
		AccessKey: "minioadmin",
		SecretKey: "minioadmin",
		Bucket:    "traceforge-dedup",
		UseSSL:    false,
	}
}

// Интеграционный тест: packer поверх реального MinIO. Проверяет, что HTTP Range
// достаёт ровно один zstd-фрейм из сегмента и фрагмент восстанавливается
// байт-в-байт. Скипается при недоступном MinIO (требует поднятого стенда).
func TestMinio_PackRestore(t *testing.T) {
	ctx := context.Background()
	store, err := NewMinioBlobStore(ctx, testMinioConfig())
	if err != nil {
		t.Skipf("minio недоступен: %v", err)
	}

	p, err := NewSegmentPacker(store, DefaultPackerConfig())
	if err != nil {
		t.Fatalf("packer: %v", err)
	}
	defer p.Close(ctx)

	inputs := []string{
		"системная инструкция (whole-field) — уникальная #1",
		strings.Repeat("реплика диалога ", 300),
		"короткий уникальный промпт #2",
	}
	entries := make([]Entry, len(inputs))
	for i, s := range inputs {
		e, err := p.Add(ctx, wholeFieldChunk(s))
		if err != nil {
			t.Fatalf("Add[%d]: %v", i, err)
		}
		entries[i] = e
	}
	if err := p.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	for i, e := range entries {
		got, err := p.Get(ctx, e)
		if err != nil {
			t.Fatalf("Get[%d] из MinIO: %v", i, err)
		}
		if string(got) != inputs[i] {
			t.Errorf("фрагмент[%d]: restore не совпал с оригиналом", i)
		}
	}
}
