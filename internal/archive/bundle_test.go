package archive

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

// memStore — in-memory реализация dedup.BlobStore для теста пакера без MinIO.
type memStore struct{ objs map[string][]byte }

func (m *memStore) PutObject(_ context.Context, key string, data []byte) error {
	m.objs[key] = append([]byte(nil), data...)
	return nil
}

func (m *memStore) GetRange(_ context.Context, key string, offset int64, length int) ([]byte, error) {
	b, ok := m.objs[key]
	if !ok {
		return nil, fmt.Errorf("нет объекта %s", key)
	}
	return append([]byte(nil), b[offset:offset+int64(length)]...), nil
}

// TestBundleFrameRoundtrip: трейс = один zstd-фрейм; range-read по (offset,len) из
// бандла -> decode -> ровно тонкие спаны трейса (JSONL). Плюс раскладка по часам.
func TestBundleFrameRoundtrip(t *testing.T) {
	ctx := context.Background()
	store := &memStore{objs: map[string][]byte{}}
	bp, err := NewBundlePacker(store, BundleConfig{FlushBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	defer bp.Close()

	h10 := time.Date(2026, 7, 15, 10, 0, 0, 0, time.UTC)
	h11 := time.Date(2026, 7, 15, 11, 0, 0, 0, time.UTC)

	// tA: два спана в час 10; tB: один спан в час 10; tC: один спан в час 11.
	bp.Add("tA", "s1", h10.Add(2*time.Second), []byte(`{"span":"A1"}`))
	bp.Add("tA", "s1", h10.Add(1*time.Second), []byte(`{"span":"A2"}`)) // раньше -> minTS
	bp.Add("tB", "s1", h10.Add(5*time.Second), []byte(`{"span":"B1"}`))
	bp.Add("tC", "s2", h11.Add(1*time.Second), []byte(`{"span":"C1"}`))

	rows, err := bp.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("жду 3 строки (tA,tB,tC), получил %d", len(rows))
	}

	byTrace := map[string]Row{}
	for _, r := range rows {
		byTrace[r.TraceID] = r
	}
	// tA и tB — в одном бандле (час 10), tC — в другом (час 11).
	if byTrace["tA"].BundleKey != byTrace["tB"].BundleKey {
		t.Errorf("tA и tB должны быть в одном бандле часа 10")
	}
	if byTrace["tC"].BundleKey == byTrace["tA"].BundleKey {
		t.Errorf("tC (час 11) должен быть в отдельном бандле")
	}
	// minTS трейса tA — самый ранний спан (h10+1s).
	if !byTrace["tA"].TS.Equal(h10.Add(1 * time.Second)) {
		t.Errorf("tA.TS должен быть minTS спанов, got %v", byTrace["tA"].TS)
	}
	if !strings.Contains(byTrace["tC"].BundleKey, "hour=11") {
		t.Errorf("бандл tC должен быть в hour=11, got %s", byTrace["tC"].BundleKey)
	}

	// Восстановление фрейма: range-read -> zstd decode -> JSONL спанов трейса.
	dec, _ := zstd.NewReader(nil)
	defer dec.Close()
	frame, err := store.GetRange(ctx, byTrace["tA"].BundleKey, int64(byTrace["tA"].Offset), int(byTrace["tA"].Len))
	if err != nil {
		t.Fatal(err)
	}
	data, err := dec.DecodeAll(frame, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"span":"A1"}` + "\n" + `{"span":"A2"}` + "\n"
	if string(data) != want {
		t.Errorf("фрейм tA:\n got %q\nwant %q", data, want)
	}
}

// TestBundleLateSpanNewPart: поздний спан того же часа после флаша -> новый part
// (новая строка индекса), а не потеря/переписывание.
func TestBundleLateSpanNewPart(t *testing.T) {
	ctx := context.Background()
	store := &memStore{objs: map[string][]byte{}}
	bp, _ := NewBundlePacker(store, BundleConfig{FlushBytes: 1 << 30})
	defer bp.Close()

	h := time.Date(2026, 7, 15, 9, 0, 0, 0, time.UTC)
	bp.Add("tX", "s1", h.Add(1*time.Second), []byte(`{"span":"X1"}`))
	rows1, _ := bp.Flush(ctx)

	// Поздний спан того же трейса/часа приходит после флаша.
	bp.Add("tX", "s1", h.Add(30*time.Second), []byte(`{"span":"X2"}`))
	rows2, _ := bp.Flush(ctx)

	if len(rows1) != 1 || len(rows2) != 1 {
		t.Fatalf("жду по 1 строке в каждом флаше, got %d и %d", len(rows1), len(rows2))
	}
	if rows1[0].BundleKey == rows2[0].BundleKey {
		t.Errorf("поздний спан должен уехать в НОВЫЙ part, а не тот же бандл")
	}
	if len(store.objs) != 2 {
		t.Errorf("жду 2 объекта-бандла (два part часа 09), got %d", len(store.objs))
	}
}
