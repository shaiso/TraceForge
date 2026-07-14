package dedup

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// memBlobStore — in-memory BlobStore для тестов packer'а без поднятого MinIO.
type memBlobStore struct {
	mu   sync.Mutex
	objs map[string][]byte
	puts int // сколько раз вызывали PutObject (счётчик сегментов)
}

func newMemBlobStore() *memBlobStore {
	return &memBlobStore{objs: make(map[string][]byte)}
}

func (m *memBlobStore) PutObject(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objs[key] = append([]byte(nil), data...)
	m.puts++
	return nil
}

func (m *memBlobStore) GetRange(_ context.Context, key string, offset int64, length int) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objs[key]
	if !ok {
		return nil, fmt.Errorf("нет объекта %s", key)
	}
	if offset < 0 || int(offset)+length > len(data) {
		return nil, fmt.Errorf("диапазон вне объекта %s: [%d:+%d] в %d", key, offset, length, len(data))
	}
	return append([]byte(nil), data[offset:int(offset)+length]...), nil
}

func wholeFieldChunk(s string) Chunk {
	sum := sha256.Sum256([]byte(s))
	return Chunk{Hash: append([]byte(nil), sum[:]...), Data: []byte(s)}
}

// Кладём несколько фрагментов, флашим и проверяем, что каждый восстанавливается
// байт-в-байт по своему Entry (range-read одного фрейма).
func TestPacker_AddFlushGet(t *testing.T) {
	store := newMemBlobStore()
	p, err := NewSegmentPacker(store, DefaultPackerConfig())
	if err != nil {
		t.Fatalf("packer: %v", err)
	}
	ctx := context.Background()

	inputs := []string{
		"системная инструкция агента-антиспам",
		strings.Repeat("длинный диалог со спамером ", 50),
		"", // пустой фрагмент — граничный случай
		"ещё один уникальный промпт",
	}

	entries := make([]Entry, len(inputs))
	for i, s := range inputs {
		e, err := p.Add(ctx, wholeFieldChunk(s))
		if err != nil {
			t.Fatalf("Add[%d]: %v", i, err)
		}
		entries[i] = e
	}

	// До флаша объекта в хранилище ещё нет — Get не должен читать «из воздуха».
	if _, err := p.Get(ctx, entries[0]); err == nil {
		t.Errorf("Get до Flush должен падать (объект не выгружен)")
	}

	if err := p.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Все фрагменты в одном сегменте (порог не достигнут) → один PutObject.
	if store.puts != 1 {
		t.Errorf("ожидали 1 сегмент, получили %d", store.puts)
	}

	for i, e := range entries {
		got, err := p.Get(ctx, e)
		if err != nil {
			t.Fatalf("Get[%d]: %v", i, err)
		}
		if string(got) != inputs[i] {
			t.Errorf("фрагмент[%d]: got %q, want %q", i, got, inputs[i])
		}
	}
}

// Порог флаша: много фрагментов → несколько сегментов, все восстановимы.
func TestPacker_FlushBySize(t *testing.T) {
	store := newMemBlobStore()
	// Маленький порог, чтобы каждый ~1КБ фрагмент почти сразу переполнял сегмент.
	p, err := NewSegmentPacker(store, PackerConfig{FlushBytes: 512, Prefix: "seg/"})
	if err != nil {
		t.Fatalf("packer: %v", err)
	}
	ctx := context.Background()

	const n = 40
	inputs := make([]string, n)
	entries := make([]Entry, n)
	for i := 0; i < n; i++ {
		// Уникальный слабосжимаемый контент, чтобы фрейм был заметного размера.
		inputs[i] = fmt.Sprintf("фрагмент-%03d-", i) + strings.Repeat(fmt.Sprintf("%x", i*2654435761&0xff), 200)
		e, err := p.Add(ctx, wholeFieldChunk(inputs[i]))
		if err != nil {
			t.Fatalf("Add[%d]: %v", i, err)
		}
		entries[i] = e
	}
	// Flush (не Close) — финальный сегмент выгружаем, но декодер оставляем для Get.
	if err := p.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	defer p.Close(ctx)

	if store.puts < 2 {
		t.Errorf("ожидали несколько сегментов при малом пороге, получили %d", store.puts)
	}
	// Фрагменты разложены по нескольким сегментам.
	segs := map[string]struct{}{}
	for _, e := range entries {
		segs[e.SegmentKey] = struct{}{}
	}
	if len(segs) < 2 {
		t.Errorf("ожидали >1 сегмента в Entry, получили %d", len(segs))
	}

	for i, e := range entries {
		got, err := p.Get(ctx, e)
		if err != nil {
			t.Fatalf("Get[%d]: %v", i, err)
		}
		if string(got) != inputs[i] {
			t.Errorf("фрагмент[%d] не совпал после restore", i)
		}
	}
}

// Конкурентный Add: воркеры пишут параллельно, все фрагменты восстановимы.
func TestPacker_ConcurrentAdd(t *testing.T) {
	store := newMemBlobStore()
	p, err := NewSegmentPacker(store, PackerConfig{FlushBytes: 4096, Prefix: "seg/"})
	if err != nil {
		t.Fatalf("packer: %v", err)
	}
	ctx := context.Background()

	const n = 64
	var wg sync.WaitGroup
	entries := make([]Entry, n)
	inputs := make([]string, n)
	for i := 0; i < n; i++ {
		inputs[i] = fmt.Sprintf("конкурентный-фрагмент-%03d-%s", i, strings.Repeat("x", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e, err := p.Add(ctx, wholeFieldChunk(inputs[i]))
			if err != nil {
				t.Errorf("Add[%d]: %v", i, err)
				return
			}
			entries[i] = e
		}(i)
	}
	wg.Wait()
	if err := p.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	defer p.Close(ctx)

	for i, e := range entries {
		got, err := p.Get(ctx, e)
		if err != nil {
			t.Fatalf("Get[%d]: %v", i, err)
		}
		if string(got) != inputs[i] {
			t.Errorf("фрагмент[%d] не совпал после конкурентной записи", i)
		}
	}
}
