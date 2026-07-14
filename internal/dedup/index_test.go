package dedup

import (
	"context"
	"crypto/rand"
	"os"
	"sync"
	"testing"

	"traceforge/internal/dbmigrate"
)

func testDSN() string {
	if v := os.Getenv("TRACEFORGE_PG_DSN"); v != "" {
		return v
	}
	return "postgres://traceforge:traceforge@localhost:5433/traceforge?sslmode=disable"
}

// openTestIndex накатывает миграции и открывает пул; при недоступности БД
// тест скипается (интеграционный — требует поднятого postgres из compose).
func openTestIndex(t *testing.T) (*Index, func()) {
	t.Helper()
	dsn := testDSN()
	if err := dbmigrate.Postgres(dsn); err != nil {
		t.Skipf("postgres недоступен, миграции не накатились: %v", err)
	}
	pool, err := OpenPool(context.Background(), dsn)
	if err != nil {
		t.Skipf("postgres недоступен: %v", err)
	}
	return NewIndex(pool), func() { pool.Close() }
}

func randHash(t *testing.T) []byte {
	t.Helper()
	h := make([]byte, 32)
	if _, err := rand.Read(h); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return h
}

func TestIndex_PutLookup(t *testing.T) {
	ix, closeFn := openTestIndex(t)
	defer closeFn()
	ctx := context.Background()

	hash := randHash(t)
	defer ix.pool.Exec(ctx, "DELETE FROM dedup_index WHERE hash = $1", hash)

	// miss до вставки
	if _, found, err := ix.Lookup(ctx, hash); err != nil || found {
		t.Fatalf("ожидали miss: found=%v err=%v", found, err)
	}

	want := Entry{Hash: hash, SegmentKey: "seg-test", Offset: 42, Len: 128, CRC: 0xDEADBEEF}
	inserted, err := ix.Put(ctx, want)
	if err != nil || !inserted {
		t.Fatalf("первый Put: inserted=%v err=%v", inserted, err)
	}

	// hit после вставки
	got, found, err := ix.Lookup(ctx, hash)
	if err != nil || !found {
		t.Fatalf("ожидали hit: found=%v err=%v", found, err)
	}
	if got.SegmentKey != want.SegmentKey || got.Offset != want.Offset || got.Len != want.Len || got.CRC != want.CRC {
		t.Errorf("lookup вернул %+v, ожидали %+v", got, want)
	}

	// повторный Put того же хэша → не вставляет
	inserted2, err := ix.Put(ctx, want)
	if err != nil || inserted2 {
		t.Errorf("повторный Put: inserted=%v err=%v (ожидали false)", inserted2, err)
	}
}

func TestIndex_ConcurrentPut(t *testing.T) {
	ix, closeFn := openTestIndex(t)
	defer closeFn()
	ctx := context.Background()

	hash := randHash(t)
	defer ix.pool.Exec(ctx, "DELETE FROM dedup_index WHERE hash = $1", hash)

	const n = 16
	var wg sync.WaitGroup
	var mu sync.Mutex
	insertedCount := 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ins, err := ix.Put(ctx, Entry{Hash: hash, SegmentKey: "seg", Offset: int64(i), Len: 10, CRC: 1})
			if err != nil {
				t.Errorf("Put: %v", err)
				return
			}
			if ins {
				mu.Lock()
				insertedCount++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	if insertedCount != 1 {
		t.Errorf("конкурентная запись одного hash: inserted=%d, ожидали 1", insertedCount)
	}

	// в таблице ровно одна строка на этот хэш
	var rows int
	if err := ix.pool.QueryRow(ctx, "SELECT count(*) FROM dedup_index WHERE hash = $1", hash).Scan(&rows); err != nil {
		t.Fatalf("count: %v", err)
	}
	if rows != 1 {
		t.Errorf("строк с hash в таблице: %d, ожидали 1", rows)
	}
}
