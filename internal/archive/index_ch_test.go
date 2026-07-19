package archive

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"traceforge/internal/dbmigrate"
)

// chDSN — адрес нашего ClickHouse из стенда (переопределяемо через CH_DSN).
func chDSN() string {
	if v := os.Getenv("CH_DSN"); v != "" {
		return v
	}
	return "clickhouse://traceforge:traceforge@localhost:9002/traceforge"
}

// TestArchiveIndexRoundtrip — интеграционный тест T1: миграция накатывается, запись
// пакетом, одиночный и диапазонный lookup, идемпотентность через ReplacingMergeTree
// (SELECT FINAL схлопывает повторную вставку той же строки). Скипается без CH.
func TestArchiveIndexRoundtrip(t *testing.T) {
	ctx := context.Background()

	if err := dbmigrate.ClickHouse(chDSN()); err != nil {
		t.Skipf("CH недоступен (нужен make up): %v", err)
	}
	ix, err := Open(ctx, chDSN())
	if err != nil {
		t.Skipf("CH недоступен: %v", err)
	}
	defer ix.Close()

	// Уникальный trace_id — чтобы прогоны не мешали друг другу; после теста удаляем.
	tid := fmt.Sprintf("t-test-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_ = ix.conn.Exec(context.Background(), `DELETE FROM archive_index WHERE trace_id = ?`, tid)
	})
	now := time.Now().Truncate(time.Millisecond)
	rows := []Row{
		{TraceID: tid, SessionID: "sess-A", TS: now, BundleKey: "date=x/hour=10/part-0.jsonl.zst", Offset: 0, Len: 111},
		{TraceID: tid, SessionID: "sess-A", TS: now.Add(-2 * time.Hour), BundleKey: "date=x/hour=08/part-0.jsonl.zst", Offset: 111, Len: 222},
	}
	if err := ix.PutBatch(ctx, rows); err != nil {
		t.Fatalf("PutBatch: %v", err)
	}

	// Одиночный lookup: растянутый по часам трейс -> обе строки, по возрастанию ts.
	got, err := ix.LookupTrace(ctx, tid)
	if err != nil {
		t.Fatalf("LookupTrace: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("LookupTrace: жду 2 строки (трейс в 2 бандлах), получил %d", len(got))
	}
	if !got[0].TS.Before(got[1].TS) {
		t.Errorf("LookupTrace: строки не по возрастанию ts: %v", got)
	}
	if got[1].Len != 111 || got[1].Offset != 0 {
		t.Errorf("LookupTrace: поздний бандл должен нести offset=0,len=111, got %+v", got[1])
	}

	// Диапазонный lookup: окно 3ч захватывает оба фрейма трейса.
	rng, err := ix.LookupRange(ctx, now.Add(-3*time.Hour), now.Add(time.Minute), "")
	if err != nil {
		t.Fatalf("LookupRange: %v", err)
	}
	var seen int
	for _, r := range rng {
		if r.TraceID == tid {
			seen++
		}
	}
	if seen != 2 {
		t.Fatalf("LookupRange: жду 2 строки нашего трейса в окне, получил %d", seen)
	}

	// Фильтр по session_id, которого нет -> нашего трейса не видно.
	other, err := ix.LookupRange(ctx, now.Add(-3*time.Hour), now.Add(time.Minute), "sess-NONE")
	if err != nil {
		t.Fatalf("LookupRange filter: %v", err)
	}
	for _, r := range other {
		if r.TraceID == tid {
			t.Errorf("LookupRange: фильтр session_id пропустил чужой трейс: %+v", r)
		}
	}

	// Идемпотентность: повторная вставка тех же строк -> FINAL схлопывает до 2.
	if err := ix.PutBatch(ctx, rows); err != nil {
		t.Fatalf("PutBatch (повтор): %v", err)
	}
	dedup, err := ix.LookupTrace(ctx, tid)
	if err != nil {
		t.Fatalf("LookupTrace (после повтора): %v", err)
	}
	if len(dedup) != 2 {
		t.Errorf("идемпотентность: после повторной вставки жду 2 строки (FINAL дедуп), получил %d", len(dedup))
	}
}
