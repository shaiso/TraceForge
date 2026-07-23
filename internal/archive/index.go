// Package archive — постоянный архив тонких спанов: часовые бандлы в S3 и
// archive_index в ClickHouse («где лежит трейс»). Index — клиент над archive_index:
// пакетная запись из P2 и lookup'ы для Restore (одиночный по trace_id, диапазонный
// по ts). Дедуп ReplacingMergeTree разрешается на чтении через SELECT ... FINAL.
package archive

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Row — строка archive_index: координаты фрейма трейса внутри часового бандла.
type Row struct {
	TraceID   string
	SessionID string
	TS        time.Time // время события (min ts спанов трейса в бандле)
	BundleKey string
	Offset    uint64 // смещение zstd-фрейма трейса в бандле
	Len       uint32 // длина сжатого фрейма
}

// Index — клиент archive_index (ClickHouse).
type Index struct {
	conn driver.Conn
}

// Open подключается к CH по dsn (clickhouse://user:pass@host:port/db) и проверяет связь.
func Open(ctx context.Context, dsn string) (*Index, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("archive: разбор ch dsn: %w", err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("archive: подключение ch: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("archive: ping ch: %w", err)
	}
	return &Index{conn: conn}, nil
}

// Close закрывает соединение.
func (ix *Index) Close() error { return ix.conn.Close() }

const insertStmt = `INSERT INTO archive_index (trace_id, session_id, ts, bundle_key, offset_, len_)`

// PutBatch пишет строки пакетно (inserted_at проставит DEFAULT now() — версия для
// ReplacingMergeTree). Звать после флаша бандла в S3, до commit Kafka-оффсета.
func (ix *Index) PutBatch(ctx context.Context, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := ix.conn.PrepareBatch(ctx, insertStmt)
	if err != nil {
		return fmt.Errorf("archive: prepare batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(r.TraceID, r.SessionID, r.TS, r.BundleKey, r.Offset, r.Len); err != nil {
			return fmt.Errorf("archive: append %s: %w", r.TraceID, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("archive: send batch: %w", err)
	}
	return nil
}

const selectCols = `SELECT trace_id, session_id, ts, bundle_key, offset_, len_ FROM archive_index FINAL`

// LookupTrace возвращает координаты трейса — обычно одна строка, но растянутый по
// часам трейс даёт несколько (по фрейму в каждом бандле); Restore склеит по порядку ts.
func (ix *Index) LookupTrace(ctx context.Context, traceID string) ([]Row, error) {
	rows, err := ix.conn.Query(ctx, selectCols+` WHERE trace_id = ? ORDER BY ts`, traceID)
	if err != nil {
		return nil, fmt.Errorf("archive: lookup %s: %w", traceID, err)
	}
	return scanRows(rows)
}

// LookupRange возвращает трейсы окна [from, to] (опц. фильтр по session_id),
// отсортированные по (bundle_key, offset) — так RestoreRange качает каждый бандл раз.
func (ix *Index) LookupRange(ctx context.Context, from, to time.Time, sessionID string) ([]Row, error) {
	q := selectCols + ` WHERE ts BETWEEN ? AND ?`
	args := []any{from, to}
	if sessionID != "" {
		q += ` AND session_id = ?`
		args = append(args, sessionID)
	}
	q += ` ORDER BY bundle_key, offset_`
	rows, err := ix.conn.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("archive: lookup range: %w", err)
	}
	return scanRows(rows)
}

// LookupSession возвращает все фреймы сессии (по всем её трейсам/часам),
// отсортированные по времени события — под LoadSession в SDK.
func (ix *Index) LookupSession(ctx context.Context, sessionID string) ([]Row, error) {
	rows, err := ix.conn.Query(ctx, selectCols+` WHERE session_id = ? ORDER BY ts`, sessionID)
	if err != nil {
		return nil, fmt.Errorf("archive: lookup session %s: %w", sessionID, err)
	}
	return scanRows(rows)
}

// SampleTraceIDs возвращает до n различных trace_id из индекса (для verify-джоба и
// смоука — взять образцы для проверки восстановимости).
func (ix *Index) SampleTraceIDs(ctx context.Context, n int) ([]string, error) {
	rows, err := ix.conn.Query(ctx, `SELECT DISTINCT trace_id FROM archive_index FINAL LIMIT ?`, n)
	if err != nil {
		return nil, fmt.Errorf("archive: sample: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("archive: sample scan: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func scanRows(rows driver.Rows) ([]Row, error) {
	defer rows.Close()
	var out []Row
	for rows.Next() {
		var r Row
		if err := rows.Scan(&r.TraceID, &r.SessionID, &r.TS, &r.BundleKey, &r.Offset, &r.Len); err != nil {
			return nil, fmt.Errorf("archive: scan: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
