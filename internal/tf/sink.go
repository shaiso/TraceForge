package tf

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// CHSink — приёмник результатов в ClickHouse. Процессор не поднимает коннект сам:
// раннер получает CHSink и зовёт WriteResults. Только вставка (факт-таблицы и
// витрины считаются на чтении / MV), под модель «сессию нельзя закрывать».
type CHSink struct {
	conn driver.Conn
}

// OpenCH подключается к ClickHouse (dsn clickhouse://user:pass@host:port/db).
func OpenCH(ctx context.Context, dsn string) (*CHSink, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("tf: разбор ch dsn: %w", err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("tf: подключение ch: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("tf: ping ch: %w", err)
	}
	return &CHSink{conn: conn}, nil
}

// Close закрывает соединение.
func (s *CHSink) Close() error { return s.conn.Close() }

// WriteResults пишет строки одной таблицы пакетным INSERT. Колонки берутся из
// ключей Values (сортировкой — стабильный порядок), их же перечисляет INSERT, так
// что схема таблицы процессору знать не нужно. Все строки обязаны нести один набор
// ключей. Значения — уже правильных Go-типов под типы столбцов CH (time.Time,
// uint32/64, int64, float64, string).
func (s *CHSink) WriteResults(ctx context.Context, table string, rows []Result) error {
	if len(rows) == 0 {
		return nil
	}
	cols := sortedKeys(rows[0].Values)
	stmt := fmt.Sprintf("INSERT INTO %s (%s)", table, strings.Join(cols, ", "))
	batch, err := s.conn.PrepareBatch(ctx, stmt)
	if err != nil {
		return fmt.Errorf("tf: prepare batch %s: %w", table, err)
	}
	vals := make([]any, len(cols))
	for _, r := range rows {
		for i, c := range cols {
			vals[i] = r.Values[c]
		}
		if err := batch.Append(vals...); err != nil {
			return fmt.Errorf("tf: append %s: %w", table, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("tf: send batch %s: %w", table, err)
	}
	return nil
}

// writeGrouped группирует результаты по таблице и пишет каждую пачку.
func (s *CHSink) writeGrouped(ctx context.Context, results []Result) (int, error) {
	if len(results) == 0 {
		return 0, nil
	}
	byTable := map[string][]Result{}
	var order []string
	for _, r := range results {
		if _, ok := byTable[r.Table]; !ok {
			order = append(order, r.Table)
		}
		byTable[r.Table] = append(byTable[r.Table], r)
	}
	for _, t := range order {
		if err := s.WriteResults(ctx, t, byTable[t]); err != nil {
			return 0, err
		}
	}
	return len(results), nil
}

func sortedKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
