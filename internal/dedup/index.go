package dedup

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Entry — строка dedup-index: хэш фрагмента и где его байты лежат в сегменте S3.
type Entry struct {
	Hash       []byte
	SegmentKey string
	Offset     int64
	Len        int32
	CRC        uint32
}

// Index — dedup-index поверх Postgres. Хранит уникальные хэши фрагментов и их
// расположение; идемпотентность at-least-once обеспечивается PRIMARY KEY(hash)
// + ON CONFLICT DO NOTHING (тот же кусок → тот же хэш → не задвоится).
type Index struct {
	pool *pgxpool.Pool
}

func NewIndex(pool *pgxpool.Pool) *Index {
	return &Index{pool: pool}
}

// OpenPool создаёт пул соединений и проверяет доступность БД.
func OpenPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("dedup: пул postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("dedup: ping postgres: %w", err)
	}
	return pool, nil
}

// Lookup ищет фрагмент по хэшу. found=false — хэша нет (это miss, надо упаковать).
func (ix *Index) Lookup(ctx context.Context, hash []byte) (Entry, bool, error) {
	e := Entry{Hash: hash}
	var crc int64
	err := ix.pool.QueryRow(ctx,
		`SELECT segment_key, offset_, len_, crc FROM dedup_index WHERE hash = $1`,
		hash,
	).Scan(&e.SegmentKey, &e.Offset, &e.Len, &crc)
	if errors.Is(err, pgx.ErrNoRows) {
		return Entry{}, false, nil
	}
	if err != nil {
		return Entry{}, false, fmt.Errorf("dedup lookup %s: %w", hex.EncodeToString(hash), err)
	}
	e.CRC = uint32(crc)
	return e, true, nil
}

// Put вставляет фрагмент, если его ещё нет. inserted=true — строку записали мы;
// inserted=false — хэш уже был (гонка воркеров разрешена ON CONFLICT, наши байты
// в сегменте становятся сиротой, но restore пойдёт по победившей строке).
func (ix *Index) Put(ctx context.Context, e Entry) (inserted bool, err error) {
	tag, err := ix.pool.Exec(ctx,
		`INSERT INTO dedup_index (hash, segment_key, offset_, len_, crc)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (hash) DO NOTHING`,
		e.Hash, e.SegmentKey, e.Offset, e.Len, int64(e.CRC),
	)
	if err != nil {
		return false, fmt.Errorf("dedup put %s: %w", hex.EncodeToString(e.Hash), err)
	}
	return tag.RowsAffected() == 1, nil
}
