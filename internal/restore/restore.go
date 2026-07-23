// Package restore восстанавливает полные трейсы из архива: тонкие спаны из бандла
// (S3) + резолв ссылок ref:sha256 из CAS (dedup). Два режима над одним нижним слоем:
// одиночный (trace_id) и диапазонный (окно). Диапазонный качает каждый фрейм один
// раз и держит кэш фрагментов на всё окно (частые системки резолвятся однократно).
// Нижний резолв рефов переиспользует dedup.RestoreSpan.
package restore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/klauspost/compress/zstd"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"

	"traceforge/internal/archive"
	"traceforge/internal/dedup"
)

// ErrNotFound — трейса нет в archive_index.
var ErrNotFound = errors.New("restore: трейс не найден в архиве")

// IndexLookup — то, что restore берёт от archive_index (интерфейс для тестируемости).
type IndexLookup interface {
	LookupTrace(ctx context.Context, traceID string) ([]archive.Row, error)
	LookupRange(ctx context.Context, from, to time.Time, sessionID string) ([]archive.Row, error)
}

// RestoredSpan — восстановленный спан с денормализованными service/scope и полным
// resource/scope (T0). Resource/ScopeFull nil для тонких спанов старого формата
// (без этих полей) — тогда доступны только денормализованные service/scope-строки.
type RestoredSpan struct {
	Service   string
	Scope     string
	Resource  *resourcepb.Resource           // полный resource (service.version и пр.)
	ScopeFull *commonpb.InstrumentationScope // полный scope: name+version+attributes
	Span      *tracepb.Span
}

// Trace — полный восстановленный трейс.
type Trace struct {
	TraceID string
	Spans   []RestoredSpan
}

// Restorer собирает трейсы из архива. bundles — BlobStore над бакетом бандлов;
// cas — источник фрагментов CAS (обычно dedup.IndexPacker над dedup-index + сегментами).
type Restorer struct {
	index   IndexLookup
	bundles dedup.BlobStore
	cas     dedup.ChunkSource
	dec     *zstd.Decoder
}

// New создаёт Restorer.
func New(index IndexLookup, bundles dedup.BlobStore, cas dedup.ChunkSource) (*Restorer, error) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("restore: zstd reader: %w", err)
	}
	return &Restorer{index: index, bundles: bundles, cas: cas, dec: dec}, nil
}

// Close освобождает декодер.
func (r *Restorer) Close() { r.dec.Close() }

// thinEnvelope — тонкий спан из бандла: service/scope + protojson спана с ref.
// Resource/ScopeFull (T0) — полные resource/scope, optional (старые бандлы без них).
type thinEnvelope struct {
	Service   string          `json:"service"`
	Scope     string          `json:"scope"`
	Resource  json.RawMessage `json:"resource,omitempty"`
	ScopeFull json.RawMessage `json:"scope_full,omitempty"`
	Span      json.RawMessage `json:"span"`
}

// Restore восстанавливает трейс по trace_id. Растянутый по часам трейс лежит в
// нескольких бандлах (по строке archive_index на каждый) — склеиваем все фреймы.
func (r *Restorer) Restore(ctx context.Context, traceID string) (*Trace, error) {
	rows, err := r.index.LookupTrace(ctx, traceID)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrNotFound
	}
	tr := &Trace{TraceID: traceID}
	for _, row := range rows {
		spans, err := r.restoreFrame(ctx, row, r.cas)
		if err != nil {
			return nil, err
		}
		tr.Spans = append(tr.Spans, spans...)
	}
	return tr, nil
}

// RestoreRange восстанавливает трейсы окна [from, to] (опц. session_id). Читает
// каждый фрейм один раз и резолвит фрагменты CAS через ОБЩИЙ кэш окна — частая
// системка резолвится однократно, а не на каждый трейс («Langfuse-просмотрщик»).
func (r *Restorer) RestoreRange(ctx context.Context, from, to time.Time, sessionID string) ([]*Trace, error) {
	rows, err := r.index.LookupRange(ctx, from, to, sessionID)
	if err != nil {
		return nil, err
	}
	cached := &cachingSource{inner: r.cas, cache: map[[32]byte][]byte{}}
	byTrace := map[string]*Trace{}
	var order []string
	for _, row := range rows {
		spans, err := r.restoreFrame(ctx, row, cached)
		if err != nil {
			return nil, err
		}
		tr := byTrace[row.TraceID]
		if tr == nil {
			tr = &Trace{TraceID: row.TraceID}
			byTrace[row.TraceID] = tr
			order = append(order, row.TraceID)
		}
		tr.Spans = append(tr.Spans, spans...)
	}
	out := make([]*Trace, 0, len(order))
	for _, id := range order {
		out = append(out, byTrace[id])
	}
	return out, nil
}

// DecodeThinLine разбирает ОДНУ строку тонкого спана (JSON-конверт) в RestoredSpan
// с рефами ВНУТРИ (не резолвит). Это форма записи в spans.thin (Kafka) — её же
// читает SDK-стрим. Рефы резолвятся по требованию (dedup.RestoreSpan / SDK).
func DecodeThinLine(line []byte) (RestoredSpan, error) {
	var env thinEnvelope
	if err := json.Unmarshal(line, &env); err != nil {
		return RestoredSpan{}, fmt.Errorf("restore: envelope: %w", err)
	}
	span := &tracepb.Span{}
	if err := protojson.Unmarshal(env.Span, span); err != nil {
		return RestoredSpan{}, fmt.Errorf("restore: protojson спана: %w", err)
	}
	rs := RestoredSpan{Service: env.Service, Scope: env.Scope, Span: span}
	if len(env.Resource) > 0 {
		res := &resourcepb.Resource{}
		if err := protojson.Unmarshal(env.Resource, res); err != nil {
			return RestoredSpan{}, fmt.Errorf("restore: protojson resource: %w", err)
		}
		rs.Resource = res
	}
	if len(env.ScopeFull) > 0 {
		sc := &commonpb.InstrumentationScope{}
		if err := protojson.Unmarshal(env.ScopeFull, sc); err != nil {
			return RestoredSpan{}, fmt.Errorf("restore: protojson scope: %w", err)
		}
		rs.ScopeFull = sc
	}
	return rs, nil
}

// DecodeThinFrame разбирает один zstd-фрейм бандла (сырые байты фрейма) в тонкие
// спаны: zstd decode -> JSONL -> DecodeThinLine на строку. Рефы НЕ резолвятся —
// это ленивый нижний слой, общий для жадного restore (который затем зовёт
// dedup.RestoreSpan) и SDK (internal/tf, резолвит контент по требованию).
// Декодер передаётся аргументом: у каждого владельца свой, без общего состояния.
func DecodeThinFrame(dec *zstd.Decoder, frame []byte) ([]RestoredSpan, error) {
	data, err := dec.DecodeAll(frame, nil)
	if err != nil {
		return nil, fmt.Errorf("restore: decode фрейма: %w", err)
	}
	var out []RestoredSpan
	for _, ln := range bytes.Split(data, []byte("\n")) {
		if len(ln) == 0 {
			continue
		}
		rs, err := DecodeThinLine(ln)
		if err != nil {
			return nil, err
		}
		out = append(out, rs)
	}
	return out, nil
}

// restoreFrame: range-read фрейма трейса из бандла -> DecodeThinFrame -> резолв
// рефов из CAS (жадный путь одиночного/диапазонного restore).
func (r *Restorer) restoreFrame(ctx context.Context, row archive.Row, cas dedup.ChunkSource) ([]RestoredSpan, error) {
	frame, err := r.bundles.GetRange(ctx, row.BundleKey, int64(row.Offset), int(row.Len))
	if err != nil {
		return nil, fmt.Errorf("restore: range-read %s: %w", row.BundleKey, err)
	}
	spans, err := DecodeThinFrame(r.dec, frame)
	if err != nil {
		return nil, err
	}
	for i := range spans {
		if err := dedup.RestoreSpan(ctx, spans[i].Span, cas); err != nil {
			return nil, err
		}
	}
	return spans, nil
}

// cachingSource — декоратор ChunkSource с кэшем hash->байты на время одного окна.
// Ключ — [32]byte (sha256): массив сравним и годится в ключ мапы без heap-аллокации
// (в отличие от string/hex), консистентно с pending в dedup/processor.go.
type cachingSource struct {
	inner dedup.ChunkSource
	cache map[[32]byte][]byte
}

func (c *cachingSource) Fetch(ctx context.Context, hash []byte) ([]byte, error) {
	var k [32]byte
	copy(k[:], hash)
	if v, ok := c.cache[k]; ok {
		return v, nil
	}
	v, err := c.inner.Fetch(ctx, hash)
	if err != nil {
		return nil, err
	}
	c.cache[k] = append([]byte(nil), v...)
	return v, nil
}
