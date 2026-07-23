package tf

import (
	"container/list"
	"context"
	"fmt"
	"time"

	"github.com/klauspost/compress/zstd"

	"traceforge/internal/archive"
	"traceforge/internal/dedup"
	"traceforge/internal/restore"
)

// Archive — батчевый источник: произвольный доступ к постоянному архиву. Отдаёт
// те же Span/Trace/Session, что и стрим (см. SpanFromThinLine) — один тип на оба режима.
type Archive interface {
	// LoadRange отдаёт сессии окна ПОТОКОВО (callback на сессию), обрабатывая и
	// отпуская: день целиком в память не собирается (развёрнутый контент туда не влезет).
	LoadRange(ctx context.Context, from, to time.Time, sessionID string, fn func(Session) error) error
	LoadSession(ctx context.Context, sessionID string) (Session, error)
	LoadTrace(ctx context.Context, traceID string) (Trace, error)
}

// ArchiveSource реализует Archive поверх нижних слоёв Sprint 01–03: archive_index
// (где лежит трейс) + бандлы S3 (тонкие спаны) + CAS (резолв ref по требованию).
// Ленивый путь: кадры декодируются с ссылками внутри (restore.DecodeThinFrame), а
// текст разворачивается только при обращении к Prompt/Completion.
type ArchiveSource struct {
	index      *archive.Index
	bundles    dedup.BlobStore
	cas        dedup.ChunkSource
	dec        *zstd.Decoder
	cacheBytes int64
}

// DefaultCacheBytes — кап кэша фрагментов на окно (горячие системки резолвятся раз,
// уникальные завершения вытесняются → память ограничена независимо от размера окна).
const DefaultCacheBytes = 256 << 20

// NewArchiveSource собирает источник. index/bundles/cas — те же типы, что у restore
// (archive.Index, dedup.BlobStore над бакетом бандлов, dedup.IndexPacker как CAS).
func NewArchiveSource(index *archive.Index, bundles dedup.BlobStore, cas dedup.ChunkSource) (*ArchiveSource, error) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("tf: zstd reader: %w", err)
	}
	return &ArchiveSource{index: index, bundles: bundles, cas: cas, dec: dec, cacheBytes: DefaultCacheBytes}, nil
}

// Close освобождает декодер.
func (a *ArchiveSource) Close() { a.dec.Close() }

// LoadRange: индекс окна -> группировка строк по сессии (строки индекса малы) ->
// на каждую сессию декодируем её фреймы и отдаём callback'у, затем отпускаем.
// Кэш фрагментов общий на всё окно.
func (a *ArchiveSource) LoadRange(ctx context.Context, from, to time.Time, sessionID string, fn func(Session) error) error {
	rows, err := a.index.LookupRange(ctx, from, to, sessionID)
	if err != nil {
		return err
	}
	bySession, order := groupBySession(rows)
	win := newLRU(a.cas, a.cacheBytes)
	for _, sid := range order {
		sess, err := a.buildSession(ctx, sid, bySession[sid], win)
		if err != nil {
			return err
		}
		if err := fn(sess); err != nil {
			return err
		}
	}
	return nil
}

// LoadSession загружает одну сессию (по всем её трейсам/часам).
func (a *ArchiveSource) LoadSession(ctx context.Context, sessionID string) (Session, error) {
	rows, err := a.index.LookupSession(ctx, sessionID)
	if err != nil {
		return Session{}, err
	}
	win := newLRU(a.cas, a.cacheBytes)
	return a.buildSession(ctx, sessionID, rows, win)
}

// LoadTrace загружает один трейс (склеивает фреймы растянутого по часам трейса).
func (a *ArchiveSource) LoadTrace(ctx context.Context, traceID string) (Trace, error) {
	rows, err := a.index.LookupTrace(ctx, traceID)
	if err != nil {
		return Trace{}, err
	}
	win := newLRU(a.cas, a.cacheBytes)
	tr := Trace{TraceID: traceID}
	for _, row := range rows {
		spans, err := a.decodeFrame(ctx, row, win)
		if err != nil {
			return Trace{}, err
		}
		tr.Spans = append(tr.Spans, spans...)
	}
	return tr, nil
}

// buildSession декодирует фреймы сессии и группирует спаны по трейсам.
func (a *ArchiveSource) buildSession(ctx context.Context, sessionID string, rows []archive.Row, win dedup.ChunkSource) (Session, error) {
	byTrace := map[string]int{}
	sess := Session{SessionID: sessionID}
	for _, row := range rows {
		spans, err := a.decodeFrame(ctx, row, win)
		if err != nil {
			return Session{}, err
		}
		i, ok := byTrace[row.TraceID]
		if !ok {
			byTrace[row.TraceID] = len(sess.Traces)
			sess.Traces = append(sess.Traces, Trace{TraceID: row.TraceID})
			i = len(sess.Traces) - 1
		}
		sess.Traces[i].Spans = append(sess.Traces[i].Spans, spans...)
	}
	return sess, nil
}

// decodeFrame: range-read фрейма трейса -> DecodeThinFrame (ссылки внутри) -> Span
// с резолвером win (кэш окна). Каждый фрейм читается один раз.
func (a *ArchiveSource) decodeFrame(ctx context.Context, row archive.Row, win dedup.ChunkSource) ([]Span, error) {
	frame, err := a.bundles.GetRange(ctx, row.BundleKey, int64(row.Offset), int(row.Len))
	if err != nil {
		return nil, fmt.Errorf("tf: range-read %s: %w", row.BundleKey, err)
	}
	decoded, err := restore.DecodeThinFrame(a.dec, frame)
	if err != nil {
		return nil, err
	}
	out := make([]Span, 0, len(decoded))
	for _, rs := range decoded {
		out = append(out, spanFromRestored(rs, win))
	}
	return out, nil
}

// SpanFromThinLine строит tf.Span из одной записи spans.thin (Kafka) — стрим-путь.
// Тот же Span, что отдаёт архив: логика процессора одинакова для обоих режимов.
func SpanFromThinLine(line []byte, cas dedup.ChunkSource) (Span, error) {
	rs, err := restore.DecodeThinLine(line)
	if err != nil {
		return Span{}, err
	}
	return spanFromRestored(rs, cas), nil
}

func spanFromRestored(rs restore.RestoredSpan, cas dedup.ChunkSource) Span {
	return Span{Service: rs.Service, Scope: rs.Scope, Resource: rs.Resource, span: rs.Span, cas: cas}
}

// groupBySession раскладывает строки индекса по session_id (порядок первого
// появления). Строки индекса малы (~100 Б) — их материализация памяти не рвёт.
func groupBySession(rows []archive.Row) (map[string][]archive.Row, []string) {
	m := map[string][]archive.Row{}
	var order []string
	for _, r := range rows {
		if _, ok := m[r.SessionID]; !ok {
			order = append(order, r.SessionID)
		}
		m[r.SessionID] = append(m[r.SessionID], r)
	}
	return m, order
}

// --- кэш фрагментов на окно: bounded LRU поверх ChunkSource ---

// lruCache — резолвер CAS с LRU-кэшем, ограниченным по суммарным байтам. Общая
// системка тянется из CAS однократно и остаётся горячей; уникальные завершения
// вытесняются → память ограничена независимо от размера окна.
type lruCache struct {
	inner   dedup.ChunkSource
	maxSize int64
	size    int64
	ll      *list.List
	items   map[string]*list.Element
}

type lruEntry struct {
	key  string
	data []byte
}

func newLRU(inner dedup.ChunkSource, maxSize int64) *lruCache {
	return &lruCache{inner: inner, maxSize: maxSize, ll: list.New(), items: map[string]*list.Element{}}
}

// Fetch: кэш-хит -> двигаем в голову; промах -> тянем из inner, кладём, вытесняем хвост.
func (c *lruCache) Fetch(ctx context.Context, hash []byte) ([]byte, error) {
	k := string(hash)
	if el, ok := c.items[k]; ok {
		c.ll.MoveToFront(el)
		return el.Value.(*lruEntry).data, nil
	}
	data, err := c.inner.Fetch(ctx, hash)
	if err != nil {
		return nil, err
	}
	cp := append([]byte(nil), data...)
	el := c.ll.PushFront(&lruEntry{key: k, data: cp})
	c.items[k] = el
	c.size += int64(len(cp))
	for c.size > c.maxSize && c.ll.Len() > 1 {
		c.evictTail()
	}
	return cp, nil
}

func (c *lruCache) evictTail() {
	el := c.ll.Back()
	if el == nil {
		return
	}
	c.ll.Remove(el)
	e := el.Value.(*lruEntry)
	delete(c.items, e.key)
	c.size -= int64(len(e.data))
}
