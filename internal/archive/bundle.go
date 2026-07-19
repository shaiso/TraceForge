package archive

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"

	"traceforge/internal/dedup"
)

// BundleConfig — параметры пакера бандлов.
type BundleConfig struct {
	// Prefix — префикс ключа бандла в бакете (по умолчанию пусто: ключ начинается
	// с date=...). Бакет архива задаётся при создании BlobStore.
	Prefix string
	// FlushBytes — порог суммарного буфера (сырые байты тонких спанов), при котором
	// архиватор флашит ВСЕ накопленные бандлы. Коалесцирует несколько poll-батчей
	// в один бандл на час -> меньше мелких объектов.
	FlushBytes int
}

// DefaultBundleConfig — 32 МБ буфера, пустой префикс.
func DefaultBundleConfig() BundleConfig {
	return BundleConfig{Prefix: "", FlushBytes: 32 << 20}
}

// BundlePacker собирает тонкие спаны в часовые бандлы. Гранулярность — ПОФРЕЙМОВО:
// все тонкие спаны одного трейса (в пределах часа) кодируются ОДНИМ zstd-фреймом;
// фреймы пишутся встык в объект бандла, а (offset, len) фрейма едут в archive_index.
// Так одиночный restore = range-read одного фрейма, не всего часа (как CAS-сегменты P1).
//
// Буфер по (date, hour) из ts СОБЫТИЯ спана (не wall-clock): поздний спан ложится в
// свой час. Флаш всех бакетов -> по одному объекту part-<uuid> на час-бакет; для того
// же часа при следующем флаше появится новый part (поздние спаны -> новая строка индекса).
type BundlePacker struct {
	store dedup.BlobStore
	enc   *zstd.Encoder
	cfg   BundleConfig

	mu      sync.Mutex
	buckets map[string]*hourBucket // "date=../hour=.." -> бакет
	raw     int                    // суммарный вес сырых строк в буфере
}

type hourBucket struct {
	date   string
	hour   int
	traces map[string]*traceAccum
	order  []string // порядок первого появления трейса — стабильный вывод
}

type traceAccum struct {
	sessionID string
	minTS     time.Time
	lines     [][]byte // тонкие спаны трейса (JSON-строки)
}

// NewBundlePacker создаёт пакер поверх хранилища архива.
func NewBundlePacker(store dedup.BlobStore, cfg BundleConfig) (*BundlePacker, error) {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, fmt.Errorf("archive: zstd encoder: %w", err)
	}
	return &BundlePacker{store: store, enc: enc, cfg: cfg, buckets: map[string]*hourBucket{}}, nil
}

// Add кладёт один тонкий спан в буфер бакета его часа/трейса (по ts события).
func (bp *BundlePacker) Add(traceID, sessionID string, ts time.Time, line []byte) {
	utc := ts.UTC()
	date := utc.Format("2006-01-02")
	hour := utc.Hour()
	bkey := fmt.Sprintf("date=%s/hour=%02d", date, hour)

	bp.mu.Lock()
	defer bp.mu.Unlock()
	b := bp.buckets[bkey]
	if b == nil {
		b = &hourBucket{date: date, hour: hour, traces: map[string]*traceAccum{}}
		bp.buckets[bkey] = b
	}
	ta := b.traces[traceID]
	if ta == nil {
		ta = &traceAccum{sessionID: sessionID, minTS: ts}
		b.traces[traceID] = ta
		b.order = append(b.order, traceID)
	}
	if ts.Before(ta.minTS) {
		ta.minTS = ts
	}
	if ta.sessionID == "" {
		ta.sessionID = sessionID
	}
	ta.lines = append(ta.lines, append([]byte(nil), line...))
	bp.raw += len(line)
}

// Buffered — сколько сырых байт сейчас в буфере (для порога флаша).
func (bp *BundlePacker) Buffered() int {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return bp.raw
}

// Flush пишет ВСЕ накопленные бандлы (по объекту на час-бакет) в S3 и возвращает
// строки archive_index. Порядок долговечности архиватора: Flush -> PutBatch(индекс)
// -> commit Kafka-оффсета. Ошибка флаша (частичная запись) оставляет сироту-бандл —
// безопасно: строк в индексе нет, при переигровке пересоберётся новый part.
func (bp *BundlePacker) Flush(ctx context.Context) ([]Row, error) {
	bp.mu.Lock()
	buckets := bp.buckets
	bp.buckets = map[string]*hourBucket{}
	bp.raw = 0
	bp.mu.Unlock()

	var rows []Row
	for _, b := range buckets {
		key := fmt.Sprintf("%sdate=%s/hour=%02d/part-%s.jsonl.zst", bp.cfg.Prefix, b.date, b.hour, uuid.NewString())
		var buf bytes.Buffer
		bundleRows := make([]Row, 0, len(b.order))
		for _, tid := range b.order {
			ta := b.traces[tid]
			// Фрейм трейса = его тонкие спаны как JSONL, одним zstd-фреймом.
			var src bytes.Buffer
			for _, ln := range ta.lines {
				src.Write(ln)
				src.WriteByte('\n')
			}
			frame := bp.enc.EncodeAll(src.Bytes(), nil)
			bundleRows = append(bundleRows, Row{
				TraceID:   tid,
				SessionID: ta.sessionID,
				TS:        ta.minTS,
				BundleKey: key,
				Offset:    uint64(buf.Len()),
				Len:       uint32(len(frame)),
			})
			buf.Write(frame)
		}
		if buf.Len() == 0 {
			continue
		}
		if err := bp.store.PutObject(ctx, key, buf.Bytes()); err != nil {
			return nil, fmt.Errorf("archive: флаш бандла %s: %w", key, err)
		}
		rows = append(rows, bundleRows...)
	}
	return rows, nil
}

// Close освобождает zstd-энкодер.
func (bp *BundlePacker) Close() { bp.enc.Close() }
