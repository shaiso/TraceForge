package dedup

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"sync"

	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
)

// BlobStore — минимальный контракт объектного хранилища, который нужен packer'у:
// положить объект целиком и прочитать байтовый диапазон. Абстракция нужна, чтобы
// SegmentPacker тестировался на in-memory реализации без поднятого MinIO;
// прод-реализация (MinioBlobStore) живёт в minio.go.
type BlobStore interface {
	// PutObject кладёт объект key целиком (перезапись допустима, но мы всегда
	// пишем уникальный key seg-<uuid>, так что перезаписей нет).
	PutObject(ctx context.Context, key string, data []byte) error
	// GetRange читает length байт объекта key начиная со смещения offset.
	GetRange(ctx context.Context, key string, offset int64, length int) ([]byte, error)
}

// PackerConfig задаёт порог флаша и префикс ключей сегментов.
type PackerConfig struct {
	// FlushBytes — размер сегмента (по сжатым байтам), при достижении которого
	// буфер выгружается в хранилище. Согласовано 64–128 МБ.
	FlushBytes int
	// Prefix — префикс ключа объекта, напр. "dedup/segments/".
	Prefix string
}

// DefaultPackerConfig — 96 МБ (середина согласованного 64–128 МБ) и канонический
// префикс сегментов.
func DefaultPackerConfig() PackerConfig {
	return PackerConfig{
		FlushBytes: 96 << 20,
		Prefix:     "dedup/segments/",
	}
}

// SegmentPacker собирает уникальные фрагменты в сегменты и отдаёт их обратно при
// восстановлении. Каждый фрагмент жмётся ОТДЕЛЬНЫМ самостоятельным zstd-фреймом;
// фреймы пишутся встык в буфер сегмента, поэтому Entry.Offset/Len адресуют сжатый
// суб-блоб внутри объекта — restore = GetRange одного фрейма → DecodeAll.
//
// Потокобезопасен: воркеры P1 зовут Add конкурентно, доступ к буферу под мьютексом.
// (Плану «одна packer-горутина + канал» это эквивалентно по инвариантам, но проще:
// Add сразу возвращает Entry, не гоняя запрос-ответ через канал.)
type SegmentPacker struct {
	store BlobStore
	cfg   PackerConfig
	enc   *zstd.Encoder
	dec   *zstd.Decoder

	mu     sync.Mutex
	segKey string       // ключ текущего открытого сегмента ("" — пустой)
	buf    bytes.Buffer // сжатые фреймы текущего сегмента встык
}

// NewSegmentPacker создаёт packer поверх хранилища. enc/dec — долгоживущие,
// EncodeAll/DecodeAll безопасны для конкурентного вызова; Close освобождает их.
func NewSegmentPacker(store BlobStore, cfg PackerConfig) (*SegmentPacker, error) {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, fmt.Errorf("dedup: zstd encoder: %w", err)
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		enc.Close()
		return nil, fmt.Errorf("dedup: zstd decoder: %w", err)
	}
	return &SegmentPacker{store: store, cfg: cfg, enc: enc, dec: dec}, nil
}

// Add упаковывает фрагмент в текущий сегмент и возвращает его Entry (segment_key,
// offset, len, crc) — эту строку конвейер кладёт в dedup-index. При достижении
// порога сегмент флашится ВНУТРИ Add: возвращаемый Entry уже указывает на
// выгруженный объект. Инвариант offset-after-flush: коммитить Kafka-оффсет можно
// лишь после Flush всех сегментов, которые получили фрагменты обрабатываемого
// сообщения (см. T6).
//
// crc считается на исходных (несжатых) байтах — это чек целостности restore.
func (p *SegmentPacker) Add(ctx context.Context, chunk Chunk) (Entry, error) {
	frame := p.enc.EncodeAll(chunk.Data, nil)
	crc := crc32.ChecksumIEEE(chunk.Data)

	p.mu.Lock()
	if p.segKey == "" {
		p.segKey = p.cfg.Prefix + "seg-" + uuid.NewString() + ".zst"
	}
	e := Entry{
		Hash:       chunk.Hash,
		SegmentKey: p.segKey,
		Offset:     int64(p.buf.Len()),
		Len:        int32(len(frame)),
		CRC:        crc,
	}
	p.buf.Write(frame)
	full := p.buf.Len() >= p.cfg.FlushBytes
	p.mu.Unlock()

	if full {
		if err := p.Flush(ctx); err != nil {
			return Entry{}, err
		}
	}
	return e, nil
}

// Flush выгружает текущий сегмент в хранилище и открывает новый. no-op на пустом
// буфере. После успешного PutObject все выданные для этого сегмента Entry
// становятся долговечными (объект существует).
func (p *SegmentPacker) Flush(ctx context.Context) error {
	p.mu.Lock()
	if p.buf.Len() == 0 {
		p.mu.Unlock()
		return nil
	}
	key := p.segKey
	data := append([]byte(nil), p.buf.Bytes()...) // копия: buf переиспользуем
	p.buf.Reset()
	p.segKey = ""
	p.mu.Unlock()

	if err := p.store.PutObject(ctx, key, data); err != nil {
		return fmt.Errorf("dedup: флаш сегмента %s: %w", key, err)
	}
	return nil
}

// Get восстанавливает исходные байты фрагмента по Entry: range-read одного
// zstd-фрейма → DecodeAll → проверка crc. Основа обратимости и Restore API.
func (p *SegmentPacker) Get(ctx context.Context, e Entry) ([]byte, error) {
	frame, err := p.store.GetRange(ctx, e.SegmentKey, e.Offset, int(e.Len))
	if err != nil {
		return nil, fmt.Errorf("dedup: range-read %s[%d:+%d]: %w", e.SegmentKey, e.Offset, e.Len, err)
	}
	data, err := p.dec.DecodeAll(frame, nil)
	if err != nil {
		return nil, fmt.Errorf("dedup: decode фрейма %s: %w", e.SegmentKey, err)
	}
	if got := crc32.ChecksumIEEE(data); got != e.CRC {
		return nil, fmt.Errorf("dedup: crc mismatch в %s: got %08x, want %08x", e.SegmentKey, got, e.CRC)
	}
	return data, nil
}

// Close выгружает недописанный сегмент и освобождает zstd-ресурсы.
func (p *SegmentPacker) Close(ctx context.Context) error {
	err := p.Flush(ctx)
	p.enc.Close()
	p.dec.Close()
	return err
}
