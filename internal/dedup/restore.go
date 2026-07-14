package dedup

import (
	"context"
	"fmt"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// ChunkSource достаёт исходные байты фрагмента по хэшу: индекс (hash -> Entry) +
// сегмент (Entry -> байты). Обычная реализация — pair (Index, SegmentPacker).
type ChunkSource interface {
	Fetch(ctx context.Context, hash []byte) ([]byte, error)
}

// IndexPacker связывает dedup-index и packer в единый ChunkSource для restore.
type IndexPacker struct {
	Index  *Index
	Packer *SegmentPacker
}

// Fetch: hash -> Entry (индекс) -> байты фрагмента (range-read сегмента).
func (s IndexPacker) Fetch(ctx context.Context, hash []byte) ([]byte, error) {
	e, found, err := s.Index.Lookup(ctx, hash)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("dedup restore: хэш не найден в индексе")
	}
	return s.Packer.Get(ctx, e)
}

// ResolveRef восстанавливает исходное значение поля по ссылке: склеивает байты
// фрагментов из CAS в порядке, зафиксированном в ref (для FastCDC их несколько).
func ResolveRef(ctx context.Context, ref string, src ChunkSource) (string, error) {
	hashes, ok := ParseRef(ref)
	if !ok {
		return "", fmt.Errorf("dedup restore: %q не ссылка", ref)
	}
	var buf []byte
	for _, h := range hashes {
		data, err := src.Fetch(ctx, h)
		if err != nil {
			return "", err
		}
		buf = append(buf, data...)
	}
	return string(buf), nil
}

// RestoreSpan возвращает спан к исходному виду по месту: каждое строковое значение
// (атрибут спана или атрибут события), являющееся ссылкой, заменяется обратно на
// оригинальный контент из CAS. Обратная операция к Processor — доказательство
// обратимости (T7) и основа Restore API.
func RestoreSpan(ctx context.Context, span *tracepb.Span, src ChunkSource) error {
	if span == nil {
		return nil
	}
	for _, kv := range span.GetAttributes() {
		if err := restoreValue(ctx, kv.GetValue(), src); err != nil {
			return err
		}
	}
	for _, ev := range span.GetEvents() {
		for _, kv := range ev.GetAttributes() {
			if err := restoreValue(ctx, kv.GetValue(), src); err != nil {
				return err
			}
		}
	}
	return nil
}

func restoreValue(ctx context.Context, v *commonpb.AnyValue, src ChunkSource) error {
	s := v.GetStringValue()
	if !IsRef(s) {
		return nil
	}
	orig, err := ResolveRef(ctx, s, src)
	if err != nil {
		return err
	}
	v.Value = &commonpb.AnyValue_StringValue{StringValue: orig}
	return nil
}
