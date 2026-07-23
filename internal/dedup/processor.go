package dedup

import (
	"context"
	"encoding/json"
	"fmt"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"

	"traceforge/internal/otlp"
)

// Stats — сквозные счётчики прогона P1 (основа метрики экономии «X -> Y» и T8).
type Stats struct {
	Spans        int64
	Candidates   int64 // тяжёлых полей извлечено
	Chunks       int64 // фрагментов всего (для whole-field == Candidates)
	Misses       int64 // новых фрагментов (уникальный контент -> в сегмент)
	Hits         int64 // фрагментов, уже лежавших в CAS (сэкономлено)
	ContentBytes int64 // суммарный вес исходного контента полей-кандидатов
	StoredBytes  int64 // вес уникальных фрагментов (реально в сегментах)
	RefBytes     int64 // вес ссылок, заменивших контент в тонких спанах
}

// Processor — конвейер P1 над одним спаном: extract -> chunk -> lookup/pack ->
// замена контента на ref -> тонкий спан. НЕ потокобезопасен: рассчитан на один
// consumer-горутин на инстанс (партиционный параллелизм — позже, отдельными
// инстансами Processor). Инвариант долговечности: фрагменты буферизуются в
// packer, но в индекс попадают только в Commit (после Flush сегмента).
type Processor struct {
	ex      *Extractor
	ch      Chunker
	ix      *Index
	pk      *SegmentPacker
	pending map[[32]byte]Entry // новые фрагменты этого батча, ещё не в индексе
	Stats   Stats
}

func NewProcessor(ex *Extractor, ch Chunker, ix *Index, pk *SegmentPacker) *Processor {
	return &Processor{
		ex:      ex,
		ch:      ch,
		ix:      ix,
		pk:      pk,
		pending: make(map[[32]byte]Entry),
	}
}

// thinEnvelope — тонкий спан, формат v1: денормализованные service/scope + сам
// OTLP-спан (protojson) с тяжёлым контентом, заменённым на ref. Обратимость
// бит-в-бит: restore меняет ref обратно на байты из CAS -> исходный спан.
//
// Resource/ScopeFull (T0): тонкий спан — единственный носитель истории после
// retention Kafka, поэтому он обязан нести ВСЁ, кроме вынесенного в CAS. Голых
// Service/Scope-строк недостаточно: resource-атрибуты сверх service.name (напр.
// service.version) и scope version/attributes иначе молча теряются. Поля optional
// (omitempty) — старые тонкие спаны без них декодируются как раньше.
type thinEnvelope struct {
	Service   string          `json:"service"`
	Scope     string          `json:"scope,omitempty"`
	Resource  json.RawMessage `json:"resource,omitempty"`   // полный resource (protojson)
	ScopeFull json.RawMessage `json:"scope_full,omitempty"` // полный scope: name+version+attrs
	Span      json.RawMessage `json:"span"`
}

// Process обрабатывает один спан: мутирует f.Span (тяжёлые поля -> ref), заносит
// новые фрагменты в packer и возвращает сериализованный тонкий спан для spans.thin.
func (p *Processor) Process(ctx context.Context, f otlp.FlatSpan) ([]byte, error) {
	p.Stats.Spans++

	// Карта атрибутов спана строится до мутации: template-aware читает отсюда
	// идентичность шаблона (gen_ai.prompt.name/version); whole-field/FastCDC игнорят.
	attrs := spanAttrsMap(f.Span)

	for _, c := range p.ex.Extract(f.Span) {
		chunks := p.ch.Split(c.Value, attrs)
		p.Stats.Candidates++
		p.Stats.ContentBytes += int64(len(c.Value))

		for _, ch := range chunks {
			p.Stats.Chunks++
			if err := p.intern(ctx, ch); err != nil {
				return nil, err
			}
		}

		ref := MakeRef(chunks)
		p.Stats.RefBytes += int64(len(ref))
		setSpanValue(f.Span, c.Loc, ref)
	}

	return marshalThin(f)
}

// intern кладёт фрагмент в CAS, если его там ещё нет: сперва индекс (durable),
// затем pending (буфер этого батча). Реальная запись в индекс — в Commit.
func (p *Processor) intern(ctx context.Context, ch Chunk) error {
	var h [32]byte
	copy(h[:], ch.Hash)

	if _, ok := p.pending[h]; ok {
		p.Stats.Hits++
		return nil
	}
	_, found, err := p.ix.Lookup(ctx, ch.Hash)
	if err != nil {
		return err
	}
	if found {
		p.Stats.Hits++
		return nil
	}

	e, err := p.pk.Add(ctx, ch)
	if err != nil {
		return err
	}
	p.pending[h] = e
	p.Stats.Misses++
	p.Stats.StoredBytes += int64(len(ch.Data))
	return nil
}

// Commit фиксирует накопленный батч в правильном порядке долговечности:
// Flush сегмента (байты в S3) -> Put фрагментов в индекс (ссылки валидны).
// Звать ДО produce тонких спанов и ДО коммита Kafka-оффсета.
func (p *Processor) Commit(ctx context.Context) error {
	if err := p.pk.Flush(ctx); err != nil {
		return err
	}
	for _, e := range p.pending {
		if _, err := p.ix.Put(ctx, e); err != nil {
			return err
		}
	}
	p.pending = make(map[[32]byte]Entry)
	return nil
}

// Close дописывает недофлашенный сегмент и освобождает ресурсы packer.
func (p *Processor) Close(ctx context.Context) error {
	return p.pk.Close(ctx)
}

// setSpanValue заменяет строковое значение поля (атрибут спана или атрибут
// события) на ref по месту, найденному экстрактором.
func setSpanValue(span *tracepb.Span, loc Location, ref string) {
	repl := &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: ref}}
	if loc.EventIndex < 0 {
		for _, kv := range span.GetAttributes() {
			if kv.GetKey() == loc.AttrKey {
				kv.Value = repl
				return
			}
		}
		return
	}
	events := span.GetEvents()
	if loc.EventIndex >= len(events) {
		return
	}
	for _, kv := range events[loc.EventIndex].GetAttributes() {
		if kv.GetKey() == loc.AttrKey {
			kv.Value = repl
			return
		}
	}
}

// spanAttrsMap собирает строковые атрибуты спана в map для чанкера. Короткие
// поля идентичности шаблона (template_id/name/version) не дедуплицируются и
// остаются в спане, поэтому доступны здесь как есть.
func spanAttrsMap(span *tracepb.Span) map[string]any {
	attrs := span.GetAttributes()
	m := make(map[string]any, len(attrs))
	for _, kv := range attrs {
		if s := kv.GetValue().GetStringValue(); s != "" {
			m[kv.GetKey()] = s
		}
	}
	return m
}

func marshalThin(f otlp.FlatSpan) ([]byte, error) {
	spanJSON, err := protojson.Marshal(f.Span)
	if err != nil {
		return nil, fmt.Errorf("dedup: protojson спана: %w", err)
	}
	env := thinEnvelope{Service: f.ServiceName, Span: spanJSON}
	// Полный resource (service.version и пр.) — иначе теряется всё сверх service.name.
	if f.Resource != nil {
		resJSON, err := protojson.Marshal(f.Resource)
		if err != nil {
			return nil, fmt.Errorf("dedup: protojson resource: %w", err)
		}
		env.Resource = resJSON
	}
	// Scope: строку-имя оставляем для дешёвой денормализации; полный scope (с
	// version/attributes) кладём рядом для бит-в-бит восстановления.
	if f.Scope != nil {
		env.Scope = f.Scope.GetName()
		scopeJSON, err := protojson.Marshal(f.Scope)
		if err != nil {
			return nil, fmt.Errorf("dedup: protojson scope: %w", err)
		}
		env.ScopeFull = scopeJSON
	}
	return json.Marshal(env)
}
