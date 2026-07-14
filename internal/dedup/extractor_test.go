package dedup

import (
	"crypto/sha256"
	"sort"
	"strings"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func strAttr(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   k,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}},
	}
}

// big — строка гарантированно длиннее порога 1 КБ.
var big = strings.Repeat("системная инструкция ", 100) // ~2100 байт

// paths вытаскивает пути кандидатов, отсортированные, для стабильного сравнения.
func paths(cs []Candidate) []string {
	var p []string
	for _, c := range cs {
		p = append(p, c.Path)
	}
	sort.Strings(p)
	return p
}

func TestExtract_SchemaB(t *testing.T) {
	span := &tracepb.Span{Attributes: []*commonpb.KeyValue{
		strAttr("gen_ai.system_instructions", big),
		strAttr("gen_ai.input.messages", big),
		strAttr("gen_ai.output.messages", "короткий ответ"), // < 1 КБ → мимо
		strAttr("some.other.big", big),                      // не в списке → мимо
		strAttr("gen_ai.request.model", "gpt-4o-mini"),      // мелкое, не в списке
	}}

	got := paths(NewExtractor(DefaultExtractConfig()).Extract(span))
	want := []string{"gen_ai.input.messages", "gen_ai.system_instructions"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("схема B: got %v, want %v", got, want)
	}
}

func TestExtract_SchemaA(t *testing.T) {
	span := &tracepb.Span{Attributes: []*commonpb.KeyValue{
		strAttr("gen_ai.prompt.0.role", "system"), // мелкое → мимо
		strAttr("gen_ai.prompt.0.content", big),   // шаблон gen_ai.prompt.*.content
		strAttr("gen_ai.prompt.1.content", big),
		strAttr("gen_ai.completion.0.content", big),
	}}

	got := paths(NewExtractor(DefaultExtractConfig()).Extract(span))
	want := []string{
		"gen_ai.completion.0.content",
		"gen_ai.prompt.0.content",
		"gen_ai.prompt.1.content",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("схема A: got %v, want %v", got, want)
	}
}

// Ключевое DoD-свойство: один и тот же текст системки в схемах A и B даёт
// один и тот же логический кусок (одинаковый sha256), несмотря на разные пути.
func TestExtract_SameChunkAcrossSchemas(t *testing.T) {
	ex := NewExtractor(DefaultExtractConfig())

	spanB := &tracepb.Span{Attributes: []*commonpb.KeyValue{
		strAttr("gen_ai.system_instructions", big),
	}}
	spanA := &tracepb.Span{Attributes: []*commonpb.KeyValue{
		strAttr("gen_ai.prompt.0.content", big),
	}}

	cb := ex.Extract(spanB)
	ca := ex.Extract(spanA)
	if len(cb) != 1 || len(ca) != 1 {
		t.Fatalf("ожидали по одному кандидату: B=%d A=%d", len(cb), len(ca))
	}
	hb := sha256.Sum256([]byte(cb[0].Value))
	ha := sha256.Sum256([]byte(ca[0].Value))
	if hb != ha {
		t.Errorf("один текст в разных схемах дал разные хэши")
	}
}

func TestExtract_ConfigurablePaths(t *testing.T) {
	span := &tracepb.Span{Attributes: []*commonpb.KeyValue{
		strAttr("gen_ai.system_instructions", big),
		strAttr("custom.field", big),
	}}

	// Кастомный конфиг: только custom.field.
	cfg := ExtractConfig{Paths: []string{"custom.field"}, MinSize: 1024}
	got := paths(NewExtractor(cfg).Extract(span))
	if len(got) != 1 || got[0] != "custom.field" {
		t.Errorf("конфиг-лист не применился: got %v", got)
	}
}

func TestExtract_Event(t *testing.T) {
	span := &tracepb.Span{Events: []*tracepb.Span_Event{
		{
			Name:       "gen_ai.content.prompt",
			Attributes: []*commonpb.KeyValue{strAttr("content", big)},
		},
	}}
	got := NewExtractor(DefaultExtractConfig()).Extract(span)
	if len(got) != 1 || got[0].Loc.EventIndex != 0 || got[0].Loc.AttrKey != "content" {
		t.Errorf("событие схемы C не извлеклось: %+v", got)
	}
}
