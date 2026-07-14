package dedup

import (
	"slices"
	"strings"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// LLM-контент живёт в 3 схемах (semconv мигрируют), поэтому набор полей-кандидатов
// на дедуп — конфигурируемый список путей, а не хардкод:
//   - схема B: gen_ai.system_instructions, gen_ai.input.messages, gen_ai.output.messages;
//   - схема A: gen_ai.prompt.{N}.content, gen_ai.completion.{N}.content (динамический N → шаблон с '*');
//   - схема C: контент в span events (gen_ai.content.prompt / gen_ai.content.completion);
//   - доменные: greeting.text, turn.input.
// Дедуплицируем только строки >= порога — мелочь (роли, метки) не стоит хэша/индекса.

// ExtractConfig — конфиг экстрактора (в проде приедет из YAML P1).
type ExtractConfig struct {
	// Paths — ключи атрибутов спана. Сегмент '*' матчит любой один сегмент
	// (dot-разделённый), чтобы покрыть динамические индексы схемы A.
	Paths []string
	// Events — имена span events, чьи строковые атрибуты считаем кандидатами (схема C).
	Events []string
	// MinSize — минимальный размер строки в байтах для дедупа.
	MinSize int
}

// DefaultExtractConfig — набор по умолчанию: все три схемы + доменные поля, порог 1 КБ.
func DefaultExtractConfig() ExtractConfig {
	return ExtractConfig{
		Paths: []string{
			// схема B
			"gen_ai.system_instructions",
			"gen_ai.input.messages",
			"gen_ai.output.messages",
			// схема A (динамический индекс)
			"gen_ai.prompt.*.content",
			"gen_ai.completion.*.content",
			// доменные
			"greeting.text",
			"turn.input",
		},
		Events:  []string{"gen_ai.content.prompt", "gen_ai.content.completion"},
		MinSize: 1024,
	}
}

// Location — где в спане лежит значение, чтобы заменить текст на ref.
// EventIndex == -1 — атрибут самого спана; >= 0 — индекс span event, из чьих
// атрибутов взято значение.
type Location struct {
	EventIndex int
	AttrKey    string
}

// Candidate — извлечённое поле-кандидат на дедуп.
type Candidate struct {
	Path  string // логический идентификатор поля (для метрик/отладки)
	Value string // содержимое — то, что уйдёт в Chunker
	Loc   Location
}

// Extractor находит поля-кандидаты в спане по конфигу путей.
type Extractor struct {
	cfg ExtractConfig
}

func NewExtractor(cfg ExtractConfig) *Extractor {
	return &Extractor{cfg: cfg}
}

// Extract возвращает все поля спана (и его событий), которые подходят под конфиг
// и не короче MinSize.
func (e *Extractor) Extract(span *tracepb.Span) []Candidate {
	if span == nil {
		return nil
	}
	var out []Candidate

	// 1. Атрибуты самого спана (схемы A, B, доменные).
	for _, kv := range span.GetAttributes() {
		s := kv.GetValue().GetStringValue()
		if len(s) < e.cfg.MinSize {
			continue
		}
		if e.matchPath(kv.GetKey()) {
			out = append(out, Candidate{
				Path:  kv.GetKey(),
				Value: s,
				Loc:   Location{EventIndex: -1, AttrKey: kv.GetKey()},
			})
		}
	}

	// 2. Атрибуты span events (схема C).
	for ei, ev := range span.GetEvents() {
		if !e.matchEvent(ev.GetName()) {
			continue
		}
		for _, kv := range ev.GetAttributes() {
			s := kv.GetValue().GetStringValue()
			if len(s) < e.cfg.MinSize {
				continue
			}
			out = append(out, Candidate{
				Path:  "event:" + ev.GetName() + ":" + kv.GetKey(),
				Value: s,
				Loc:   Location{EventIndex: ei, AttrKey: kv.GetKey()},
			})
		}
	}

	return out
}

func (e *Extractor) matchPath(key string) bool {
	for _, p := range e.cfg.Paths {
		if matchSegments(p, key) {
			return true
		}
	}
	return false
}

func (e *Extractor) matchEvent(name string) bool {
	return slices.Contains(e.cfg.Events, name)
}

// matchSegments сопоставляет ключ с шаблоном, где '*' заменяет один
// dot-разделённый сегмент (напр. "gen_ai.prompt.*.content" ~ "gen_ai.prompt.7.content").
func matchSegments(pattern, key string) bool {
	if pattern == key {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return false
	}
	ps := strings.Split(pattern, ".")
	ks := strings.Split(key, ".")
	if len(ps) != len(ks) {
		return false
	}
	for i := range ps {
		if ps[i] != "*" && ps[i] != ks[i] {
			return false
		}
	}
	return true
}
