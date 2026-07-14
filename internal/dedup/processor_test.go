package dedup

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"

	"traceforge/internal/otlp"
)

// newProcessorForTest собирает Processor поверх реального Postgres (dedup-index) и
// in-memory packer. Скипается при недоступном Postgres.
func newProcessorForTest(t *testing.T) (*Processor, *SegmentPacker, func()) {
	t.Helper()
	ix, closeFn := openTestIndex(t)

	packer, err := NewSegmentPacker(newMemBlobStore(), DefaultPackerConfig())
	if err != nil {
		t.Fatalf("packer: %v", err)
	}
	proc := NewProcessor(
		NewExtractor(DefaultExtractConfig()),
		WholeFieldChunker{},
		ix,
		packer,
	)
	return proc, packer, func() {
		packer.Close(context.Background())
		closeFn()
	}
}

func spanWithSystem(traceID byte, sys string) otlp.FlatSpan {
	return otlp.FlatSpan{
		ServiceName: "antispam-agent",
		Scope:       &commonpb.InstrumentationScope{Name: "test-scope"},
		Span: &tracepb.Span{
			TraceId: []byte{traceID, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			SpanId:  []byte{1, 2, 3, 4, 5, 6, 7, 8},
			Name:    "chat gpt-4o-mini",
			Attributes: []*commonpb.KeyValue{
				strAttr("gen_ai.system_instructions", sys),
				strAttr("gen_ai.request.model", "gpt-4o-mini"), // короткое поле — остаётся как есть
			},
		},
	}
}

// Полный проход P1: два спана с ОДИНАКОВЫМ большим системным промптом ->
// 1 miss + 1 hit, контент заменён на ref, а через packer.Get восстанавливается
// исходный текст байт-в-байт.
func TestProcessor_DedupAndReversibility(t *testing.T) {
	proc, packer, cleanup := newProcessorForTest(t)
	defer cleanup()
	ctx := context.Background()

	sys := strings.Repeat("держи спамера на линии, не выдавай себя. ", 60) // ~2.4 КБ

	// Чистим индекс от этого хэша, чтобы тест был повторяемым.
	chunk := WholeFieldChunker{}.Split(sys, nil)[0]
	defer proc.ix.pool.Exec(ctx, "DELETE FROM dedup_index WHERE hash = $1", chunk.Hash)

	f1 := spanWithSystem(0xA1, sys)
	thin1, err := proc.Process(ctx, f1)
	if err != nil {
		t.Fatalf("Process f1: %v", err)
	}
	f2 := spanWithSystem(0xB2, sys)
	thin2, err := proc.Process(ctx, f2)
	if err != nil {
		t.Fatalf("Process f2: %v", err)
	}

	// Второй спан с тем же контентом — дедуп внутри батча (pending): 1 miss, 1 hit.
	if proc.Stats.Misses != 1 || proc.Stats.Hits != 1 {
		t.Errorf("miss=%d hit=%d, ожидали 1/1", proc.Stats.Misses, proc.Stats.Hits)
	}

	// Контент в тонком спане заменён на ref, короткое поле — нетронуто.
	ref := extractSysRef(t, thin1)
	if !IsRef(ref) {
		t.Errorf("system_instructions в тонком спане = %q, ожидали ref", ref)
	}
	if got := extractAttr(t, thin1, "gen_ai.request.model"); got != "gpt-4o-mini" {
		t.Errorf("короткое поле изменилось: %q", got)
	}
	// Оба спана ссылаются на один и тот же хэш.
	if ref != extractSysRef(t, thin2) {
		t.Errorf("одинаковый контент дал разные ref")
	}

	// Фиксируем батч (Flush сегмента + Put в индекс) и восстанавливаем оригинал.
	if err := proc.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	hashes, ok := ParseRef(ref)
	if !ok || len(hashes) != 1 {
		t.Fatalf("ParseRef(%q): ok=%v n=%d", ref, ok, len(hashes))
	}
	e, found, err := proc.ix.Lookup(ctx, hashes[0])
	if err != nil || !found {
		t.Fatalf("Lookup после Commit: found=%v err=%v", found, err)
	}
	restored, err := packer.Get(ctx, e)
	if err != nil {
		t.Fatalf("restore Get: %v", err)
	}
	if string(restored) != sys {
		t.Errorf("restore не совпал с оригиналом (len got=%d want=%d)", len(restored), len(sys))
	}
}

// extractSysRef достаёт значение gen_ai.system_instructions из тонкого спана.
func extractSysRef(t *testing.T, thin []byte) string {
	t.Helper()
	return extractAttr(t, thin, "gen_ai.system_instructions")
}

func extractAttr(t *testing.T, thin []byte, key string) string {
	t.Helper()
	var env struct {
		Service string          `json:"service"`
		Span    json.RawMessage `json:"span"`
	}
	if err := json.Unmarshal(thin, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	var span tracepb.Span
	if err := protojson.Unmarshal(env.Span, &span); err != nil {
		t.Fatalf("protojson span: %v", err)
	}
	v, _ := otlp.AttrString(span.GetAttributes(), key)
	return v
}
