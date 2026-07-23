package tf

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/klauspost/compress/zstd"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"

	"traceforge/internal/restore"
)

// --- фейки/помощники ---

// countingCAS — фейк CAS, считает Fetch по хэшу (для проверки ленивости и кэша).
type countingCAS struct {
	data  map[string][]byte // string(hash) -> байты
	calls map[string]int
}

func newCAS() *countingCAS {
	return &countingCAS{data: map[string][]byte{}, calls: map[string]int{}}
}

func (c *countingCAS) put(content string) string {
	h := sha256.Sum256([]byte(content))
	c.data[string(h[:])] = []byte(content)
	return "ref:sha256:" + hex.EncodeToString(h[:])
}

func (c *countingCAS) Fetch(_ context.Context, hash []byte) ([]byte, error) {
	c.calls[string(hash)]++
	v, ok := c.data[string(hash)]
	if !ok {
		return nil, fmt.Errorf("нет фрагмента")
	}
	return append([]byte(nil), v...), nil
}

func (c *countingCAS) total() int {
	n := 0
	for _, v := range c.calls {
		n += v
	}
	return n
}

func strAttr(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}
func intAttr(k string, v int64) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: v}}}
}
func floatAttr(k string, v float64) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: v}}}
}

func mkSpan(start, end uint64, attrs ...*commonpb.KeyValue) *tracepb.Span {
	return &tracepb.Span{
		TraceId:           []byte("trace-id-16bytes"),
		SpanId:            []byte("span8byt"),
		Name:              "chat gpt-4o-mini",
		StartTimeUnixNano: start,
		EndTimeUnixNano:   end,
		Attributes:        attrs,
	}
}

// --- тесты ---

// TestGetters_Typed: типизированные геттеры читают span-атрибуты без CAS.
func TestGetters_Typed(t *testing.T) {
	sp := Span{span: mkSpan(0, 50_000_000,
		strAttr("session.id", "sess-1"),
		strAttr("user.id", "user-7"),
		strAttr("app.version", "v4"),
		strAttr("gen_ai.request.model", "gpt-4o-mini"),
		intAttr("gen_ai.usage.input_tokens", 320),
		floatAttr("gen_ai.request.temperature", 0.42),
	)}
	if sp.SessionID() != "sess-1" || sp.UserID() != "user-7" || sp.Version() != "v4" {
		t.Fatalf("строковые геттеры: %q %q %q", sp.SessionID(), sp.UserID(), sp.Version())
	}
	if sp.String("gen_ai.request.model") != "gpt-4o-mini" {
		t.Errorf("model: %q", sp.String("gen_ai.request.model"))
	}
	if sp.Int("gen_ai.usage.input_tokens") != 320 {
		t.Errorf("input_tokens: %d", sp.Int("gen_ai.usage.input_tokens"))
	}
	if sp.Float("gen_ai.request.temperature") != 0.42 {
		t.Errorf("temperature: %v", sp.Float("gen_ai.request.temperature"))
	}
	if sp.Duration().Milliseconds() != 50 {
		t.Errorf("duration: %v", sp.Duration())
	}
	// Version-фолбэк на gen_ai.prompt.version.
	sp2 := Span{span: mkSpan(0, 0, strAttr("gen_ai.prompt.version", "v3"))}
	if sp2.Version() != "v3" {
		t.Errorf("version fallback: %q", sp2.Version())
	}
}

// TestPrompt_Schemas: Prompt/Completion нормализуют схемы A/B/C (контент инлайн, без CAS).
func TestPrompt_Schemas(t *testing.T) {
	ctx := context.Background()
	// Схема B: system_instructions + input/output.messages (JSON).
	b := Span{span: mkSpan(0, 0,
		strAttr("gen_ai.system_instructions", "SYS"),
		strAttr("gen_ai.input.messages", `[{"role":"user","content":"привет"}]`),
		strAttr("gen_ai.output.messages", `[{"role":"assistant","content":"здравствуйте"}]`),
	)}
	assertContains(t, "B.Prompt", must(b.Prompt(ctx)), "SYS", "привет")
	assertContains(t, "B.Completion", must(b.Completion(ctx)), "здравствуйте")
	if !b.IsLLM() {
		t.Error("B: IsLLM=false")
	}

	// Схема A: плоские gen_ai.prompt.{N} / gen_ai.completion.{N}.
	a := Span{span: mkSpan(0, 0,
		strAttr("gen_ai.prompt.0.role", "system"), strAttr("gen_ai.prompt.0.content", "SYS-A"),
		strAttr("gen_ai.prompt.1.role", "user"), strAttr("gen_ai.prompt.1.content", "вопрос"),
		strAttr("gen_ai.completion.0.role", "assistant"), strAttr("gen_ai.completion.0.content", "ответ"),
	)}
	assertContains(t, "A.Prompt", must(a.Prompt(ctx)), "SYS-A", "вопрос")
	assertContains(t, "A.Completion", must(a.Completion(ctx)), "ответ")

	// Схема C: контент в span events.
	c := Span{span: &tracepb.Span{Events: []*tracepb.Span_Event{
		{Name: "gen_ai.content.prompt", Attributes: []*commonpb.KeyValue{strAttr("content", "промпт-C")}},
		{Name: "gen_ai.content.completion", Attributes: []*commonpb.KeyValue{strAttr("content", "ответ-C")}},
	}}}
	assertContains(t, "C.Prompt", must(c.Prompt(ctx)), "промпт-C")
	assertContains(t, "C.Completion", must(c.Completion(ctx)), "ответ-C")
	if !c.IsLLM() {
		t.Error("C: IsLLM=false")
	}
}

// TestLazyResolve: тайминги/токены НЕ дёргают CAS; Prompt дёргает и отдаёт бит-в-бит.
func TestLazyResolve(t *testing.T) {
	ctx := context.Background()
	cas := newCAS()
	sysContent := "СИСТЕМНАЯ ИНСТРУКЦИЯ " + fmt.Sprintf("%02000d", 7) // крупный контент
	ref := cas.put(sysContent)

	sp := Span{
		span: mkSpan(0, 50_000_000,
			intAttr("gen_ai.usage.input_tokens", 100),
			strAttr("gen_ai.system_instructions", ref),
			strAttr("gen_ai.input.messages", cas.put(`[{"role":"user","content":"q"}]`)),
			strAttr("gen_ai.output.messages", cas.put(`[{"role":"assistant","content":"a"}]`)),
		),
		cas: cas,
	}

	// Доступ к таймингам/токенам — без CAS.
	_ = sp.Duration()
	_ = sp.Int("gen_ai.usage.input_tokens")
	if cas.total() != 0 {
		t.Fatalf("тайминги дёрнули CAS %d раз", cas.total())
	}

	// Prompt разворачивает контент из CAS и отдаёт исходник бит-в-бит.
	got := must(sp.Prompt(ctx))
	assertContains(t, "Prompt", got, sysContent)
	if cas.total() == 0 {
		t.Fatal("Prompt не дёрнул CAS")
	}
}

// TestWindowCache: два спана ссылаются на ОДИН фрагмент -> inner CAS.Fetch один раз.
func TestWindowCache(t *testing.T) {
	ctx := context.Background()
	cas := newCAS()
	shared := "ОБЩАЯ СИСТЕМКА " + fmt.Sprintf("%01000d", 3)
	ref := cas.put(shared)
	win := newLRU(cas, DefaultCacheBytes)

	for i := 0; i < 2; i++ {
		sp := Span{span: mkSpan(0, 0, strAttr("gen_ai.system_instructions", ref)), cas: win}
		if _, err := sp.Prompt(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// В countingCAS ровно один хэш; он должен быть запрошен единожды.
	for h, n := range cas.calls {
		if n != 1 {
			t.Errorf("фрагмент %x: fetch=%d, ждали 1 (кэш окна)", h[:4], n)
		}
	}
}

// TestBuildDialog: реплики user/assistant в хронологии, включая растянутую сессию.
func TestBuildDialog(t *testing.T) {
	ctx := context.Background()
	// Ход 1 (t=100) и ход 2 (t=200, через «месяц») — кумулятивная история схемы B.
	turn1 := Span{span: mkSpan(100, 150,
		strAttr("session.id", "s"),
		strAttr("gen_ai.input.messages", `[{"role":"user","content":"u1"}]`),
		strAttr("gen_ai.output.messages", `[{"role":"assistant","content":"a1"}]`),
	)}
	turn2 := Span{span: mkSpan(200, 250,
		strAttr("session.id", "s"),
		strAttr("gen_ai.input.messages", `[{"role":"user","content":"u1"},{"role":"assistant","content":"a1"},{"role":"user","content":"u2"}]`),
		strAttr("gen_ai.output.messages", `[{"role":"assistant","content":"a2"}]`),
	)}
	// Трейсы намеренно в обратном порядке — BuildDialog сортирует по времени.
	sess := Session{SessionID: "s", Traces: []Trace{
		{TraceID: "t2", Spans: []Span{turn2}},
		{TraceID: "t1", Spans: []Span{turn1}},
	}}
	dlg, err := BuildDialog(ctx, sess)
	if err != nil {
		t.Fatal(err)
	}
	want := []Message{{"user", "u1"}, {"assistant", "a1"}, {"user", "u2"}, {"assistant", "a2"}}
	if len(dlg) != len(want) {
		t.Fatalf("реплик %d, ждали %d: %+v", len(dlg), len(want), dlg)
	}
	for i := range want {
		if dlg[i] != want[i] {
			t.Errorf("реплика[%d]=%+v, ждали %+v", i, dlg[i], want[i])
		}
	}
}

// TestBothModes_IdenticalSpan: один и тот же процессор над Span из архива (zstd-фрейм)
// и из стрима (строка spans.thin) даёт идентичный результат — «один тип, оба режима».
func TestBothModes_IdenticalSpan(t *testing.T) {
	ctx := context.Background()
	cas := newCAS()
	ref := cas.put("СИСТЕМКА " + fmt.Sprintf("%01500d", 1))
	span := mkSpan(10, 60,
		strAttr("session.id", "s9"),
		strAttr("gen_ai.request.model", "gpt-4o-mini"),
		intAttr("gen_ai.usage.output_tokens", 42),
		strAttr("gen_ai.system_instructions", ref),
	)
	line := thinLine(t, "dialog-agent", span)

	// Стрим: строка spans.thin -> Span.
	streamSpan, err := SpanFromThinLine(line, cas)
	if err != nil {
		t.Fatal(err)
	}
	// Архив: та же строка в zstd-фрейме -> Span.
	enc, _ := zstd.NewWriter(nil)
	defer enc.Close()
	dec, _ := zstd.NewReader(nil)
	defer dec.Close()
	frame := enc.EncodeAll(append(line, '\n'), nil)
	decoded, err := restore.DecodeThinFrame(dec, frame)
	if err != nil {
		t.Fatal(err)
	}
	archiveSpan := spanFromRestored(decoded[0], cas)

	// Чистая логика процессора над Span.
	proc := func(s Span) string {
		p, _ := s.Prompt(ctx)
		return fmt.Sprintf("%s|%s|%d|%d", s.SessionID(), s.String("gen_ai.request.model"),
			s.Int("gen_ai.usage.output_tokens"), len(p))
	}
	if proc(streamSpan) != proc(archiveSpan) {
		t.Fatalf("режимы разошлись:\n stream=%s\narchive=%s", proc(streamSpan), proc(archiveSpan))
	}
}

// --- мелочи ---

func thinLine(t *testing.T, service string, span *tracepb.Span) []byte {
	t.Helper()
	spanJSON, err := protojson.Marshal(span)
	if err != nil {
		t.Fatal(err)
	}
	line, err := json.Marshal(struct {
		Service string          `json:"service"`
		Scope   string          `json:"scope"`
		Span    json.RawMessage `json:"span"`
	}{service, "scope", spanJSON})
	if err != nil {
		t.Fatal(err)
	}
	return line
}

// must разворачивает (string, error); в тесте паника = падение. Принимает ровно
// пару возвратов Prompt/Completion, поэтому без *testing.T (спред-аргумент).
func must(s string, err error) string {
	if err != nil {
		panic(err)
	}
	return s
}

func assertContains(t *testing.T, what, got string, subs ...string) {
	t.Helper()
	for _, sub := range subs {
		if !contains(got, sub) {
			t.Errorf("%s: не содержит %q (got %.60q)", what, sub, got)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
