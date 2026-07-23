package tf

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"

	"traceforge/internal/dedup"
	"traceforge/internal/otlp"
)

// --- типизированные геттеры (без CAS: span-атрибуты малы, лежат в теле) ---

// attr находит AnyValue span-атрибута по ключу.
func (s Span) attr(key string) *commonpb.AnyValue {
	for _, kv := range s.span.GetAttributes() {
		if kv.GetKey() == key {
			return kv.GetValue()
		}
	}
	return nil
}

// String возвращает строковый атрибут ("" если нет). Может вернуть ссылку
// ref:sha256 для тяжёлых полей — текст доставать через Prompt/Completion.
func (s Span) String(key string) string { return s.attr(key).GetStringValue() }

// Int возвращает целочисленный атрибут (0 если нет/не число).
func (s Span) Int(key string) int64 { return s.attr(key).GetIntValue() }

// Float возвращает атрибут double (0 если нет).
func (s Span) Float(key string) float64 { return s.attr(key).GetDoubleValue() }

// Bool возвращает булев атрибут (false если нет).
func (s Span) Bool(key string) bool { return s.attr(key).GetBoolValue() }

// Name — имя операции спана.
func (s Span) Name() string { return s.span.GetName() }

// TraceID / SpanID — канонические hex-идентификаторы.
func (s Span) TraceID() string { return otlp.TraceIDHex(s.span) }
func (s Span) SpanID() string  { return otlp.SpanIDHex(s.span) }

// StartTime — время начала спана (UTC — как весь пайплайн: архиватор бандлит по
// ts.UTC(), чтобы календарный день колонок Date считался однозначно, без сдвига зоны).
func (s Span) StartTime() time.Time { return time.Unix(0, int64(s.span.GetStartTimeUnixNano())).UTC() }

// Duration — длительность спана (end−start). Не дёргает CAS
func (s Span) Duration() time.Duration {
	return time.Duration(s.span.GetEndTimeUnixNano() - s.span.GetStartTimeUnixNano())
}

// StatusCode — код статуса ("UNSET"/"OK"/"ERROR" без префикса).
func (s Span) StatusCode() string {
	return strings.TrimPrefix(s.span.GetStatus().GetCode().String(), "STATUS_CODE_")
}

// StatusMessage — сообщение статуса (для error-спанов).
func (s Span) StatusMessage() string { return s.span.GetStatus().GetMessage() }

// FirstString возвращает первый элемент строкового массива-атрибута (напр.
// gen_ai.response.finish_reasons), "" если нет.
func (s Span) FirstString(key string) string {
	arr := s.attr(key).GetArrayValue()
	if arr == nil || len(arr.GetValues()) == 0 {
		return ""
	}
	return arr.GetValues()[0].GetStringValue()
}

// ParentSpanID — hex родительского спана ("" у корня).
func (s Span) ParentSpanID() string {
	return hex.EncodeToString(s.span.GetParentSpanId())
}

// IsRoot — корневой спан трейса (нет родителя). Якорь для строки user_sessions
// (одна на трейс), чтобы не задваивать её на каждом дочернем спане.
func (s Span) IsRoot() bool { return len(s.span.GetParentSpanId()) == 0 }

// --- удобные доменные геттеры (атрибуты «на трейс» пробрасываются baggage) ---

// SessionID — session.id (пусто, если baggage не настроен: см. метрику покрытия T7).
func (s Span) SessionID() string { return s.String("session.id") }

// UserID — user.id абонента (для витрины «обращения пользователя за N дней»).
func (s Span) UserID() string { return s.String("user.id") }

// Version — версия промпта/приложения: app.version (baggage), фолбэк
// gen_ai.prompt.version. Без молчаливого фолбэка на пустоту — см. покрытие (T7).
func (s Span) Version() string {
	if v := s.String("app.version"); v != "" {
		return v
	}
	return s.String("gen_ai.prompt.version")
}

// --- контент: нормализация схем A/B/C + ленивый резолв ref из CAS ---

// Ключи контента (согласованы с dedup.DefaultExtractConfig — единый источник схем).
const (
	keySystem     = "gen_ai.system_instructions" // схема B
	keyInputMsgs  = "gen_ai.input.messages"      // схема B
	keyOutputMsgs = "gen_ai.output.messages"     // схема B
	prefixPromptA = "gen_ai.prompt."             // схема A: gen_ai.prompt.{N}.role/content
	prefixComplA  = "gen_ai.completion."         // схема A
	evPrompt      = "gen_ai.content.prompt"      // схема C (span event)
	evCompletion  = "gen_ai.content.completion"  // схема C
)

// resolve разворачивает значение: если это ссылка ref:sha256 — тянет из CAS,
// иначе возвращает как есть (короткий недедуплицированный контент).
func (s Span) resolve(ctx context.Context, val string) (string, error) {
	if !dedup.IsRef(val) {
		return val, nil
	}
	if s.cas == nil {
		return val, nil // источник без CAS (напр. тесты таймингов) — ссылку не разворачиваем
	}
	return dedup.ResolveRef(ctx, val, s.cas)
}

// promptMessages нормализует вход LLM (system + история) в реплики, разворачивая
// тяжёлый контент из CAS. Прозрачно для схем A/B/C.
func (s Span) promptMessages(ctx context.Context) ([]Message, error) {
	// Схема B: system_instructions + input.messages (JSON-массив).
	if sys := s.String(keySystem); sys != "" {
		out, err := s.schemaBMessages(ctx, keySystem, keyInputMsgs, "system")
		return out, err
	}
	if s.String(keyInputMsgs) != "" {
		return s.schemaBMessages(ctx, "", keyInputMsgs, "")
	}
	// Схема A: gen_ai.prompt.{N}.role/content.
	if msgs, ok, err := s.schemaAMessages(ctx, prefixPromptA); ok || err != nil {
		return msgs, err
	}
	// Схема C: span event gen_ai.content.prompt.
	if m, ok, err := s.eventMessage(ctx, evPrompt, "user"); ok || err != nil {
		if err != nil {
			return nil, err
		}
		return []Message{m}, nil
	}
	return nil, nil
}

// completionMessages нормализует выход LLM в реплики (ассистент).
func (s Span) completionMessages(ctx context.Context) ([]Message, error) {
	if s.String(keyOutputMsgs) != "" { // схема B
		return s.schemaBMessages(ctx, "", keyOutputMsgs, "")
	}
	if msgs, ok, err := s.schemaAMessages(ctx, prefixComplA); ok || err != nil { // схема A
		return msgs, err
	}
	if m, ok, err := s.eventMessage(ctx, evCompletion, "assistant"); ok || err != nil { // схема C
		if err != nil {
			return nil, err
		}
		return []Message{m}, nil
	}
	return nil, nil
}

// schemaBMessages разбирает JSON-массив сообщений (input/output.messages),
// опц. с ведущим system-полем.
func (s Span) schemaBMessages(ctx context.Context, sysKey, arrKey, sysRole string) ([]Message, error) {
	var out []Message
	if sysKey != "" {
		sys, err := s.resolve(ctx, s.String(sysKey))
		if err != nil {
			return nil, err
		}
		if sys != "" {
			out = append(out, Message{Role: sysRole, Content: sys})
		}
	}
	raw, err := s.resolve(ctx, s.String(arrKey))
	if err != nil {
		return nil, err
	}
	if raw != "" {
		var msgs []Message
		if err := json.Unmarshal([]byte(raw), &msgs); err != nil {
			// не JSON — считаем цельной репликой
			out = append(out, Message{Role: "user", Content: raw})
		} else {
			out = append(out, msgs...)
		}
	}
	return out, nil
}

// schemaAMessages собирает плоские gen_ai.{prefix}{N}.role/content по возрастанию N.
func (s Span) schemaAMessages(ctx context.Context, prefix string) ([]Message, bool, error) {
	roles := map[int]string{}
	content := map[int]string{}
	var idxs []int
	for _, kv := range s.span.GetAttributes() {
		k := kv.GetKey()
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		rest := strings.TrimPrefix(k, prefix) // "{N}.role" | "{N}.content"
		dot := strings.IndexByte(rest, '.')
		if dot <= 0 {
			continue
		}
		n := atoiSafe(rest[:dot])
		if n < 0 {
			continue
		}
		switch rest[dot+1:] {
		case "role":
			roles[n] = kv.GetValue().GetStringValue()
			idxs = appendUniq(idxs, n)
		case "content":
			content[n] = kv.GetValue().GetStringValue()
			idxs = appendUniq(idxs, n)
		}
	}
	if len(idxs) == 0 {
		return nil, false, nil
	}
	sort.Ints(idxs)
	out := make([]Message, 0, len(idxs))
	for _, n := range idxs {
		c, err := s.resolve(ctx, content[n])
		if err != nil {
			return nil, true, err
		}
		out = append(out, Message{Role: roles[n], Content: c})
	}
	return out, true, nil
}

// eventMessage достаёт контент из span event (схема C).
func (s Span) eventMessage(ctx context.Context, evName, role string) (Message, bool, error) {
	for _, ev := range s.span.GetEvents() {
		if ev.GetName() != evName {
			continue
		}
		for _, kv := range ev.GetAttributes() {
			if v := kv.GetValue().GetStringValue(); v != "" {
				c, err := s.resolve(ctx, v)
				if err != nil {
					return Message{}, true, err
				}
				return Message{Role: role, Content: c}, true, nil
			}
		}
	}
	return Message{}, false, nil
}

// Prompt возвращает текст входа LLM (system+история) независимо от схемы A/B/C.
// Первое обращение дёргает CAS; тайминги/токены — не дёргают.
func (s Span) Prompt(ctx context.Context) (string, error) {
	msgs, err := s.promptMessages(ctx)
	if err != nil {
		return "", err
	}
	return joinContents(msgs), nil
}

// Completion возвращает текст выхода LLM независимо от схемы A/B/C.
func (s Span) Completion(ctx context.Context) (string, error) {
	msgs, err := s.completionMessages(ctx)
	if err != nil {
		return "", err
	}
	return joinContents(msgs), nil
}

// IsLLM — спан несёт диалоговый обмен (есть completion), т.е. годится в BuildDialog.
func (s Span) IsLLM() bool {
	return s.String(keyOutputMsgs) != "" ||
		s.hasAttrPrefix(prefixComplA) ||
		s.hasEvent(evCompletion)
}

// --- группировки и сборка диалога ---

// GroupByTrace группирует спаны по trace_id (порядок первого появления).
func GroupByTrace(spans []Span) []Trace {
	byID := map[string]int{}
	var out []Trace
	for _, sp := range spans {
		id := sp.TraceID()
		i, ok := byID[id]
		if !ok {
			byID[id] = len(out)
			out = append(out, Trace{TraceID: id})
			i = len(out) - 1
		}
		out[i].Spans = append(out[i].Spans, sp)
	}
	return out
}

// GroupBySession группирует трейсы по session.id (берётся из первого спана трейса).
func GroupBySession(traces []Trace) []Session {
	byID := map[string]int{}
	var out []Session
	for _, tr := range traces {
		sid := traceSessionID(tr)
		i, ok := byID[sid]
		if !ok {
			byID[sid] = len(out)
			out = append(out, Session{SessionID: sid})
			i = len(out) - 1
		}
		out[i].Traces = append(out[i].Traces, tr)
	}
	return out
}

// BuildDialog собирает реплики user/assistant сессии в хронологии — через все
// трейсы, включая растянутые по времени. На вход классификатору (T6), LLM-анализу
// и датасетам. Устойчиво к кумулятивной истории (схема B генератора: input.messages
// содержит всю предысторию) и к «дельтовой»: на каждый LLM-спан берём последнюю
// реплику пользователя из входа + ответ ассистента из выхода.
func BuildDialog(ctx context.Context, sess Session) ([]Message, error) {
	spans := sess.flatSpansByTime()
	var dialog []Message
	for _, sp := range spans {
		if !sp.IsLLM() {
			continue
		}
		pm, err := sp.promptMessages(ctx)
		if err != nil {
			return nil, err
		}
		if u := lastUser(pm); u != "" {
			dialog = append(dialog, Message{Role: "user", Content: u})
		}
		cm, err := sp.completionMessages(ctx)
		if err != nil {
			return nil, err
		}
		for _, m := range cm {
			dialog = append(dialog, Message{Role: "assistant", Content: m.Content})
		}
	}
	return dialog, nil
}

// flatSpansByTime — все спаны сессии, отсортированные по времени начала.
func (sess Session) flatSpansByTime() []Span {
	var spans []Span
	for _, tr := range sess.Traces {
		spans = append(spans, tr.Spans...)
	}
	sort.SliceStable(spans, func(i, j int) bool {
		return spans[i].span.GetStartTimeUnixNano() < spans[j].span.GetStartTimeUnixNano()
	})
	return spans
}

// --- мелкие помощники ---

func (s Span) hasAttrPrefix(prefix string) bool {
	for _, kv := range s.span.GetAttributes() {
		if strings.HasPrefix(kv.GetKey(), prefix) {
			return true
		}
	}
	return false
}

func (s Span) hasEvent(name string) bool {
	for _, ev := range s.span.GetEvents() {
		if ev.GetName() == name {
			return true
		}
	}
	return false
}

func traceSessionID(tr Trace) string {
	for _, sp := range tr.Spans {
		if sid := sp.SessionID(); sid != "" {
			return sid
		}
	}
	return ""
}

func joinContents(msgs []Message) string {
	var b strings.Builder
	for i, m := range msgs {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(m.Content)
	}
	return b.String()
}

func lastUser(msgs []Message) string {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == "user" {
			return msgs[i].Content
		}
	}
	return ""
}

func atoiSafe(s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return -1
		}
		n = n*10 + int(c-'0')
	}
	return n
}

func appendUniq(s []int, v int) []int {
	for _, x := range s {
		if x == v {
			return s
		}
	}
	return append(s, v)
}
