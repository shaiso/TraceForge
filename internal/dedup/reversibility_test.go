package dedup

import (
	"context"
	"fmt"
	"strings"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"traceforge/internal/otlp"
)

// contentPool — набор «тяжёлых» полей (>1 КБ). Раздаём по кругу, чтобы создать
// сильное дублирование (дедуп реально срабатывает: miss на первом, hit дальше).
func contentPool() []string {
	base := []string{
		"системная инструкция: удерживай спамера на линии, не выдавай себя, ",
		"диалог: — Здравствуйте, вас беспокоит служба безопасности банка. — ",
		"промпт классификатора намерений: определи, спам это или нет, верни ",
		"ответ агента: конечно, я вас слушаю, расскажите подробнее про вашу ",
		"справочные данные о продукте и правилах разговора с абонентом, а также ",
	}
	pool := make([]string, len(base))
	for i, s := range base {
		// Раздуваем до ~1.5 КБ детерминированно, чтобы поле прошло порог MinSize.
		pool[i] = strings.Repeat(s, 25) + fmt.Sprintf("#%d", i)
	}
	return pool
}

// makeSpan строит спан в одной из схем A/B/C, чтобы тест покрыл все пути
// экстрактора и restore. attrs заполняются из пула по индексу i.
func makeSpan(i int, pool []string) *tracepb.Span {
	a := pool[i%len(pool)]
	b := pool[(i*7+3)%len(pool)]
	tid := make([]byte, 16)
	sid := make([]byte, 8)
	for j := range tid {
		tid[j] = byte(i >> (j % 4))
	}
	for j := range sid {
		sid[j] = byte(i*3 + j)
	}
	span := &tracepb.Span{
		TraceId: tid,
		SpanId:  sid,
		Name:    "op",
		Attributes: []*commonpb.KeyValue{
			strAttr("gen_ai.request.model", "gpt-4o-mini"), // короткое — не трогается
		},
	}
	switch i % 3 {
	case 0: // схема B
		span.Attributes = append(span.Attributes,
			strAttr("gen_ai.system_instructions", a),
			strAttr("gen_ai.input.messages", b))
	case 1: // схема A (плоские индексы)
		span.Attributes = append(span.Attributes,
			strAttr("gen_ai.prompt.0.content", a),
			strAttr("gen_ai.completion.0.content", b))
	case 2: // схема C (контент в событии)
		span.Events = []*tracepb.Span_Event{{
			Name:       "gen_ai.content.prompt",
			Attributes: []*commonpb.KeyValue{strAttr("content", a)},
		}}
	}
	return span
}

// T7: обратимость на ≥1000 спанов, 100% восстановления. Прогоняем весь конвейер
// P1 (extract -> chunk -> intern -> замена на ref), фиксируем CAS и восстанавливаем
// каждый спан обратно; каждый обязан совпасть с оригиналом (proto.Equal).
func TestReversibility_WholeField(t *testing.T) {
	proc, packer, cleanup := newProcessorForTest(t)
	defer cleanup()
	ctx := context.Background()
	src := IndexPacker{Index: proc.ix, Packer: packer}

	pool := contentPool()
	const n = 1200

	// Оригиналы храним копиями (proc.Process мутирует спан по месту).
	originals := make([]*tracepb.Span, n)
	work := make([]*tracepb.Span, n)
	hashesToClean := map[[32]byte][]byte{}
	for _, c := range pool {
		h := WholeFieldChunker{}.Split(c, nil)[0]
		var k [32]byte
		copy(k[:], h.Hash)
		hashesToClean[k] = h.Hash
	}
	defer func() {
		for _, h := range hashesToClean {
			proc.ix.pool.Exec(ctx, "DELETE FROM dedup_index WHERE hash = $1", h)
		}
	}()

	for i := 0; i < n; i++ {
		span := makeSpan(i, pool)
		originals[i] = proto.Clone(span).(*tracepb.Span)
		work[i] = span
		f := otlp.FlatSpan{ServiceName: "svc", Span: span}
		if _, err := proc.Process(ctx, f); err != nil {
			t.Fatalf("Process[%d]: %v", i, err)
		}
	}

	// Дедуп реально сработал: и уникальные, и повторные фрагменты есть.
	if proc.Stats.Spans < 1000 {
		t.Fatalf("обработано спанов %d, ожидали >= 1000", proc.Stats.Spans)
	}
	if proc.Stats.Misses == 0 || proc.Stats.Hits == 0 {
		t.Fatalf("дедуп не отработал: miss=%d hit=%d", proc.Stats.Misses, proc.Stats.Hits)
	}

	// Фиксируем сегмент(ы) и индекс.
	if err := proc.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Восстанавливаем каждый спан и сверяем с оригиналом бит-в-бит.
	restoredFields := 0
	for i := 0; i < n; i++ {
		before := countRefs(work[i])
		restoredFields += before
		if err := RestoreSpan(ctx, work[i], src); err != nil {
			t.Fatalf("RestoreSpan[%d]: %v", i, err)
		}
		if countRefs(work[i]) != 0 {
			t.Fatalf("спан[%d]: после restore остались ref", i)
		}
		if !proto.Equal(work[i], originals[i]) {
			t.Fatalf("спан[%d]: restore != оригинал", i)
		}
	}

	// Каждый спан нёс минимум одно тяжёлое поле → ссылок было не меньше числа спанов.
	if restoredFields < n {
		t.Errorf("восстановлено полей %d, ожидали >= %d", restoredFields, n)
	}
	t.Logf("T7: спанов=%d поля-ссылки=%d уник=%d дублей=%d — 100%% обратимо",
		n, restoredFields, proc.Stats.Misses, proc.Stats.Hits)
}

// countRefs считает строковые значения-ссылки в атрибутах спана и его событий.
func countRefs(span *tracepb.Span) int {
	n := 0
	for _, kv := range span.GetAttributes() {
		if IsRef(kv.GetValue().GetStringValue()) {
			n++
		}
	}
	for _, ev := range span.GetEvents() {
		for _, kv := range ev.GetAttributes() {
			if IsRef(kv.GetValue().GetStringValue()) {
				n++
			}
		}
	}
	return n
}
