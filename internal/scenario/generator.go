// Package scenario генерирует реалистичный OTLP-поток анти-спам прода:
// сессии-«звонки», три ресурса (greeter / dialog-agent / extractor), контент
// LLM в схемах A или B и атрибуты «на трейс» (session.id, app.version),
// проброшенные через baggage + BaggageSpanProcessor — целевой механизм, а не
// ручное копирование.
package scenario

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/processors/baggagecopy"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"
)

// Имена трёх ресурсов (service.name) — чтобы пачки в Kafka были многоресурсными.
const (
	svcGreeter   = "greeter"
	svcDialog    = "dialog-agent"
	svcExtractor = "extractor"
)

// Config — параметры запуска генератора.
type Config struct {
	Endpoint string        // OTLP/HTTP endpoint коллектора (host:port)
	RPS      int           // суммарная скорость НОВЫХ трейсов в секунду
	Workers  int           // число параллельных «звонков» (сессий)
	Duration time.Duration // длительность генерации
	Schema   string        // схема контента LLM: "A" (плоская) или "B" (input/output messages)
	// LongContent — режим длинных диалогов: реплики раздуваются до ~2.5 КБ, а число
	// ходов растёт. Транскрипт достигает десятков КБ с длинными общими префиксами —
	// на таком FastCDC даёт партиал-дедуп, а whole-field почти нет (для сравнения в T8).
	LongContent bool
	// Realistic — вариативность под выразительные витрины (Sprint 04): разные причины
	// завершения, finish_reasons, редкие error-статусы, think-time (ненулевой latency и
	// разброс удержания). НЕ влияет на дедуплицируемый контент (systemа/диалог) — экономия
	// дедупа не меняется. user.id и scam.type пишутся всегда (нужны витринам, дешёвые).
	Realistic bool
}

// stats — атомарные счётчики прогона.
type stats struct{ sessions, traces, spans, bytes int64 }

// Generator держит по одному TracerProvider на каждый ресурс (service.name
// задаётся на слое Resource, поэтому нужен отдельный провайдер на сервис) и
// общий rate-лимитер, гейтящий создание новых трейсов.
type Generator struct {
	cfg Config
	tps []*sdktrace.TracerProvider
	tr  map[string]trace.Tracer
	lim *rate.Limiter
	st  stats
}

// New поднимает три провайдера, экспортирующих в один и тот же коллектор.
func New(ctx context.Context, cfg Config) (*Generator, error) {
	g := &Generator{
		cfg: cfg,
		tr:  map[string]trace.Tracer{},
		lim: rate.NewLimiter(rate.Limit(cfg.RPS), max(cfg.RPS, 1)),
	}
	for _, svc := range []string{svcGreeter, svcDialog, svcExtractor} {
		tp, err := newProvider(ctx, cfg.Endpoint, svc)
		if err != nil {
			_ = g.shutdown(context.Background())
			return nil, fmt.Errorf("provider %s: %w", svc, err)
		}
		g.tps = append(g.tps, tp)
		g.tr[svc] = tp.Tracer("traceforge/loadgen")
	}
	return g, nil
}

// newProvider создаёт TracerProvider с OTLP/HTTP-экспортёром, ресурсом
// (service.name) и BaggageSpanProcessor, который копирует членов baggage
// (session.id, app.version) в атрибуты КАЖДОГО спана на старте.
func newProvider(ctx context.Context, endpoint, svc string) (*sdktrace.TracerProvider, error) {
	exp, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(endpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}
	res := resource.NewWithAttributes(semconv.SchemaURL,
		semconv.ServiceName(svc),
		semconv.ServiceVersion("1.0.0"),
	)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(exp),
		sdktrace.WithSpanProcessor(baggagecopy.NewSpanProcessor(baggagecopy.AllowAllMembers)),
	)
	return tp, nil
}

// Run запускает воркеры на Config.Duration, периодически печатает счётчики и
// корректно флашит провайдеры на выходе.
func (g *Generator) Run(ctx context.Context) error {
	runCtx, cancel := context.WithTimeout(ctx, g.cfg.Duration)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < g.cfg.Workers; i++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(seed, seed*2654435761+1))
			// runSession возвращает false, когда лимитер больше не выдаст токен до
			// дедлайна — тогда воркер завершается, а не крутится вхолостую.
			for g.runSession(runCtx, r) {
			}
		}(uint64(i) + 1)
	}

	done := make(chan struct{})
	go g.report(runCtx, done)

	wg.Wait()
	close(done)
	g.printStats("[final]")

	shCtx, shCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shCancel()
	return g.shutdown(shCtx)
}

// report печатает промежуточные счётчики раз в 5 секунд.
func (g *Generator) report(ctx context.Context, done chan struct{}) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-t.C:
			g.printStats("[stats]")
		}
	}
}

func (g *Generator) printStats(tag string) {
	log.Printf("%s sessions=%d traces=%d spans=%d text=%s",
		tag,
		atomic.LoadInt64(&g.st.sessions),
		atomic.LoadInt64(&g.st.traces),
		atomic.LoadInt64(&g.st.spans),
		humanBytes(atomic.LoadInt64(&g.st.bytes)),
	)
}

// newTrace ждёт токен лимитера — так мы гейтим именно скорость новых трейсов.
// Возвращает false, если контекст завершился (пора останавливаться).
func (g *Generator) newTrace(ctx context.Context) bool {
	return g.lim.Wait(ctx) == nil
}

// runSession проигрывает один «звонок»: сначала трейс greeter, затем 3–10
// диалоговых трейсов. session.id и app.version кладутся в baggage один раз и
// далее сами размазываются по всем спанам сессии. Возвращает false, если
// контекст/лимитер больше не позволяют начать новый трейс — сигнал воркеру
// остановиться.
func (g *Generator) runSession(ctx context.Context, r *rand.Rand) bool {
	sid := uuid.NewString()
	uid := userIDs[r.IntN(len(userIDs))] // абонент из небольшого пула -> «обращения за N дней»
	ver := "v3"
	if r.IntN(2) == 0 {
		ver = "v4"
	}
	sctx := baggage.ContextWithBaggage(ctx, mustBaggage(sid, ver, uid))

	// Сессию считаем только после первого реально выданного токена.
	if !g.newTrace(ctx) {
		return false
	}
	atomic.AddInt64(&g.st.sessions, 1)
	g.greeterTrace(sctx, r)

	// Сценарий завершения выбирается на сессию: задаёт число ходов и «хвост» диалога
	// (сигналы для классификатора причин, T6). Без -realistic все сессии «исчерпаны».
	end := endExhausted
	if g.cfg.Realistic {
		end = pickEndScenario(r)
	}
	turns := g.turnsFor(end, r)

	history := make([]message, 0, 20)
	for i := 0; i < turns; i++ {
		if !g.newTrace(ctx) {
			return false
		}
		g.dialogTrace(sctx, r, ver, &history, turnOpts{end: end, last: i == turns-1})
	}
	return true
}

// turnsFor — число ходов под сценарий завершения (короткий обрыв vs исчерпание).
func (g *Generator) turnsFor(end endScenario, r *rand.Rand) int {
	switch end {
	case endHungUp:
		return 2 + r.IntN(2) // 2..3 — спамер бросил рано
	case endExhausted:
		if g.cfg.LongContent {
			return 12 + r.IntN(4) // 12..15
		}
		return 8 + r.IntN(3) // 8..10 — удержание сработало
	default:
		if g.cfg.LongContent {
			return 8 + r.IntN(8) // 8..15
		}
		return 3 + r.IntN(8) // 3..10
	}
}

// mustBaggage собирает baggage из session.id, app.version и user.id. Ключи —
// валидные W3C-токены; значения размазываются по всем спанам сессии BaggageSpanProcessor'ом.
func mustBaggage(sid, ver, uid string) baggage.Baggage {
	m1, _ := baggage.NewMember("session.id", sid)
	m2, _ := baggage.NewMember("app.version", ver)
	m3, _ := baggage.NewMember("user.id", uid)
	b, _ := baggage.New(m1, m2, m3)
	return b
}

// greeterTrace — трейс из одного спана pick_greeting c повторяемым приветствием.
func (g *Generator) greeterTrace(ctx context.Context, r *rand.Rand) {
	_, span := g.tr[svcGreeter].Start(ctx, "pick_greeting")
	idx := r.IntN(len(greetings))
	g.strAttr(span, "greeting.text", greetings[idx])
	span.SetAttributes(attribute.String("greeting.template_id", fmt.Sprintf("greet-%02d", idx)))
	span.End()
	atomic.AddInt64(&g.st.traces, 1)
	atomic.AddInt64(&g.st.spans, 1)
}

// turnOpts — параметры одного хода: сценарий завершения сессии и признак последнего
// хода (на нём проявляется «хвост» — обрыв, глупость агента, прощание спамера).
type turnOpts struct {
	end  endScenario
	last bool
}

// dialogTrace строит дерево одного хода диалога:
//
//	agent_turn (dialog-agent, root)
//	├── classify_intent (dialog-agent)
//	├── chat gpt-4o-mini (dialog-agent)
//	└── chat qwen-lite    (extractor, ~35% случаев) — тот же trace_id, другой ресурс
//
// history растёт от хода к ходу — это будущая цель FastCDC.
func (g *Generator) dialogTrace(ctx context.Context, r *rand.Rand, ver string, history *[]message, opts turnOpts) {
	tctx, root := g.tr[svcDialog].Start(ctx, "agent_turn")
	spanCount := int64(1)

	_, cls := g.tr[svcDialog].Start(tctx, "classify_intent")
	cls.SetAttributes(
		attribute.String("intent.label", g.intentFor(r, opts)),
		attribute.Float64("intent.confidence", 0.5+r.Float64()*0.5),
	)
	cls.End()
	spanCount++

	// Реплика спамера дополняет растущий префикс диалога; на прощальном хвосте —
	// маркерная реплика «закругления», которую ловит классификатор причин.
	phrase := spammerPhrases[r.IntN(len(spammerPhrases))]
	if g.cfg.Realistic && opts.last && opts.end == endSpammerBye {
		phrase = byePhrases[r.IntN(len(byePhrases))]
	}
	reply := agentReplies[r.IntN(len(agentReplies))]
	if g.cfg.LongContent {
		phrase = padTo(phrase, 2500)
		reply = padTo(reply, 2500)
	}
	*history = append(*history, message{Role: "user", Content: phrase})

	_, chat := g.tr[svcDialog].Start(tctx, "chat gpt-4o-mini")
	g.applyLLM(chat, r, ver, *history, reply, "gpt-4o-mini", "openai", g.finishFor(r, opts))
	g.think(r) // think-time -> ненулевой latency и разброс удержания (реалистичность)
	if g.cfg.Realistic && opts.last && opts.end == endConnLost {
		chat.SetStatus(codes.Error, "upstream connection reset")
	}
	chat.End()
	spanCount++
	*history = append(*history, message{Role: "assistant", Content: reply})

	// Иногда параллельно отрабатывает extractor на своей модели/ресурсе.
	if r.Float64() < 0.35 {
		k := r.IntN(len(extractorOutputs))
		_, ex := g.tr[svcExtractor].Start(tctx, "chat qwen-lite")
		g.applyLLM(ex, r, ver, *history, extractorOutputs[k], "qwen-lite", "alibaba", "stop")
		// scam.type денормализуем на спан (из JSON-выхода) — витрине не нужен CAS.
		ex.SetAttributes(attribute.String("scam.type", extractorScamTypes[k]))
		ex.End()
		spanCount++
	}

	root.End()
	atomic.AddInt64(&g.st.traces, 1)
	atomic.AddInt64(&g.st.spans, spanCount)
}

// intentFor — метка намерения; на прощальном хвосте — "goodbye".
func (g *Generator) intentFor(r *rand.Rand, opts turnOpts) string {
	if g.cfg.Realistic && opts.last && opts.end == endSpammerBye {
		return "goodbye"
	}
	return intents[r.IntN(len(intents))]
}

// finishFor — причина остановки генерации: обычно "stop"; на хвосте «агент сглупил» —
// content_filter/length. Случайная вариативность — только на НЕ-последних ходах, чтобы
// сигнал последнего хода (его читает классификатор) оставался чистым.
func (g *Generator) finishFor(r *rand.Rand, opts turnOpts) string {
	if !g.cfg.Realistic {
		return "stop"
	}
	if opts.last {
		if opts.end == endAgentConfused {
			if r.IntN(2) == 0 {
				return "content_filter"
			}
			return "length"
		}
		return "stop"
	}
	if r.Float64() < 0.05 { // распределение finish_reason в витрине (не влияет на классификатор)
		return "length"
	}
	return "stop"
}

// think — небольшая думающая пауза LLM-спана (реалистичный latency и разброс удержания).
func (g *Generator) think(r *rand.Rand) {
	if !g.cfg.Realistic {
		return
	}
	d := time.Duration(8+r.IntN(40)) * time.Millisecond
	if r.Float64() < 0.1 { // редкий «долгий» ход -> хвост распределения удержания
		d += time.Duration(150+r.IntN(350)) * time.Millisecond
	}
	time.Sleep(d)
}

// applyLLM раскладывает контент LLM на спан согласно выбранной схеме и добавляет
// числовые/метаданные атрибуты (пища для P3). Дедуп-кандидаты (system, input,
// output) прогоняются через strAttr, чтобы попасть в счётчик текстовых байт.
func (g *Generator) applyLLM(span trace.Span, r *rand.Rand, ver string, msgs []message, output, model, provider, finish string) {
	sys := systemInstructions[ver]
	switch g.cfg.Schema {
	case "A": // deprecated, но массовая: плоские gen_ai.prompt.{N} / gen_ai.completion.{N}
		g.strAttr(span, "gen_ai.prompt.0.role", "system")
		g.strAttr(span, "gen_ai.prompt.0.content", sys)
		for i, m := range msgs {
			g.strAttr(span, fmt.Sprintf("gen_ai.prompt.%d.role", i+1), m.Role)
			g.strAttr(span, fmt.Sprintf("gen_ai.prompt.%d.content", i+1), m.Content)
		}
		g.strAttr(span, "gen_ai.completion.0.role", "assistant")
		g.strAttr(span, "gen_ai.completion.0.content", output)
	default: // "B": JSON-строки system_instructions / input.messages / output.messages
		g.strAttr(span, "gen_ai.system_instructions", sys)
		g.strAttr(span, "gen_ai.input.messages", toJSON(msgs))
		g.strAttr(span, "gen_ai.output.messages", toJSON([]message{{Role: "assistant", Content: output}}))
	}
	span.SetAttributes(
		// Идентичность промпт-шаблона (semconv) — вход для template-aware дедупа (T9/фаза 2):
		// системка версионируется (v3/v4), имя стабильно. Научрук добавит template_id/vars.
		attribute.String("gen_ai.prompt.name", "antispam.system"),
		attribute.String("gen_ai.prompt.version", ver),
		attribute.String("gen_ai.request.model", model),
		attribute.Float64("gen_ai.request.temperature", 0.2+r.Float64()*0.7),
		attribute.Int("gen_ai.usage.input_tokens", estTokens(sys, msgs)),
		attribute.Int("gen_ai.usage.output_tokens", len(output)/4),
		attribute.StringSlice("gen_ai.response.finish_reasons", []string{finish}),
		attribute.String("gen_ai.provider.name", provider),
	)
}

// strAttr ставит строковый атрибут и учитывает его размер в счётчике текстовых байт.
func (g *Generator) strAttr(span trace.Span, key, val string) {
	span.SetAttributes(attribute.String(key, val))
	atomic.AddInt64(&g.st.bytes, int64(len(val)))
}

// shutdown флашит и закрывает все провайдеры; возвращает первую ошибку.
func (g *Generator) shutdown(ctx context.Context) error {
	var firstErr error
	for _, tp := range g.tps {
		if err := tp.Shutdown(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
