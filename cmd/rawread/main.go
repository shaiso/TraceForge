// Команда rawread читает топик otlp.raw.spans, десериализует OTLP-пачки и
// обходит их через internal/otlp. Это одновременно: (1) проверка кодировки и
// парсинга, (2) каркас будущего консьюмера P1, (3) смоук-ассерты и первая
// оценка экономии дедупа «X -> Y» ещё до написания самого P1.
package main

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/encoding/protojson"

	"traceforge/internal/otlp"
)

// dedupThreshold — минимальный размер строки, попадающей в whole-field превью
// (совпадает с идеей первого режима чанкера P1: строки > ~1 КБ целиком в CAS).
const dedupThreshold = 1024

func main() {
	brokers := flag.String("brokers", "localhost:19092", "seed-брокеры Kafka/Redpanda через запятую")
	topic := flag.String("topic", "otlp.raw.spans", "топик для чтения")
	group := flag.String("group", "", "consumer group (пусто = прямое чтение партиций с начала)")
	duration := flag.Duration("duration", 15*time.Second, "сколько читать")
	limit := flag.Int("limit", 0, "максимум сообщений (0 = без лимита)")
	jsonDump := flag.Bool("json", false, "печатать полный спан как protojson")
	smoke := flag.Bool("smoke", false, "прогнать смоук-ассерты и выйти с ненулевым кодом при провале")
	flag.Parse()

	seeds := strings.Split(*brokers, ",")
	opts := []kgo.Opt{
		kgo.SeedBrokers(seeds...),
		kgo.ConsumeTopics(*topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}
	if *group != "" {
		opts = append(opts, kgo.ConsumerGroup(*group))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("kafka client: %v", err)
	}
	defer cl.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()

	agg := newAggregator()
	log.Printf("rawread: брокеры=%s топик=%s читаю до %s ...", *brokers, *topic, *duration)

	for {
		fs := cl.PollFetches(ctx)
		if fs.IsClientClosed() {
			break
		}
		if errs := fs.Errors(); len(errs) > 0 {
			// Таймаут контекста — штатное завершение чтения.
			if ctx.Err() != nil {
				break
			}
			for _, e := range errs {
				log.Printf("fetch error: %v", e.Err)
			}
			break
		}

		stopNow := false
		fs.EachRecord(func(rec *kgo.Record) {
			req, err := otlp.Unmarshal(rec.Value)
			if err != nil {
				log.Printf("unmarshal: %v", err)
				return
			}
			agg.messages++
			otlp.Walk(req, func(f otlp.FlatSpan) {
				agg.observe(f)
				if *smoke {
					return
				}
				if *jsonDump {
					b, _ := protojson.Marshal(f.Span)
					fmt.Printf("%-13s | %s\n", f.ServiceName, b)
					return
				}
				sid, _ := otlp.AttrString(f.Span.GetAttributes(), "session.id")
				ver, _ := otlp.AttrString(f.Span.GetAttributes(), "app.version")
				fmt.Printf("%-13s | %s | %-22s | sid=%s ver=%s | str=%dB\n",
					f.ServiceName, otlp.TraceIDHex(f.Span), f.Span.GetName(),
					short(sid), ver, otlp.StringAttrsSize(f.Span))
			})
			if *limit > 0 && agg.messages >= int64(*limit) {
				stopNow = true
			}
		})
		if stopNow {
			break
		}
	}

	agg.printSummary()
	if *smoke && !agg.smokeAsserts() {
		os.Exit(1)
	}
}

// aggregator копит сквозную статистику прогона: счётчики, множество сервисов,
// повторяемость system_instructions и превью whole-field дедупа по байтам.
type aggregator struct {
	messages          int64
	spans             int64
	spansWithSession  int64
	services          map[string]struct{}
	seenChunk         map[[32]byte]struct{}
	totalStrBytes     int64
	dupStrBytes       int64
	sysSeen           map[[32]byte]struct{}
	sysTotal, sysDup  int64
}

func newAggregator() *aggregator {
	return &aggregator{
		services:  map[string]struct{}{},
		seenChunk: map[[32]byte]struct{}{},
		sysSeen:   map[[32]byte]struct{}{},
	}
}

func (a *aggregator) observe(f otlp.FlatSpan) {
	a.spans++
	a.services[f.ServiceName] = struct{}{}

	if sid, ok := otlp.AttrString(f.Span.GetAttributes(), "session.id"); ok && sid != "" {
		a.spansWithSession++
	}

	// Повторяемость системного промпта — предвестник метрики дедупа.
	if sys := systemText(f); sys != "" {
		h := sha256.Sum256([]byte(sys))
		a.sysTotal++
		if _, dup := a.sysSeen[h]; dup {
			a.sysDup++
		} else {
			a.sysSeen[h] = struct{}{}
		}
	}

	// Превью whole-field дедупа: строки >= порога хэшируем, повторные считаем экономией.
	for _, kv := range f.Span.GetAttributes() {
		s := kv.GetValue().GetStringValue()
		if s == "" {
			continue
		}
		a.totalStrBytes += int64(len(s))
		if len(s) >= dedupThreshold {
			h := sha256.Sum256([]byte(s))
			if _, dup := a.seenChunk[h]; dup {
				a.dupStrBytes += int64(len(s))
			} else {
				a.seenChunk[h] = struct{}{}
			}
		}
	}
}

func (a *aggregator) printSummary() {
	afterDedup := a.totalStrBytes - a.dupStrBytes
	log.Printf("---- итоги ----")
	log.Printf("сообщений: %d, спанов: %d, с session.id: %d (%.1f%%)",
		a.messages, a.spans, a.spansWithSession, pct(a.spansWithSession, a.spans))
	log.Printf("разных service.name: %d", len(a.services))
	log.Printf("system_instructions: всего %d, повторов %d (%.1f%% дублей)",
		a.sysTotal, a.sysDup, pct(a.sysDup, a.sysTotal))
	log.Printf("текст в строковых атрибутах: %s всего", humanBytes(a.totalStrBytes))
	log.Printf("превью whole-field дедупа (строки >= %dБ): %s -> %s (экономия %s, %.1f%%)",
		dedupThreshold, humanBytes(a.totalStrBytes), humanBytes(afterDedup),
		humanBytes(a.dupStrBytes), pct(a.dupStrBytes, a.totalStrBytes))
}

// smokeAsserts проверяет DoD-условия спринта; возвращает true, если всё зелёное.
func (a *aggregator) smokeAsserts() bool {
	ok := true
	check := func(cond bool, msg string) {
		status := "OK"
		if !cond {
			status = "FAIL"
			ok = false
		}
		log.Printf("[smoke] %-4s %s", status, msg)
	}
	check(a.messages > 0, "в топике есть сообщения")
	check(a.spans > 0 && a.spansWithSession == a.spans, "у 100% спанов есть session.id (baggage работает)")
	check(len(a.services) >= 3, ">= 3 разных service.name")
	dupRatio := 0.0
	if a.sysTotal > 0 {
		dupRatio = float64(a.sysDup) / float64(a.sysTotal)
	}
	check(dupRatio > 0.8, "доля дублей system_instructions > 80%")
	return ok
}

// systemText достаёт системный промпт независимо от схемы (B: gen_ai.system_instructions,
// A: gen_ai.prompt.0.content) — экстрактор дедуп-кандидатов по списку путей, не хардкод.
func systemText(f otlp.FlatSpan) string {
	if v, ok := otlp.AttrString(f.Span.GetAttributes(), "gen_ai.system_instructions"); ok && v != "" {
		return v
	}
	if v, ok := otlp.AttrString(f.Span.GetAttributes(), "gen_ai.prompt.0.content"); ok && v != "" {
		return v
	}
	return ""
}

func short(s string) string {
	if len(s) <= 8 {
		return s
	}
	return s[:8]
}

func pct(part, total int64) float64 {
	if total == 0 {
		return 0
	}
	return 100 * float64(part) / float64(total)
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(n)/float64(div), "KMGTPE"[exp])
}
