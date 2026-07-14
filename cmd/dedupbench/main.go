// Команда dedupbench — офлайн-сравнение экономии дедупа whole-field vs FastCDC на
// живом корпусе otlp.raw.spans. Ничего не пишет (ни Postgres, ни S3): считает в
// памяти уникальные фрагменты каждым чанкером и печатает «X -> Y» для обоих. Это
// главный демо-замер спринта. Для наглядного преимущества FastCDC генерь трафик
// с -long (длинные диалоги с общими префиксами): make smoke-dedup.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"traceforge/internal/dedup"
	"traceforge/internal/otlp"
)

func main() {
	brokers := flag.String("brokers", "localhost:19092", "seed-брокеры Kafka/Redpanda через запятую")
	topic := flag.String("topic", "otlp.raw.spans", "входной топик сырых OTLP-пачек")
	duration := flag.Duration("duration", 20*time.Second, "сколько читать топик")
	minSize := flag.Int("min-size", 1024, "минимальный размер строки для дедупа, байт")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ConsumeTopics(*topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		log.Fatalf("dedupbench: kafka: %v", err)
	}
	defer cl.Close()

	exCfg := dedup.DefaultExtractConfig()
	exCfg.MinSize = *minSize
	ex := dedup.NewExtractor(exCfg)

	whole := dedup.NewMeter(ex, dedup.WholeFieldChunker{})
	fast := dedup.NewMeter(ex, dedup.NewFastCDC(dedup.DefaultFastCDCConfig()))

	log.Printf("dedupbench: читаю %s до %s (whole-field vs fastcdc)...", *topic, *duration)
	var spans int64
	for {
		fs := cl.PollFetches(ctx)
		if fs.IsClientClosed() {
			break
		}
		if errs := fs.Errors(); len(errs) > 0 {
			if ctx.Err() != nil {
				break // штатное завершение по таймауту
			}
			for _, e := range errs {
				log.Printf("dedupbench: fetch: %v", e.Err)
			}
			break
		}
		fs.EachRecord(func(rec *kgo.Record) {
			req, err := otlp.Unmarshal(rec.Value)
			if err != nil {
				return
			}
			otlp.Walk(req, func(f otlp.FlatSpan) {
				spans++
				whole.Observe(f.Span)
				fast.Observe(f.Span)
			})
		})
	}

	printReport(spans, whole, fast)
}

func printReport(spans int64, whole, fast *dedup.Meter) {
	fmt.Println()
	fmt.Println("================ dedup bench (без компрессии) ================")
	fmt.Printf("спанов: %d, тяжёлых полей: %d, контент: %s\n",
		spans, whole.Fields, humanBytes(whole.Content))
	fmt.Println("-------------------------------------------------------------")
	row := func(name string, m *dedup.Meter) {
		fmt.Printf("%-11s | %7s -> %-8s | фрагм %6d (уник %6d) | экономия %6s (%.1f%%)\n",
			name, humanBytes(m.Content), humanBytes(m.UniqueBytes),
			m.Chunks, m.UniqueChunks, humanBytes(m.Saved()), m.SavedPct())
	}
	row("whole-field", whole)
	row("fastcdc", fast)
	fmt.Println("-------------------------------------------------------------")
	if delta := fast.Saved() - whole.Saved(); delta > 0 {
		fmt.Printf("FastCDC экономит на %s больше (партиал-дедуп общих префиксов)\n", humanBytes(delta))
	} else {
		fmt.Printf("на этом корпусе выигрыша FastCDC нет (поля меньше CDC-границы; нужен -long)\n")
	}
	fmt.Println("=============================================================")
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
