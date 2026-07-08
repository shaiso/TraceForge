// Команда loadgen гонит реалистичный OTLP-поток в коллектор: сессии-«звонки»,
// три ресурса, растущие диалоги и дубли system_instructions.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"traceforge/internal/scenario"
)

func main() {
	endpoint := flag.String("endpoint", "localhost:4318", "OTLP/HTTP endpoint коллектора (host:port)")
	rps := flag.Int("rps", 50, "суммарная скорость новых трейсов в секунду")
	workers := flag.Int("workers", 16, "число параллельных сессий-звонков")
	duration := flag.Duration("duration", 60*time.Second, "длительность генерации")
	schema := flag.String("schema", "B", "схема контента LLM: A (плоская) или B (input/output messages)")
	flag.Parse()

	if *schema != "A" && *schema != "B" {
		log.Fatalf("некорректная -schema=%q (нужно A или B)", *schema)
	}

	// Ctrl-C прерывает генерацию досрочно, провайдеры всё равно флашатся.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	gen, err := scenario.New(ctx, scenario.Config{
		Endpoint: *endpoint,
		RPS:      *rps,
		Workers:  *workers,
		Duration: *duration,
		Schema:   *schema,
	})
	if err != nil {
		log.Fatalf("инициализация генератора: %v", err)
	}

	log.Printf("loadgen старт: endpoint=%s rps=%d workers=%d duration=%s schema=%s",
		*endpoint, *rps, *workers, *duration, *schema)
	if err := gen.Run(ctx); err != nil {
		log.Fatalf("прогон: %v", err)
	}
	log.Printf("loadgen готово")
}
