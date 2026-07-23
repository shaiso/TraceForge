package main

import (
	"context"

	"traceforge/internal/tf"
)

// p3Processor — вся логика P3 на SDK. Читает поля со спана типизированными
// геттерами (без CAS — числа/метаданные лежат в теле тонкого спана) и раскладывает
// в факт-таблицу tf_spans. На корневом спане трейса добавляет строку user_sessions
// (под сценарий «обращения пользователя за N дней»). Тяжёлый текст не трогаем —
// P3 из тех консюмеров, которым нужны тайминги/токены/версии, а не диалог.
type p3Processor struct{}

func (p *p3Processor) Process(_ context.Context, s tf.Span) ([]tf.Result, error) {
	ts := s.StartTime()
	out := []tf.Result{{Table: "tf_spans", Values: map[string]any{
		"trace_id":          s.TraceID(),
		"span_id":           s.SpanID(),
		"session_id":        s.SessionID(), // пусто => группа '' в CH, не фолбэк на trace_id
		"ts":                ts,
		"service":           s.Service,
		"span_name":         s.Name(),
		"version":           s.Version(),
		"model":             s.String("gen_ai.request.model"),
		"provider":          s.String("gen_ai.provider.name"),
		"latency_ms":        uint32(s.Duration().Milliseconds()),
		"input_tokens":      uint32(s.Int("gen_ai.usage.input_tokens")),
		"output_tokens":     uint32(s.Int("gen_ai.usage.output_tokens")),
		"temperature":       float32(s.Float("gen_ai.request.temperature")),
		"finish_reason":     s.FirstString("gen_ai.response.finish_reasons"),
		"status_code":       s.StatusCode(),
		"intent":            s.String("intent.label"),         // спаны classify_intent
		"scam_type":         s.String("scam.type"),            // спаны extractor
		"greeting_template": s.String("greeting.template_id"), // спаны greeter
	}}}

	// user_sessions — одна строка на трейс (на корне), только если известен абонент.
	if s.IsRoot() && s.UserID() != "" && s.SessionID() != "" {
		out = append(out, tf.Result{Table: "user_sessions", Values: map[string]any{
			"user_id":    s.UserID(),
			"session_id": s.SessionID(),
			"trace_id":   s.TraceID(),
			"date":       ts,
		}})
	}
	return out, nil
}
