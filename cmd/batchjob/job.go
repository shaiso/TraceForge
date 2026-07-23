package main

import (
	"context"
	"time"

	"traceforge/internal/tf"
)

// endReasonJob — BatchProcessor: по трейсам одной сессии собирает диалог целиком и
// классифицирует причину завершения. Дописывает результат в session_end_reason,
// где стрим (P3) считает удержание/версии по той же session_id — витрина «причины
// × версия» собирается СОВМЕСТНО стримом и батчем.
type endReasonJob struct {
	clf Classifier
}

func (j *endReasonJob) Process(ctx context.Context, traces []tf.Trace) ([]tf.Result, error) {
	sess := tf.Session{Traces: traces}
	sessionID, date, lastLLM := sessionMeta(traces)
	if sessionID == "" {
		return nil, nil // спаны без session.id — сессионную аналитику не строим (см. покрытие T7)
	}

	dialog, err := tf.BuildDialog(ctx, sess)
	if err != nil {
		return nil, err
	}

	sig := Signals{Turns: countTurns(dialog), LastUserText: lastUser(dialog)}
	if lastLLM != nil {
		sig.LastStatus = lastLLM.StatusCode()
		sig.LastFinish = lastLLM.FirstString("gen_ai.response.finish_reasons")
	}
	reason := j.clf.Classify(dialog, sig)

	return []tf.Result{{Table: "session_end_reason", Values: map[string]any{
		"session_id": sessionID,
		"end_reason": reason,
		"turns":      uint32(sig.Turns),
		"date":       date,
	}}}, nil
}

// sessionMeta извлекает session_id, дату (min ts) и последний ОСНОВНОЙ диалоговый
// спан (dialog-agent chat). Именно на нём живут сигналы завершения (error-статус,
// finish content_filter/length); side-агент extractor стартует позже в том же ходу
// и маскировал бы их, попади он в «последний LLM-спан» по одному лишь времени.
func sessionMeta(traces []tf.Trace) (sessionID string, date time.Time, lastLLM *tf.Span) {
	var minTS, maxLLM time.Time
	for ti := range traces {
		for si := range traces[ti].Spans {
			sp := traces[ti].Spans[si]
			if sessionID == "" {
				sessionID = sp.SessionID()
			}
			ts := sp.StartTime()
			if minTS.IsZero() || ts.Before(minTS) {
				minTS = ts
			}
			if sp.Service == "dialog-agent" && sp.IsLLM() && (maxLLM.IsZero() || ts.After(maxLLM)) {
				maxLLM = ts
				s := sp
				lastLLM = &s
			}
		}
	}
	return sessionID, minTS, lastLLM
}

func countTurns(dialog []tf.Message) int {
	n := 0
	for _, m := range dialog {
		if m.Role == "user" {
			n++
		}
	}
	return n
}

func lastUser(dialog []tf.Message) string {
	for i := len(dialog) - 1; i >= 0; i-- {
		if dialog[i].Role == "user" {
			return dialog[i].Content
		}
	}
	return ""
}
