package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"traceforge/internal/restore"
)

type server struct {
	restorer *restore.Restorer
	lf       *langfuseClient
	mtr      *metrics
}

// spanOut/traceOut — форма JSON-ответа: денормализованные service/scope + спан
// (protojson, тяжёлый контент уже подставлен обратно из CAS).
type spanOut struct {
	Service string          `json:"service"`
	Scope   string          `json:"scope,omitempty"`
	Span    json.RawMessage `json:"span"`
}

type traceOut struct {
	TraceID string    `json:"trace_id"`
	Spans   []spanOut `json:"spans"`
}

type traceResp struct {
	traceOut
	Push *pushResult `json:"push,omitempty"`
}

type rangeResp struct {
	Count  int         `json:"count"`
	Traces []traceOut  `json:"traces"`
	Push   *pushResult `json:"push,omitempty"`
}

// handleTrace: GET /traces/{trace_id} — одиночный restore (саппорт-кейс).
func (s *server) handleTrace(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	traceID := r.PathValue("trace_id")
	push := r.URL.Query().Get("push") == "langfuse"

	tr, err := s.restorer.Restore(r.Context(), traceID)
	if errors.Is(err, restore.ErrNotFound) {
		s.mtr.observe("single", "not_found", start, 0)
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "трейс не найден в архиве", "trace_id": traceID})
		return
	}
	if err != nil {
		s.mtr.observe("single", "error", start, 0)
		s.mtr.refErr()
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error(), "trace_id": traceID})
		return
	}
	out, err := toTraceOut(tr)
	if err != nil {
		s.mtr.observe("single", "error", start, 0)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	resp := traceResp{traceOut: out}
	if push {
		resp.Push = s.pushLangfuse(r.Context(), tr.Spans)
	}
	s.mtr.observe("single", "ok", start, len(tr.Spans))
	writeJSON(w, http.StatusOK, resp)
}

// handleRange: GET /traces?from=&to=[&session_id=] — диапазонный restore окна.
func (s *server) handleRange(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	q := r.URL.Query()
	from, err1 := time.Parse(time.RFC3339, q.Get("from"))
	to, err2 := time.Parse(time.RFC3339, q.Get("to"))
	if err1 != nil || err2 != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "нужны from/to в формате RFC3339 (напр. 2026-07-15T10:00:00Z)"})
		return
	}
	if to.Before(from) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "to раньше from"})
		return
	}
	push := q.Get("push") == "langfuse"

	traces, err := s.restorer.RestoreRange(r.Context(), from, to, q.Get("session_id"))
	if err != nil {
		s.mtr.observe("range", "error", start, 0)
		s.mtr.refErr()
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}

	resp := rangeResp{Count: len(traces), Traces: make([]traceOut, 0, len(traces))}
	var spanCount int
	for _, tr := range traces {
		out, err := toTraceOut(tr)
		if err != nil {
			s.mtr.observe("range", "error", start, 0)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		resp.Traces = append(resp.Traces, out)
		spanCount += len(tr.Spans)
	}
	if push {
		var all []restore.RestoredSpan
		for _, tr := range traces {
			all = append(all, tr.Spans...)
		}
		resp.Push = s.pushLangfuse(r.Context(), all)
	}
	s.mtr.observe("range", "ok", start, spanCount)
	writeJSON(w, http.StatusOK, resp)
}

func toTraceOut(tr *restore.Trace) (traceOut, error) {
	out := traceOut{TraceID: tr.TraceID, Spans: make([]spanOut, 0, len(tr.Spans))}
	for _, rs := range tr.Spans {
		b, err := protojson.Marshal(rs.Span)
		if err != nil {
			return traceOut{}, err
		}
		out.Spans = append(out.Spans, spanOut{Service: rs.Service, Scope: rs.Scope, Span: b})
	}
	return out, nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
