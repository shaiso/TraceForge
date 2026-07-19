package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"

	"traceforge/internal/restore"
)

// langfuseClient заливает восстановленные трейсы в Langfuse по OTLP/HTTP. Живой тест
// отложен (нужен profile full + ingest-ключи), но код-путь готов: пересобираем
// ResourceSpans/ScopeSpans из плоских спанов и POST-им ExportTraceServiceRequest.
type langfuseClient struct {
	endpoint string // база OTLP, напр. http://localhost:3000/api/public/otel
	public   string
	secret   string
	http     *http.Client
}

type pushResult struct {
	OK     bool   `json:"ok"`
	Pushed int    `json:"pushed"`
	Error  string `json:"error,omitempty"`
}

// pushLangfuse — обёртка с метрикой для хендлеров.
func (s *server) pushLangfuse(ctx context.Context, spans []restore.RestoredSpan) *pushResult {
	n, err := s.lf.Push(ctx, spans)
	res := &pushResult{OK: err == nil, Pushed: n}
	if err != nil {
		res.Error = err.Error()
	}
	s.mtr.pushDone(err == nil)
	return res
}

// Push пересобирает OTLP и заливает пачкой в Langfuse.
func (c *langfuseClient) Push(ctx context.Context, spans []restore.RestoredSpan) (int, error) {
	if len(spans) == 0 {
		return 0, nil
	}
	body, err := proto.Marshal(buildExportRequest(spans))
	if err != nil {
		return 0, fmt.Errorf("marshal otlp: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/v1/traces", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	if c.public != "" {
		req.SetBasicAuth(c.public, c.secret)
	}
	cl := c.http
	if cl == nil {
		cl = &http.Client{Timeout: 15 * time.Second}
	}
	resp, err := cl.Do(req)
	if err != nil {
		return 0, fmt.Errorf("push langfuse: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return 0, fmt.Errorf("langfuse ответил %d: %s", resp.StatusCode, string(b))
	}
	return len(spans), nil
}

// buildExportRequest группирует плоские спаны обратно в ResourceSpans (по service)
// -> ScopeSpans (по scope) -> Spans — упаковка OTLP, обратная otlp.Walk.
func buildExportRequest(spans []restore.RestoredSpan) *collectortracepb.ExportTraceServiceRequest {
	byService := map[string]map[string][]*tracepb.Span{}
	var serviceOrder []string
	scopeOrder := map[string][]string{}
	for _, rs := range spans {
		if _, ok := byService[rs.Service]; !ok {
			byService[rs.Service] = map[string][]*tracepb.Span{}
			serviceOrder = append(serviceOrder, rs.Service)
		}
		if _, ok := byService[rs.Service][rs.Scope]; !ok {
			scopeOrder[rs.Service] = append(scopeOrder[rs.Service], rs.Scope)
		}
		byService[rs.Service][rs.Scope] = append(byService[rs.Service][rs.Scope], rs.Span)
	}

	var rss []*tracepb.ResourceSpans
	for _, svc := range serviceOrder {
		res := &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strAttr("service.name", svc)}}
		var sss []*tracepb.ScopeSpans
		for _, scope := range scopeOrder[svc] {
			sss = append(sss, &tracepb.ScopeSpans{
				Scope: &commonpb.InstrumentationScope{Name: scope},
				Spans: byService[svc][scope],
			})
		}
		rss = append(rss, &tracepb.ResourceSpans{Resource: res, ScopeSpans: sss})
	}
	return &collectortracepb.ExportTraceServiceRequest{ResourceSpans: rss}
}

func strAttr(key, val string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: key, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: val}}}
}
