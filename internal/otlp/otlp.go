// Package otlp — хелперы для обхода OTLP-пейлоада трейсов
// (ResourceSpans -> ScopeSpans -> Span) и денормализации атрибутов уровня
// ресурса (например, service.name) вниз, на отдельные спаны.
//
// Обязательный способ доступа — двойной вложенный цикл по
// ResourceSpans/ScopeSpans; все консьюмеры traceforge (сейчас rawread, позже
// P1 dedup) используют этот пакет, чтобы обход жил ровно в одном месте.
package otlp

import (
	"encoding/hex"

	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// FlatSpan — один спан вместе с контекстом ресурса/скоупа, в котором он лежал.
// ServiceName для удобства денормализован со слоя ресурса.
type FlatSpan struct {
	ServiceName string
	Resource    *resourcepb.Resource
	Scope       *commonpb.InstrumentationScope
	Span        *tracepb.Span
}

// Unmarshal декодирует OTLP/proto ExportTraceServiceRequest — ровно тот пейлоад,
// который kafka exporter коллектора пишет с encoding=otlp_proto.
func Unmarshal(b []byte) (*collectortracepb.ExportTraceServiceRequest, error) {
	req := &collectortracepb.ExportTraceServiceRequest{}
	if err := proto.Unmarshal(b, req); err != nil {
		return nil, err
	}
	return req, nil
}

// Walk обходит каждый спан в запросе двойным вложенным циклом и вызывает fn
// с уже денормализованным service.name.
func Walk(req *collectortracepb.ExportTraceServiceRequest, fn func(FlatSpan)) {
	for _, rs := range req.GetResourceSpans() {
		res := rs.GetResource()
		svc := ServiceName(res)
		for _, ss := range rs.GetScopeSpans() {
			scope := ss.GetScope()
			for _, sp := range ss.GetSpans() {
				fn(FlatSpan{ServiceName: svc, Resource: res, Scope: scope, Span: sp})
			}
		}
	}
}

// ServiceName достаёт service.name из атрибутов ресурса ("" если его нет).
func ServiceName(res *resourcepb.Resource) string {
	if res == nil {
		return ""
	}
	v, _ := AttrString(res.GetAttributes(), "service.name")
	return v
}

// AttrString возвращает строковое значение атрибута с заданным ключом.
func AttrString(attrs []*commonpb.KeyValue, key string) (string, bool) {
	for _, kv := range attrs {
		if kv.GetKey() == key {
			return kv.GetValue().GetStringValue(), true
		}
	}
	return "", false
}

// TraceIDHex рендерит trace_id спана как 32 hex-символа в нижнем регистре —
// канонический ключ traceforge.
func TraceIDHex(sp *tracepb.Span) string {
	return hex.EncodeToString(sp.GetTraceId())
}

// SpanIDHex рендерит span_id спана как 16 hex-символов в нижнем регистре.
func SpanIDHex(sp *tracepb.Span) string {
	return hex.EncodeToString(sp.GetSpanId())
}

// StringAttrsSize суммирует длину в байтах всех строковых атрибутов спана —
// прокси-оценка веса текстового пейлоада, на который нацелится P1 dedup.
func StringAttrsSize(sp *tracepb.Span) int {
	n := 0
	for _, kv := range sp.GetAttributes() {
		n += len(kv.GetValue().GetStringValue())
	}
	return n
}
