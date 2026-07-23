package dedup

import (
	"encoding/json"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"traceforge/internal/otlp"
)

// T0: тонкий спан — единственный носитель истории после retention Kafka, поэтому
// marshalThin обязан нести ВСЁ, кроме вынесенного в CAS. Тест сверяет, что raw ->
// thin -> decode сохраняет не только тяжёлые поля (это TestReversibility), но и
// resource-атрибуты сверх service.name, scope version/attributes, а также span
// events / links / status / атрибуты (они внутри protojson спана, покрыты
// proto.Equal). Инфра не нужна: marshalThin чист (только protojson).
func TestThinEnvelope_FullFidelity(t *testing.T) {
	res := &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
		strAttr("service.name", "dialog-agent"),
		strAttr("service.version", "1.0.0"),       // раньше молча терялось
		strAttr("deployment.environment", "prod"), // и это тоже
	}}
	scope := &commonpb.InstrumentationScope{
		Name:       "traceforge/loadgen",
		Version:    "2.1.0", // раньше молча терялось
		Attributes: []*commonpb.KeyValue{strAttr("scope.kind", "llm")},
	}
	span := &tracepb.Span{
		TraceId:           []byte("0123456789abcdef"),
		SpanId:            []byte("01234567"),
		Name:              "chat gpt-4o-mini",
		StartTimeUnixNano: 1_700_000_000_000_000_000,
		EndTimeUnixNano:   1_700_000_000_050_000_000,
		Status:            &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR, Message: "upstream timeout"},
		Attributes: []*commonpb.KeyValue{
			strAttr("session.id", "sess-42"),
			strAttr("gen_ai.request.model", "gpt-4o-mini"),
		},
		Events: []*tracepb.Span_Event{{
			Name:         "gen_ai.content.completion",
			TimeUnixNano: 1_700_000_000_040_000_000,
			Attributes:   []*commonpb.KeyValue{strAttr("finish_reason", "stop")},
		}},
		Links: []*tracepb.Span_Link{{
			TraceId:    []byte("fedcba9876543210"),
			SpanId:     []byte("76543210"),
			Attributes: []*commonpb.KeyValue{strAttr("link.rel", "parent-call")},
		}},
	}

	origSpan := proto.Clone(span).(*tracepb.Span)
	f := otlp.FlatSpan{ServiceName: "dialog-agent", Resource: res, Scope: scope, Span: span}

	thin, err := marshalThin(f)
	if err != nil {
		t.Fatalf("marshalThin: %v", err)
	}
	var env thinEnvelope
	if err := json.Unmarshal(thin, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}

	// Денормализованные строки на месте (дешёвая маршрутизация + back-compat).
	if env.Service != "dialog-agent" || env.Scope != "traceforge/loadgen" {
		t.Fatalf("денорм строки: service=%q scope=%q", env.Service, env.Scope)
	}
	// Полные resource/scope записаны, а не потеряны.
	if len(env.Resource) == 0 || len(env.ScopeFull) == 0 {
		t.Fatalf("resource/scope не записаны в thin: resource=%d scope_full=%d", len(env.Resource), len(env.ScopeFull))
	}

	// Восстановление бит-в-бит по каждому уровню.
	gotSpan := &tracepb.Span{}
	if err := protojson.Unmarshal(env.Span, gotSpan); err != nil {
		t.Fatalf("protojson span: %v", err)
	}
	if !proto.Equal(gotSpan, origSpan) {
		t.Errorf("span != оригинал (events/links/status/attrs потеряны?)")
	}
	gotRes := &resourcepb.Resource{}
	if err := protojson.Unmarshal(env.Resource, gotRes); err != nil {
		t.Fatalf("protojson resource: %v", err)
	}
	if !proto.Equal(gotRes, res) {
		t.Errorf("resource != оригинал: service.version/deployment.environment потеряны")
	}
	gotScope := &commonpb.InstrumentationScope{}
	if err := protojson.Unmarshal(env.ScopeFull, gotScope); err != nil {
		t.Fatalf("protojson scope: %v", err)
	}
	if !proto.Equal(gotScope, scope) {
		t.Errorf("scope != оригинал: version/attributes потеряны")
	}
}
