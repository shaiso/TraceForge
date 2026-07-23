package restore_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"traceforge/internal/archive"
	"traceforge/internal/restore"
)

// --- фейки внешних слоёв ---

type memStore struct{ objs map[string][]byte }

func (m *memStore) PutObject(_ context.Context, key string, data []byte) error {
	m.objs[key] = append(m.objs[key], data...)
	return nil
}
func (m *memStore) GetRange(_ context.Context, key string, off int64, length int) ([]byte, error) {
	b, ok := m.objs[key]
	if !ok {
		return nil, fmt.Errorf("нет %s", key)
	}
	return append([]byte(nil), b[off:off+int64(length)]...), nil
}

type fakeIndex struct {
	byTrace map[string][]archive.Row
	all     []archive.Row
}

func (f *fakeIndex) LookupTrace(_ context.Context, id string) ([]archive.Row, error) {
	return f.byTrace[id], nil
}
func (f *fakeIndex) LookupRange(_ context.Context, _, _ time.Time, _ string) ([]archive.Row, error) {
	return f.all, nil
}

// countingCAS — фейк CAS, считает обращения по хэшу (для проверки кэша).
type countingCAS struct {
	data  map[string][]byte
	calls map[string]int
}

func (c *countingCAS) Fetch(_ context.Context, hash []byte) ([]byte, error) {
	k := hex.EncodeToString(hash)
	c.calls[k]++
	v, ok := c.data[k]
	if !ok {
		return nil, fmt.Errorf("нет фрагмента %s", k)
	}
	return append([]byte(nil), v...), nil
}

// --- помощники сборки тонкого спана-фрейма ---

func refFor(content string) (ref, hexHash string) {
	h := sha256.Sum256([]byte(content))
	hexHash = hex.EncodeToString(h[:])
	return "ref:sha256:" + hexHash, hexHash
}

func thinLine(t *testing.T, service, ref string) []byte {
	span := &tracepb.Span{
		Name: "op",
		Attributes: []*commonpb.KeyValue{
			{Key: "session.id", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "s1"}}},
			{Key: "gen_ai.system_instructions", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: ref}}},
		},
	}
	spanJSON, err := protojson.Marshal(span)
	if err != nil {
		t.Fatal(err)
	}
	env, err := json.Marshal(struct {
		Service string          `json:"service"`
		Scope   string          `json:"scope"`
		Span    json.RawMessage `json:"span"`
	}{service, "scope", spanJSON})
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func putFrame(store *memStore, enc *zstd.Encoder, key, traceID string, line []byte) archive.Row {
	frame := enc.EncodeAll(append(line, '\n'), nil)
	off := len(store.objs[key])
	store.objs[key] = append(store.objs[key], frame...)
	return archive.Row{TraceID: traceID, BundleKey: key, Offset: uint64(off), Len: uint32(len(frame))}
}

func sysInstr(rs restore.RestoredSpan) string {
	for _, kv := range rs.Span.GetAttributes() {
		if kv.GetKey() == "gen_ai.system_instructions" {
			return kv.GetValue().GetStringValue()
		}
	}
	return ""
}

// TestDecodeThinFrame_ResourceScope (T0): нижний декодер восстанавливает полные
// resource/scope, а не только денормализованные service/scope-строки.
func TestDecodeThinFrame_ResourceScope(t *testing.T) {
	enc, _ := zstd.NewWriter(nil)
	defer enc.Close()
	dec, _ := zstd.NewReader(nil)
	defer dec.Close()

	res := &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
		{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "dialog-agent"}}},
		{Key: "service.version", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "1.0.0"}}},
	}}
	scope := &commonpb.InstrumentationScope{Name: "traceforge/loadgen", Version: "2.1.0"}
	span := &tracepb.Span{Name: "op"}

	resJSON, _ := protojson.Marshal(res)
	scopeJSON, _ := protojson.Marshal(scope)
	spanJSON, _ := protojson.Marshal(span)
	line, err := json.Marshal(struct {
		Service   string          `json:"service"`
		Scope     string          `json:"scope"`
		Resource  json.RawMessage `json:"resource,omitempty"`
		ScopeFull json.RawMessage `json:"scope_full,omitempty"`
		Span      json.RawMessage `json:"span"`
	}{"dialog-agent", scope.Name, resJSON, scopeJSON, spanJSON})
	if err != nil {
		t.Fatal(err)
	}

	frame := enc.EncodeAll(append(line, '\n'), nil)
	spans, err := restore.DecodeThinFrame(dec, frame)
	if err != nil {
		t.Fatalf("DecodeThinFrame: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("жду 1 спан, got %d", len(spans))
	}
	rs := spans[0]
	if rs.Resource == nil || !proto.Equal(rs.Resource, res) {
		t.Errorf("resource не восстановлен: %v", rs.Resource)
	}
	if rs.ScopeFull == nil || !proto.Equal(rs.ScopeFull, scope) {
		t.Errorf("scope не восстановлен: %v", rs.ScopeFull)
	}
}

// TestRestoreSingle: ref во фрейме бандла резолвится в исходный контент из CAS.
func TestRestoreSingle(t *testing.T) {
	ctx := context.Background()
	enc, _ := zstd.NewWriter(nil)
	defer enc.Close()
	store := &memStore{objs: map[string][]byte{}}

	content := "ВОССТАНОВЛЕННАЯ СИСТЕМКА " + fmt.Sprintf("%01000d", 7) // >1КБ
	ref, hexHash := refFor(content)
	cas := &countingCAS{data: map[string][]byte{hexHash: []byte(content)}, calls: map[string]int{}}

	row := putFrame(store, enc, "bundleA", "tX", thinLine(t, "svc", ref))
	idx := &fakeIndex{byTrace: map[string][]archive.Row{"tX": {row}}}

	r, err := restore.New(idx, store, cas)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	tr, err := r.Restore(ctx, "tX")
	if err != nil {
		t.Fatal(err)
	}
	if len(tr.Spans) != 1 {
		t.Fatalf("жду 1 спан, got %d", len(tr.Spans))
	}
	if got := sysInstr(tr.Spans[0]); got != content {
		t.Errorf("ref не резолвнулся в контент: got %.40q...", got)
	}

	if _, err := r.Restore(ctx, "нет-такого"); err != restore.ErrNotFound {
		t.Errorf("жду ErrNotFound, got %v", err)
	}
}

// TestRestoreRangeCache: два трейса окна ссылаются на ОДИН хэш -> CAS.Fetch один раз.
func TestRestoreRangeCache(t *testing.T) {
	ctx := context.Background()
	enc, _ := zstd.NewWriter(nil)
	defer enc.Close()
	store := &memStore{objs: map[string][]byte{}}

	content := "ОБЩАЯ СИСТЕМКА НА ВСЁ ОКНО " + fmt.Sprintf("%01000d", 3)
	ref, hexHash := refFor(content)
	cas := &countingCAS{data: map[string][]byte{hexHash: []byte(content)}, calls: map[string]int{}}

	rowX := putFrame(store, enc, "bundleA", "tX", thinLine(t, "svc", ref))
	rowY := putFrame(store, enc, "bundleA", "tY", thinLine(t, "svc", ref))
	idx := &fakeIndex{all: []archive.Row{rowX, rowY}}

	r, err := restore.New(idx, store, cas)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	traces, err := r.RestoreRange(ctx, time.Now().Add(-time.Hour), time.Now(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(traces) != 2 {
		t.Fatalf("жду 2 трейса, got %d", len(traces))
	}
	if cas.calls[hexHash] != 1 {
		t.Errorf("кэш окна: общий хэш должен тянуться 1 раз, а тянулся %d", cas.calls[hexHash])
	}
	for _, tr := range traces {
		if got := sysInstr(tr.Spans[0]); got != content {
			t.Errorf("трейс %s: ref не резолвнулся", tr.TraceID)
		}
	}
}
