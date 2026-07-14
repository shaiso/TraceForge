package dedup

import (
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func spanWithField(key, val string) *tracepb.Span {
	return &tracepb.Span{Attributes: []*commonpb.KeyValue{strAttr(key, val)}}
}

// Дубль поля целиком: whole-field засчитывает второй показ как экономию.
func TestMeter_WholeFieldDedup(t *testing.T) {
	m := NewMeter(NewExtractor(DefaultExtractConfig()), WholeFieldChunker{})
	val := string(pseudoRandom(3000, 1))
	m.Observe(spanWithField("gen_ai.system_instructions", val))
	m.Observe(spanWithField("gen_ai.system_instructions", val)) // тот же контент

	if m.Fields != 2 || m.UniqueChunks != 1 {
		t.Fatalf("fields=%d uniqueChunks=%d, ожидали 2/1", m.Fields, m.UniqueChunks)
	}
	if m.Saved() != int64(len(val)) {
		t.Errorf("экономия %d, ожидали %d (один дубль поля)", m.Saved(), len(val))
	}
}

// На пересекающихся крупных полях FastCDC экономит строго больше whole-field:
// сдвинутая копия делит с оригиналом почти все фрагменты, а whole-field — ни одного.
func TestMeter_FastCDCBeatsWholeOnOverlap(t *testing.T) {
	ex := NewExtractor(DefaultExtractConfig())
	whole := NewMeter(ex, WholeFieldChunker{})
	fast := NewMeter(ex, NewFastCDC(DefaultFastCDCConfig()))

	base := pseudoRandom(40*1024, 5)
	shifted := append([]byte("ПРЕФИКС ДИАЛОГА "), base...)

	for _, m := range []*Meter{whole, fast} {
		m.Observe(spanWithField("gen_ai.input.messages", string(base)))
		m.Observe(spanWithField("gen_ai.input.messages", string(shifted)))
	}

	if whole.Saved() != 0 {
		t.Errorf("whole-field на сдвинутой копии сэкономил %d, ожидали 0", whole.Saved())
	}
	if fast.Saved() <= whole.Saved() {
		t.Errorf("FastCDC экономия %d не больше whole-field %d", fast.Saved(), whole.Saved())
	}
	t.Logf("overlap: whole-field экономия=%d, fastcdc экономия=%d", whole.Saved(), fast.Saved())
}
