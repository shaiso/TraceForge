package dedup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"traceforge/internal/otlp"
)

// pseudoRandom — детерминированный поток «случайных» байт (sha256-цепочка). Нужна
// энтропия, чтобы gear-hash реально находил content-defined границы.
func pseudoRandom(n int, seed uint64) []byte {
	out := make([]byte, 0, n)
	var ctr [8]byte
	binary.BigEndian.PutUint64(ctr[:], seed)
	block := sha256.Sum256(ctr[:])
	for len(out) < n {
		out = append(out, block[:]...)
		block = sha256.Sum256(block[:])
	}
	return out[:n]
}

func hashSet(chunks []Chunk) map[[32]byte]int {
	m := make(map[[32]byte]int, len(chunks))
	for _, c := range chunks {
		var k [32]byte
		copy(k[:], c.Hash)
		m[k]++
	}
	return m
}

// Конкатенация фрагментов == оригинал, и hash каждого = sha256(Data).
func TestFastCDC_Reassemble(t *testing.T) {
	c := NewFastCDC(DefaultFastCDCConfig())
	data := pseudoRandom(50*1024, 1)
	chunks := c.Split(string(data), nil)
	if len(chunks) < 2 {
		t.Fatalf("ожидали несколько фрагментов, получили %d", len(chunks))
	}
	var reasm []byte
	for _, ch := range chunks {
		sum := sha256.Sum256(ch.Data)
		if !bytes.Equal(sum[:], ch.Hash) {
			t.Fatalf("hash фрагмента != sha256(Data)")
		}
		reasm = append(reasm, ch.Data...)
	}
	if !bytes.Equal(reasm, data) {
		t.Fatalf("реассембл != оригинал (got %d, want %d байт)", len(reasm), len(data))
	}
}

func TestFastCDC_Deterministic(t *testing.T) {
	c := NewFastCDC(DefaultFastCDCConfig())
	data := string(pseudoRandom(40*1024, 2))
	a, b := c.Split(data, nil), c.Split(data, nil)
	if len(a) != len(b) {
		t.Fatalf("разное число фрагментов: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if !bytes.Equal(a[i].Hash, b[i].Hash) {
			t.Fatalf("фрагмент[%d] недетерминирован", i)
		}
	}
}

// Границы размеров: все фрагменты <= MaxSize; все, кроме последнего, >= MinSize.
func TestFastCDC_SizeBounds(t *testing.T) {
	cfg := DefaultFastCDCConfig()
	c := NewFastCDC(cfg)
	chunks := c.Split(string(pseudoRandom(200*1024, 3)), nil)
	for i, ch := range chunks {
		if len(ch.Data) > cfg.MaxSize {
			t.Errorf("фрагмент[%d] = %d > MaxSize %d", i, len(ch.Data), cfg.MaxSize)
		}
		if i < len(chunks)-1 && len(ch.Data) < cfg.MinSize {
			t.Errorf("фрагмент[%d] = %d < MinSize %d", i, len(ch.Data), cfg.MinSize)
		}
	}
	avg := 200 * 1024 / len(chunks)
	if avg < cfg.MinSize || avg > cfg.MaxSize {
		t.Errorf("средний размер %d вне [%d, %d]", avg, cfg.MinSize, cfg.MaxSize)
	}
	t.Logf("FastCDC: 200КБ -> %d фрагментов, средний %d байт", len(chunks), avg)
}

// Главное свойство CDC: вставка в НАЧАЛЕ поля не сдвигает все границы — хвостовые
// фрагменты те же и дедуплицируются. whole-field тут дал бы 0 общих.
func TestFastCDC_ShiftResistance(t *testing.T) {
	c := NewFastCDC(DefaultFastCDCConfig())
	base := pseudoRandom(64*1024, 7)
	orig := c.Split(string(base), nil)
	shifted := c.Split(string(append([]byte("НОВЫЙ ПРЕФИКС ДИАЛОГА. "), base...)), nil)

	set := hashSet(orig)
	shared := 0
	for _, ch := range shifted {
		var k [32]byte
		copy(k[:], ch.Hash)
		if set[k] > 0 {
			shared++
		}
	}
	ratio := float64(shared) / float64(len(orig))
	if ratio < 0.5 {
		t.Fatalf("после сдвига общих фрагментов %d/%d (%.0f%%), ожидали > 50%%",
			shared, len(orig), ratio*100)
	}
	t.Logf("устойчивость к сдвигу: %d/%d общих фрагментов (%.0f%%)", shared, len(orig), ratio*100)
}

// Мультичанковая обратимость через конвейер: поле в несколько фрагментов ->
// ref:sha256:h1,h2,... -> RestoreSpan склеивает обратно бит-в-бит. Требует Postgres.
func TestFastCDC_MultiChunkReversibility(t *testing.T) {
	ix, closeFn := openTestIndex(t)
	defer closeFn()
	packer, err := NewSegmentPacker(newMemBlobStore(), DefaultPackerConfig())
	if err != nil {
		t.Fatalf("packer: %v", err)
	}
	defer packer.Close(context.Background())

	chunker := NewFastCDC(DefaultFastCDCConfig())
	proc := NewProcessor(NewExtractor(DefaultExtractConfig()), chunker, ix, packer)
	ctx := context.Background()
	src := IndexPacker{Index: ix, Packer: packer}

	big := string(pseudoRandom(40*1024, 11)) // ~40 КБ -> заведомо много фрагментов
	// Чистим индекс от фрагментов этого поля.
	defer func() {
		for _, ch := range chunker.Split(big, nil) {
			ix.pool.Exec(ctx, "DELETE FROM dedup_index WHERE hash = $1", ch.Hash)
		}
	}()

	span := &tracepb.Span{
		TraceId:    []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanId:     []byte{1, 2, 3, 4, 5, 6, 7, 8},
		Name:       "big",
		Attributes: []*commonpb.KeyValue{strAttr("gen_ai.input.messages", big)},
	}
	orig := proto.Clone(span).(*tracepb.Span)

	thin, err := proc.Process(ctx, otlp.FlatSpan{ServiceName: "svc", Span: span})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if proc.Stats.Chunks < 2 {
		t.Fatalf("ожидали мультичанк, фрагментов=%d", proc.Stats.Chunks)
	}
	// В тонком спане поле — мультичанковый ref.
	if h, _ := ParseRef(refOf(t, thin, "gen_ai.input.messages")); len(h) < 2 {
		t.Fatalf("ожидали ref из >= 2 хэшей")
	}

	if err := proc.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := RestoreSpan(ctx, span, src); err != nil {
		t.Fatalf("RestoreSpan: %v", err)
	}
	if !proto.Equal(span, orig) {
		t.Fatalf("мультичанковый restore != оригинал")
	}
}

func refOf(t *testing.T, thin []byte, key string) string {
	t.Helper()
	return extractAttr(t, thin, key)
}
