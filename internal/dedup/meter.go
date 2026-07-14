package dedup

import tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

// Meter — офлайн-измеритель экономии дедупа для заданного чанкера: гоняет спаны
// через extractor+chunker в памяти (без Postgres/S3) и копит уникальные фрагменты.
// Используется для честного сравнения whole-field vs FastCDC на одном корпусе:
// знаменатель (Content) одинаков, меняется только чанкер.
type Meter struct {
	ex   *Extractor
	ch   Chunker
	seen map[[32]byte]struct{}

	Content      int64 // суммарные байты полей-кандидатов (= сумма байт всех фрагментов)
	UniqueBytes  int64 // байты уникальных фрагментов (то, что реально хранилось бы)
	Fields       int64 // тяжёлых полей извлечено
	Chunks       int64 // фрагментов всего
	UniqueChunks int64 // уникальных фрагментов
}

func NewMeter(ex *Extractor, ch Chunker) *Meter {
	return &Meter{ex: ex, ch: ch, seen: make(map[[32]byte]struct{})}
}

// Observe прогоняет один спан: извлекает поля-кандидаты, режет чанкером и
// засчитывает первый показ каждого хэша как уникальный (экономия — повторы).
func (m *Meter) Observe(span *tracepb.Span) {
	for _, c := range m.ex.Extract(span) {
		m.Fields++
		m.Content += int64(len(c.Value))
		for _, ch := range m.ch.Split(c.Value, nil) {
			m.Chunks++
			var k [32]byte
			copy(k[:], ch.Hash)
			if _, ok := m.seen[k]; ok {
				continue
			}
			m.seen[k] = struct{}{}
			m.UniqueChunks++
			m.UniqueBytes += int64(len(ch.Data))
		}
	}
}

// Saved — сэкономленные байты (дубли, устранённые дедупом, без учёта компрессии).
func (m *Meter) Saved() int64 { return m.Content - m.UniqueBytes }

// SavedPct — доля экономии в процентах.
func (m *Meter) SavedPct() float64 {
	if m.Content == 0 {
		return 0
	}
	return 100 * float64(m.Saved()) / float64(m.Content)
}
