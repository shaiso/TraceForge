// Package dedup — ядро P1: разбиение тяжёлого текстового контента спанов на
// дедуплицируемые фрагменты и всё, что вокруг (index, сегменты, тонкие спаны).
//
// Этот файл — сменный слой чанкинга. Остальной конвейер работает только через
// интерфейс Chunker и не знает, whole-field это, FastCDC или (позже)
// template-aware. Так баги чанкинга изолированы от багов конвейера, а режим
// переключается конфигом `chunker:`.
package dedup

import (
	"crypto/sha256"
	"fmt"
)

// Chunk — один фрагмент контента поля: его sha256 и сами байты. Hash адресует
// фрагмент в CAS (dedup-index + сегменты S3), Data — то, что кладём в сегмент
// при первом появлении хэша.
type Chunk struct {
	Hash []byte // sha256(Data), 32 байта
	Data []byte
}

// Chunker разбивает значение одного поля на фрагменты. attrs — полный набор
// атрибутов спана: whole-field и FastCDC его игнорируют, а будущий
// template-aware достанет оттуда template_id/vars.
type Chunker interface {
	Split(value string, attrs map[string]any) []Chunk
}

// Kind — идентификатор режима чанкинга (значение конфига `chunker:`).
type Kind string

const (
	KindWholeField    Kind = "whole-field"
	KindFastCDC       Kind = "fastcdc"
	KindTemplateAware Kind = "template-aware"
)

// New возвращает чанкер по имени режима. Конвейер зовёт New один раз на старте
// и дальше держит только интерфейс.
func New(kind Kind) (Chunker, error) {
	switch kind {
	case KindWholeField:
		return WholeFieldChunker{}, nil
	case KindFastCDC:
		return NewFastCDC(DefaultFastCDCConfig()), nil
	case KindTemplateAware:
		// Фаза 2: пока делегирует в FastCDC (см. template.go).
		return NewTemplateAware(NewFastCDC(DefaultFastCDCConfig())), nil
	default:
		return nil, fmt.Errorf("dedup: неизвестный чанкер %q", kind)
	}
}

// WholeFieldChunker — простейший режим: поле целиком = один фрагмент,
// hash = sha256(поля). Ловит только полные дубли (повторяющиеся системки), зато
// предсказуем — используется как отладочный тумблер и fallback. Логика перенесена
// из превью-замера rawread (Sprint 01) и обёрнута в интерфейс Chunker.
type WholeFieldChunker struct{}

// Split возвращает ровно один Chunk на всё поле. attrs не используется.
func (WholeFieldChunker) Split(value string, _ map[string]any) []Chunk {
	sum := sha256.Sum256([]byte(value))
	// Копируем массив в срез: hash живёт дольше локальной переменной,
	// а Data — независимая копия байтов поля.
	return []Chunk{{
		Hash: append([]byte(nil), sum[:]...),
		Data: []byte(value),
	}}

}
