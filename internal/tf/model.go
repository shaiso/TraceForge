// Package tf — SDK для анализа трейсов, не зная, как устроено хранилище.
//
// Способ писать процессоры над трейсами анти-спам платформы, не касаясь Kafka, S3,
// CAS, ссылок ref:sha256, бандлов и трёх схем gen_ai.*. Процессор получает tf.Span /
// tf.Trace, у которых тяжёлый текст доступен как обычное поле (span.Prompt()) —
// разворачивается прозрачно и по требованию из CAS. Инфраструктуру (загрузку окна,
// консюминг, offset, запись) берут на себя источники (source.go) и раннер (processor.go).
//
// Один и тот же тип Span на оба режима: LoadRange (архив, батч) и стрим spans.thin
// отдают одинаковые Span/Trace — одна логика работает и батчем, и потоком.
package tf

import (
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"traceforge/internal/dedup"
)

// Span — один спан с ленивым доступом к тяжёлому контенту. Числовые/метаданные
// (тайминги, токены, версии) читаются из span-атрибутов без обращения к CAS;
// текст (Prompt/Completion) разворачивается из CAS только при первом обращении.
type Span struct {
	Service  string               // денормализованный service.name
	Scope    string               // имя scope (инструментации)
	Resource *resourcepb.Resource // полный resource (nil для тонких спанов старого формата)

	span *tracepb.Span     // OTLP-спан; строковые контент-поля — ссылки ref:sha256
	cas  dedup.ChunkSource // резолвер CAS (обычно с кэшем на окно); nil если контент не нужен
}

// Trace — виртуальная группировка спанов по trace_id (физически «трейса» нет).
type Trace struct {
	TraceID string
	Spans   []Span
}

// Session — все трейсы одного «звонка» (session.id), в хронологии.
type Session struct {
	SessionID string
	Traces    []Trace
}

// Message — одна реплика диалога (нормализована из любой схемы контента).
type Message struct {
	Role    string
	Content string
}

// Result — одна строка, которую процессор отдаёт на запись (в ClickHouse). Table —
// целевая витрина/факт-таблица (один процессор может писать в несколько — раннер
// группирует по Table). Values ключуются по имени столбца; WriteResults
// раскладывает по колонкам (порядок колонок — сортировкой ключей, INSERT их же перечисляет).
type Result struct {
	Table  string
	Values map[string]any
}

// Proto возвращает нижележащий OTLP-спан (для процессоров, которым нужен сырой
// доступ). Контент-поля в нём — ссылки; текст брать через Prompt/Completion.
func (s Span) Proto() *tracepb.Span { return s.span }
