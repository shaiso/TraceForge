# TraceForge

Пллатформа стриминговой и батчевой обработки трейсов LLM-агентов, работающая **side-tap рядом с
Langfuse**: OTel Collector дублирует OTLP-поток в Kafka, а конвейер обрабатывает
его — дедуп → архив → агрегации. Langfuse остаётся и служит UI.
TraceForge делает то, чего Langfuse на объёмах не даёт: дедуп промптов, ретеншен, кастомные агрегации.

```
LLM-агенты (OTel SDK)
        │
        ▼
OTel Collector ──fan-out──┬──► Langfuse            side-tap: поток не трогаем, служит UI
                          └──► Kafka: otlp.raw.spans
                                       │
                                       ▼
                                P1 dedup           → CAS: Postgres dedup_index + MinIO сегменты
                                       │
                                       ▼
                               Kafka: spans.thin   тяжёлый контент вынесен → ref:sha256
                                       │
                    ┌──────────────────┴──────────────────┐   независимые consumer groups
                    ▼                                      ▼
              P2 archiver                            P3 chsink           
              → бандлы S3 + archive_index (CH)       → tf_spans + user_sessions (CH)
                    │
                    ▼
              архив (бандлы + archive_index + CAS)
                    │
                    ├──► batchjob     → session_end_reason (CH)         
                    └──► restore-api  → trace_id / окно → полный трейс (бит-в-бит)
```

# SDK анализа трейсов (`internal/tf`)

Способ писать анализ трейсов, **не зная, как устроено хранилище** (бандлы, сегменты,
`ref:sha256`, dedup-index, три схемы `gen_ai.*`). Процессор получает `tf.Span`/`tf.Trace`,
у которых тяжёлый текст доступен как обычное поле (`span.Prompt(ctx)`) — разворачивается
прозрачно из CAS **по требованию**. Инфраструктуру (загрузка окна, консюминг, offset,
запись, метрики, graceful shutdown) берут на себя источники и раннер.

Три принципа:
- **Один тип `tf.Span` на оба режима.** `LoadRange` (архив, батч) и подписка на `spans.thin`
  (поток) отдают одинаковые `Span` → одна логика работает и батчем, и стримом.
- **Ленивый резолв.** Числа/метаданные (тайминги, токены, версии) читаются из тела спана
  без CAS; текст (`Prompt`/`Completion`) тянется из CAS только при обращении.
- **Процессор пишет только `Process()`.** Всё остальное — раннер.

## Модель данных

```go
type Span    struct { Service, Scope string; Resource *resourcepb.Resource; /*…lazy…*/ }
type Trace   struct { TraceID string; Spans []Span }
type Session struct { SessionID string; Traces []Trace }
type Message struct { Role, Content string }              // реплика диалога
type Result  struct { Table string; Values map[string]any } // строка на запись в ClickHouse
```

`Result` — что процессор отдаёт на запись: `Table` — целевая таблица, `Values` —
столбец→значение (типы должны совпадать с колонками CH: `time.Time`, `uint32`, `float32`,
`string`). Раннер группирует `Result` по `Table` и пишет пачками — процессору схему таблицы
знать не нужно.

## Как написать стрим-процессор (sink-сервис)

Спан за спаном из `spans.thin`. Подходит, когда результат виден по одному спану (факт-таблица,
счётчики). Пишешь `Process` + wiring в `main` — см. `cmd/chsink`.

**1. Логика — реализуй `StreamProcessor`:**
```go
type myProcessor struct{}

func (p *myProcessor) Process(_ context.Context, s tf.Span) ([]tf.Result, error) {
    return []tf.Result{{Table: "tf_spans", Values: map[string]any{
        "trace_id":     s.TraceID(),
        "session_id":   s.SessionID(),
        "ts":           s.StartTime(),
        "service":      s.Service,
        "version":      s.Version(),
        "model":        s.String("gen_ai.request.model"),
        "latency_ms":   uint32(s.Duration().Milliseconds()),
        "input_tokens": uint32(s.Int("gen_ai.usage.input_tokens")),
        "status_code":  s.StatusCode(),
    }}}, nil
}
```

**2. Wiring в `main` — подними приёмник, CAS и запусти раннер:**
```go
// Приёмник результатов (ClickHouse): миграции + коннект.
dbmigrate.ClickHouse(chDSN)
sink, _ := tf.OpenCH(ctx, chDSN)
defer sink.Close()

// CAS для ленивого резолва ref (нужен, ТОЛЬКО если процессор зовёт Prompt/Completion).
pool, _ := dedup.OpenPool(ctx, pgDSN)
casStore, _ := dedup.NewMinioBlobStore(ctx, dedup.MinioConfig{
    Endpoint: "localhost:9000", AccessKey: "minioadmin", SecretKey: "minioadmin", Bucket: "traceforge-dedup",
})
packer, _ := dedup.NewSegmentPacker(casStore, dedup.DefaultPackerConfig())
cas := dedup.IndexPacker{Index: dedup.NewIndex(pool), Packer: packer}

// Раннер владеет consumer group, offset-семантикой (запись → commit), батчингом и /metrics.
tf.RunStream(ctx, tf.StreamDeps{
    Brokers:     []string{"localhost:19092"},
    Topic:       "spans.thin",
    Group:       "my-sink",     // СВОЯ consumer group — работает параллельно другим консюмерам
    CAS:         cas,
    Sink:        sink,
    MetricsAddr: ":9466",       // "" = без /metrics
}, &myProcessor{})
```
Порядок долговечности — «данные раньше указателя»: раннер сначала пишет `Result` в ClickHouse,
затем коммитит Kafka-оффсет. At-least-once безопасен (`ReplacingMergeTree` схлопывает повтор).

## Как написать батч-джоб

По окну из архива, сессия целиком. Подходит, когда решение видно только по всему диалогу
(причина завершения, разметка датасета) — стриму это недоступно. Пишешь `Process` + wiring —
см. `cmd/batchjob`.

**1. Логика — реализуй `BatchProcessor`** (получает трейсы ОДНОЙ сессии):
```go
type myJob struct{}

func (j *myJob) Process(ctx context.Context, traces []tf.Trace) ([]tf.Result, error) {
    dialog, err := tf.BuildDialog(ctx, tf.Session{Traces: traces}) // реплики в хронологии
    if err != nil {
        return nil, err
    }
    // …анализ dialog (эвристика/LLM)…
    return []tf.Result{{Table: "session_end_reason", Values: map[string]any{ /* … */ }}}, nil
}
```

**2. Wiring в `main` — подними архив-источник и приёмник, запусти раннер:**
```go
dbmigrate.ClickHouse(chDSN)
sink, _ := tf.OpenCH(ctx, chDSN)
defer sink.Close()

index, _ := archive.Open(ctx, chDSN)                                  // archive_index (CH)
bundles, _ := dedup.NewMinioBlobStore(ctx, dedup.MinioConfig{ /* бакет бандлов */ })
cas := dedup.IndexPacker{Index: dedup.NewIndex(pool), Packer: packer} // как в стриме
src, _ := tf.NewArchiveSource(index, bundles, cas)
defer src.Close()

// Раннер: LoadRange(окно) → на каждую сессию Process → запись пачками. Ни Kafka, ни offset.
tf.RunBatch(ctx, tf.BatchDeps{Source: src, Sink: sink}, from, to, "", &myJob{})
```
Обработка потоковая: сессия обработана — отпущена (день целиком в память не собирается).

## Справочник методов

### `tf.Span` — доступ к полям (без CAS)
| Метод | Возвращает |
|---|---|
| `String(key) / Int(key) / Float(key) / Bool(key)` | типизированное значение атрибута спана |
| `Name()` | имя операции |
| `TraceID() / SpanID() / ParentSpanID()` | канонические hex-идентификаторы |
| `IsRoot()` | корневой спан трейса (нет родителя) |
| `StartTime()` (UTC) / `Duration()` | тайминги |
| `StatusCode()` / `StatusMessage()` | статус спана (`UNSET/OK/ERROR` + текст) |
| `FirstString(key)` | первый элемент строкового массива (напр. `finish_reasons`) |
| `SessionID() / UserID() / Version()` | доменные геттеры (baggage: `session.id`/`user.id`/`app.version`) |
| `Proto()` | сырой `*tracepb.Span` (аварийный люк; контент — ссылки) |

### `tf.Span` — контент (ленивый резолв из CAS)
| Метод | Возвращает |
|---|---|
| `Prompt(ctx) (string, error)` | текст входа LLM независимо от схемы A/B/C |
| `Completion(ctx) (string, error)` | текст выхода LLM независимо от схемы A/B/C |
| `IsLLM()` | спан несёт диалоговый обмен (есть completion) |

### Группировки и диалог
| Функция | Назначение |
|---|---|
| `GroupByTrace(spans) []Trace` | плоские спаны → трейсы по `trace_id` |
| `GroupBySession(traces) []Session` | трейсы → сессии по `session.id` |
| `BuildDialog(ctx, session) ([]Message, error)` | реплики user/assistant в хронологии (через все трейсы) |

### Источники и приёмник
| API | Назначение |
|---|---|
| `NewArchiveSource(index, bundles, cas)` | батч-источник над архивом |
| `Archive.LoadRange(ctx, from, to, sessionID, fn)` | окно из архива, потоково по сессиям (callback) |
| `Archive.LoadSession(ctx, id) / LoadTrace(ctx, id)` | точечная загрузка |
| `SpanFromThinLine(line, cas) (Span, error)` | строка `spans.thin` (Kafka) → `Span` (стрим-путь) |
| `OpenCH(ctx, dsn)` → `CHSink.WriteResults(ctx, table, rows)` | запись в ClickHouse |

### Раннеры (инфраструктура)
| API | Назначение |
|---|---|
| `RunStream(ctx, StreamDeps, StreamProcessor)` | consumer group + offset + запись + метрики |
| `RunBatch(ctx, BatchDeps, from, to, sessionFilter, BatchProcessor)` | окно + потоковая обработка по сессиям + запись |

Признак попадания: новый процессор — это `Process()` в ~30–40 строк, потому что загрузка,
резолв ссылок, сборка диалога и запись уже есть.


## Запуск процессоров по отдельности

Каждый процессор — отдельный бинарь; можно гонять точечно (флаги имеют дефолты под локальный
стенд). Полезно, чтобы наблюдать один этап или запустить только свой sink/джоб:
```
go run ./cmd/loadgen    -rps=50 -duration=60s -schema=B        # генерация трафика в коллектор
go run ./cmd/dedup      -chunker=fastcdc -duration=45s         # P1: otlp.raw.spans -> spans.thin + CAS
go run ./cmd/archiver   -duration=30s                          # P2: spans.thin -> бандлы + archive_index
go run ./cmd/chsink     -duration=30s                          # P3 стрим: spans.thin -> tf_spans + user_sessions
go run ./cmd/batchjob   --from 2026-07-22T00:00:00Z --to 2026-07-24T00:00:00Z  # батч -> session_end_reason
go run ./cmd/restore-api -addr=:8090                           # HTTP restore (trace_id / окно)
```
- `-duration` (стрим-процессоры): сколько работать, `0` = до Ctrl-C.
- `-group` (стрим): своя consumer group — новый процессор читает `spans.thin` **параллельно** остальным, не мешая им.
- Общие флаги коннектов: `-brokers`, `-ch-dsn`, `-pg-dsn`, `-minio-*`, `-cas-bucket`. Полный список — `go run ./cmd/<name> -h`.

## Миграции ClickHouse / Postgres

Миграции **накатываются автоматически** при старте процессора, которому нужна БД
(`cmd/dedup` → Postgres `dedup_index`; `cmd/archiver`/`cmd/chsink`/`cmd/batchjob` → ClickHouse
`archive_index`/`tf_spans`/…). Отдельный шаг не нужен — `dbmigrate` идемпотентен (повторный
старт без новых миграций = no-op).

**Добавить таблицу/колонку:**
1. Положи пару файлов `migrations/clickhouse/00000N_<name>.up.sql` и `.down.sql` (или в
   `migrations/postgres/`). Они встраиваются в бинарь (`go:embed`), нумерация — по возрастанию.
2. **Не ставь `;` внутри `--` комментариев** в CH-миграции: golang-migrate режет мульти-стейтмент
   по `;`, не понимая комментов, → падение `code 62 Empty query`.
3. Накат — просто следующий запуск любого процессора этой БД.

**Если миграция упала на полпути** (частичный DDL) → состояние «dirty», следующий старт
откажется мигрировать. Починка для ClickHouse (`schema_migrations`, драйвер читает последнюю
строку по `sequence` — ставим её на последнюю УСПЕШНУЮ версию, `dirty=0`):
```
docker compose -f deploy/docker-compose.yml exec -T clickhouse \
  clickhouse-client -u traceforge --password traceforge -d traceforge --multiquery \
  -q "TRUNCATE TABLE schema_migrations; INSERT INTO schema_migrations (version, dirty, sequence) VALUES (<last_ok_version>, 0, 1);"
```


# Restore API и разгрузка Langfuse

Два инструмента вокруг архива (не часть SDK): вернуть трейс и безопасно почистить Langfuse.

## restore-api — восстановление трейсов + выгрузка обратно в Langfuse

HTTP-сервис над архивом (`cmd/restore-api`): по `trace_id` или окну достаёт **полный трейс
бит-в-бит** (тонкий спан из бандла + резолв всех `ref` из CAS). Запуск: `go run ./cmd/restore-api -addr=:8090`.

```
GET /traces/{trace_id}                  # один трейс (саппорт-кейс)
GET /traces?from=&to=[&session_id=]     # окно трейсов (RFC3339) — «Langfuse-просмотрщик»
GET /healthz    GET /metrics
```

**Выгрузка обратно в Langfuse** — добавь `?push=langfuse` к любому эндпоинту: восстановленные спаны
пересобираются в OTLP и заливаются в Langfuse по OTLP/HTTP → трейс снова виден в его UI.
```
GET /traces/{id}?push=langfuse
GET /traces?from=2026-07-15T10:00:00Z&to=2026-07-15T11:00:00Z&push=langfuse
```
Ключи Langfuse — флагами `-langfuse-endpoint`, `-langfuse-public-key`, `-langfuse-secret-key`.
Смысл: история пережила retention Kafka в архиве, а Langfuse остаётся UI → трейс можно «вернуть»
для просмотра. Код-путь готов; живой тест требует поднятого Langfuse (profile full + ingest-ключи) — хвост T8.

## retention — предохранитель перед чисткой Langfuse

Джоб разгрузки (`cmd/retention verify`): прежде чем удалять данные в Langfuse, доказывает, что окно
**восстановимо из архива** (реальный restore + crc на выборке), печатает **план удаления** и отдаёт
машиночитаемый код выхода (0 = можно чистить, 1 = нельзя).
```
make retention-verify        # verify окна + план удаления (FROM/TO по умолчанию — вчера..завтра)
# или напрямую:
go run ./cmd/retention verify --from <RFC3339> --to <RFC3339> --project antispam --sample 10
```
Само **удаление в Langfuse — вручную** 
авто-удаление — заложенная точка расширения (dry-run → подтверждение → аудит).

Петля разгрузки замкнута: **архивируем → verify (зелёный) → чистят Langfuse → при нужде возвращаем
трейс через `?push=langfuse`**.

# Быстрый старт

```
make up                       # поднять стенд
make demo-p1                  # gen -> P1 dedup (CAS), срезы
make smoke-restore            # gen -> P1 -> P2 -> архив -> restore (бит-в-бит)
make smoke-p3                 # gen -> P1 -> P2 -> P3 -> батч -> витрины (одной командой)
make marts                    # продуктовые витрины + аудит качества (с таймингом)
make seed-marts SEED_ROWS=3000000 && make marts   # замер витрин «на объёме»
```