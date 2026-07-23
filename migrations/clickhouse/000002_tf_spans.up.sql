-- P3 CH-sink (Sprint 04): агрегационный слой поверх тонких спанов. Пишется из
-- cmd/chsink (StreamProcessor на SDK) и cmd/batchjob (BatchProcessor). Только
-- вставка, витрины считаются на чтении (GROUP BY / FINAL). Идемпотентность
-- at-least-once — ReplacingMergeTree (как archive_index), не UPDATE.

-- tf_spans: факт-таблица, строка на спан. Числовые/метаданные денормализованы со
-- спана, тяжёлый текст в неё НЕ кладём (он в CAS, доступен через restore/SDK).
-- span_id в ORDER BY — чтобы повтор при переигровке (тот же спан) схлопывался, а
-- не затирал соседний спан той же сессии/времени.
CREATE TABLE IF NOT EXISTS tf_spans (
    trace_id          String,
    span_id           String,
    session_id        String,                 -- пусто => отдельная группа '' (не фолбэк на trace_id)
    ts                DateTime64(3),
    service           String,
    span_name         String,
    version           String,                 -- app.version (baggage) / gen_ai.prompt.version
    model             String,
    provider          String,
    latency_ms        UInt32,
    input_tokens      UInt32,
    output_tokens     UInt32,
    temperature       Float32,
    finish_reason     String,
    status_code       String,                 -- UNSET | OK | ERROR
    intent            String,                 -- intent.label (спаны classify_intent)
    scam_type         String,                 -- scam.type (спаны extractor)
    greeting_template String,                 -- greeting.template_id (спаны greeter)
    inserted_at       DateTime DEFAULT now()  -- версия ReplacingMergeTree
) ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toDate(ts)
ORDER BY (session_id, ts, span_id);

-- user_sessions: под сценарий «обращения пользователя за N дней». Одна строка на
-- (user, session, trace) — chsink пишет её на КОРНЕВОМ спане трейса.
CREATE TABLE IF NOT EXISTS user_sessions (
    user_id     String,
    session_id  String,
    trace_id    String,
    date        Date,
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(date)
ORDER BY (user_id, date, session_id, trace_id);

-- session_end_reason: причина завершения диалога (батч, T6). Одна строка на сессию,
-- переклассификация (напр. смена эвристики на LLM) обновляется по classified_at.
-- version СЮДА НЕ кладём намеренно: витрина «причины × версия» ДЖОЙНИТ end_reason
-- (батч) с tf_spans.version (стрим) по session_id — это и есть доказательство, что
-- витрина собирается совместно стримом и батчем.
CREATE TABLE IF NOT EXISTS session_end_reason (
    session_id    String,
    end_reason    String,
    turns         UInt32,                 -- глубина диалога (число ходов) — батч-свойство
    date          Date,
    classified_at DateTime DEFAULT now()  -- версия ReplacingMergeTree
) ENGINE = ReplacingMergeTree(classified_at)
PARTITION BY toYYYYMM(date)
ORDER BY (session_id);
