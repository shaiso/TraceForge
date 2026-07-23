-- Быстрая массовая заливка агрегационного слоя под ЗАМЕР ВИТРИН НА ОБЪЁМЕ (T7 DoD:
-- «время на объёме, близком к 3 млн»). Гонять пайплайн часами не нужно — исходная
-- жалоба была «на наших объёмах не работает», а её проверяет тайминг запроса, не
-- способ заполнения. Число строк — параметр {rows} (make seed-marts SEED_ROWS=...).
--
-- Порядок: make reset-p3 (чистый слой) -> make seed-marts -> make marts.
-- numbers_mt — многопоточная генерация; заливка 3 млн строк — секунды.

INSERT INTO tf_spans
    (trace_id, span_id, session_id, ts, service, span_name, version, model, provider,
     latency_ms, input_tokens, output_tokens, temperature, finish_reason, status_code,
     intent, scam_type, greeting_template)
SELECT
    lower(hex(cityHash64(number)))     AS trace_id,
    lower(hex(cityHash64(number + 1))) AS span_id,
    concat('sess-', toString(intDiv(number, 15))) AS session_id, -- ~15 спанов/сессия
    now() - toIntervalSecond(number % 259200)     AS ts,         -- разброс ~3 суток
    arrayElement(['greeter', 'dialog-agent', 'extractor'], toInt32(number % 3) + 1) AS service,
    arrayElement(['pick_greeting', 'agent_turn', 'classify_intent', 'chat gpt-4o-mini', 'chat qwen-lite'], toInt32(number % 5) + 1) AS span_name,
    if(number % 2 = 0, 'v3', 'v4')                 AS version,
    if(number % 3 = 2, 'qwen-lite', 'gpt-4o-mini') AS model,
    if(number % 3 = 2, 'alibaba', 'openai')        AS provider,
    toUInt32(20 + number % 400)                    AS latency_ms,
    toUInt32(200 + number % 600)                   AS input_tokens,
    toUInt32(50 + number % 200)                    AS output_tokens,
    toFloat32(0.2 + (number % 70) / 100.0)         AS temperature,
    arrayElement(['stop', 'stop', 'stop', 'length', 'content_filter'], toInt32(number % 5) + 1) AS finish_reason,
    if(number % 40 = 0, 'ERROR', 'UNSET')          AS status_code,
    arrayElement(['greeting', 'sales_pitch', 'phishing', 'request_personal_data', 'threat', 'smalltalk', 'goodbye', 'unknown'], toInt32(number % 8) + 1) AS intent,
    arrayElement(['fake_bank', 'prize_fraud', 'safe_account', 'gov_services', 'delivery_fee', 'relative_in_trouble', 'investment'], toInt32(number % 7) + 1) AS scam_type,
    concat('greet-', leftPad(toString(number % 20), 2, '0')) AS greeting_template
FROM numbers_mt({rows:UInt64});

INSERT INTO session_end_reason (session_id, end_reason, turns, date)
SELECT
    concat('sess-', toString(number)) AS session_id,
    arrayElement(['dialog_exhausted', 'dialog_exhausted', 'spammer_hung_up', 'agent_confused', 'connection_lost'], toInt32(number % 5) + 1) AS end_reason,
    toUInt32(2 + number % 14) AS turns,
    toDate(now() - toIntervalSecond(number % 259200)) AS date
FROM numbers_mt(intDiv({rows:UInt64}, 15));

INSERT INTO user_sessions (user_id, session_id, trace_id, date)
SELECT
    concat('+7900', leftPad(toString(1000000 + number % 40), 7, '0')) AS user_id,
    concat('sess-', toString(intDiv(number, 4))) AS session_id,
    lower(hex(cityHash64(number))) AS trace_id,
    toDate(now() - toIntervalSecond(number % 259200)) AS date
FROM numbers_mt(intDiv({rows:UInt64}, 4));
