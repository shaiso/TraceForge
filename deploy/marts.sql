-- Витрины Sprint 04 (T7): срезы, которые в Langfuse либо невозможны, либо неудобны
-- на объёмах. Считаются НА ЧТЕНИИ поверх tf_spans (стрим P3) + session_end_reason
-- (батч T6) + user_sessions. FINAL схлопывает возможные повторы at-least-once
-- (ReplacingMergeTree) — та же идемпотентность-на-чтении, что у archive_index.
--
-- Прогон с таймингом: make marts (clickhouse-client --time печатает мс на срез).

SELECT '=== 1. Причины завершения × версия промпта (СОВМЕСТНО стрим+батч) ===' AS mart FORMAT TSVRaw;
-- Джойн: end_reason из батча (session_end_reason) × version из стрима (tf_spans).
-- Это и есть доказательство «витрина собирается совместно стримом и батчем».
SELECT s.ver AS version, r.end_reason AS end_reason, count() AS sessions
FROM (SELECT session_id, end_reason FROM session_end_reason FINAL) AS r
INNER JOIN (
    SELECT session_id, any(version) AS ver
    FROM tf_spans FINAL
    WHERE session_id != '' AND version != ''
    GROUP BY session_id
) AS s ON s.session_id = r.session_id
GROUP BY version, end_reason
ORDER BY version, sessions DESC;

SELECT '=== 2. Время удержания по версиям (avg / p50 / p90, мс) ===' AS mart FORMAT TSVRaw;
-- Удержание сессии = max(ts) − min(ts). Сессию не закрываем: продолжение через месяц
-- просто расширит окно, длительность пересчитается сама.
SELECT version,
       count() AS sessions,
       round(avg(hold_ms)) AS avg_ms,
       round(quantile(0.5)(hold_ms)) AS p50_ms,
       round(quantile(0.9)(hold_ms)) AS p90_ms
FROM (
    SELECT session_id, any(version) AS version,
           dateDiff('millisecond', min(ts), max(ts)) AS hold_ms
    FROM tf_spans FINAL
    WHERE session_id != ''
    GROUP BY session_id
)
GROUP BY version ORDER BY version;

SELECT '=== 2b. Динамика удержания по дням ===' AS mart FORMAT TSVRaw;
SELECT day, count() AS sessions, round(avg(hold_ms)) AS avg_hold_ms
FROM (
    SELECT session_id, toDate(min(ts)) AS day,
           dateDiff('millisecond', min(ts), max(ts)) AS hold_ms
    FROM tf_spans FINAL
    WHERE session_id != ''
    GROUP BY session_id
)
GROUP BY day ORDER BY day;

SELECT '=== 3. Глубина диалога и воронка (сколько ходов дожил) ===' AS mart FORMAT TSVRaw;
SELECT turns, sessions,
       sum(sessions) OVER (ORDER BY turns DESC) AS reached_at_least
FROM (
    SELECT turns, count() AS sessions
    FROM session_end_reason FINAL
    GROUP BY turns
)
ORDER BY turns;

SELECT '=== 4. Latency и токены по моделям и версиям ===' AS mart FORMAT TSVRaw;
SELECT model, version,
       count() AS spans,
       round(avg(latency_ms)) AS avg_latency_ms,
       round(quantile(0.9)(latency_ms)) AS p90_latency_ms,
       round(avg(input_tokens)) AS avg_in_tok,
       round(avg(output_tokens)) AS avg_out_tok
FROM tf_spans FINAL
WHERE model != ''
GROUP BY model, version ORDER BY model, version;

SELECT '=== 5. Обращения пользователя за 3 дня ===' AS mart FORMAT TSVRaw;
SELECT user_id,
       count(DISTINCT session_id) AS sessions,
       count(DISTINCT trace_id) AS traces
FROM user_sessions FINAL
WHERE date >= today() - 3
GROUP BY user_id ORDER BY sessions DESC LIMIT 15;

SELECT '=== АУДИТ КАЧЕСТВА ДАННЫХ ===' AS mart FORMAT TSVRaw;
-- Без покрытия витрины голословны: пустой session_id/version тихо искажает срезы.
-- Это ещё и рычаг для разговора про baggage: «X% спанов без session.id».
SELECT round(100 * countIf(session_id != '') / count(), 1) AS session_id_coverage_pct,
       round(100 * countIf(version != '') / count(), 1)    AS version_coverage_pct,
       round(100 * countIf(span_name LIKE 'chat%' AND model != '' AND input_tokens > 0 AND output_tokens > 0)
             / countIf(span_name LIKE 'chat%'), 1)         AS gen_ai_full_on_chat_pct,
       count() AS total_spans
FROM tf_spans FINAL;
