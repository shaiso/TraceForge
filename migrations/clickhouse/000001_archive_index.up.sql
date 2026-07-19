-- archive_index: «где лежит трейс» — (bundle_key, offset, len) на каждый трейс
-- в часовом бандле. Пишется из P2 (архиватор) пакетными INSERT.
--
-- ReplacingMergeTree(inserted_at): рестарт архиватора переигрывает незакоммиченные
-- спаны -> те же (trace_id, ts, bundle_key) строки схлопываются, побеждает свежая.
-- Трейс, размазанный по часам, даёт РАЗНЫЕ (ts, bundle_key) -> обе строки живут
-- (каждая адресует свой фрейм). Дедуп на чтении: SELECT ... FINAL.
--
-- offset_/len_ адресуют zstd-фрейм трейса внутри бандла (пофреймовая гранулярность,
-- как CAS-сегменты в P1): одиночный restore = range-read одного фрейма, не всего часа.
CREATE TABLE IF NOT EXISTS archive_index (
    trace_id    String,
    session_id  String,
    ts          DateTime64(3),                 -- время события (min ts спанов трейса в бандле)
    bundle_key  String,                        -- archive/date=YYYY-MM-DD/hour=HH/part-N.jsonl.zst
    offset_     UInt64,                         -- смещение фрейма трейса в бандле
    len_        UInt32,                         -- длина сжатого фрейма
    inserted_at DateTime DEFAULT now()          -- версия для ReplacingMergeTree
) ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toDate(ts)
ORDER BY (trace_id, ts, bundle_key);
