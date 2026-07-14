-- dedup-index: canonical hash -> расположение фрагмента в сегменте S3.
-- offset_/len_ — имена с подчёркиванием, т.к. offset/len зарезервированы в SQL.
CREATE TABLE IF NOT EXISTS dedup_index (
    hash        BYTEA       PRIMARY KEY,          -- sha256 фрагмента
    segment_key TEXT        NOT NULL,             -- ключ сегмента в S3 (seg-<uuid>.zst)
    offset_     BIGINT      NOT NULL,             -- смещение фрагмента в сегменте
    len_        INTEGER     NOT NULL,             -- длина фрагмента в байтах
    crc         BIGINT      NOT NULL,             -- crc32 фрагмента (проверка при restore)
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
