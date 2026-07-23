COMPOSE  := docker compose -f deploy/docker-compose.yml
RPS      ?= 50
DURATION ?= 60s
SCHEMA   ?= B
CHUNKER  ?= fastcdc

SEED_ROWS ?= 3000000

.PHONY: build tidy up down logs gen read smoke dedup bench smoke-dedup demo-p1 inspect reset-p1 \
        archiver restore-api retention-verify reset-p2 smoke-restore inspect-archive \
        chsink batchjob marts seed-marts reset-p3 smoke-p3

## build: собрать все бинари
build:
	go build ./...

## tidy: подтянуть зависимости
tidy:
	go mod tidy

## up: поднять стенд (platform-ветка)
up:
	$(COMPOSE) up -d
	@echo "жду готовности стенда..."
	@$(COMPOSE) exec -T redpanda rpk topic list -X brokers=localhost:9092 || true

## down: снести стенд вместе с томами
down:
	$(COMPOSE) down -v

## logs: логи коллектора
logs:
	$(COMPOSE) logs -f otel-collector

## gen: прогнать генератор (RPS=.. DURATION=.. SCHEMA=A|B)
gen:
	go run ./cmd/loadgen -rps=$(RPS) -duration=$(DURATION) -schema=$(SCHEMA)

## dedup: запустить P1-процессор (CHUNKER=whole-field|fastcdc)
dedup:
	go run ./cmd/dedup -chunker=$(CHUNKER) -duration=$(DURATION)

## bench: офлайн-сравнение экономии whole-field vs FastCDC на топике
bench:
	go run ./cmd/dedupbench -duration=20s

## smoke-dedup: длинные диалоги -> сравнение экономии чанкеров спринт 2
smoke-dedup:
	$(COMPOSE) up -d
	@echo "жду 15с, пока стенд устаканится..."
	@sleep 15
	go run ./cmd/loadgen -rps=$(RPS) -duration=$(DURATION) -schema=B -long
	@echo "сравниваю whole-field vs FastCDC..."
	go run ./cmd/dedupbench -duration=25s

## smoke: end-to-end проверка — up, генерация, чтение с ассертами для спринта 1
smoke:
	$(COMPOSE) up -d
	@echo "жду 15с, пока стенд устаканится..."
	@sleep 15
	go run ./cmd/loadgen -rps=$(RPS) -duration=$(DURATION) -schema=B -long
	@echo "читаю обратно со смоук-ассертами..."
	go run ./cmd/rawread -smoke -duration=30s

## read: прочитать топик и напечатать спаны спринт 1
read:
	go run ./cmd/rawread -duration=15s

## archiver: запустить P2-архиватор (spans.thin -> бандлы S3 + archive_index CH)
archiver:
	go run ./cmd/archiver -duration=$(DURATION)

## restore-api: поднять HTTP restore-сервис (GET /traces/{id}, /traces?from=&to=)
restore-api:
	go run ./cmd/restore-api -addr=:8090

## retention-verify: verify окна + план удаления (FROM/TO в RFC3339)
FROM ?= $(shell date -u -v-1d +%Y-%m-%dT00:00:00Z)
TO   ?= $(shell date -u -v+1d +%Y-%m-%dT00:00:00Z)
retention-verify:
	go run ./cmd/retention verify --from $(FROM) --to $(TO) --project antispam --sample 10

## reset-p2: чистый старт архива — TRUNCATE archive_index + очистка бакета бандлов
reset-p2:
	@echo ">>> чищу archive_index и бакет traceforge-archive..."
	@$(COMPOSE) exec -T clickhouse clickhouse-client -u traceforge --password traceforge -d traceforge -q "TRUNCATE TABLE IF EXISTS archive_index;" >/dev/null 2>&1 || true
	@$(COMPOSE) exec -T minio sh -c \
	  "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; \
	   mc rm --recursive --force local/traceforge-archive >/dev/null 2>&1 || true"
	@echo ">>> состояние P2 очищено."

## smoke-restore: end-to-end петля gen -> P1 -> P2 -> архив -> restore (одиночный+диапазон)
smoke-restore:
	$(COMPOSE) up -d
	@echo "жду 15с, пока стенд устаканится..."
	@sleep 15
	@$(MAKE) --no-print-directory reset-p1
	@$(MAKE) --no-print-directory reset-p2
	@echo ">>> gen -> P1 -> P2 (наполняю архив)..."
	go run ./cmd/loadgen -rps=$(RPS) -duration=$(DURATION) -schema=B -long
	go run ./cmd/dedup -chunker=$(CHUNKER) -duration=45s
	go run ./cmd/archiver -duration=30s
	@echo ">>> restore: живой E2E (одиночный, все ref резолвятся)..."
	go test ./internal/restore/... -run TestRestoreFromLiveArchive -count=1 -v
	@echo ">>> retention verify (диапазон окна + план удаления)..."
	@$(MAKE) --no-print-directory retention-verify

## inspect-archive: срезы архива (archive_index + бандлы в S3)
inspect-archive:
	@echo "===== ClickHouse: archive_index ====="
	@$(COMPOSE) exec -T clickhouse clickhouse-client -u traceforge --password traceforge -d traceforge -q \
	  "SELECT count() AS rows, uniqExact(trace_id) AS traces, uniqExact(bundle_key) AS bundles FROM archive_index FINAL;"
	@$(COMPOSE) exec -T clickhouse clickhouse-client -u traceforge --password traceforge -d traceforge -q \
	  "SELECT partition, sum(rows) AS rows FROM system.parts WHERE table='archive_index' AND active GROUP BY partition ORDER BY partition;"
	@echo "===== MinIO: бандлы архива ====="
	@$(COMPOSE) exec -T minio sh -c \
	  "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; mc ls --recursive --summarize local/traceforge-archive" 2>/dev/null | tail -8

## reset-p1: чистый старт CAS — TRUNCATE dedup_index + очистка бакета MinIO + пересоздание spans.thin
reset-p1:
	@echo ">>> чищу dedup_index, бакет MinIO и топик spans.thin (чистый замер с нуля)..."
	@$(COMPOSE) exec -T postgres psql -U traceforge -c "TRUNCATE dedup_index;" >/dev/null 2>&1 || true
	@$(COMPOSE) exec -T minio sh -c \
	  "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; \
	   mc rm --recursive --force local/traceforge-dedup >/dev/null 2>&1; \
	   mc mb -p local/traceforge-dedup >/dev/null 2>&1 || true"
	@$(COMPOSE) exec -T redpanda sh -c \
	  "rpk topic delete spans.thin -X brokers=redpanda:9092 >/dev/null 2>&1; \
	   rpk topic create spans.thin -X brokers=redpanda:9092 --partitions 6 --replicas 1 \
	     --topic-config retention.ms=604800000 >/dev/null 2>&1 || true"
	@echo ">>> состояние P1 очищено."

## demo-p1: ПОЛНЫЙ прогон P1 с ЗАПИСЬЮ (чистый старт -> генерация -> dedup в CAS -> срезы)
demo-p1:
	$(COMPOSE) up -d
	@echo "жду 15с, пока стенд устаканится..."
	@sleep 15
	@$(MAKE) --no-print-directory reset-p1
	@echo ">>> [1/3] генерирую реальный трафик на $(DURATION) (long-диалоги)..."
	go run ./cmd/loadgen -rps=$(RPS) -duration=$(DURATION) -schema=B -long
	@echo ">>> [2/3] P1 dedup С ЗАПИСЬЮ (chunker=$(CHUNKER)); живой /metrics: http://localhost:9464/metrics"
	go run ./cmd/dedup -chunker=$(CHUNKER) -duration=45s
	@echo ">>> [3/3] срезы результата:"
	@$(MAKE) --no-print-directory inspect

## inspect: срезы состояния CAS после прогона (index / S3 / thin / UI) — можно звать отдельно
inspect:
	@echo "===== POSTGRES: dedup_index ====="
	@$(COMPOSE) exec -T postgres psql -U traceforge -c \
	  "SELECT count(*) AS уник_фрагментов, pg_size_pretty(sum(len_)) AS сжатый_объём, pg_size_pretty(avg(len_)::bigint) AS ср_фрагмент FROM dedup_index;"
	@echo "--- топ-5 сегментов по числу фрагментов ---"
	@$(COMPOSE) exec -T postgres psql -U traceforge -c \
	  "SELECT segment_key, count(*) AS фрагментов, pg_size_pretty(sum(len_)) AS объём FROM dedup_index GROUP BY segment_key ORDER BY 2 DESC LIMIT 5;"
	@echo "===== MinIO: сегменты в бакете traceforge-dedup ====="
	@$(COMPOSE) exec -T minio sh -c \
	  "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; mc ls --recursive --summarize local/traceforge-dedup" 2>/dev/null | tail -8
	@echo "===== Redpanda: spans.thin (сообщений по партициям) ====="
	@$(COMPOSE) exec -T redpanda rpk topic describe spans.thin -X brokers=localhost:9092 -p 2>/dev/null | head -8
	@echo "--- пример тонкого спана (первое сообщение) ---"
	@$(COMPOSE) exec -T redpanda rpk topic consume spans.thin -X brokers=localhost:9092 -n 1 -o start -f '%v' 2>/dev/null | head -c 600; echo
	@echo "===== UI / метрики ====="
	@echo "  Redpanda Console : http://localhost:8080         (топики, сообщения spans.thin)"
	@echo "  MinIO Console    : http://localhost:9001         (minioadmin/minioadmin, бакет traceforge-dedup)"
	@echo "  P1 /metrics      : http://localhost:9464/metrics (ТОЛЬКО пока запущен make dedup)"

# ---- Sprint 04: SDK-процессоры (P3 стрим + батч) + витрины ----

CH_CLIENT := $(COMPOSE) exec -T clickhouse clickhouse-client -u traceforge --password traceforge -d traceforge

## chsink: запустить P3 CH-sink (spans.thin -> tf_spans + user_sessions), на SDK
chsink:
	go run ./cmd/chsink -duration=$(DURATION)

## batchjob: классификация причин завершения по окну (FROM/TO) -> session_end_reason
batchjob:
	go run ./cmd/batchjob --from $(FROM) --to $(TO)

## marts: продуктовые витрины + аудит качества с таймингом (make marts)
marts:
	@$(CH_CLIENT) --multiquery --time --format=PrettyCompact < deploy/marts.sql

## seed-marts: массовая заливка слоя под замер витрин «на объёме» (SEED_ROWS=..)
seed-marts:
	@echo ">>> заливаю ~$(SEED_ROWS) строк в tf_spans (+session_end_reason, +user_sessions)..."
	@$(CH_CLIENT) --multiquery --param_rows=$(SEED_ROWS) < deploy/seed_marts.sql
	@$(CH_CLIENT) -q "SELECT 'tf_spans' AS t, count() FROM tf_spans UNION ALL SELECT 'user_sessions', count() FROM user_sessions UNION ALL SELECT 'session_end_reason', count() FROM session_end_reason"

## reset-p3: чистый агрегационный слой (TRUNCATE tf_spans/user_sessions/session_end_reason)
reset-p3:
	@echo ">>> чищу tf_spans, user_sessions, session_end_reason..."
	@$(CH_CLIENT) --multiquery -q "TRUNCATE TABLE IF EXISTS tf_spans; TRUNCATE TABLE IF EXISTS user_sessions; TRUNCATE TABLE IF EXISTS session_end_reason;" 2>/dev/null || true
	@echo ">>> слой P3 очищен."

## smoke-p3: вся петля gen -> P1 -> P2 -> P3(стрим) -> батч -> витрины одной командой
smoke-p3:
	$(COMPOSE) up -d
	@echo "жду 15с, пока стенд устаканится..."
	@sleep 15
	@$(MAKE) --no-print-directory reset-p1
	@$(MAKE) --no-print-directory reset-p2
	@$(MAKE) --no-print-directory reset-p3
	@echo ">>> gen -> P1 -> P2 -> P3 -> батч..."
	go run ./cmd/loadgen -rps=$(RPS) -duration=$(DURATION) -schema=B
	go run ./cmd/dedup -chunker=$(CHUNKER) -duration=45s
	go run ./cmd/archiver -duration=30s
	go run ./cmd/chsink -duration=30s
	@$(MAKE) --no-print-directory batchjob
	@echo ">>> витрины (стрим+батч совместно):"
	@$(MAKE) --no-print-directory marts