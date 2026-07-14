COMPOSE  := docker compose -f deploy/docker-compose.yml
RPS      ?= 50
DURATION ?= 60s
SCHEMA   ?= B
CHUNKER  ?= fastcdc

.PHONY: build tidy up down logs gen read smoke dedup bench smoke-dedup demo-p1 inspect reset-p1

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

## reset-p1: чистый старт CAS — TRUNCATE dedup_index + очистка бакета MinIO + пересоздание spans.thin
reset-p1:
	@echo ">>> чищу dedup_index, бакет MinIO и топик spans.thin (чистый замер с нуля)..."
	@$(COMPOSE) exec -T postgres psql -U traceforge -c "TRUNCATE dedup_index;" >/dev/null
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