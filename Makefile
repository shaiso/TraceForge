# Makefile traceforge, sprint 01. Цели: up / down / gen / read / smoke / build.

COMPOSE  := docker compose -f deploy/docker-compose.yml
RPS      ?= 50
DURATION ?= 60s
SCHEMA   ?= B

.PHONY: build tidy up down logs gen read smoke

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

## read: прочитать топик и напечатать спаны
read:
	go run ./cmd/rawread -duration=15s

## smoke: end-to-end проверка — up, генерация, чтение с ассертами
smoke:
	$(COMPOSE) up -d
	@echo "жду 15с, пока стенд устаканится..."
	@sleep 15
	go run ./cmd/loadgen -rps=$(RPS) -duration=$(DURATION) -schema=$(SCHEMA)
	@echo "читаю обратно со смоук-ассертами..."
	go run ./cmd/rawread -smoke -duration=30s
