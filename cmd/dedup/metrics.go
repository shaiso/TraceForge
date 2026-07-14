package main

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"traceforge/internal/dedup"
)

// metrics — Prometheus-зеркало счётчиков P1. Значения кумулятивные, поэтому
// заведены как Gauge и выставляются целиком из dedup.Stats после каждого батча
// (проще, чем считать дельты; для демо/наблюдаемости достаточно).
type metrics struct {
	spans      prometheus.Gauge
	candidates prometheus.Gauge
	chunks     prometheus.Gauge
	misses     prometheus.Gauge
	hits       prometheus.Gauge
	content    prometheus.Gauge
	stored     prometheus.Gauge
	refs       prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) *metrics {
	g := func(name, help string) prometheus.Gauge {
		m := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "traceforge", Subsystem: "dedup", Name: name, Help: help,
		})
		reg.MustRegister(m)
		return m
	}
	return &metrics{
		spans:      g("spans", "обработано спанов"),
		candidates: g("candidate_fields", "извлечено тяжёлых полей"),
		chunks:     g("chunks", "фрагментов всего"),
		misses:     g("chunk_misses", "уникальных фрагментов (записано в CAS)"),
		hits:       g("chunk_hits", "фрагментов найдено в CAS (дедуп)"),
		content:    g("content_bytes", "исходный вес контента полей-кандидатов"),
		stored:     g("stored_bytes", "вес уникальных фрагментов (до компрессии)"),
		refs:       g("ref_bytes", "вес ссылок в тонких спанах"),
	}
}

// update выставляет gauge'и из текущих кумулятивных счётчиков процессора.
func (m *metrics) update(s *dedup.Stats) {
	m.spans.Set(float64(s.Spans))
	m.candidates.Set(float64(s.Candidates))
	m.chunks.Set(float64(s.Chunks))
	m.misses.Set(float64(s.Misses))
	m.hits.Set(float64(s.Hits))
	m.content.Set(float64(s.ContentBytes))
	m.stored.Set(float64(s.StoredBytes))
	m.refs.Set(float64(s.RefBytes))
}

// serveMetrics поднимает /metrics в фоне (пустой addr — метрики выключены).
func serveMetrics(addr string, reg *prometheus.Registry) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	go func() {
		log.Printf("dedup: метрики на http://%s/metrics", addr)
		if err := http.ListenAndServe(addr, mux); err != nil && err != http.ErrServerClosed {
			log.Printf("dedup: сервер метрик остановлен: %v", err)
		}
	}()
}
