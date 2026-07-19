package main

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// metrics — Prometheus-наблюдаемость restore-api: latency, исходы (hit/miss/error),
// размер окна (спанов), ошибки резолва рефов, исходы push в Langfuse.
type metrics struct {
	reqs    *prometheus.CounterVec   // {mode, status}
	latency *prometheus.HistogramVec // {mode}
	spans   *prometheus.CounterVec   // {mode} — суммарно восстановлено спанов
	refErrs prometheus.Counter
	push    *prometheus.CounterVec // {status}
}

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		reqs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "traceforge", Subsystem: "restore", Name: "requests_total",
			Help: "restore-запросы по режиму и исходу",
		}, []string{"mode", "status"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "traceforge", Subsystem: "restore", Name: "latency_seconds",
			Help: "латентность restore по режиму", Buckets: prometheus.DefBuckets,
		}, []string{"mode"}),
		spans: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "traceforge", Subsystem: "restore", Name: "spans_total",
			Help: "восстановлено спанов (размер окна) по режиму",
		}, []string{"mode"}),
		refErrs: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "traceforge", Subsystem: "restore", Name: "ref_resolve_errors_total",
			Help: "ошибки резолва ссылок/чтения бандла/сегмента",
		}),
		push: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "traceforge", Subsystem: "restore", Name: "langfuse_push_total",
			Help: "исходы push в Langfuse",
		}, []string{"status"}),
	}
	reg.MustRegister(m.reqs, m.latency, m.spans, m.refErrs, m.push)
	return m
}

// observe фиксирует исход запроса: счётчик, латентность и размер окна.
func (m *metrics) observe(mode, status string, start time.Time, spans int) {
	m.reqs.WithLabelValues(mode, status).Inc()
	m.latency.WithLabelValues(mode).Observe(time.Since(start).Seconds())
	if spans > 0 {
		m.spans.WithLabelValues(mode).Add(float64(spans))
	}
}

func (m *metrics) refErr() { m.refErrs.Inc() }

func (m *metrics) pushDone(ok bool) {
	status := "ok"
	if !ok {
		status = "error"
	}
	m.push.WithLabelValues(status).Inc()
}
