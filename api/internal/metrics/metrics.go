// Package metrics exposes the service's Prometheus instrumentation. HTTP request
// timings are recorded directly by the middleware; the counters accumulated by
// the ingest writer, NOTIFY listener, job queue, WebSocket hub, and CDC consumer
// are bridged onto Prometheus by a pull-time collector that reads their existing
// snapshots, so those components stay free of a Prometheus dependency.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics owns a dedicated registry and the HTTP instruments.
type Metrics struct {
	reg          *prometheus.Registry
	httpDuration *prometheus.HistogramVec
	httpInFlight prometheus.Gauge
}

// New builds a registry with the Go runtime and process collectors plus the
// HTTP request instruments.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{
		reg: reg,
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ais_http_request_duration_seconds",
			Help:    "HTTP request latency by method, matched route, and status.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route", "status"}),
		httpInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ais_http_requests_in_flight",
			Help: "In-flight HTTP requests.",
		}),
	}
	reg.MustRegister(m.httpDuration, m.httpInFlight)
	return m
}

// Handler serves the metrics registry in the Prometheus text exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// HTTPMiddleware records duration and in-flight count for every request, keyed
// on the matched chi route pattern so path parameters do not explode label
// cardinality.
func (m *Metrics) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.httpInFlight.Inc()
		defer m.httpInFlight.Dec()

		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		start := time.Now()
		next.ServeHTTP(ww, r)

		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			route = "unmatched"
		}
		m.httpDuration.WithLabelValues(r.Method, route, strconv.Itoa(ww.Status())).
			Observe(time.Since(start).Seconds())
	})
}

// RegisterSources wires the pull-time bridge that surfaces the components'
// existing counters. Call once, after the components are constructed.
func (m *Metrics) RegisterSources(s Sources) {
	m.reg.MustRegister(newBridge(s))
}
