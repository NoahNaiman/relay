// Package metrics defines relay's Prometheus metrics.
package metrics

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/NoahNaiman/relay/internal/store"
)

// Metrics holds every relay metric on its own registry.
type Metrics struct {
	Registry         *prometheus.Registry
	JobsEnqueued     prometheus.Counter
	Deliveries       *prometheus.CounterVec
	DeliveryDuration prometheus.Histogram
	QueueDepth       *prometheus.GaugeVec
	BreakerOpen      *prometheus.GaugeVec
}

// New registers all metrics, plus the standard Go and process collectors.
func New() *Metrics {
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
		JobsEnqueued: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "relay_jobs_enqueued_total",
			Help: "Jobs accepted by EnqueueJob (idempotent repeats are not counted).",
		}),
		Deliveries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "relay_deliveries_total",
			Help: "Delivery attempts by the job's resulting state.",
		}, []string{"result"}),
		DeliveryDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "relay_delivery_duration_seconds",
			Help:    "Duration of delivery HTTP requests.",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 12), // 5ms .. ~10s
		}),
		QueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "relay_queue_depth",
			Help: "Number of jobs in each status, refreshed periodically.",
		}, []string{"status"}),
		BreakerOpen: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "relay_breaker_open",
			Help: "1 while an endpoint's circuit breaker is open or half-open in this process.",
		}, []string{"endpoint_id"}),
	}
	m.Registry.MustRegister(m.JobsEnqueued, m.Deliveries, m.DeliveryDuration, m.QueueDepth, m.BreakerOpen,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	for _, r := range []string{"delivered", "retry", "dead"} {
		m.Deliveries.WithLabelValues(r)
	}
	return m
}

// Delivery implements dispatch.Observer.
func (m *Metrics) Delivery(result string, d time.Duration) {
	m.Deliveries.WithLabelValues(result).Inc()
	m.DeliveryDuration.Observe(d.Seconds())
}

// BreakerChanged implements dispatch.Observer.
func (m *Metrics) BreakerChanged(endpointID uuid.UUID, open bool) {
	v := 0.0
	if open {
		v = 1
	}
	m.BreakerOpen.WithLabelValues(endpointID.String()).Set(v)
}

// RefreshQueueDepth sets relay_queue_depth once.
func (m *Metrics) RefreshQueueDepth(ctx context.Context, s store.Store) error {
	counts, err := s.CountByStatus(ctx)
	if err != nil {
		return err
	}
	for st, n := range counts {
		m.QueueDepth.WithLabelValues(string(st)).Set(float64(n))
	}
	return nil
}

// RunQueueDepth refreshes relay_queue_depth every interval until ctx ends.
func (m *Metrics) RunQueueDepth(ctx context.Context, s store.Store, interval time.Duration, log *slog.Logger) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if err := m.RefreshQueueDepth(ctx, s); err != nil && ctx.Err() == nil {
			log.Warn("queue depth refresh failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Handler serves the registry in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
