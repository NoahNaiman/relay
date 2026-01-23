package metrics

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/NoahNaiman/relay/internal/store"
	"github.com/NoahNaiman/relay/internal/store/memory"
)

func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	b, _ := io.ReadAll(rec.Body)
	return string(b)
}

func TestMetrics(t *testing.T) {
	m := New()
	m.JobsEnqueued.Inc()
	m.Delivery("delivered", 20*time.Millisecond)
	m.Delivery("retry", time.Second)
	ep := uuid.New()
	m.BreakerChanged(ep, true)

	s := memory.New()
	e, _ := s.CreateEndpoint(context.Background(), store.Endpoint{URL: "http://x", Secret: "k", MaxRPS: 1, MaxAttempts: 1})
	_, _, _ = s.Enqueue(context.Background(), store.NewJob{EndpointID: e.ID})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { m.RunQueueDepth(ctx, s, time.Millisecond, slog.New(slog.DiscardHandler)); close(done) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	body := scrape(t, m)
	for _, want := range []string{
		"relay_jobs_enqueued_total 1",
		`relay_deliveries_total{result="delivered"} 1`,
		`relay_deliveries_total{result="retry"} 1`,
		`relay_deliveries_total{result="dead"} 0`,
		"relay_delivery_duration_seconds_count 2",
		`relay_queue_depth{status="pending"} 1`,
		`relay_queue_depth{status="dead"} 0`,
		`relay_breaker_open{endpoint_id="` + ep.String() + `"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q", want)
		}
	}
	m.BreakerChanged(ep, false)
	if !strings.Contains(scrape(t, m), `relay_breaker_open{endpoint_id="`+ep.String()+`"} 0`) {
		t.Error("breaker gauge should drop to 0 when closed")
	}
}
