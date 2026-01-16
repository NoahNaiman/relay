package dispatch

import (
	"testing"
	"time"

	"github.com/NoahNaiman/relay/internal/delivery"
	"github.com/NoahNaiman/relay/internal/store"
)

func TestPolicyDecide(t *testing.T) {
	now := time.Unix(1000, 0)
	p := Policy{Backoff: delivery.Backoff{Base: time.Second, Cap: time.Minute, Rand: func(n int64) int64 { return n - 1 }}}
	cases := []struct {
		name     string
		attempts int
		verdict  delivery.Verdict
		want     store.Result
	}{
		{"delivered", 0, delivery.Verdict{Class: delivery.Delivered}, store.Result{Outcome: store.OutcomeDelivered}},
		{"permanent", 2, delivery.Verdict{Class: delivery.Permanent, Reason: "HTTP 404"}, store.Result{Outcome: store.OutcomeDead, Error: "HTTP 404"}},
		{"retry uses backoff", 2, delivery.Verdict{Class: delivery.Retryable, Reason: "HTTP 503"},
			store.Result{Outcome: store.OutcomeRetry, NextAttemptAt: now.Add(4 * time.Second), Error: "HTTP 503"}},
		{"retry-after wins over backoff", 0, delivery.Verdict{Class: delivery.Retryable, RetryAfter: 7 * time.Second, Reason: "HTTP 429"},
			store.Result{Outcome: store.OutcomeRetry, NextAttemptAt: now.Add(7 * time.Second), Error: "HTTP 429"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.Decide(tc.attempts, tc.verdict, now); got != tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestConfigFrom(t *testing.T) {
	c := ConfigFrom(configDefault())
	if c.Workers != 16 || c.Lease != 30*time.Second || c.BreakerThreshold != 5 || c.ShutdownTimeout != 15*time.Second {
		t.Fatalf("unexpected: %+v", c)
	}
}
