package main

import (
	"strings"
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	var d []time.Duration
	for i := 1; i <= 100; i++ {
		d = append(d, time.Duration(i)*time.Millisecond)
	}
	for p, want := range map[float64]time.Duration{0.5: 50 * time.Millisecond, 0.9: 90 * time.Millisecond, 0.99: 99 * time.Millisecond, 1: 100 * time.Millisecond} {
		if got := percentile(d, p); got != want {
			t.Errorf("p%.0f = %s, want %s", p*100, got, want)
		}
	}
	if percentile(nil, 0.5) != 0 {
		t.Error("empty input")
	}
}

func TestPrint(t *testing.T) {
	start := time.Unix(0, 0)
	r := result{enqueueStart: start, enqueueEnd: start.Add(time.Second), lastDelivery: start.Add(2 * time.Second),
		delivered: 9, latencies: []time.Duration{time.Millisecond}}
	var b strings.Builder
	r.print(&b, options{n: 10, rate: 10, concurrency: 1, payload: 1, endpoints: 1})
	for _, want := range []string{"10 ok", "9 delivered", "p50=1.0ms", "1 jobs not delivered"} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("output missing %q:\n%s", want, b.String())
		}
	}
}
