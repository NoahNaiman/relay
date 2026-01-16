package delivery

import (
	"testing"
	"time"
)

func TestBackoffCeiling(t *testing.T) {
	b := Backoff{Base: time.Second, Cap: 10 * time.Minute}
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{-1, time.Second},
		{0, time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{5, 32 * time.Second},
		{9, 512 * time.Second},
		{10, 10 * time.Minute}, // 1024s > cap
		{24, 10 * time.Minute},
		{1000, 10 * time.Minute}, // must not overflow
	}
	for _, tc := range cases {
		if got := b.ceiling(tc.attempt); got != tc.want {
			t.Errorf("ceiling(%d) = %s, want %s", tc.attempt, got, tc.want)
		}
	}
}

func TestBackoffFullJitter(t *testing.T) {
	var gotN int64
	b := Backoff{Base: 10 * time.Millisecond, Cap: 200 * time.Millisecond, Rand: func(n int64) int64 { gotN = n; return n - 1 }}
	if d := b.Delay(2); d != 40*time.Millisecond || gotN != int64(40*time.Millisecond)+1 {
		t.Fatalf("Delay(2) = %s (n=%d); full jitter must draw from [0, ceiling]", d, gotN)
	}
	b.Rand = func(int64) int64 { return 0 }
	if d := b.Delay(3); d != 0 {
		t.Fatalf("lower bound of jitter must be 0, got %s", d)
	}
	// Real randomness stays within bounds.
	b.Rand = nil
	for i := range 1000 {
		if d := b.Delay(i % 8); d < 0 || d > 200*time.Millisecond {
			t.Fatalf("Delay out of range: %s", d)
		}
	}
	if d := (Backoff{}).Delay(3); d != 0 {
		t.Fatalf("zero backoff should be 0, got %s", d)
	}
}
