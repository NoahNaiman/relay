package delivery

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/NoahNaiman/relay/internal/clock"
)

func TestBreaker(t *testing.T) {
	type step struct {
		op        string // allow, ok, fail, release, advance
		advance   time.Duration
		wantAllow bool
		state     string // expected state after the step; empty skips the check
	}
	const cooldown = 30 * time.Second
	cases := []struct {
		name  string
		steps []step
	}{
		{"stays closed below threshold", []step{
			{op: "fail", state: "closed"}, {op: "fail", state: "closed"}, {op: "fail", state: "closed"},
			{op: "fail", state: "closed"}, {op: "allow", wantAllow: true, state: "closed"},
		}},
		{"success resets the consecutive count", []step{
			{op: "fail"}, {op: "fail"}, {op: "fail"}, {op: "fail"}, {op: "ok", state: "closed"},
			{op: "fail"}, {op: "fail"}, {op: "fail"}, {op: "fail", state: "closed"},
		}},
		{"opens after five failures and rejects", []step{
			{op: "fail"}, {op: "fail"}, {op: "fail"}, {op: "fail"}, {op: "fail", state: "open"},
			{op: "allow", wantAllow: false, state: "open"},
			{op: "advance", advance: cooldown - time.Millisecond},
			{op: "allow", wantAllow: false, state: "open"},
		}},
		{"half-open probe success closes", []step{
			{op: "fail"}, {op: "fail"}, {op: "fail"}, {op: "fail"}, {op: "fail", state: "open"},
			{op: "advance", advance: cooldown},
			{op: "allow", wantAllow: true, state: "half-open"},
			{op: "allow", wantAllow: false, state: "half-open"}, // only one probe
			{op: "ok", state: "closed"},
			{op: "allow", wantAllow: true, state: "closed"},
		}},
		{"half-open probe failure reopens", []step{
			{op: "fail"}, {op: "fail"}, {op: "fail"}, {op: "fail"}, {op: "fail", state: "open"},
			{op: "advance", advance: cooldown},
			{op: "allow", wantAllow: true, state: "half-open"},
			{op: "fail", state: "open"},
			{op: "allow", wantAllow: false, state: "open"},
			{op: "advance", advance: cooldown},
			{op: "allow", wantAllow: true, state: "half-open"},
		}},
		{"released probe lets the next one through", []step{
			{op: "fail"}, {op: "fail"}, {op: "fail"}, {op: "fail"}, {op: "fail"},
			{op: "advance", advance: cooldown},
			{op: "allow", wantAllow: true, state: "half-open"},
			{op: "release", state: "half-open"},
			{op: "allow", wantAllow: true, state: "half-open"},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clk := clock.NewFake(time.Unix(0, 0))
			var transitions []BreakerState
			b := NewBreaker(5, cooldown, clk, func(s BreakerState) { transitions = append(transitions, s) })
			for i, s := range tc.steps {
				switch s.op {
				case "allow":
					ok, retryAt := b.Allow()
					if ok != s.wantAllow {
						t.Fatalf("step %d: allow = %v, want %v", i, ok, s.wantAllow)
					}
					if !ok && !retryAt.After(clk.Now()) {
						t.Fatalf("step %d: rejected without a future retryAt", i)
					}
				case "ok":
					b.Success()
				case "fail":
					b.Failure()
				case "release":
					b.Release()
				case "advance":
					clk.Advance(s.advance)
					continue
				}
				if s.state != "" && b.State().String() != s.state {
					t.Fatalf("step %d (%s): state %s, want %s", i, s.op, b.State(), s.state)
				}
			}
			for i := 1; i < len(transitions); i++ {
				if transitions[i] == transitions[i-1] {
					t.Fatal("onChange must fire only on real transitions")
				}
			}
		})
	}
}

func TestBreakerRetryAtIsEndOfCooldown(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	b := NewBreaker(1, 30*time.Second, clk, nil)
	b.Failure()
	clk.Advance(10 * time.Second)
	if ok, at := b.Allow(); ok || !at.Equal(time.Unix(30, 0)) {
		t.Fatalf("retryAt = %v, want end of cooldown", at)
	}
}

func TestBreakerStateString(t *testing.T) {
	if Closed.String() != "closed" || Open.String() != "open" || HalfOpen.String() != "half-open" {
		t.Fatal("state strings")
	}
}

func TestBreakersRegistry(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	changes := map[uuid.UUID]BreakerState{}
	r := NewBreakers(1, time.Second, clk, func(id uuid.UUID, s BreakerState) { changes[id] = s })
	a, b := uuid.New(), uuid.New()
	if first := r.For(a); r.For(a) != first {
		t.Fatal("registry must return the same breaker per endpoint")
	}
	r.For(a).Failure()
	if r.For(b).State() != Closed || changes[a] != Open {
		t.Fatalf("breakers must be independent per endpoint: %v", changes)
	}
	NewBreakers(1, time.Second, clk, nil).For(a).Failure() // nil callback is allowed
}
