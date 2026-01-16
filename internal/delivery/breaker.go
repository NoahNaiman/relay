package delivery

import (
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/NoahNaiman/relay/internal/clock"
)

// BreakerState is the state of a circuit breaker.
type BreakerState int

// Breaker states.
const (
	Closed BreakerState = iota
	Open
	HalfOpen
)

func (s BreakerState) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	default:
		return "half-open"
	}
}

// Breaker is a consecutive-failure circuit breaker. It opens after Threshold
// consecutive failures, stays open for Cooldown, then lets exactly one probe
// through (half-open). The probe's success closes it; its failure reopens it.
type Breaker struct {
	threshold int
	cooldown  time.Duration
	clock     clock.Clock
	onChange  func(BreakerState)

	mu        sync.Mutex
	state     BreakerState
	failures  int
	openUntil time.Time
	probing   bool
}

// NewBreaker returns a closed breaker. onChange, if non-nil, is called (with
// the lock held, so it must not call back into the breaker) on every transition.
func NewBreaker(threshold int, cooldown time.Duration, clk clock.Clock, onChange func(BreakerState)) *Breaker {
	return &Breaker{threshold: threshold, cooldown: cooldown, clock: clk, onChange: onChange}
}

func (b *Breaker) setState(s BreakerState) {
	if b.state == s {
		return
	}
	b.state = s
	if b.onChange != nil {
		b.onChange(s)
	}
}

// Allow reports whether a request may proceed. When it may not, retryAt is
// when the caller should try again (the end of the cooldown).
func (b *Breaker) Allow() (ok bool, retryAt time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.clock.Now()
	switch b.state {
	case Closed:
		return true, time.Time{}
	case Open:
		if now.Before(b.openUntil) {
			return false, b.openUntil
		}
		b.setState(HalfOpen)
		b.probing = true
		return true, time.Time{}
	default: // HalfOpen: one probe at a time
		if !b.probing {
			b.probing = true
			return true, time.Time{}
		}
		return false, now.Add(b.cooldown)
	}
}

// Success records a successful request.
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.probing = false
	b.setState(Closed)
}

// Failure records a failed request.
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	if b.state == HalfOpen || b.failures >= b.threshold {
		b.probing = false
		b.openUntil = b.clock.Now().Add(b.cooldown)
		b.setState(Open)
	}
}

// Release ends a probe that produced neither success nor failure (for example
// a permanent 4xx), so the next request can probe instead.
func (b *Breaker) Release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.probing = false
}

// State returns the current state without side effects.
func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Breakers is a registry of per-endpoint breakers, created on first use.
type Breakers struct {
	threshold int
	cooldown  time.Duration
	clock     clock.Clock
	onChange  func(endpointID uuid.UUID, s BreakerState)

	mu sync.Mutex
	m  map[uuid.UUID]*Breaker
}

// NewBreakers returns an empty registry.
func NewBreakers(threshold int, cooldown time.Duration, clk clock.Clock, onChange func(uuid.UUID, BreakerState)) *Breakers {
	return &Breakers{threshold: threshold, cooldown: cooldown, clock: clk, onChange: onChange, m: map[uuid.UUID]*Breaker{}}
}

// For returns the breaker for an endpoint.
func (r *Breakers) For(endpointID uuid.UUID) *Breaker {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.m[endpointID]
	if !ok {
		var cb func(BreakerState)
		if r.onChange != nil {
			cb = func(s BreakerState) { r.onChange(endpointID, s) }
		}
		b = NewBreaker(r.threshold, r.cooldown, r.clock, cb)
		r.m[endpointID] = b
	}
	return b
}
