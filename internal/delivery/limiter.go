package delivery

import (
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/time/rate"

	"github.com/NoahNaiman/relay/internal/clock"
)

// Limiters is a registry of per-endpoint token buckets. Each bucket refills
// at max_rps tokens per second with a burst of max_rps. It is in-process only.
type Limiters struct {
	clock clock.Clock

	mu sync.Mutex
	m  map[uuid.UUID]*rate.Limiter
}

// NewLimiters returns an empty registry.
func NewLimiters(clk clock.Clock) *Limiters {
	return &Limiters{clock: clk, m: map[uuid.UUID]*rate.Limiter{}}
}

func (l *Limiters) get(endpointID uuid.UUID, maxRPS int) *rate.Limiter {
	l.mu.Lock()
	defer l.mu.Unlock()
	lim, ok := l.m[endpointID]
	if !ok {
		lim = rate.NewLimiter(rate.Limit(maxRPS), maxRPS)
		l.m[endpointID] = lim
	}
	return lim
}

// Reserve takes a token for the endpoint and returns how long the caller must
// wait before sending. If the wait exceeds maxWait, no token is taken and
// ok is false; the caller should reschedule the job after the returned delay.
func (l *Limiters) Reserve(endpointID uuid.UUID, maxRPS int, maxWait time.Duration) (delay time.Duration, ok bool) {
	now := l.clock.Now()
	r := l.get(endpointID, maxRPS).ReserveN(now, 1)
	if !r.OK() {
		return maxWait, false
	}
	delay = r.DelayFrom(now)
	if delay > maxWait {
		r.CancelAt(now)
		return delay, false
	}
	return delay, true
}
