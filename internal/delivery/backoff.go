package delivery

import (
	"math/rand/v2"
	"time"
)

// Backoff computes exponential backoff with full jitter:
// rand(0, min(Cap, Base·2^attempt)).
type Backoff struct {
	Base time.Duration
	Cap  time.Duration
	// Rand returns a value in [0, n). It defaults to math/rand/v2.Int64N.
	Rand func(n int64) int64
}

// Delay returns the wait before the next attempt, where attempt is the number
// of attempts already made before the one that just failed (0 for the first).
func (b Backoff) Delay(attempt int) time.Duration {
	ceiling := b.ceiling(attempt)
	if ceiling <= 0 {
		return 0
	}
	r := b.Rand
	if r == nil {
		r = rand.Int64N
	}
	return time.Duration(r(int64(ceiling) + 1))
}

func (b Backoff) ceiling(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	d := b.Base
	for range attempt {
		if d >= b.Cap/2 {
			return b.Cap
		}
		d *= 2
	}
	return min(d, b.Cap)
}
