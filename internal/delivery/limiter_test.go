package delivery

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/NoahNaiman/relay/internal/clock"
)

func TestLimiters(t *testing.T) {
	clk := clock.NewFake(time.Unix(1000, 0))
	l := NewLimiters(clk)
	ep, other := uuid.New(), uuid.New()

	// A fresh bucket allows a burst of max_rps immediately.
	for i := range 5 {
		if d, ok := l.Reserve(ep, 5, time.Second); !ok || d != 0 {
			t.Fatalf("burst request %d: delay=%s ok=%v", i, d, ok)
		}
	}
	// The sixth waits one refill interval (1/5 s).
	if d, ok := l.Reserve(ep, 5, time.Second); !ok || d != 200*time.Millisecond {
		t.Fatalf("sixth: delay=%s ok=%v, want 200ms", d, ok)
	}
	// Other endpoints are independent.
	if d, ok := l.Reserve(other, 1, time.Second); !ok || d != 0 {
		t.Fatalf("other endpoint: delay=%s ok=%v", d, ok)
	}
	// A wait longer than maxWait is refused and takes no token.
	if d, ok := l.Reserve(other, 1, 100*time.Millisecond); ok || d != time.Second {
		t.Fatalf("over maxWait: delay=%s ok=%v, want 1s refused", d, ok)
	}
	clk.Advance(time.Second)
	if d, ok := l.Reserve(other, 1, 0); !ok || d != 0 {
		t.Fatalf("after refill: delay=%s ok=%v (a refused reservation must not consume a token)", d, ok)
	}
}
