//go:build chaos

package chaos

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/NoahNaiman/relay/internal/store"
)

// TestStoreChaos hammers the store with workers that complete, retry, crash
// (abandon without writing) and oversleep their leases, then proves that no
// job was lost and that every provably stale write was rejected.
func TestStoreChaos(t *testing.T) {
	const (
		jobs        = 5000
		workers     = 8
		batch       = 25
		lease       = 200 * time.Millisecond
		maxAttempts = 5
		pComplete   = 0.60
		pRetry      = 0.25 // the remaining 15% are abandoned
		pOversleep  = 0.05
		budget      = 30 * time.Second
	)
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed=%d", seed)

	s := newStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	start := time.Now()

	ep, err := s.CreateEndpoint(ctx, store.Endpoint{URL: "http://chaos.test", Secret: "x", MaxRPS: 10, MaxAttempts: maxAttempts})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]uuid.UUID, 0, jobs)
	for range jobs {
		j, _, err := s.Enqueue(ctx, store.NewJob{EndpointID: ep.ID, Payload: []byte("{}")})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, j.ID)
	}
	t.Logf("enqueued %d jobs in %s", jobs, time.Since(start).Round(time.Millisecond))

	var (
		claims, abandoned, oversleeps  atomic.Int64
		knownStale, knownStaleRejected atomic.Int64
		otherStaleRejected             atomic.Int64
		deliveredMu                    sync.Mutex
		deliveredWrites                = map[uuid.UUID]int{}
		slow                           sync.WaitGroup
		failures                       = make(chan error, 64)
	)
	fail := func(err error) {
		select {
		case failures <- err:
		default:
		}
		cancel()
	}

	complete := func(c store.Claimed, r store.Result, oversleep bool) {
		if oversleep {
			time.Sleep(lease + 50*time.Millisecond)
		}
		// If the lease token has changed, another worker reclaimed the job and
		// this write is provably stale. Tokens never revert, so this check is sound.
		stale := false
		if oversleep {
			cur, err := s.GetJob(ctx, c.Job.ID)
			if err != nil {
				fail(err)
				return
			}
			stale = cur.LeaseToken != c.Job.LeaseToken
		}
		_, err := s.Complete(ctx, c.Job.ID, c.Job.LeaseToken, r)
		switch {
		case errors.Is(err, store.ErrStaleLease):
			if stale {
				knownStaleRejected.Add(1)
			} else {
				otherStaleRejected.Add(1)
			}
		case err != nil:
			if ctx.Err() == nil {
				fail(err)
			}
		default:
			if r.Outcome == store.OutcomeDelivered {
				deliveredMu.Lock()
				deliveredWrites[c.Job.ID]++
				deliveredMu.Unlock()
			}
		}
		if stale {
			knownStale.Add(1)
		}
	}

	done := func() bool {
		counts, err := s.CountByStatus(ctx)
		if err != nil {
			if ctx.Err() == nil {
				fail(err)
			}
			return true
		}
		return counts[store.StatusPending] == 0 && counts[store.StatusInFlight] == 0
	}

	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, uint64(w)))
			for ctx.Err() == nil {
				claimed, err := s.Claim(ctx, batch, lease)
				if err != nil {
					if ctx.Err() == nil {
						fail(err)
					}
					return
				}
				if len(claimed) == 0 {
					if done() {
						return
					}
					time.Sleep(20 * time.Millisecond)
					continue
				}
				claims.Add(int64(len(claimed)))
				for _, c := range claimed {
					p := rng.Float64()
					var r store.Result
					switch {
					case p < pComplete:
						r = store.Result{Outcome: store.OutcomeDelivered}
					case p < pComplete+pRetry:
						r = store.Result{
							Outcome:       store.OutcomeRetry,
							NextAttemptAt: time.Now().Add(time.Duration(rng.IntN(50)) * time.Millisecond),
							Error:         "chaos: simulated failure",
						}
					default:
						abandoned.Add(1) // simulated crash: write nothing
						continue
					}
					if rng.Float64() < pOversleep {
						oversleeps.Add(1)
						slow.Add(1)
						go func() {
							defer slow.Done()
							complete(c, r, true)
						}()
						continue
					}
					complete(c, r, false)
				}
			}
		}()
	}
	wg.Wait()
	slow.Wait()
	close(failures)
	for err := range failures {
		t.Fatalf("worker error: %v", err)
	}
	if ctx.Err() != nil {
		t.Fatalf("did not finish within %s", budget)
	}
	elapsed := time.Since(start)

	// Zero lost jobs: every enqueued job reached a terminal state.
	var delivered, dead int
	for _, id := range ids {
		j, err := s.GetJob(context.Background(), id)
		if err != nil {
			t.Fatalf("job %s lost: %v", id, err)
		}
		switch j.Status {
		case store.StatusDelivered:
			delivered++
			if deliveredWrites[id] != 1 {
				t.Errorf("job %s is delivered but the store accepted %d delivered writes", id, deliveredWrites[id])
			}
		case store.StatusDead:
			dead++
			if deliveredWrites[id] != 0 {
				t.Errorf("job %s is dead but the store accepted a delivered write", id)
			}
		default:
			t.Errorf("job %s lost in status %s", id, j.Status)
		}
	}
	for id, n := range deliveredWrites {
		if n > 1 {
			t.Errorf("job %s delivered %d times by the store's accounting", id, n)
		}
	}
	counts, err := s.CountByStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if counts[store.StatusDelivered]+counts[store.StatusDead] != jobs {
		t.Errorf("store counts %v do not add up to %d", counts, jobs)
	}
	if knownStale.Load() != knownStaleRejected.Load() {
		t.Errorf("%d provably stale writes, only %d rejected", knownStale.Load(), knownStaleRejected.Load())
	}
	if knownStale.Load() == 0 {
		t.Error("no provably stale writes were exercised; the guard went untested")
	}
	if abandoned.Load() == 0 {
		t.Error("no jobs were abandoned; lease recovery went untested")
	}
	t.Logf("elapsed=%s delivered=%d dead=%d lost=%d claims=%d abandoned=%d oversleeps=%d stale_rejected(provable)=%d/%d stale_rejected(other)=%d",
		elapsed.Round(time.Millisecond), delivered, dead, jobs-delivered-dead, claims.Load(), abandoned.Load(),
		oversleeps.Load(), knownStaleRejected.Load(), knownStale.Load(), otherStaleRejected.Load())
}
