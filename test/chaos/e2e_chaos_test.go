//go:build chaos

package chaos

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/NoahNaiman/relay/internal/config"
	"github.com/NoahNaiman/relay/internal/delivery"
	"github.com/NoahNaiman/relay/internal/dispatch"
	"github.com/NoahNaiman/relay/internal/store"
)

// TestEndToEndChaos runs three dispatchers against a flaky receiver and
// crashes a random one every 500ms, without graceful shutdown. It proves
// that every job still reaches a terminal state and that every delivered job
// was actually received.
func TestEndToEndChaos(t *testing.T) {
	const (
		jobs        = 2000
		endpoints   = 4
		instances   = 3
		failureRate = 0.30
		killEvery   = 500 * time.Millisecond
		budget      = 60 * time.Second
	)
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed=%d", seed)

	// All timing comes from configuration, exactly as the server reads it.
	cfg, err := config.FromLookup(func(k string) (string, bool) {
		v, ok := map[string]string{
			"RELAY_LEASE":            "2s",
			"RELAY_REQUEST_TIMEOUT":  "1s",
			"RELAY_BACKOFF_BASE":     "10ms",
			"RELAY_BACKOFF_CAP":      "200ms",
			"RELAY_BREAKER_COOLDOWN": "200ms",
			"RELAY_POLL_INTERVAL":    "20ms",
		}[k]
		return v, ok
	})
	if err != nil {
		t.Fatal(err)
	}
	dcfg := dispatch.ConfigFrom(cfg)

	// The receiver fails 30% of requests and records every successful receipt.
	var (
		rmu        sync.Mutex
		rng        = rand.New(rand.NewPCG(seed, 0))
		received   = map[string]int{}
		badSigs    atomic.Int64
		requests   atomic.Int64
		secretByEP = map[string]string{}
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		body, _ := io.ReadAll(r.Body)
		if err := delivery.Verify(secretByEP[r.URL.Path], r.Header, body, time.Now(), 5*time.Minute); err != nil {
			badSigs.Add(1)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		rmu.Lock()
		fail := rng.Float64() < failureRate
		if !fail {
			received[r.Header.Get(delivery.HeaderJobID)]++
		}
		rmu.Unlock()
		if fail {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := newStore(t)
	ctx := context.Background()
	start := time.Now()
	var eps []store.Endpoint
	for i := range endpoints {
		path := "/hook/" + string(rune('a'+i))
		secret := uuid.NewString()
		secretByEP[path] = secret
		ep, err := s.CreateEndpoint(ctx, store.Endpoint{URL: srv.URL + path, Secret: secret, MaxRPS: 5000, MaxAttempts: 8})
		if err != nil {
			t.Fatal(err)
		}
		eps = append(eps, ep)
	}
	ids := make([]uuid.UUID, 0, jobs)
	for i := range jobs {
		j, _, err := s.Enqueue(ctx, store.NewJob{EndpointID: eps[i%endpoints].ID, Payload: []byte(`{"chaos":true}`)})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, j.ID)
	}
	t.Logf("enqueued %d jobs across %d endpoints in %s", jobs, endpoints, time.Since(start).Round(time.Millisecond))

	type instance struct {
		d    *dispatch.Dispatcher
		stop context.CancelFunc
		done chan error
	}
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	launch := func() *instance {
		ictx, cancel := context.WithCancel(context.Background())
		in := &instance{d: dispatch.New(s, dcfg, dispatch.WithLogger(logger)), stop: cancel, done: make(chan error, 1)}
		go func() { in.done <- in.d.Run(ictx) }()
		return in
	}
	running := make([]*instance, instances)
	for i := range running {
		running[i] = launch()
	}

	allTerminal := func() bool {
		c, err := s.CountByStatus(ctx)
		if err != nil {
			t.Fatalf("CountByStatus: %v", err)
		}
		return c[store.StatusPending] == 0 && c[store.StatusInFlight] == 0
	}

	deadline := time.Now().Add(budget)
	kills := 0
	ticker := time.NewTicker(killEvery)
	defer ticker.Stop()
	for !allTerminal() {
		if time.Now().After(deadline) {
			c, _ := s.CountByStatus(ctx)
			t.Fatalf("not finished within %s: %v", budget, c)
		}
		<-ticker.C
		// Crash a random instance (no graceful shutdown) and start a fresh one.
		i := rand.IntN(instances)
		running[i].d.Kill()
		if err := <-running[i].done; !errors.Is(err, dispatch.ErrKilled) {
			t.Fatalf("killed instance returned %v", err)
		}
		running[i].stop()
		running[i] = launch()
		kills++
	}
	for _, in := range running {
		in.stop()
		if err := <-in.done; err != nil {
			t.Fatalf("graceful stop: %v", err)
		}
	}
	elapsed := time.Since(start)

	var delivered, dead, duplicates int
	rmu.Lock()
	defer rmu.Unlock()
	for _, id := range ids {
		j, err := s.GetJob(ctx, id)
		if err != nil {
			t.Fatalf("job %s lost: %v", id, err)
		}
		switch j.Status {
		case store.StatusDelivered:
			delivered++
			n := received[id.String()]
			if n == 0 {
				t.Errorf("job %s marked delivered but never received", id)
			}
			duplicates += max(0, n-1)
		case store.StatusDead:
			dead++
		default:
			t.Errorf("job %s lost in status %s", id, j.Status)
		}
	}
	if badSigs.Load() != 0 {
		t.Errorf("%d requests failed signature verification", badSigs.Load())
	}
	if kills == 0 {
		t.Error("no instances were killed; chaos went untested")
	}
	lost := jobs - delivered - dead
	if lost != 0 {
		t.Errorf("%d jobs lost", lost)
	}
	t.Logf("elapsed=%s delivered=%d dead=%d lost=%d kills=%d requests=%d duplicate_receipts=%d duplicate_rate=%.2f%%",
		elapsed.Round(time.Millisecond), delivered, dead, lost, kills, requests.Load(), duplicates,
		100*float64(duplicates)/float64(max(delivered, 1)))
}
