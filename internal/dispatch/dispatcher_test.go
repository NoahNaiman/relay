package dispatch

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/NoahNaiman/relay/internal/clock"
	"github.com/NoahNaiman/relay/internal/config"
	"github.com/NoahNaiman/relay/internal/delivery"
	"github.com/NoahNaiman/relay/internal/store"
	"github.com/NoahNaiman/relay/internal/store/memory"
)

func configDefault() config.Config { return config.Default() }

func testConfig() Config {
	return Config{
		Workers:          4,
		BatchSize:        4,
		PollInterval:     5 * time.Millisecond,
		Lease:            5 * time.Second,
		RequestTimeout:   2 * time.Second,
		ShutdownTimeout:  2 * time.Second,
		BackoffBase:      time.Millisecond,
		BackoffCap:       5 * time.Millisecond,
		BreakerThreshold: 5,
		BreakerCooldown:  time.Second,
	}
}

// receiver is an httptest server whose behavior is scripted per request.
type receiver struct {
	*httptest.Server
	hits    atomic.Int64
	mu      sync.Mutex
	headers []http.Header
}

func newReceiver(t *testing.T, respond func(n int64, w http.ResponseWriter, r *http.Request)) *receiver {
	rc := &receiver{}
	rc.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Read the body first: the server only notices a client disconnect
		// (and cancels r.Context) once the body has been consumed.
		_, _ = io.Copy(io.Discard, r.Body)
		n := rc.hits.Add(1)
		rc.mu.Lock()
		rc.headers = append(rc.headers, r.Header.Clone())
		rc.mu.Unlock()
		respond(n, w, r)
	}))
	t.Cleanup(rc.Close)
	return rc
}

func status(code int) func(int64, http.ResponseWriter, *http.Request) {
	return func(_ int64, w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }
}

type fixture struct {
	t     *testing.T
	store *memory.Store
	clock clock.Clock
	obs   *recordingObserver
}

func newFixture(t *testing.T, clk clock.Clock) *fixture {
	return &fixture{t: t, store: memory.New(memory.WithClock(clk)), clock: clk, obs: &recordingObserver{}}
}

func (f *fixture) endpoint(url string, maxRPS, maxAttempts int) store.Endpoint {
	e, err := f.store.CreateEndpoint(context.Background(), store.Endpoint{URL: url, Secret: "k", MaxRPS: maxRPS, MaxAttempts: maxAttempts})
	if err != nil {
		f.t.Fatal(err)
	}
	return e
}

func (f *fixture) enqueue(ep store.Endpoint) uuid.UUID {
	j, _, err := f.store.Enqueue(context.Background(), store.NewJob{EndpointID: ep.ID, Payload: []byte(`{"n":1}`)})
	if err != nil {
		f.t.Fatal(err)
	}
	return j.ID
}

func (f *fixture) job(id uuid.UUID) store.Job {
	j, err := f.store.GetJob(context.Background(), id)
	if err != nil {
		f.t.Fatal(err)
	}
	return j
}

// start runs a dispatcher until the test ends (or stop is called).
func (f *fixture) start(cfg Config, opts ...Option) (d *Dispatcher, stop func() error) {
	opts = append([]Option{WithClock(f.clock), WithObserver(f.obs), WithRand(func(int64) int64 { return 0 })}, opts...)
	d = New(f.store, cfg, opts...)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- d.Run(ctx) }()
	var once sync.Once
	var err error
	stop = func() error {
		once.Do(func() { cancel(); err = <-errCh })
		return err
	}
	f.t.Cleanup(func() { _ = stop() })
	return d, stop
}

func (f *fixture) waitFor(what string, cond func() bool) {
	f.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	f.t.Fatalf("timed out waiting for %s", what)
}

func (f *fixture) waitStatus(id uuid.UUID, want store.Status) store.Job {
	f.t.Helper()
	var j store.Job
	f.waitFor("job "+string(want), func() bool { j = f.job(id); return j.Status == want })
	return j
}

type recordingObserver struct {
	mu       sync.Mutex
	results  map[string]int
	breakers map[uuid.UUID]bool
}

func (o *recordingObserver) Delivery(result string, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.results == nil {
		o.results = map[string]int{}
	}
	o.results[result]++
}

func (o *recordingObserver) BreakerChanged(id uuid.UUID, open bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.breakers == nil {
		o.breakers = map[uuid.UUID]bool{}
	}
	o.breakers[id] = open
}

func (o *recordingObserver) count(result string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.results[result]
}

func (o *recordingObserver) breakerOpen(id uuid.UUID) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.breakers[id]
}

func TestDeliversSignedRequest(t *testing.T) {
	f := newFixture(t, clock.Real{})
	rc := newReceiver(t, status(http.StatusOK))
	ep := f.endpoint(rc.URL, 10, 8)
	id := f.enqueue(ep)
	f.start(testConfig())

	j := f.waitStatus(id, store.StatusDelivered)
	if j.Attempts != 1 || rc.hits.Load() != 1 {
		t.Fatalf("attempts=%d hits=%d", j.Attempts, rc.hits.Load())
	}
	h := rc.headers[0]
	if h.Get(delivery.HeaderJobID) != id.String() || h.Get(delivery.HeaderAttempt) != "1" {
		t.Fatalf("headers: %v", h)
	}
	if err := delivery.Verify("k", h, []byte(`{"n":1}`), time.Now(), 5*time.Minute); err != nil {
		t.Fatalf("signature: %v", err)
	}
	if f.obs.count("delivered") != 1 {
		t.Fatalf("observer: %v", f.obs.results)
	}
}

func TestRetryThenSuccess(t *testing.T) {
	f := newFixture(t, clock.Real{})
	rc := newReceiver(t, func(n int64, w http.ResponseWriter, _ *http.Request) {
		if n <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	id := f.enqueue(f.endpoint(rc.URL, 100, 8))
	f.start(testConfig())

	j := f.waitStatus(id, store.StatusDelivered)
	if j.Attempts != 3 || j.LastError != "HTTP 503" {
		t.Fatalf("attempts=%d last_error=%q", j.Attempts, j.LastError)
	}
	if got := rc.headers[2].Get(delivery.HeaderAttempt); got != "3" {
		t.Fatalf("third request Relay-Attempt = %s", got)
	}
	if f.obs.count("retry") != 2 || f.obs.count("delivered") != 1 {
		t.Fatalf("observer: %v", f.obs.results)
	}
}

func TestRetriesExhaustedGoDead(t *testing.T) {
	f := newFixture(t, clock.Real{})
	rc := newReceiver(t, status(http.StatusInternalServerError))
	cfg := testConfig()
	cfg.BreakerThreshold = 100
	id := f.enqueue(f.endpoint(rc.URL, 100, 3))
	f.start(cfg)

	j := f.waitStatus(id, store.StatusDead)
	if j.Attempts != 3 || rc.hits.Load() != 3 {
		t.Fatalf("attempts=%d hits=%d", j.Attempts, rc.hits.Load())
	}
	if f.obs.count("dead") != 1 || f.obs.count("retry") != 2 {
		t.Fatalf("observer: %v", f.obs.results)
	}
}

func TestPermanentFailure(t *testing.T) {
	f := newFixture(t, clock.Real{})
	rc := newReceiver(t, status(http.StatusNotFound))
	id := f.enqueue(f.endpoint(rc.URL, 10, 8))
	bad := f.enqueue(f.endpoint("ftp://nope.test", 10, 8))
	f.start(testConfig())

	j := f.waitStatus(id, store.StatusDead)
	if j.Attempts != 1 || j.LastError != "HTTP 404" {
		t.Fatalf("attempts=%d last_error=%q", j.Attempts, j.LastError)
	}
	if j := f.waitStatus(bad, store.StatusDead); j.Attempts != 1 {
		t.Fatalf("invalid URL should die on the first attempt: %+v", j)
	}
}

func TestRetryAfterIsHonored(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC))
	f := newFixture(t, clk)
	rc := newReceiver(t, func(n int64, w http.ResponseWriter, _ *http.Request) {
		if n == 1 {
			w.Header().Set("Retry-After", "42")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	id := f.enqueue(f.endpoint(rc.URL, 10, 8))
	start := clk.Now()
	f.start(testConfig())

	var j store.Job
	f.waitFor("first attempt", func() bool { j = f.job(id); return j.Attempts == 1 && j.Status == store.StatusPending })
	if want := start.Add(42 * time.Second); !j.NextAttemptAt.Equal(want) {
		t.Fatalf("next_attempt_at = %v, want %v", j.NextAttemptAt, want)
	}
	clk.Advance(41 * time.Second)
	time.Sleep(30 * time.Millisecond)
	if rc.hits.Load() != 1 {
		t.Fatal("retried before Retry-After elapsed")
	}
	clk.Advance(time.Second)
	f.waitStatus(id, store.StatusDelivered)
}

func TestBreakerOpensAndRecovers(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC))
	f := newFixture(t, clk)
	var healthy atomic.Bool
	rc := newReceiver(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		if healthy.Load() {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusBadGateway)
	})
	cfg := testConfig()
	cfg.Workers, cfg.BatchSize = 1, 1 // serial, so exactly Threshold requests trip it
	cfg.BreakerThreshold = 3
	cfg.BreakerCooldown = 30 * time.Second
	ep := f.endpoint(rc.URL, 100, 25)
	ids := []uuid.UUID{f.enqueue(ep), f.enqueue(ep), f.enqueue(ep), f.enqueue(ep)}
	opened := clk.Now()
	f.start(cfg)

	// Backoff jitter is pinned to 0, so failed jobs are due again immediately
	// and the breaker trips after exactly three requests.
	f.waitFor("breaker to open", func() bool { return f.obs.breakerOpen(ep.ID) })
	f.waitFor("all jobs rescheduled behind the breaker", func() bool {
		for _, id := range ids {
			if j := f.job(id); j.Status != store.StatusPending || !j.NextAttemptAt.Equal(opened.Add(cfg.BreakerCooldown)) {
				return false
			}
		}
		return true
	})
	if rc.hits.Load() != 3 {
		t.Fatalf("hits = %d, want 3 (open breaker must stop requests)", rc.hits.Load())
	}
	attempts := 0
	for _, id := range ids {
		j := f.job(id)
		attempts += j.Attempts
		if j.LastError != msgBreakerOpen {
			t.Fatalf("last_error = %q", j.LastError)
		}
	}
	if attempts != 3 {
		t.Fatalf("total attempts = %d; jobs rescheduled by the breaker must not count an attempt", attempts)
	}

	healthy.Store(true)
	clk.Advance(cfg.BreakerCooldown)
	for _, id := range ids {
		f.waitStatus(id, store.StatusDelivered)
	}
	if f.obs.breakerOpen(ep.ID) {
		t.Fatal("successful probe should close the breaker")
	}
}

func TestRateLimitReschedules(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC))
	f := newFixture(t, clk)
	rc := newReceiver(t, status(http.StatusOK))
	cfg := testConfig()
	cfg.MaxLimiterWait = time.Millisecond
	ep := f.endpoint(rc.URL, 1, 8)
	a, b := f.enqueue(ep), f.enqueue(ep)
	f.start(cfg)

	f.waitFor("one delivery and one rate-limited reschedule", func() bool {
		ja, jb := f.job(a), f.job(b)
		return (ja.Status == store.StatusDelivered) != (jb.Status == store.StatusDelivered) &&
			(ja.LastError == msgRateLimited || jb.LastError == msgRateLimited)
	})
	limited := f.job(a)
	if limited.Status == store.StatusDelivered {
		limited = f.job(b)
	}
	if limited.Attempts != 0 || limited.Status != store.StatusPending {
		t.Fatalf("rate-limited job must not count an attempt: %+v", limited)
	}
	clk.Advance(time.Second)
	f.waitStatus(limited.ID, store.StatusDelivered)
}

func TestLimiterWaitsWhenShort(t *testing.T) {
	f := newFixture(t, clock.Real{})
	rc := newReceiver(t, status(http.StatusOK))
	cfg := testConfig()
	cfg.MaxLimiterWait = time.Second
	ep := f.endpoint(rc.URL, 20, 8) // burst 20, then one every 50ms
	var ids []uuid.UUID
	for range 22 {
		ids = append(ids, f.enqueue(ep))
	}
	f.start(cfg)
	for _, id := range ids {
		if j := f.waitStatus(id, store.StatusDelivered); j.Attempts != 1 || j.LastError != "" {
			t.Fatalf("short limiter waits should sleep, not reschedule: %+v", j)
		}
	}
}

func TestGracefulShutdownReleasesLeases(t *testing.T) {
	f := newFixture(t, clock.Real{})
	arrived := make(chan struct{}, 10)
	unblock := make(chan struct{})
	rc := newReceiver(t, func(_ int64, w http.ResponseWriter, r *http.Request) {
		arrived <- struct{}{}
		select {
		case <-unblock:
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusOK)
	})
	t.Cleanup(func() { close(unblock) })
	cfg := testConfig()
	cfg.ShutdownTimeout = 100 * time.Millisecond
	id := f.enqueue(f.endpoint(rc.URL, 10, 8))
	_, stop := f.start(cfg)
	<-arrived

	start := time.Now()
	if err := stop(); err != nil {
		t.Fatalf("Run returned %v", err)
	}
	if took := time.Since(start); took < cfg.ShutdownTimeout || took > 2*time.Second {
		t.Fatalf("shutdown took %s; it should wait for the deadline and then return promptly", took)
	}
	j := f.job(id)
	if j.Status != store.StatusPending || j.Attempts != 0 || j.LeaseToken != uuid.Nil || j.NextAttemptAt.After(time.Now()) {
		t.Fatalf("lease should be released as pending, due now, no attempt counted: %+v", j)
	}
}

func TestGracefulShutdownFinishesInFlight(t *testing.T) {
	f := newFixture(t, clock.Real{})
	arrived := make(chan struct{}, 1)
	rc := newReceiver(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		arrived <- struct{}{}
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})
	id := f.enqueue(f.endpoint(rc.URL, 10, 8))
	_, stop := f.start(testConfig())
	<-arrived
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if j := f.job(id); j.Status != store.StatusDelivered {
		t.Fatalf("in-flight delivery should finish during shutdown: %+v", j)
	}
}

func TestKillWritesNothing(t *testing.T) {
	f := newFixture(t, clock.Real{})
	arrived := make(chan struct{}, 1)
	rc := newReceiver(t, func(_ int64, w http.ResponseWriter, r *http.Request) {
		arrived <- struct{}{}
		<-r.Context().Done()
		w.WriteHeader(http.StatusOK)
	})
	id := f.enqueue(f.endpoint(rc.URL, 10, 8))
	d, stop := f.start(testConfig())
	<-arrived
	d.Kill()
	if err := stop(); !errors.Is(err, ErrKilled) {
		t.Fatalf("Run returned %v, want ErrKilled", err)
	}
	time.Sleep(20 * time.Millisecond)
	if j := f.job(id); j.Status != store.StatusInFlight {
		t.Fatalf("a killed dispatcher must leave its lease to expire: %+v", j)
	}
}

func TestExpiredLeaseIsRedeliveredAndStaleWriteRejected(t *testing.T) {
	f := newFixture(t, clock.Real{})
	var first atomic.Bool
	first.Store(true)
	rc := newReceiver(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		if first.CompareAndSwap(true, false) {
			time.Sleep(300 * time.Millisecond) // outlives its 100ms lease
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	cfg := testConfig()
	cfg.Lease = 100 * time.Millisecond
	cfg.RequestTimeout = time.Second
	id := f.enqueue(f.endpoint(rc.URL, 10, 8))
	f.start(cfg)

	j := f.waitStatus(id, store.StatusDelivered)
	time.Sleep(400 * time.Millisecond) // let the slow worker's stale write land
	j2 := f.job(id)
	if j2.Status != store.StatusDelivered || j2.Attempts != j.Attempts {
		t.Fatalf("stale write overwrote the result: %+v", j2)
	}
	if rc.hits.Load() != 2 {
		t.Fatalf("hits = %d, want 2", rc.hits.Load())
	}
}

func TestClaimErrorsAreSurvived(t *testing.T) {
	s := &flakyStore{Store: memory.New()}
	ep, _ := s.CreateEndpoint(context.Background(), store.Endpoint{URL: "http://127.0.0.1:1", Secret: "k", MaxRPS: 1, MaxAttempts: 1})
	_, _, _ = s.Enqueue(context.Background(), store.NewJob{EndpointID: ep.ID})
	d := New(s, testConfig())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for s.calls.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if s.calls.Load() < 3 {
		t.Fatal("dispatcher should keep polling after claim errors")
	}
}

type flakyStore struct {
	store.Store
	calls atomic.Int64
}

func (f *flakyStore) Claim(context.Context, int, time.Duration) ([]store.Claimed, error) {
	f.calls.Add(1)
	return nil, errors.New("database is down")
}

func TestCompleteErrorIsLogged(t *testing.T) {
	s := &failingCompleteStore{Store: memory.New()}
	rc := newReceiver(t, status(http.StatusOK))
	ep, _ := s.CreateEndpoint(context.Background(), store.Endpoint{URL: rc.URL, Secret: "k", MaxRPS: 10, MaxAttempts: 1})
	j, _, _ := s.Enqueue(context.Background(), store.NewJob{EndpointID: ep.ID})
	d := New(s, testConfig())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for s.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetJob(context.Background(), j.ID)
	if got.Status != store.StatusInFlight {
		t.Fatalf("a failed completion leaves the job leased for later recovery: %+v", got)
	}
}

type failingCompleteStore struct {
	store.Store
	calls atomic.Int64
}

func (f *failingCompleteStore) Complete(context.Context, uuid.UUID, uuid.UUID, store.Result) (store.Status, error) {
	f.calls.Add(1)
	return "", errors.New("write failed: " + strconv.Itoa(int(f.calls.Load())))
}

// Regression test: with fewer workers than the batch size, a claim that fills
// every free slot must not wait for the next poll tick. Before the fix this
// delivered one claim's worth of jobs per PollInterval (about 25s here).
func TestBusyQueueDoesNotWaitForPollTick(t *testing.T) {
	f := newFixture(t, clock.Real{})
	rc := newReceiver(t, status(http.StatusOK))
	cfg := testConfig()
	cfg.Workers, cfg.BatchSize = 2, 32
	cfg.PollInterval = time.Second
	ep := f.endpoint(rc.URL, 1000, 8)
	var ids []uuid.UUID
	for range 50 {
		ids = append(ids, f.enqueue(ep))
	}
	start := time.Now()
	f.start(cfg)
	for _, id := range ids {
		f.waitStatus(id, store.StatusDelivered)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("50 jobs took %s; a busy queue is waiting for poll ticks", took)
	}
}

func TestShutdownReleasesBufferedJobsImmediately(t *testing.T) {
	f := newFixture(t, clock.Real{})
	arrived := make(chan struct{}, 10)
	unblock := make(chan struct{})
	rc := newReceiver(t, func(_ int64, w http.ResponseWriter, r *http.Request) {
		arrived <- struct{}{}
		select {
		case <-unblock:
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusOK)
	})
	t.Cleanup(func() { close(unblock) })
	cfg := testConfig()
	cfg.Workers, cfg.BatchSize = 1, 4
	cfg.ShutdownTimeout = 300 * time.Millisecond
	ep := f.endpoint(rc.URL, 100, 8)
	var ids []uuid.UUID
	for range 3 {
		ids = append(ids, f.enqueue(ep))
	}
	_, stop := f.start(cfg)
	<-arrived // one job is in flight; the other two wait in the buffer

	done := make(chan error, 1)
	go func() { done <- stop() }()
	f.waitFor("buffered jobs released before the in-flight deadline", func() bool {
		released := 0
		for _, id := range ids {
			if j := f.job(id); j.Status == store.StatusPending && j.LeaseToken == uuid.Nil {
				released++
			}
		}
		return released == 2
	})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if j := f.job(id); j.Status != store.StatusPending || j.Attempts != 0 {
			t.Fatalf("every unfinished job should end pending with no attempt counted: %+v", j)
		}
	}
	if len(arrived) != 0 {
		t.Fatal("buffered jobs must not be delivered after shutdown begins")
	}
}

// Regression test: connections must be reused across deliveries to the same
// host. With Go's default of 2 idle connections per host, 8 busy workers open
// a new connection for most requests and eventually exhaust ephemeral ports.
func TestConnectionsAreReused(t *testing.T) {
	f := newFixture(t, clock.Real{})
	var conns atomic.Int64
	rc := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	rc.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	rc.Start()
	t.Cleanup(rc.Close)
	cfg := testConfig()
	cfg.Workers, cfg.BatchSize = 8, 16
	ep := f.endpoint(rc.URL, 100000, 8)
	var ids []uuid.UUID
	for range 400 {
		ids = append(ids, f.enqueue(ep))
	}
	f.start(cfg)
	for _, id := range ids {
		f.waitStatus(id, store.StatusDelivered)
	}
	if n := conns.Load(); n > int64(2*cfg.Workers) {
		t.Fatalf("400 deliveries opened %d connections; want at most %d (one per worker, with slack)", n, 2*cfg.Workers)
	}
}
