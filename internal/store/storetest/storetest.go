// Package storetest is the contract suite every store.Store implementation
// must pass. Implementations call Run from their own tests.
package storetest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/NoahNaiman/relay/internal/clock"
	"github.com/NoahNaiman/relay/internal/store"
)

// Factory returns a new, empty store that reads time from clk.
type Factory func(t *testing.T, clk clock.Clock) store.Store

// Epoch is the fake clock's starting point in every contract test.
var Epoch = time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)

const lease = 30 * time.Second

// Run executes the full contract suite.
func Run(t *testing.T, newStore Factory) {
	tests := []struct {
		name string
		fn   func(*testing.T, store.Store, *clock.Fake)
	}{
		{"Endpoints", testEndpoints},
		{"EnqueueAndGet", testEnqueueAndGet},
		{"IdempotentEnqueue", testIdempotentEnqueue},
		{"ClaimOnlyDue", testClaimOnlyDue},
		{"ClaimOrderAndLimit", testClaimOrderAndLimit},
		{"ConcurrentClaimersNeverOverlap", testConcurrentClaimers},
		{"ReclaimAfterLeaseExpiry", testReclaimAfterLeaseExpiry},
		{"StaleCompletionGuard", testStaleCompletionGuard},
		{"RetryScheduling", testRetryScheduling},
		{"Reschedule", testReschedule},
		{"DeadLettering", testDeadLettering},
		{"Pagination", testPagination},
		{"Replay", testReplay},
		{"CountByStatus", testCountByStatus},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clk := clock.NewFake(Epoch)
			s := newStore(t, clk)
			t.Cleanup(s.Close)
			tc.fn(t, s, clk)
		})
	}
}

func ctx() context.Context { return context.Background() }

func mustEndpoint(t *testing.T, s store.Store, maxAttempts int) store.Endpoint {
	t.Helper()
	e, err := s.CreateEndpoint(ctx(), store.Endpoint{URL: "http://example.test/hook", Secret: "s3cret", MaxRPS: 10, MaxAttempts: maxAttempts})
	if err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}
	return e
}

func mustEnqueue(t *testing.T, s store.Store, endpointID uuid.UUID) store.Job {
	t.Helper()
	j, created, err := s.Enqueue(ctx(), store.NewJob{EndpointID: endpointID, Payload: []byte(`{}`)})
	if err != nil || !created {
		t.Fatalf("Enqueue: created=%v err=%v", created, err)
	}
	return j
}

func mustClaim(t *testing.T, s store.Store, n int) []store.Claimed {
	t.Helper()
	c, err := s.Claim(ctx(), n, lease)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	return c
}

func mustClaimOne(t *testing.T, s store.Store) store.Claimed {
	t.Helper()
	c := mustClaim(t, s, 1)
	if len(c) != 1 {
		t.Fatalf("expected one claimed job, got %d", len(c))
	}
	return c[0]
}

func mustGet(t *testing.T, s store.Store, id uuid.UUID) store.Job {
	t.Helper()
	j, err := s.GetJob(ctx(), id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	return j
}

func mustComplete(t *testing.T, s store.Store, c store.Claimed, r store.Result) store.Status {
	t.Helper()
	st, err := s.Complete(ctx(), c.Job.ID, c.Job.LeaseToken, r)
	if err != nil {
		t.Fatalf("Complete(%v): %v", r.Outcome, err)
	}
	return st
}

func testEndpoints(t *testing.T, s store.Store, clk *clock.Fake) {
	e := mustEndpoint(t, s, 8)
	if e.ID == uuid.Nil || !e.CreatedAt.Equal(clk.Now()) {
		t.Fatalf("endpoint not initialized: %+v", e)
	}
	got, err := s.GetEndpoint(ctx(), e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got != e {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, e)
	}
	if _, err := s.GetEndpoint(ctx(), uuid.New()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if err := s.Ping(ctx()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func testEnqueueAndGet(t *testing.T, s store.Store, clk *clock.Fake) {
	e := mustEndpoint(t, s, 8)
	j, created, err := s.Enqueue(ctx(), store.NewJob{
		EndpointID: e.ID,
		Payload:    []byte("hello"),
		Headers:    map[string]string{"X-Trace": "abc"},
	})
	if err != nil || !created {
		t.Fatalf("Enqueue: created=%v err=%v", created, err)
	}
	if j.Status != store.StatusPending || j.Attempts != 0 || !j.NextAttemptAt.Equal(clk.Now()) {
		t.Fatalf("new job should be pending and due now: %+v", j)
	}
	got := mustGet(t, s, j.ID)
	if string(got.Payload) != "hello" || got.Headers["X-Trace"] != "abc" || got.EndpointID != e.ID {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	if _, err := s.GetJob(ctx(), uuid.New()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want ErrNotFound for unknown job, got %v", err)
	}
	if _, _, err := s.Enqueue(ctx(), store.NewJob{EndpointID: uuid.New()}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want ErrNotFound for unknown endpoint, got %v", err)
	}
	// A nil payload and nil headers are stored as empty values.
	empty, _, err := s.Enqueue(ctx(), store.NewJob{EndpointID: e.ID})
	if err != nil {
		t.Fatal(err)
	}
	empty = mustGet(t, s, empty.ID)
	if empty.Payload == nil || len(empty.Payload) != 0 || empty.Headers == nil {
		t.Fatalf("nil payload/headers should round-trip as empty: %+v", empty)
	}
}

func testIdempotentEnqueue(t *testing.T, s store.Store, _ *clock.Fake) {
	e1 := mustEndpoint(t, s, 8)
	e2 := mustEndpoint(t, s, 8)
	first, created, err := s.Enqueue(ctx(), store.NewJob{EndpointID: e1.ID, Payload: []byte("a"), IdempotencyKey: "k1"})
	if err != nil || !created {
		t.Fatalf("first enqueue: created=%v err=%v", created, err)
	}
	again, created, err := s.Enqueue(ctx(), store.NewJob{EndpointID: e1.ID, Payload: []byte("b"), IdempotencyKey: "k1"})
	if err != nil {
		t.Fatal(err)
	}
	if created || again.ID != first.ID || string(again.Payload) != "a" {
		t.Fatalf("duplicate key should return the existing job: created=%v job=%+v", created, again)
	}
	other, created, err := s.Enqueue(ctx(), store.NewJob{EndpointID: e2.ID, IdempotencyKey: "k1"})
	if err != nil || !created || other.ID == first.ID {
		t.Fatalf("same key on another endpoint must create a new job: created=%v err=%v", created, err)
	}
	a := mustEnqueue(t, s, e1.ID)
	b := mustEnqueue(t, s, e1.ID)
	if a.ID == b.ID {
		t.Fatal("jobs without a key must never be deduplicated")
	}
}

func testClaimOnlyDue(t *testing.T, s store.Store, clk *clock.Fake) {
	e := mustEndpoint(t, s, 8)
	j := mustEnqueue(t, s, e.ID)
	c := mustClaimOne(t, s)
	if c.Job.ID != j.ID || c.Job.Status != store.StatusInFlight || c.Job.LeaseToken == uuid.Nil {
		t.Fatalf("bad claim: %+v", c.Job)
	}
	if c.Endpoint.ID != e.ID || c.Endpoint.Secret != e.Secret || c.Endpoint.MaxAttempts != 8 {
		t.Fatalf("claim must carry its endpoint: %+v", c.Endpoint)
	}
	if !c.Job.LockedUntil.Equal(clk.Now().Add(lease)) {
		t.Fatalf("locked_until = %v, want now+lease", c.Job.LockedUntil)
	}
	if got := mustClaim(t, s, 10); len(got) != 0 {
		t.Fatalf("an in-flight job with a live lease must not be claimed, got %d", len(got))
	}
	mustComplete(t, s, c, store.Result{Outcome: store.OutcomeRetry, NextAttemptAt: clk.Now().Add(time.Minute)})
	if got := mustClaim(t, s, 10); len(got) != 0 {
		t.Fatal("a job scheduled in the future must not be claimed")
	}
	clk.Advance(time.Minute)
	c = mustClaimOne(t, s)
	mustComplete(t, s, c, store.Result{Outcome: store.OutcomeDelivered})
	d := mustEnqueue(t, s, e.ID)
	c = mustClaimOne(t, s)
	if c.Job.ID != d.ID {
		t.Fatal("expected the second job")
	}
	mustComplete(t, s, c, store.Result{Outcome: store.OutcomeDead})
	clk.Advance(time.Hour)
	if got := mustClaim(t, s, 10); len(got) != 0 {
		t.Fatal("delivered and dead jobs must never be claimed")
	}
}

func testClaimOrderAndLimit(t *testing.T, s store.Store, clk *clock.Fake) {
	e := mustEndpoint(t, s, 8)
	var ids []uuid.UUID
	for range 5 {
		ids = append(ids, mustEnqueue(t, s, e.ID).ID)
		clk.Advance(time.Second)
	}
	if got := mustClaim(t, s, 0); len(got) != 0 {
		t.Fatal("n=0 must claim nothing")
	}
	got := mustClaim(t, s, 3)
	if len(got) != 3 {
		t.Fatalf("limit not respected: %d", len(got))
	}
	for i, c := range got {
		if c.Job.ID != ids[i] {
			t.Fatalf("claim %d: want oldest-due first", i)
		}
	}
	if len(mustClaim(t, s, 10)) != 2 {
		t.Fatal("expected the remaining two jobs")
	}
}

func testConcurrentClaimers(t *testing.T, s store.Store, _ *clock.Fake) {
	const (
		claimers = 8
		jobs     = 500
	)
	e := mustEndpoint(t, s, 8)
	for range jobs {
		mustEnqueue(t, s, e.ID)
	}
	var (
		mu    sync.Mutex
		seen  = map[uuid.UUID]int{}
		wg    sync.WaitGroup
		errCh = make(chan error, claimers)
	)
	for range claimers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				batch, err := s.Claim(ctx(), 7, lease)
				if err != nil {
					errCh <- err
					return
				}
				if len(batch) == 0 {
					return
				}
				mu.Lock()
				for _, c := range batch {
					seen[c.Job.ID]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("Claim: %v", err)
	}
	if len(seen) != jobs {
		t.Fatalf("claimed %d distinct jobs, want %d", len(seen), jobs)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("job %s claimed %d times", id, n)
		}
	}
}

func testReclaimAfterLeaseExpiry(t *testing.T, s store.Store, clk *clock.Fake) {
	e := mustEndpoint(t, s, 8)
	mustEnqueue(t, s, e.ID)
	first := mustClaimOne(t, s)
	clk.Advance(lease) // locked_until == now: not yet expired
	if len(mustClaim(t, s, 1)) != 0 {
		t.Fatal("a lease is valid up to and including locked_until")
	}
	clk.Advance(time.Millisecond)
	second := mustClaimOne(t, s)
	if second.Job.ID != first.Job.ID {
		t.Fatal("expected the abandoned job to be reclaimed")
	}
	if second.Job.LeaseToken == first.Job.LeaseToken {
		t.Fatal("a reclaim must issue a fresh lease token")
	}
	if second.Job.Attempts != first.Job.Attempts {
		t.Fatal("reclaiming must not count an attempt")
	}
}

func testStaleCompletionGuard(t *testing.T, s store.Store, clk *clock.Fake) {
	e := mustEndpoint(t, s, 8)
	mustEnqueue(t, s, e.ID)
	slow := mustClaimOne(t, s)
	clk.Advance(lease + time.Second)
	fast := mustClaimOne(t, s)

	for _, o := range []store.Outcome{store.OutcomeDelivered, store.OutcomeRetry, store.OutcomeDead, store.OutcomeReschedule} {
		_, err := s.Complete(ctx(), slow.Job.ID, slow.Job.LeaseToken, store.Result{Outcome: o, NextAttemptAt: clk.Now()})
		if !errors.Is(err, store.ErrStaleLease) {
			t.Fatalf("stale %v: want ErrStaleLease, got %v", o, err)
		}
	}
	j := mustGet(t, s, fast.Job.ID)
	if j.Status != store.StatusInFlight || j.LeaseToken != fast.Job.LeaseToken || j.Attempts != 0 {
		t.Fatalf("stale writes must not change the job: %+v", j)
	}
	if st := mustComplete(t, s, fast, store.Result{Outcome: store.OutcomeDelivered}); st != store.StatusDelivered {
		t.Fatalf("status = %s", st)
	}
	if _, err := s.Complete(ctx(), fast.Job.ID, fast.Job.LeaseToken, store.Result{Outcome: store.OutcomeDelivered}); !errors.Is(err, store.ErrStaleLease) {
		t.Fatalf("completing twice must be rejected, got %v", err)
	}
	if _, err := s.Complete(ctx(), uuid.New(), uuid.New(), store.Result{Outcome: store.OutcomeDelivered}); !errors.Is(err, store.ErrStaleLease) {
		t.Fatalf("unknown job: want ErrStaleLease, got %v", err)
	}
	if j := mustGet(t, s, fast.Job.ID); j.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", j.Attempts)
	}
}

func testRetryScheduling(t *testing.T, s store.Store, clk *clock.Fake) {
	e := mustEndpoint(t, s, 3)
	mustEnqueue(t, s, e.ID)
	c := mustClaimOne(t, s)
	next := clk.Now().Add(10 * time.Second)
	if st := mustComplete(t, s, c, store.Result{Outcome: store.OutcomeRetry, NextAttemptAt: next, Error: "HTTP 503"}); st != store.StatusPending {
		t.Fatalf("status = %s, want pending", st)
	}
	j := mustGet(t, s, c.Job.ID)
	if j.Attempts != 1 || !j.NextAttemptAt.Equal(next) || j.LastError != "HTTP 503" ||
		j.LeaseToken != uuid.Nil || !j.LockedUntil.IsZero() {
		t.Fatalf("bad retry state: %+v", j)
	}
	clk.Advance(10 * time.Second)
	c = mustClaimOne(t, s)
	if c.Job.Attempts != 1 {
		t.Fatalf("claimed attempts = %d", c.Job.Attempts)
	}
	// A completion without an error message keeps the previous last_error.
	mustComplete(t, s, c, store.Result{Outcome: store.OutcomeRetry, NextAttemptAt: clk.Now()})
	if j := mustGet(t, s, c.Job.ID); j.LastError != "HTTP 503" || j.Attempts != 2 {
		t.Fatalf("bad state after second retry: %+v", j)
	}
}

func testReschedule(t *testing.T, s store.Store, clk *clock.Fake) {
	e := mustEndpoint(t, s, 3)
	mustEnqueue(t, s, e.ID)
	c := mustClaimOne(t, s)
	next := clk.Now().Add(30 * time.Second)
	if st := mustComplete(t, s, c, store.Result{Outcome: store.OutcomeReschedule, NextAttemptAt: next, Error: "breaker open"}); st != store.StatusPending {
		t.Fatalf("status = %s", st)
	}
	j := mustGet(t, s, c.Job.ID)
	if j.Attempts != 0 || !j.NextAttemptAt.Equal(next) || j.LastError != "breaker open" {
		t.Fatalf("reschedule must not count an attempt: %+v", j)
	}
}

func testDeadLettering(t *testing.T, s store.Store, clk *clock.Fake) {
	e := mustEndpoint(t, s, 3)
	j := mustEnqueue(t, s, e.ID)
	for attempt := 1; attempt <= 3; attempt++ {
		c := mustClaimOne(t, s)
		st := mustComplete(t, s, c, store.Result{Outcome: store.OutcomeRetry, NextAttemptAt: clk.Now(), Error: fmt.Sprintf("fail %d", attempt)})
		want := store.StatusPending
		if attempt == 3 {
			want = store.StatusDead
		}
		if st != want {
			t.Fatalf("attempt %d: status %s, want %s", attempt, st, want)
		}
	}
	got := mustGet(t, s, j.ID)
	if got.Status != store.StatusDead || got.Attempts != 3 || got.LastError != "fail 3" {
		t.Fatalf("bad dead job: %+v", got)
	}

	p := mustEnqueue(t, s, e.ID)
	c := mustClaimOne(t, s)
	if st := mustComplete(t, s, c, store.Result{Outcome: store.OutcomeDead, Error: "HTTP 404"}); st != store.StatusDead {
		t.Fatalf("permanent failure: status %s", st)
	}
	if got := mustGet(t, s, p.ID); got.Attempts != 1 || got.LastError != "HTTP 404" {
		t.Fatalf("bad permanent failure state: %+v", got)
	}
}

func killJob(t *testing.T, s store.Store) store.Job {
	t.Helper()
	c := mustClaimOne(t, s)
	mustComplete(t, s, c, store.Result{Outcome: store.OutcomeDead, Error: "HTTP 410"})
	return mustGet(t, s, c.Job.ID)
}

func testPagination(t *testing.T, s store.Store, clk *clock.Fake) {
	e := mustEndpoint(t, s, 1)
	other := mustEndpoint(t, s, 1)
	var want []uuid.UUID
	// Seven dead jobs; pairs share an updated_at so the id tiebreak is exercised.
	for i := range 7 {
		mustEnqueue(t, s, e.ID)
		want = append(want, killJob(t, s).ID)
		if i%2 == 1 {
			clk.Advance(time.Second)
		}
	}
	mustEnqueue(t, s, other.ID)
	killJob(t, s)
	mustEnqueue(t, s, e.ID) // a pending job, which must not be listed

	var (
		got    []uuid.UUID
		cursor *store.Cursor
		pages  int
	)
	for {
		jobs, next, err := s.ListDeadLetters(ctx(), e.ID, 3, cursor)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, j := range jobs {
			if j.Status != store.StatusDead || j.EndpointID != e.ID {
				t.Fatalf("unexpected job in dead letters: %+v", j)
			}
			got = append(got, j.ID)
		}
		if next == nil {
			break
		}
		if pages > 10 {
			t.Fatal("pagination does not terminate")
		}
		cursor = next
	}
	if pages != 3 || len(got) != 7 {
		t.Fatalf("pages=%d jobs=%d, want 3 pages of 7 jobs", pages, len(got))
	}
	seen := map[uuid.UUID]bool{}
	for _, id := range got {
		if seen[id] {
			t.Fatalf("job %s listed twice", id)
		}
		seen[id] = true
	}
	for _, id := range want {
		if !seen[id] {
			t.Fatalf("job %s missing", id)
		}
	}
	// Order must be strictly increasing by (updated_at, id).
	var prev *store.Job
	all, _, err := s.ListDeadLetters(ctx(), e.ID, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range all {
		j := all[i]
		if prev != nil {
			if j.UpdatedAt.Before(prev.UpdatedAt) || (j.UpdatedAt.Equal(prev.UpdatedAt) && j.ID.String() <= prev.ID.String()) {
				t.Fatal("dead letters are not ordered by (updated_at, id)")
			}
		}
		prev = &j
	}
	if empty, next, err := s.ListDeadLetters(ctx(), uuid.New(), 3, nil); err != nil || len(empty) != 0 || next != nil {
		t.Fatalf("unknown endpoint should list nothing: %v %v %v", empty, next, err)
	}
}

func testReplay(t *testing.T, s store.Store, clk *clock.Fake) {
	e := mustEndpoint(t, s, 1)
	mustEnqueue(t, s, e.ID)
	dead := killJob(t, s)
	clk.Advance(time.Hour)
	j, err := s.Replay(ctx(), dead.ID)
	if err != nil {
		t.Fatal(err)
	}
	if j.Status != store.StatusPending || j.Attempts != 0 || !j.NextAttemptAt.Equal(clk.Now()) {
		t.Fatalf("replayed job should be pending, reset and due now: %+v", j)
	}
	if _, err := s.Replay(ctx(), dead.ID); !errors.Is(err, store.ErrNotDead) {
		t.Fatalf("replaying a pending job: want ErrNotDead, got %v", err)
	}
	if _, err := s.Replay(ctx(), uuid.New()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown job: want ErrNotFound, got %v", err)
	}
	c := mustClaimOne(t, s)
	if c.Job.ID != dead.ID {
		t.Fatal("replayed job should be claimable")
	}
	if jobs, _, _ := s.ListDeadLetters(ctx(), e.ID, 10, nil); len(jobs) != 0 {
		t.Fatal("replayed job must leave the dead-letter list")
	}
}

func testCountByStatus(t *testing.T, s store.Store, _ *clock.Fake) {
	counts, err := s.CountByStatus(ctx())
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range store.AllStatuses {
		if v, ok := counts[st]; !ok || v != 0 {
			t.Fatalf("empty store: %s = %d (present=%v)", st, v, ok)
		}
	}
	e := mustEndpoint(t, s, 1)
	for range 4 {
		mustEnqueue(t, s, e.ID)
	}
	a := mustClaimOne(t, s)
	mustComplete(t, s, a, store.Result{Outcome: store.OutcomeDelivered})
	killJob(t, s)
	mustClaimOne(t, s)
	counts, err = s.CountByStatus(ctx())
	if err != nil {
		t.Fatal(err)
	}
	want := map[store.Status]int{store.StatusPending: 1, store.StatusInFlight: 1, store.StatusDelivered: 1, store.StatusDead: 1}
	for st, n := range want {
		if counts[st] != n {
			t.Fatalf("%s = %d, want %d", st, counts[st], n)
		}
	}
}
