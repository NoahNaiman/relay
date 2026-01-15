// Package memory is an in-process Store guarded by a single mutex. It has the
// same claiming and completion semantics as the Postgres store, but nothing
// survives a process restart.
package memory

import (
	"bytes"
	"context"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/NoahNaiman/relay/internal/clock"
	"github.com/NoahNaiman/relay/internal/store"
)

type idemKey struct {
	endpoint uuid.UUID
	key      string
}

// Store is the in-memory implementation of store.Store.
type Store struct {
	clock clock.Clock
	newID func() uuid.UUID

	mu        sync.Mutex
	endpoints map[uuid.UUID]store.Endpoint
	jobs      map[uuid.UUID]*store.Job
	active    map[uuid.UUID]struct{} // pending or in_flight: the only claim candidates
	idem      map[idemKey]uuid.UUID
}

// Option configures a Store.
type Option func(*Store)

// WithClock injects the time source.
func WithClock(c clock.Clock) Option { return func(s *Store) { s.clock = c } }

// WithIDs injects the UUID generator used for job IDs, endpoint IDs and lease tokens.
func WithIDs(f func() uuid.UUID) Option { return func(s *Store) { s.newID = f } }

// New returns an empty Store.
func New(opts ...Option) *Store {
	s := &Store{
		clock:     clock.Real{},
		newID:     uuid.New,
		endpoints: map[uuid.UUID]store.Endpoint{},
		jobs:      map[uuid.UUID]*store.Job{},
		active:    map[uuid.UUID]struct{}{},
		idem:      map[idemKey]uuid.UUID{},
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

var _ store.Store = (*Store)(nil)

func (s *Store) now() time.Time { return store.Timestamp(s.clock.Now()) }

func copyJob(j *store.Job) store.Job {
	c := *j
	c.Payload = bytes.Clone(j.Payload)
	c.Headers = maps.Clone(j.Headers)
	return c
}

// CreateEndpoint implements store.Store.
func (s *Store) CreateEndpoint(_ context.Context, e store.Endpoint) (store.Endpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e.ID = s.newID()
	e.CreatedAt = s.now()
	s.endpoints[e.ID] = e
	return e, nil
}

// GetEndpoint implements store.Store.
func (s *Store) GetEndpoint(_ context.Context, id uuid.UUID) (store.Endpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.endpoints[id]
	if !ok {
		return store.Endpoint{}, store.ErrNotFound
	}
	return e, nil
}

// Enqueue implements store.Store.
func (s *Store) Enqueue(_ context.Context, nj store.NewJob) (store.Job, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.endpoints[nj.EndpointID]; !ok {
		return store.Job{}, false, store.ErrNotFound
	}
	if nj.IdempotencyKey != "" {
		if id, ok := s.idem[idemKey{nj.EndpointID, nj.IdempotencyKey}]; ok {
			return copyJob(s.jobs[id]), false, nil
		}
	}
	now := s.now()
	headers := maps.Clone(nj.Headers)
	if headers == nil {
		headers = map[string]string{}
	}
	j := &store.Job{
		ID:             s.newID(),
		EndpointID:     nj.EndpointID,
		Payload:        bytes.Clone(nj.Payload),
		Headers:        headers,
		IdempotencyKey: nj.IdempotencyKey,
		Status:         store.StatusPending,
		NextAttemptAt:  now,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if j.Payload == nil {
		j.Payload = []byte{}
	}
	s.jobs[j.ID] = j
	s.active[j.ID] = struct{}{}
	if nj.IdempotencyKey != "" {
		s.idem[idemKey{nj.EndpointID, nj.IdempotencyKey}] = j.ID
	}
	return copyJob(j), true, nil
}

// GetJob implements store.Store.
func (s *Store) GetJob(_ context.Context, id uuid.UUID) (store.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return store.Job{}, store.ErrNotFound
	}
	return copyJob(j), nil
}

// Claim implements store.Store.
func (s *Store) Claim(_ context.Context, n int, lease time.Duration) ([]store.Claimed, error) {
	if n <= 0 {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	var due []*store.Job
	for id := range s.active {
		j := s.jobs[id]
		switch {
		case j.Status == store.StatusPending && !j.NextAttemptAt.After(now):
		case j.Status == store.StatusInFlight && j.LockedUntil.Before(now):
		default:
			continue
		}
		due = append(due, j)
	}
	slices.SortFunc(due, func(a, b *store.Job) int {
		if c := a.NextAttemptAt.Compare(b.NextAttemptAt); c != 0 {
			return c
		}
		return bytes.Compare(a.ID[:], b.ID[:])
	})
	if len(due) > n {
		due = due[:n]
	}
	out := make([]store.Claimed, 0, len(due))
	lockedUntil := store.Timestamp(now.Add(lease))
	for _, j := range due {
		j.Status = store.StatusInFlight
		j.LockedUntil = lockedUntil
		j.LeaseToken = s.newID()
		j.UpdatedAt = now
		out = append(out, store.Claimed{Job: copyJob(j), Endpoint: s.endpoints[j.EndpointID]})
	}
	return out, nil
}

// Complete implements store.Store.
func (s *Store) Complete(_ context.Context, jobID, leaseToken uuid.UUID, r store.Result) (store.Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[jobID]
	if !ok || j.Status != store.StatusInFlight || j.LeaseToken != leaseToken || leaseToken == uuid.Nil {
		return "", store.ErrStaleLease
	}
	now := s.now()
	switch r.Outcome {
	case store.OutcomeDelivered:
		j.Attempts++
		j.Status = store.StatusDelivered
	case store.OutcomeDead:
		j.Attempts++
		j.Status = store.StatusDead
	case store.OutcomeRetry:
		j.Attempts++
		if j.Attempts >= s.endpoints[j.EndpointID].MaxAttempts {
			j.Status = store.StatusDead
		} else {
			j.Status = store.StatusPending
			j.NextAttemptAt = store.Timestamp(r.NextAttemptAt)
		}
	case store.OutcomeReschedule:
		j.Status = store.StatusPending
		j.NextAttemptAt = store.Timestamp(r.NextAttemptAt)
	default:
		return "", store.ErrStaleLease
	}
	if r.Error != "" {
		j.LastError = r.Error
	}
	j.LockedUntil = time.Time{}
	j.LeaseToken = uuid.Nil
	j.UpdatedAt = now
	if j.Status == store.StatusDelivered || j.Status == store.StatusDead {
		delete(s.active, j.ID)
	}
	return j.Status, nil
}

// ListDeadLetters implements store.Store.
func (s *Store) ListDeadLetters(_ context.Context, endpointID uuid.UUID, limit int, after *store.Cursor) ([]store.Job, *store.Cursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var dead []*store.Job
	for _, j := range s.jobs {
		if j.EndpointID != endpointID || j.Status != store.StatusDead {
			continue
		}
		if after != nil && cmpCursor(j.UpdatedAt, j.ID, after.UpdatedAt, after.ID) <= 0 {
			continue
		}
		dead = append(dead, j)
	}
	slices.SortFunc(dead, func(a, b *store.Job) int {
		return cmpCursor(a.UpdatedAt, a.ID, b.UpdatedAt, b.ID)
	})
	var next *store.Cursor
	if limit > 0 && len(dead) > limit {
		dead = dead[:limit]
		last := dead[limit-1]
		next = &store.Cursor{UpdatedAt: last.UpdatedAt, ID: last.ID}
	}
	out := make([]store.Job, len(dead))
	for i, j := range dead {
		out[i] = copyJob(j)
	}
	return out, next, nil
}

func cmpCursor(at time.Time, aid uuid.UUID, bt time.Time, bid uuid.UUID) int {
	if c := at.Compare(bt); c != 0 {
		return c
	}
	return bytes.Compare(aid[:], bid[:])
}

// Replay implements store.Store.
func (s *Store) Replay(_ context.Context, jobID uuid.UUID) (store.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[jobID]
	if !ok {
		return store.Job{}, store.ErrNotFound
	}
	if j.Status != store.StatusDead {
		return store.Job{}, store.ErrNotDead
	}
	now := s.now()
	j.Status = store.StatusPending
	j.Attempts = 0
	j.NextAttemptAt = now
	j.UpdatedAt = now
	s.active[j.ID] = struct{}{}
	return copyJob(j), nil
}

// CountByStatus implements store.Store.
func (s *Store) CountByStatus(context.Context) (map[store.Status]int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[store.Status]int, len(store.AllStatuses))
	for _, st := range store.AllStatuses {
		out[st] = 0
	}
	for _, j := range s.jobs {
		out[j.Status]++
	}
	return out, nil
}

// Ping implements store.Store.
func (s *Store) Ping(context.Context) error { return nil }

// Close implements store.Store.
func (s *Store) Close() {}
