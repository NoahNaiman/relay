// Package postgres is the durable Store implementation, backed by pgx.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/NoahNaiman/relay/internal/clock"
	"github.com/NoahNaiman/relay/internal/store"
)

// Store is the Postgres implementation of store.Store.
type Store struct {
	pool  *pgxpool.Pool
	clock clock.Clock
	newID func() uuid.UUID
}

// Option configures a Store.
type Option func(*Store)

// WithClock injects the time source used for every timestamp the store writes.
func WithClock(c clock.Clock) Option { return func(s *Store) { s.clock = c } }

// WithIDs injects the UUID generator used for IDs and lease tokens.
func WithIDs(f func() uuid.UUID) Option { return func(s *Store) { s.newID = f } }

// Open connects to Postgres and applies migrations.
func Open(ctx context.Context, dsn string, opts ...Option) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := Migrate(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return New(pool, opts...), nil
}

// New wraps an existing pool. The schema must already be migrated.
func New(pool *pgxpool.Pool, opts ...Option) *Store {
	s := &Store{pool: pool, clock: clock.Real{}, newID: uuid.New}
	for _, o := range opts {
		o(s)
	}
	return s
}

var _ store.Store = (*Store)(nil)

// Pool exposes the underlying pool (used by tests to reset state).
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

func (s *Store) now() time.Time { return store.Timestamp(s.clock.Now()) }

const jobColumns = `id, endpoint_id, payload, headers, coalesce(idempotency_key, ''), status, attempts,
	next_attempt_at, locked_until, lease_token, coalesce(last_error, ''), created_at, updated_at`

func scanJob(row pgx.Row, extra ...any) (store.Job, error) {
	var (
		j           store.Job
		status      string
		lockedUntil *time.Time
		leaseToken  *uuid.UUID
	)
	dest := append([]any{&j.ID, &j.EndpointID, &j.Payload, &j.Headers, &j.IdempotencyKey, &status, &j.Attempts,
		&j.NextAttemptAt, &lockedUntil, &leaseToken, &j.LastError, &j.CreatedAt, &j.UpdatedAt}, extra...)
	if err := row.Scan(dest...); err != nil {
		return store.Job{}, err
	}
	j.Status = store.Status(status)
	j.NextAttemptAt = j.NextAttemptAt.UTC()
	j.CreatedAt = j.CreatedAt.UTC()
	j.UpdatedAt = j.UpdatedAt.UTC()
	if lockedUntil != nil {
		j.LockedUntil = lockedUntil.UTC()
	}
	if leaseToken != nil {
		j.LeaseToken = *leaseToken
	}
	if j.Headers == nil {
		j.Headers = map[string]string{}
	}
	if j.Payload == nil {
		j.Payload = []byte{}
	}
	return j, nil
}

// CreateEndpoint implements store.Store.
func (s *Store) CreateEndpoint(ctx context.Context, e store.Endpoint) (store.Endpoint, error) {
	e.ID = s.newID()
	e.CreatedAt = s.now()
	_, err := s.pool.Exec(ctx,
		`INSERT INTO endpoints (id, url, secret, max_rps, max_attempts, created_at) VALUES ($1, $2, $3, $4, $5, $6)`,
		e.ID, e.URL, e.Secret, e.MaxRPS, e.MaxAttempts, e.CreatedAt)
	if err != nil {
		return store.Endpoint{}, err
	}
	return e, nil
}

// GetEndpoint implements store.Store.
func (s *Store) GetEndpoint(ctx context.Context, id uuid.UUID) (store.Endpoint, error) {
	var e store.Endpoint
	err := s.pool.QueryRow(ctx,
		`SELECT id, url, secret, max_rps, max_attempts, created_at FROM endpoints WHERE id = $1`, id).
		Scan(&e.ID, &e.URL, &e.Secret, &e.MaxRPS, &e.MaxAttempts, &e.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.Endpoint{}, store.ErrNotFound
	}
	e.CreatedAt = e.CreatedAt.UTC()
	return e, err
}

// Enqueue implements store.Store.
func (s *Store) Enqueue(ctx context.Context, nj store.NewJob) (store.Job, bool, error) {
	now := s.now()
	headers := nj.Headers
	if headers == nil {
		headers = map[string]string{}
	}
	payload := nj.Payload
	if payload == nil {
		payload = []byte{}
	}
	var idem *string
	if nj.IdempotencyKey != "" {
		idem = &nj.IdempotencyKey
	}
	row := s.pool.QueryRow(ctx, `
		INSERT INTO jobs (id, endpoint_id, payload, headers, idempotency_key, status, attempts,
		                  next_attempt_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, 'pending', 0, $6, $6, $6)
		ON CONFLICT (endpoint_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
		RETURNING `+jobColumns,
		s.newID(), nj.EndpointID, payload, headers, idem, now)
	j, err := scanJob(row)
	if err == nil {
		return j, true, nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23503" { // foreign_key_violation
		return store.Job{}, false, store.ErrNotFound
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return store.Job{}, false, err
	}
	// The idempotency key already exists: return the original job.
	j, err = scanJob(s.pool.QueryRow(ctx,
		`SELECT `+jobColumns+` FROM jobs WHERE endpoint_id = $1 AND idempotency_key = $2`,
		nj.EndpointID, nj.IdempotencyKey))
	if err != nil {
		return store.Job{}, false, err
	}
	return j, false, nil
}

// GetJob implements store.Store.
func (s *Store) GetJob(ctx context.Context, id uuid.UUID) (store.Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return store.Job{}, store.ErrNotFound
	}
	return j, err
}

// Claim implements store.Store. Selection and lease assignment happen in one
// transaction; SKIP LOCKED lets concurrent claimers take disjoint rows.
func (s *Store) Claim(ctx context.Context, n int, lease time.Duration) ([]store.Claimed, error) {
	if n <= 0 {
		return nil, nil
	}
	now := s.now()
	lockedUntil := store.Timestamp(now.Add(lease))
	var out []store.Claimed
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id FROM jobs
			WHERE (status = 'pending' AND next_attempt_at <= $1)
			   OR (status = 'in_flight' AND locked_until < $1)
			ORDER BY next_attempt_at
			LIMIT $2
			FOR UPDATE SKIP LOCKED`, now, n)
		if err != nil {
			return err
		}
		ids, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
		if err != nil || len(ids) == 0 {
			return err
		}
		tokens := make([]uuid.UUID, len(ids))
		for i := range tokens {
			tokens[i] = s.newID()
		}
		rows, err = tx.Query(ctx, `
			UPDATE jobs j
			SET status = 'in_flight', locked_until = $1, lease_token = c.token, updated_at = $2
			FROM unnest($3::uuid[], $4::uuid[]) AS c(id, token), endpoints e
			WHERE j.id = c.id AND e.id = j.endpoint_id
			RETURNING j.id, j.endpoint_id, j.payload, j.headers, coalesce(j.idempotency_key, ''), j.status,
			          j.attempts, j.next_attempt_at, j.locked_until, j.lease_token, coalesce(j.last_error, ''),
			          j.created_at, j.updated_at,
			          e.id, e.url, e.secret, e.max_rps, e.max_attempts, e.created_at`,
			lockedUntil, now, ids, tokens)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var e store.Endpoint
			j, err := scanJob(rows, &e.ID, &e.URL, &e.Secret, &e.MaxRPS, &e.MaxAttempts, &e.CreatedAt)
			if err != nil {
				return err
			}
			e.CreatedAt = e.CreatedAt.UTC()
			out = append(out, store.Claimed{Job: j, Endpoint: e})
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Complete implements store.Store. The WHERE clause on status and lease_token
// is the stale-completion guard.
func (s *Store) Complete(ctx context.Context, jobID, leaseToken uuid.UUID, r store.Result) (store.Status, error) {
	switch r.Outcome {
	case store.OutcomeDelivered, store.OutcomeRetry, store.OutcomeDead, store.OutcomeReschedule:
	default:
		return "", fmt.Errorf("unknown outcome %d", r.Outcome)
	}
	var lastErr *string
	if r.Error != "" {
		lastErr = &r.Error
	}
	var status string
	err := s.pool.QueryRow(ctx, `
		UPDATE jobs j SET
			status = CASE
				WHEN $3::text = 'delivered' THEN 'delivered'
				WHEN $3::text = 'dead' THEN 'dead'
				WHEN $3::text = 'retry' AND j.attempts + 1 >= e.max_attempts THEN 'dead'
				ELSE 'pending' END,
			attempts = j.attempts + CASE WHEN $3::text = 'reschedule' THEN 0 ELSE 1 END,
			next_attempt_at = CASE WHEN $3::text IN ('retry', 'reschedule') THEN $4 ELSE j.next_attempt_at END,
			last_error = coalesce($5, j.last_error),
			locked_until = NULL,
			lease_token = NULL,
			updated_at = $6
		FROM endpoints e
		WHERE j.id = $1 AND j.status = 'in_flight' AND j.lease_token = $2 AND e.id = j.endpoint_id
		RETURNING j.status`,
		jobID, leaseToken, r.Outcome.String(), store.Timestamp(r.NextAttemptAt), lastErr, s.now()).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", store.ErrStaleLease
	}
	if err != nil {
		return "", err
	}
	return store.Status(status), nil
}

// ListDeadLetters implements store.Store.
func (s *Store) ListDeadLetters(ctx context.Context, endpointID uuid.UUID, limit int, after *store.Cursor) ([]store.Job, *store.Cursor, error) {
	var (
		rows pgx.Rows
		err  error
	)
	if after == nil {
		rows, err = s.pool.Query(ctx, `SELECT `+jobColumns+` FROM jobs
			WHERE endpoint_id = $1 AND status = 'dead'
			ORDER BY updated_at, id LIMIT $2`, endpointID, limit+1)
	} else {
		rows, err = s.pool.Query(ctx, `SELECT `+jobColumns+` FROM jobs
			WHERE endpoint_id = $1 AND status = 'dead' AND (updated_at, id) > ($3, $4)
			ORDER BY updated_at, id LIMIT $2`, endpointID, limit+1, after.UpdatedAt, after.ID)
	}
	if err != nil {
		return nil, nil, err
	}
	jobs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (store.Job, error) { return scanJob(r) })
	if err != nil {
		return nil, nil, err
	}
	var next *store.Cursor
	if len(jobs) > limit {
		jobs = jobs[:limit]
		last := jobs[limit-1]
		next = &store.Cursor{UpdatedAt: last.UpdatedAt, ID: last.ID}
	}
	return jobs, next, nil
}

// Replay implements store.Store.
func (s *Store) Replay(ctx context.Context, jobID uuid.UUID) (store.Job, error) {
	now := s.now()
	j, err := scanJob(s.pool.QueryRow(ctx, `
		UPDATE jobs SET status = 'pending', attempts = 0, next_attempt_at = $2, updated_at = $2
		WHERE id = $1 AND status = 'dead'
		RETURNING `+jobColumns, jobID, now))
	if err == nil {
		return j, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return store.Job{}, err
	}
	if _, err := s.GetJob(ctx, jobID); err != nil {
		return store.Job{}, err
	}
	return store.Job{}, store.ErrNotDead
}

// CountByStatus implements store.Store.
func (s *Store) CountByStatus(ctx context.Context) (map[store.Status]int, error) {
	out := make(map[store.Status]int, len(store.AllStatuses))
	for _, st := range store.AllStatuses {
		out[st] = 0
	}
	rows, err := s.pool.Query(ctx, `SELECT status, count(*) FROM jobs GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			st string
			n  int
		)
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		out[store.Status(st)] = n
	}
	return out, rows.Err()
}

// Ping implements store.Store.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Close implements store.Store.
func (s *Store) Close() { s.pool.Close() }
