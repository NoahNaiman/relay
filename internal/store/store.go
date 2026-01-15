// Package store defines relay's persistence contract. Two implementations
// exist: memory (for tests and single-process runs) and postgres.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// Status is the lifecycle state of a job.
type Status string

// Job statuses, matching the CHECK constraint in the Postgres schema.
const (
	StatusPending   Status = "pending"
	StatusInFlight  Status = "in_flight"
	StatusDelivered Status = "delivered"
	StatusDead      Status = "dead"
)

// AllStatuses lists every status, in lifecycle order.
var AllStatuses = []Status{StatusPending, StatusInFlight, StatusDelivered, StatusDead}

// Errors returned by every Store implementation.
var (
	ErrNotFound   = errors.New("not found")
	ErrNotDead    = errors.New("job is not dead")
	ErrStaleLease = errors.New("stale lease: job was reclaimed or already completed")
)

// Endpoint is a registered webhook receiver.
type Endpoint struct {
	ID          uuid.UUID
	URL         string
	Secret      string
	MaxRPS      int
	MaxAttempts int
	CreatedAt   time.Time
}

// Job is one payload to be delivered to one endpoint.
type Job struct {
	ID             uuid.UUID
	EndpointID     uuid.UUID
	Payload        []byte
	Headers        map[string]string
	IdempotencyKey string // empty means none
	Status         Status
	Attempts       int
	NextAttemptAt  time.Time
	LockedUntil    time.Time // zero unless in flight
	LeaseToken     uuid.UUID // uuid.Nil unless in flight
	LastError      string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// NewJob is the input to Enqueue.
type NewJob struct {
	EndpointID     uuid.UUID
	Payload        []byte
	Headers        map[string]string
	IdempotencyKey string
}

// Claimed is a job leased to one worker, together with its endpoint.
type Claimed struct {
	Job      Job
	Endpoint Endpoint
}

// Outcome is what a worker decided after attempting (or not attempting) a delivery.
type Outcome int

// Outcomes accepted by Complete.
const (
	// OutcomeDelivered marks the job delivered and counts the attempt.
	OutcomeDelivered Outcome = iota + 1
	// OutcomeRetry counts the attempt and schedules the job at Result.NextAttemptAt,
	// or dead-letters it if the endpoint's max_attempts has been reached.
	OutcomeRetry
	// OutcomeDead counts the attempt and dead-letters the job.
	OutcomeDead
	// OutcomeReschedule returns the job to pending at Result.NextAttemptAt
	// without counting an attempt (open breaker, graceful shutdown).
	OutcomeReschedule
)

func (o Outcome) String() string {
	switch o {
	case OutcomeDelivered:
		return "delivered"
	case OutcomeRetry:
		return "retry"
	case OutcomeDead:
		return "dead"
	case OutcomeReschedule:
		return "reschedule"
	default:
		return "unknown"
	}
}

// Result is a completion request.
type Result struct {
	Outcome       Outcome
	NextAttemptAt time.Time // used by OutcomeRetry and OutcomeReschedule
	Error         string    // stored in last_error when non-empty
}

// Cursor is a keyset pagination position on (updated_at, id).
type Cursor struct {
	UpdatedAt time.Time
	ID        uuid.UUID
}

// Store is the persistence contract shared by all implementations.
type Store interface {
	// CreateEndpoint stores a new endpoint, assigning ID and CreatedAt.
	CreateEndpoint(ctx context.Context, e Endpoint) (Endpoint, error)
	// GetEndpoint returns ErrNotFound for unknown IDs.
	GetEndpoint(ctx context.Context, id uuid.UUID) (Endpoint, error)

	// Enqueue inserts a pending job, due now. If the idempotency key is set and
	// already used for this endpoint, it returns the existing job and created=false.
	// It returns ErrNotFound if the endpoint does not exist.
	Enqueue(ctx context.Context, j NewJob) (job Job, created bool, err error)
	// GetJob returns ErrNotFound for unknown IDs.
	GetJob(ctx context.Context, id uuid.UUID) (Job, error)

	// Claim leases up to n due jobs: pending jobs whose next_attempt_at has passed,
	// and in-flight jobs whose lease expired. Each claim gets a fresh lease token.
	Claim(ctx context.Context, n int, lease time.Duration) ([]Claimed, error)
	// Complete records a result, conditional on the job still being in flight
	// under leaseToken. Otherwise it returns ErrStaleLease and changes nothing.
	// It returns the job's resulting status.
	Complete(ctx context.Context, jobID, leaseToken uuid.UUID, r Result) (Status, error)

	// ListDeadLetters returns dead jobs for an endpoint ordered by (updated_at, id),
	// strictly after the cursor when one is given. next is nil on the last page.
	ListDeadLetters(ctx context.Context, endpointID uuid.UUID, limit int, after *Cursor) (jobs []Job, next *Cursor, err error)
	// Replay resets a dead job's attempts and makes it due now.
	// It returns ErrNotFound or ErrNotDead.
	Replay(ctx context.Context, jobID uuid.UUID) (Job, error)

	// CountByStatus returns the number of jobs in each status.
	CountByStatus(ctx context.Context) (map[Status]int, error)
	// Ping reports whether the store is reachable.
	Ping(ctx context.Context) error
	// Close releases resources.
	Close()
}

// Timestamp normalizes a time to the precision and zone Postgres stores, so
// both implementations return identical values.
func Timestamp(t time.Time) time.Time {
	return t.UTC().Truncate(time.Microsecond)
}
