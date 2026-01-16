// Package dispatch claims due jobs from the store and delivers them with a
// bounded pool of goroutines.
package dispatch

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/NoahNaiman/relay/internal/clock"
	"github.com/NoahNaiman/relay/internal/config"
	"github.com/NoahNaiman/relay/internal/delivery"
	"github.com/NoahNaiman/relay/internal/store"
)

// ErrKilled is returned by Run after Kill.
var ErrKilled = errors.New("dispatcher killed")

// Messages stored in last_error when a job is put back without an attempt.
const (
	msgBreakerOpen = "circuit breaker open"
	msgRateLimited = "rate limited"
)

// Config controls a Dispatcher.
type Config struct {
	Workers          int
	BatchSize        int
	PollInterval     time.Duration
	Lease            time.Duration
	RequestTimeout   time.Duration
	ShutdownTimeout  time.Duration
	BackoffBase      time.Duration
	BackoffCap       time.Duration
	BreakerThreshold int
	BreakerCooldown  time.Duration
	// MaxLimiterWait is the longest a worker sleeps for a rate-limit token;
	// longer waits reschedule the job instead. Defaults to 1s.
	MaxLimiterWait time.Duration
}

// ConfigFrom extracts the dispatcher settings from the server config.
func ConfigFrom(c config.Config) Config {
	return Config{
		Workers:          c.Workers,
		BatchSize:        c.BatchSize,
		PollInterval:     c.PollInterval,
		Lease:            c.Lease,
		RequestTimeout:   c.RequestTimeout,
		ShutdownTimeout:  c.ShutdownTimeout,
		BackoffBase:      c.BackoffBase,
		BackoffCap:       c.BackoffCap,
		BreakerThreshold: c.BreakerThreshold,
		BreakerCooldown:  c.BreakerCooldown,
	}
}

// Observer receives delivery events, typically to update metrics.
type Observer interface {
	// Delivery is called once per completed attempt with the job's resulting
	// state: "delivered", "retry" or "dead".
	Delivery(result string, d time.Duration)
	// BreakerChanged is called when an endpoint's breaker opens or closes.
	BreakerChanged(endpointID uuid.UUID, open bool)
}

type nopObserver struct{}

func (nopObserver) Delivery(string, time.Duration) {}
func (nopObserver) BreakerChanged(uuid.UUID, bool) {}

// Option configures a Dispatcher.
type Option func(*Dispatcher)

// WithClock injects the time source.
func WithClock(c clock.Clock) Option { return func(d *Dispatcher) { d.clock = c } }

// WithLogger sets the logger.
func WithLogger(l *slog.Logger) Option { return func(d *Dispatcher) { d.log = l } }

// WithObserver sets the metrics observer.
func WithObserver(o Observer) Option { return func(d *Dispatcher) { d.obs = o } }

// WithHTTPClient sets the HTTP client used for deliveries.
func WithHTTPClient(c *http.Client) Option { return func(d *Dispatcher) { d.httpClient = c } }

// WithRand injects the random source used for backoff jitter.
func WithRand(f func(n int64) int64) Option { return func(d *Dispatcher) { d.rand = f } }

// Dispatcher polls the store and delivers jobs.
type Dispatcher struct {
	store      store.Store
	cfg        Config
	clock      clock.Clock
	log        *slog.Logger
	obs        Observer
	httpClient *http.Client
	rand       func(n int64) int64

	sender   *delivery.Sender
	breakers *delivery.Breakers
	limiters *delivery.Limiters
	policy   Policy

	killCtx context.Context
	kill    context.CancelFunc
}

// New builds a Dispatcher. Call Run to start it.
func New(s store.Store, cfg Config, opts ...Option) *Dispatcher {
	if cfg.MaxLimiterWait <= 0 {
		cfg.MaxLimiterWait = time.Second
	}
	d := &Dispatcher{
		store:      s,
		cfg:        cfg,
		clock:      clock.Real{},
		log:        slog.New(slog.DiscardHandler),
		obs:        nopObserver{},
		httpClient: &http.Client{},
	}
	for _, o := range opts {
		o(d)
	}
	d.killCtx, d.kill = context.WithCancel(context.Background())
	d.sender = &delivery.Sender{Client: d.httpClient, Clock: d.clock, Timeout: cfg.RequestTimeout}
	d.breakers = delivery.NewBreakers(cfg.BreakerThreshold, cfg.BreakerCooldown, d.clock,
		func(id uuid.UUID, s delivery.BreakerState) {
			d.log.Info("breaker state changed", "endpoint_id", id, "state", s.String())
			d.obs.BreakerChanged(id, s != delivery.Closed)
		})
	d.limiters = delivery.NewLimiters(d.clock)
	d.policy = Policy{Backoff: delivery.Backoff{Base: cfg.BackoffBase, Cap: cfg.BackoffCap, Rand: d.rand}}
	return d
}

// Kill simulates a crash: Run returns promptly and no further results are
// written, so this instance's leased jobs are recovered only when their
// leases expire. Intended for tests.
func (d *Dispatcher) Kill() { d.kill() }

// Run claims and delivers jobs until ctx is canceled, then shuts down
// gracefully: it stops claiming, waits up to ShutdownTimeout for in-flight
// deliveries, cancels the rest and releases their leases (pending, due now).
func (d *Dispatcher) Run(ctx context.Context) error {
	deliverCtx, cancelDeliveries := context.WithCancel(d.killCtx)
	defer cancelDeliveries()

	sem := make(chan struct{}, d.cfg.Workers)
	var wg sync.WaitGroup
	d.log.Info("dispatcher started", "workers", d.cfg.Workers, "lease", d.cfg.Lease.String())

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		select {
		case <-d.killCtx.Done():
			stop()
		case <-runCtx.Done():
		}
	}()

	for runCtx.Err() == nil {
		// Block until at least one worker is free, then take as many free
		// slots as are available, up to the batch size.
		select {
		case sem <- struct{}{}:
		case <-runCtx.Done():
			continue
		}
		slots := 1
		for slots < d.cfg.BatchSize {
			select {
			case sem <- struct{}{}:
				slots++
				continue
			default:
			}
			break
		}

		claimed, err := d.store.Claim(runCtx, slots, d.cfg.Lease)
		if err != nil && runCtx.Err() == nil {
			d.log.Error("claim failed", "err", err)
		}
		for range slots - len(claimed) {
			<-sem
		}
		for _, c := range claimed {
			wg.Add(1)
			go func() {
				defer func() { <-sem; wg.Done() }()
				d.deliver(deliverCtx, c)
			}()
		}
		if len(claimed) == slots && slots == d.cfg.BatchSize {
			continue // a full batch: there is probably more work, poll immediately
		}
		select {
		case <-time.After(d.cfg.PollInterval):
		case <-runCtx.Done():
		}
	}

	if d.killCtx.Err() != nil {
		return ErrKilled
	}
	d.log.Info("dispatcher stopping: waiting for in-flight deliveries", "timeout", d.cfg.ShutdownTimeout.String())
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(d.cfg.ShutdownTimeout):
		d.log.Warn("shutdown deadline reached: canceling deliveries and releasing leases")
		cancelDeliveries()
		<-done
	case <-d.killCtx.Done():
		return ErrKilled
	}
	if d.killCtx.Err() != nil {
		return ErrKilled
	}
	d.log.Info("dispatcher stopped")
	return nil
}

// deliver performs one attempt for a claimed job and records the result.
func (d *Dispatcher) deliver(ctx context.Context, c store.Claimed) {
	job, ep := c.Job, c.Endpoint
	log := d.log.With("job_id", job.ID, "endpoint_id", ep.ID)
	breaker := d.breakers.For(ep.ID)

	if ok, retryAt := breaker.Allow(); !ok {
		d.complete(log, job, store.Result{Outcome: store.OutcomeReschedule, NextAttemptAt: retryAt, Error: msgBreakerOpen})
		return
	}
	wait, ok := d.limiters.Reserve(ep.ID, ep.MaxRPS, d.cfg.MaxLimiterWait)
	if !ok {
		breaker.Release()
		d.complete(log, job, store.Result{Outcome: store.OutcomeReschedule, NextAttemptAt: d.clock.Now().Add(wait), Error: msgRateLimited})
		return
	}
	if wait > 0 {
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			breaker.Release()
			d.release(log, job)
			return
		}
	}

	resp := d.sender.Send(ctx, delivery.Request{
		URL:     ep.URL,
		Secret:  ep.Secret,
		JobID:   job.ID.String(),
		Attempt: job.Attempts + 1,
		Payload: job.Payload,
		Headers: job.Headers,
	})
	if ctx.Err() != nil {
		// Shutdown deadline or kill: the attempt was cut short by us, not by the
		// receiver, so it does not count.
		breaker.Release()
		d.release(log, job)
		return
	}

	verdict := delivery.Classify(resp.StatusCode, resp.Header, resp.Err)
	switch verdict.Class {
	case delivery.Delivered:
		breaker.Success()
	case delivery.Retryable:
		breaker.Failure()
	default:
		breaker.Release()
	}
	result := d.policy.Decide(job.Attempts, verdict, d.clock.Now())
	status, ok := d.complete(log, job, result)
	if !ok {
		return
	}
	label := verdict.Class.String()
	if status == store.StatusDead {
		label = delivery.Permanent.String()
	}
	d.obs.Delivery(label, resp.Duration)
	log.Debug("delivery attempt", "attempt", job.Attempts+1, "status_code", resp.StatusCode,
		"result", label, "reason", verdict.Reason, "duration_ms", resp.Duration.Milliseconds())
}

// release puts an unfinished job back to pending, due now, without counting an attempt.
func (d *Dispatcher) release(log *slog.Logger, job store.Job) {
	d.complete(log, job, store.Result{Outcome: store.OutcomeReschedule, NextAttemptAt: d.clock.Now()})
}

// complete writes a result under the job's lease token. After Kill it writes
// nothing, as a crashed process would.
func (d *Dispatcher) complete(log *slog.Logger, job store.Job, r store.Result) (store.Status, bool) {
	if d.killCtx.Err() != nil {
		return "", false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	status, err := d.store.Complete(ctx, job.ID, job.LeaseToken, r)
	switch {
	case errors.Is(err, store.ErrStaleLease):
		log.Warn("stale completion rejected: lease expired and job was reclaimed", "outcome", r.Outcome.String())
		return "", false
	case err != nil:
		log.Error("complete failed; job will be retried after its lease expires", "err", err)
		return "", false
	}
	return status, true
}
