package dispatch

import (
	"time"

	"github.com/NoahNaiman/relay/internal/delivery"
	"github.com/NoahNaiman/relay/internal/store"
)

// Policy turns a classifier verdict into a store result.
type Policy struct {
	Backoff delivery.Backoff
}

// Decide maps a verdict for a job that has made `attempts` prior attempts.
// Retryable failures wait for Retry-After when the receiver sent one, and for
// jittered exponential backoff otherwise. Whether a retry becomes a dead
// letter (max_attempts reached) is decided atomically by the store.
func (p Policy) Decide(attempts int, v delivery.Verdict, now time.Time) store.Result {
	switch v.Class {
	case delivery.Delivered:
		return store.Result{Outcome: store.OutcomeDelivered}
	case delivery.Retryable:
		wait := v.RetryAfter
		if wait <= 0 {
			wait = p.Backoff.Delay(attempts)
		}
		return store.Result{Outcome: store.OutcomeRetry, NextAttemptAt: now.Add(wait), Error: v.Reason}
	default:
		return store.Result{Outcome: store.OutcomeDead, Error: v.Reason}
	}
}
