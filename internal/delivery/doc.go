// Package delivery contains the building blocks for one webhook delivery
// attempt: request signing and verification, the HTTP sender, failure
// classification, backoff, a per-endpoint circuit breaker and a per-endpoint
// rate limiter. Nothing here touches the store.
package delivery
