// Package config loads relay's runtime configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds every tunable of the relay server. Zero values are never used
// directly: Load and Default fill in defaults, and Validate rejects nonsense.
type Config struct {
	GRPCAddr    string // RELAY_GRPC_ADDR
	HTTPAddr    string // RELAY_HTTP_ADDR
	DatabaseURL string // RELAY_DATABASE_URL; empty selects the in-memory store
	LogLevel    string // RELAY_LOG_LEVEL: debug, info, warn, error

	Workers          int           // RELAY_WORKERS
	BatchSize        int           // RELAY_BATCH_SIZE
	PollInterval     time.Duration // RELAY_POLL_INTERVAL
	Lease            time.Duration // RELAY_LEASE
	RequestTimeout   time.Duration // RELAY_REQUEST_TIMEOUT
	BackoffBase      time.Duration // RELAY_BACKOFF_BASE
	BackoffCap       time.Duration // RELAY_BACKOFF_CAP
	BreakerThreshold int           // RELAY_BREAKER_THRESHOLD
	BreakerCooldown  time.Duration // RELAY_BREAKER_COOLDOWN
	ShutdownTimeout  time.Duration // RELAY_SHUTDOWN_TIMEOUT
	MetricsInterval  time.Duration // RELAY_METRICS_INTERVAL
}

// Default returns the configuration described in the spec.
func Default() Config {
	return Config{
		GRPCAddr:         ":9090",
		HTTPAddr:         ":8080",
		LogLevel:         "info",
		Workers:          16,
		BatchSize:        32,
		PollInterval:     200 * time.Millisecond,
		Lease:            30 * time.Second,
		RequestTimeout:   10 * time.Second,
		BackoffBase:      time.Second,
		BackoffCap:       10 * time.Minute,
		BreakerThreshold: 5,
		BreakerCooldown:  30 * time.Second,
		ShutdownTimeout:  15 * time.Second,
		MetricsInterval:  5 * time.Second,
	}
}

// Load reads the configuration from the process environment.
func Load() (Config, error) {
	return FromLookup(os.LookupEnv)
}

// FromLookup builds a Config from an arbitrary lookup function, which keeps
// tests independent of the real environment.
func FromLookup(lookup func(string) (string, bool)) (Config, error) {
	c := Default()
	var errs []error
	str := func(key string, dst *string) {
		if v, ok := lookup(key); ok {
			*dst = v
		}
	}
	num := func(key string, dst *int) {
		if v, ok := lookup(key); ok {
			n, err := strconv.Atoi(v)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", key, err))
				return
			}
			*dst = n
		}
	}
	dur := func(key string, dst *time.Duration) {
		if v, ok := lookup(key); ok {
			d, err := time.ParseDuration(v)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", key, err))
				return
			}
			*dst = d
		}
	}
	str("RELAY_GRPC_ADDR", &c.GRPCAddr)
	str("RELAY_HTTP_ADDR", &c.HTTPAddr)
	str("RELAY_DATABASE_URL", &c.DatabaseURL)
	str("RELAY_LOG_LEVEL", &c.LogLevel)
	num("RELAY_WORKERS", &c.Workers)
	num("RELAY_BATCH_SIZE", &c.BatchSize)
	dur("RELAY_POLL_INTERVAL", &c.PollInterval)
	dur("RELAY_LEASE", &c.Lease)
	dur("RELAY_REQUEST_TIMEOUT", &c.RequestTimeout)
	dur("RELAY_BACKOFF_BASE", &c.BackoffBase)
	dur("RELAY_BACKOFF_CAP", &c.BackoffCap)
	num("RELAY_BREAKER_THRESHOLD", &c.BreakerThreshold)
	dur("RELAY_BREAKER_COOLDOWN", &c.BreakerCooldown)
	dur("RELAY_SHUTDOWN_TIMEOUT", &c.ShutdownTimeout)
	dur("RELAY_METRICS_INTERVAL", &c.MetricsInterval)
	if len(errs) > 0 {
		return Config{}, errors.Join(errs...)
	}
	return c, c.Validate()
}

// Validate reports every invalid field at once.
func (c Config) Validate() error {
	var errs []error
	positive := func(name string, v int) {
		if v <= 0 {
			errs = append(errs, fmt.Errorf("%s must be > 0, got %d", name, v))
		}
	}
	positiveDur := func(name string, v time.Duration) {
		if v <= 0 {
			errs = append(errs, fmt.Errorf("%s must be > 0, got %s", name, v))
		}
	}
	if c.GRPCAddr == "" {
		errs = append(errs, errors.New("RELAY_GRPC_ADDR must not be empty"))
	}
	if c.HTTPAddr == "" {
		errs = append(errs, errors.New("RELAY_HTTP_ADDR must not be empty"))
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Errorf("RELAY_LOG_LEVEL must be debug, info, warn or error, got %q", c.LogLevel))
	}
	positive("RELAY_WORKERS", c.Workers)
	positive("RELAY_BATCH_SIZE", c.BatchSize)
	positive("RELAY_BREAKER_THRESHOLD", c.BreakerThreshold)
	positiveDur("RELAY_POLL_INTERVAL", c.PollInterval)
	positiveDur("RELAY_LEASE", c.Lease)
	positiveDur("RELAY_REQUEST_TIMEOUT", c.RequestTimeout)
	positiveDur("RELAY_BACKOFF_BASE", c.BackoffBase)
	positiveDur("RELAY_BACKOFF_CAP", c.BackoffCap)
	positiveDur("RELAY_BREAKER_COOLDOWN", c.BreakerCooldown)
	positiveDur("RELAY_SHUTDOWN_TIMEOUT", c.ShutdownTimeout)
	positiveDur("RELAY_METRICS_INTERVAL", c.MetricsInterval)
	if c.BackoffCap < c.BackoffBase {
		errs = append(errs, fmt.Errorf("RELAY_BACKOFF_CAP (%s) must be >= RELAY_BACKOFF_BASE (%s)", c.BackoffCap, c.BackoffBase))
	}
	if c.RequestTimeout >= c.Lease {
		errs = append(errs, fmt.Errorf("RELAY_REQUEST_TIMEOUT (%s) must be < RELAY_LEASE (%s)", c.RequestTimeout, c.Lease))
	}
	return errors.Join(errs...)
}
