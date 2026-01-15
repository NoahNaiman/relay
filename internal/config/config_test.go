package config

import (
	"strings"
	"testing"
	"time"
)

func lookupFrom(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func TestDefaultsAreValid(t *testing.T) {
	c, err := FromLookup(lookupFrom(nil))
	if err != nil {
		t.Fatalf("defaults should validate: %v", err)
	}
	if c != Default() {
		t.Fatalf("empty env should yield defaults, got %+v", c)
	}
	if c.Workers != 16 || c.Lease != 30*time.Second || c.PollInterval != 200*time.Millisecond {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}

func TestOverrides(t *testing.T) {
	c, err := FromLookup(lookupFrom(map[string]string{
		"RELAY_GRPC_ADDR":         ":1",
		"RELAY_HTTP_ADDR":         ":2",
		"RELAY_DATABASE_URL":      "postgres://x",
		"RELAY_LOG_LEVEL":         "debug",
		"RELAY_WORKERS":           "4",
		"RELAY_BATCH_SIZE":        "8",
		"RELAY_POLL_INTERVAL":     "50ms",
		"RELAY_LEASE":             "2s",
		"RELAY_REQUEST_TIMEOUT":   "1s",
		"RELAY_BACKOFF_BASE":      "10ms",
		"RELAY_BACKOFF_CAP":       "200ms",
		"RELAY_BREAKER_THRESHOLD": "3",
		"RELAY_BREAKER_COOLDOWN":  "200ms",
		"RELAY_SHUTDOWN_TIMEOUT":  "1s",
		"RELAY_METRICS_INTERVAL":  "1s",
	}))
	if err != nil {
		t.Fatal(err)
	}
	want := Config{
		GRPCAddr: ":1", HTTPAddr: ":2", DatabaseURL: "postgres://x", LogLevel: "debug",
		Workers: 4, BatchSize: 8, PollInterval: 50 * time.Millisecond, Lease: 2 * time.Second,
		RequestTimeout: time.Second, BackoffBase: 10 * time.Millisecond, BackoffCap: 200 * time.Millisecond,
		BreakerThreshold: 3, BreakerCooldown: 200 * time.Millisecond, ShutdownTimeout: time.Second,
		MetricsInterval: time.Second,
	}
	if c != want {
		t.Fatalf("got %+v\nwant %+v", c, want)
	}
}

func TestInvalid(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"bad int", map[string]string{"RELAY_WORKERS": "x"}, "RELAY_WORKERS"},
		{"bad duration", map[string]string{"RELAY_LEASE": "soon"}, "RELAY_LEASE"},
		{"zero workers", map[string]string{"RELAY_WORKERS": "0"}, "RELAY_WORKERS must be > 0"},
		{"zero batch", map[string]string{"RELAY_BATCH_SIZE": "0"}, "RELAY_BATCH_SIZE"},
		{"negative poll", map[string]string{"RELAY_POLL_INTERVAL": "-1s"}, "RELAY_POLL_INTERVAL"},
		{"cap below base", map[string]string{"RELAY_BACKOFF_BASE": "2s", "RELAY_BACKOFF_CAP": "1s"}, "RELAY_BACKOFF_CAP"},
		{"timeout over lease", map[string]string{"RELAY_LEASE": "1s"}, "RELAY_REQUEST_TIMEOUT"},
		{"bad level", map[string]string{"RELAY_LOG_LEVEL": "loud"}, "RELAY_LOG_LEVEL"},
		{"empty grpc", map[string]string{"RELAY_GRPC_ADDR": ""}, "RELAY_GRPC_ADDR"},
		{"empty http", map[string]string{"RELAY_HTTP_ADDR": ""}, "RELAY_HTTP_ADDR"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := FromLookup(lookupFrom(tc.env))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}
