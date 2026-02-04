// Command receiver is a demo webhook receiver. It verifies relay signatures,
// deduplicates deliveries by Relay-Job-Id, and can inject failures and latency.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/NoahNaiman/relay/internal/delivery"
)

// receiver is the HTTP handler; its fields are the flags.
type receiver struct {
	secret     string
	failRate   float64
	maxLatency time.Duration
	tolerance  time.Duration
	now        func() time.Time
	rand       func() float64
	log        *slog.Logger

	mu         sync.Mutex
	seen       map[string]bool
	unique     int
	duplicates int
	injected   int
	rejected   int
}

func newReceiver(secret string, failRate float64, maxLatency time.Duration, log *slog.Logger) *receiver {
	return &receiver{
		secret: secret, failRate: failRate, maxLatency: maxLatency, tolerance: 5 * time.Minute,
		now: time.Now, rand: rand.Float64, log: log, seen: map[string]bool{},
	}
}

// Stats is served on GET /stats.
type Stats struct {
	Unique     int `json:"unique"`
	Duplicates int `json:"duplicates"`
	Injected   int `json:"injected_failures"`
	Rejected   int `json:"rejected_signatures"`
}

func (rc *receiver) stats() Stats {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return Stats{Unique: rc.unique, Duplicates: rc.duplicates, Injected: rc.injected, Rejected: rc.rejected}
}

func (rc *receiver) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /hook", rc.hook)
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(rc.stats())
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	return mux
}

func (rc *receiver) hook(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	jobID := r.Header.Get(delivery.HeaderJobID)
	if err := delivery.Verify(rc.secret, r.Header, body, rc.now(), rc.tolerance); err != nil {
		rc.mu.Lock()
		rc.rejected++
		rc.mu.Unlock()
		rc.log.Warn("rejected delivery", "job_id", jobID, "err", err)
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}
	if rc.maxLatency > 0 {
		time.Sleep(time.Duration(rc.rand() * float64(rc.maxLatency)))
	}
	if rc.rand() < rc.failRate {
		rc.mu.Lock()
		rc.injected++
		rc.mu.Unlock()
		rc.log.Info("injected failure", "job_id", jobID, "attempt", r.Header.Get(delivery.HeaderAttempt))
		http.Error(w, "injected failure", http.StatusServiceUnavailable)
		return
	}
	rc.mu.Lock()
	dup := rc.seen[jobID]
	if dup {
		rc.duplicates++
	} else {
		rc.seen[jobID] = true
		rc.unique++
	}
	rc.mu.Unlock()
	if dup {
		// At-least-once delivery: acknowledge, but do not process twice.
		rc.log.Info("duplicate delivery ignored", "job_id", jobID)
	} else {
		rc.log.Info("delivery processed", "job_id", jobID, "attempt", r.Header.Get(delivery.HeaderAttempt), "bytes", len(body))
	}
	w.WriteHeader(http.StatusNoContent)
}

func main() {
	addr := flag.String("addr", ":8081", "listen address")
	secret := flag.String("secret", os.Getenv("RECEIVER_SECRET"), "HMAC secret (default $RECEIVER_SECRET)")
	failRate := flag.Float64("fail-rate", 0, "fraction of verified deliveries to fail with 503, 0..1")
	maxLatency := flag.Duration("max-latency", 0, "random latency added per request, uniform in [0, max]")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if *secret == "" || *failRate < 0 || *failRate > 1 || *maxLatency < 0 {
		fmt.Fprintln(os.Stderr, "receiver: --secret is required, --fail-rate must be in [0,1], --max-latency must be >= 0")
		os.Exit(2)
	}
	rc := newReceiver(*secret, *failRate, *maxLatency, log)
	log.Info("receiver listening", "addr", *addr, "fail_rate", *failRate, "max_latency", maxLatency.String())
	srv := &http.Server{Addr: *addr, Handler: rc.routes(), ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil {
		log.Error("receiver stopped", "err", err)
		os.Exit(1)
	}
}
