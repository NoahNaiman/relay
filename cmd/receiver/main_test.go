package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/NoahNaiman/relay/internal/delivery"
)

func signedRequest(secret, jobID string, ts time.Time, body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/hook", bytes.NewBufferString(body))
	delivery.Sign(r.Header, secret, jobID, 1, ts, []byte(body))
	return r
}

func TestReceiver(t *testing.T) {
	now := time.Unix(1767600000, 0)
	rc := newReceiver("k", 0, 0, slog.New(slog.DiscardHandler))
	rc.now = func() time.Time { return now }
	h := rc.routes()
	do := func(r *http.Request) int {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}

	if c := do(signedRequest("k", "a", now, "{}")); c != http.StatusNoContent {
		t.Fatalf("valid: %d", c)
	}
	if c := do(signedRequest("k", "a", now, "{}")); c != http.StatusNoContent {
		t.Fatalf("duplicate must still be acknowledged: %d", c)
	}
	if c := do(signedRequest("wrong", "b", now, "{}")); c != http.StatusUnauthorized {
		t.Fatalf("bad signature: %d", c)
	}
	if c := do(signedRequest("k", "c", now.Add(-6*time.Minute), "{}")); c != http.StatusUnauthorized {
		t.Fatalf("stale timestamp: %d", c)
	}

	rc.failRate = 1
	rc.maxLatency = time.Millisecond
	if c := do(signedRequest("k", "d", now, "{}")); c != http.StatusServiceUnavailable {
		t.Fatalf("injected failure: %d", c)
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/stats", nil))
	var s Stats
	if err := json.NewDecoder(w.Body).Decode(&s); err != nil {
		t.Fatal(err)
	}
	if s != (Stats{Unique: 1, Duplicates: 1, Injected: 1, Rejected: 2}) {
		t.Fatalf("stats: %+v", s)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("healthz: %d", w.Code)
	}
}
