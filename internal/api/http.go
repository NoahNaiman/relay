package api

import (
	"context"
	"net/http"
	"time"

	"github.com/NoahNaiman/relay/internal/store"
)

// HTTPHandler serves /healthz (process is up), /readyz (store is reachable)
// and /metrics.
func HTTPHandler(s store.Store, metrics http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := s.Ping(ctx); err != nil {
			http.Error(w, "store unreachable", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.Handle("GET /metrics", metrics)
	return mux
}
