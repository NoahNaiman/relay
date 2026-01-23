// Command relay runs the webhook delivery server: gRPC API on RELAY_GRPC_ADDR,
// health/readiness/metrics on RELAY_HTTP_ADDR, and the dispatcher.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	relayv1 "github.com/NoahNaiman/relay/gen/relay/v1"
	"github.com/NoahNaiman/relay/internal/api"
	"github.com/NoahNaiman/relay/internal/config"
	"github.com/NoahNaiman/relay/internal/dispatch"
	"github.com/NoahNaiman/relay/internal/metrics"
	"github.com/NoahNaiman/relay/internal/store"
	"github.com/NoahNaiman/relay/internal/store/memory"
	"github.com/NoahNaiman/relay/internal/store/postgres"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid configuration:", err)
		os.Exit(2)
	}
	log := newLogger(cfg.LogLevel)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err = run(ctx, cfg, log)
	stop()
	if err != nil {
		log.Error("relay exited with error", "err", err)
		os.Exit(1)
	}
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	_ = l.UnmarshalText([]byte(level)) // validated by config
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}

func openStore(ctx context.Context, cfg config.Config, log *slog.Logger) (store.Store, error) {
	if cfg.DatabaseURL == "" {
		log.Warn("RELAY_DATABASE_URL is empty: using the in-memory store; jobs will not survive a restart")
		return memory.New(), nil
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// Postgres may still be starting (docker compose); retry briefly.
	var lastErr error
	for ctx.Err() == nil {
		s, err := postgres.Open(ctx, cfg.DatabaseURL)
		if err == nil {
			return s, nil
		}
		lastErr = err
		log.Warn("waiting for postgres", "err", err)
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
	return nil, lastErr
}

// run starts every component and blocks until ctx is canceled, then shuts down
// in order: stop accepting RPCs, drain the dispatcher, stop HTTP, close the store.
func run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	s, err := openStore(ctx, cfg, log)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer s.Close()

	m := metrics.New()
	grpcLn, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return err
	}
	httpLn, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return err
	}

	gs := grpc.NewServer()
	relayv1.RegisterRelayServiceServer(gs, api.New(s, log, m.JobsEnqueued.Inc))
	hs := &http.Server{Handler: api.HTTPHandler(s, m.Handler()), ReadHeaderTimeout: 5 * time.Second}
	d := dispatch.New(s, dispatch.ConfigFrom(cfg), dispatch.WithLogger(log), dispatch.WithObserver(m))

	errCh := make(chan error, 2)
	go func() { errCh <- gs.Serve(grpcLn) }()
	go func() {
		if err := hs.Serve(httpLn); !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	bgCtx, cancelBg := context.WithCancel(context.Background())
	defer cancelBg()
	go m.RunQueueDepth(bgCtx, s, cfg.MetricsInterval, log)
	dispatchCtx, stopDispatch := context.WithCancel(context.Background())
	dispatchDone := make(chan error, 1)
	go func() { dispatchDone <- d.Run(dispatchCtx) }()

	log.Info("relay started", "grpc_addr", grpcLn.Addr().String(), "http_addr", httpLn.Addr().String(),
		"store", map[bool]string{true: "memory", false: "postgres"}[cfg.DatabaseURL == ""])

	var runErr error
	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")
	case runErr = <-errCh:
		log.Error("server failed", "err", runErr)
	}

	gs.GracefulStop()
	stopDispatch()
	if err := <-dispatchDone; err != nil {
		log.Error("dispatcher stopped with error", "err", err)
	}
	cancelBg()
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = hs.Shutdown(shutCtx)
	log.Info("relay stopped")
	return runErr
}
