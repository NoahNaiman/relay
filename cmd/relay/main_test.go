package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	relayv1 "github.com/NoahNaiman/relay/gen/relay/v1"
	"github.com/NoahNaiman/relay/internal/config"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// TestRunServesAndShutsDown starts the whole server on the memory store,
// delivers one job end to end, and checks probes, metrics and shutdown.
func TestRunServesAndShutsDown(t *testing.T) {
	received := make(chan struct{}, 1)
	hook := http.Server{Handler: http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		received <- struct{}{}
	})}
	hookLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = hook.Serve(hookLn) }()
	t.Cleanup(func() { _ = hook.Close() })

	cfg := config.Default()
	cfg.GRPCAddr, cfg.HTTPAddr = freeAddr(t), freeAddr(t)
	cfg.PollInterval = 10 * time.Millisecond
	cfg.MetricsInterval = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, slog.New(slog.DiscardHandler)) }()

	httpURL := "http://" + cfg.HTTPAddr
	waitOK(t, httpURL+"/readyz")
	waitOK(t, httpURL+"/healthz")

	conn, err := grpc.NewClient(cfg.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	client := relayv1.NewRelayServiceClient(conn)
	ep, err := client.RegisterEndpoint(ctx, &relayv1.RegisterEndpointRequest{Url: "http://" + hookLn.Addr().String(), Secret: "k"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.EnqueueJob(ctx, &relayv1.EnqueueJobRequest{EndpointId: ep.GetEndpoint().GetId(), Payload: []byte("{}")}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("job was not delivered")
	}
	time.Sleep(50 * time.Millisecond)
	body := get(t, httpURL+"/metrics")
	for _, want := range []string{
		"relay_jobs_enqueued_total 1",
		`relay_deliveries_total{result="delivered"} 1`,
		`relay_queue_depth{status="delivered"} 1`,
		"relay_delivery_duration_seconds_count 1",
	} {
		if !contains(body, want) {
			t.Errorf("metrics missing %q", want)
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("run did not shut down")
	}
}

func TestRunFailsOnBadAddress(t *testing.T) {
	cfg := config.Default()
	cfg.GRPCAddr = "256.0.0.1:bad"
	if err := run(context.Background(), cfg, slog.New(slog.DiscardHandler)); err == nil {
		t.Fatal("want listen error")
	}
}

func waitOK(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url) //nolint:noctx // test helper
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s never returned 200", url)
}

func get(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url) //nolint:noctx // test helper
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }
