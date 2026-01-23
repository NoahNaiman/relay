package api

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	relayv1 "github.com/NoahNaiman/relay/gen/relay/v1"
	"github.com/NoahNaiman/relay/internal/metrics"
	"github.com/NoahNaiman/relay/internal/store"
	"github.com/NoahNaiman/relay/internal/store/memory"
)

type harness struct {
	client   relayv1.RelayServiceClient
	store    *memory.Store
	enqueued int
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{store: memory.New()}
	ln := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	relayv1.RegisterRelayServiceServer(gs, New(h.store, slog.New(slog.DiscardHandler), func() { h.enqueued++ }))
	go func() { _ = gs.Serve(ln) }()
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return ln.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(1<<20)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	h.client = relayv1.NewRelayServiceClient(conn)
	return h
}

func wantCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	if status.Code(err) != code {
		t.Fatalf("got %v, want %s", err, code)
	}
}

func (h *harness) endpoint(t *testing.T, maxAttempts int32) *relayv1.Endpoint {
	t.Helper()
	r, err := h.client.RegisterEndpoint(context.Background(), &relayv1.RegisterEndpointRequest{Url: "https://example.test/hook", Secret: "k", MaxAttempts: maxAttempts})
	if err != nil {
		t.Fatal(err)
	}
	return r.GetEndpoint()
}

func TestRegisterEndpoint(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	r, err := h.client.RegisterEndpoint(ctx, &relayv1.RegisterEndpointRequest{Url: "http://example.test/x", Secret: "k"})
	if err != nil {
		t.Fatal(err)
	}
	e := r.GetEndpoint()
	if e.GetMaxRps() != DefaultMaxRPS || e.GetMaxAttempts() != DefaultMaxAttempts || e.GetId() == "" || e.GetCreatedAt() == nil {
		t.Fatalf("defaults not applied: %v", e)
	}
	r, err = h.client.RegisterEndpoint(ctx, &relayv1.RegisterEndpointRequest{Url: "https://example.test", Secret: "k", MaxRps: 3, MaxAttempts: 25})
	if err != nil || r.GetEndpoint().GetMaxRps() != 3 || r.GetEndpoint().GetMaxAttempts() != 25 {
		t.Fatalf("explicit values: %v %v", r, err)
	}

	bad := []*relayv1.RegisterEndpointRequest{
		{Url: "ftp://example.test", Secret: "k"},
		{Url: "example.test/hook", Secret: "k"},
		{Url: "http://", Secret: "k"},
		{Url: "::bad", Secret: "k"},
		{Url: "http://example.test"},
		{Url: "http://example.test", Secret: "k", MaxRps: -1},
		{Url: "http://example.test", Secret: "k", MaxAttempts: 26},
		{Url: "http://example.test", Secret: "k", MaxAttempts: -1},
	}
	for _, req := range bad {
		_, err := h.client.RegisterEndpoint(ctx, req)
		wantCode(t, err, codes.InvalidArgument)
	}
}

func TestEnqueueAndGetJob(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	ep := h.endpoint(t, 0)

	r, err := h.client.EnqueueJob(ctx, &relayv1.EnqueueJobRequest{EndpointId: ep.GetId(), Payload: []byte(`{}`), Headers: map[string]string{"X-A": "1"}})
	if err != nil {
		t.Fatal(err)
	}
	j := r.GetJob()
	if j.GetStatus() != relayv1.JobStatus_JOB_STATUS_PENDING || j.GetAttempts() != 0 || j.GetNextAttemptAt() == nil {
		t.Fatalf("job: %v", j)
	}
	got, err := h.client.GetJob(ctx, &relayv1.GetJobRequest{JobId: j.GetId()})
	if err != nil || got.GetJob().GetId() != j.GetId() {
		t.Fatalf("GetJob: %v %v", got, err)
	}
	if h.enqueued != 1 {
		t.Fatalf("enqueue counter = %d", h.enqueued)
	}

	// Exactly at the limit is fine; one byte over is not.
	_, err = h.client.EnqueueJob(ctx, &relayv1.EnqueueJobRequest{EndpointId: ep.GetId(), Payload: make([]byte, MaxPayloadBytes)})
	if err != nil {
		t.Fatalf("256 KB payload should be accepted: %v", err)
	}
	_, err = h.client.EnqueueJob(ctx, &relayv1.EnqueueJobRequest{EndpointId: ep.GetId(), Payload: make([]byte, MaxPayloadBytes+1)})
	wantCode(t, err, codes.InvalidArgument)

	_, err = h.client.EnqueueJob(ctx, &relayv1.EnqueueJobRequest{EndpointId: "nope"})
	wantCode(t, err, codes.InvalidArgument)
	_, err = h.client.EnqueueJob(ctx, &relayv1.EnqueueJobRequest{EndpointId: uuid.NewString()})
	wantCode(t, err, codes.NotFound)
	_, err = h.client.EnqueueJob(ctx, &relayv1.EnqueueJobRequest{EndpointId: ep.GetId(), Headers: map[string]string{"Bad Header": "x"}})
	wantCode(t, err, codes.InvalidArgument)
	_, err = h.client.EnqueueJob(ctx, &relayv1.EnqueueJobRequest{EndpointId: ep.GetId(), Headers: map[string]string{"X-Ok": "a\r\nInjected: 1"}})
	wantCode(t, err, codes.InvalidArgument)
	many := map[string]string{}
	for i := range maxHeaders + 1 {
		many["X-H"+strings.Repeat("a", i)] = "v"
	}
	_, err = h.client.EnqueueJob(ctx, &relayv1.EnqueueJobRequest{EndpointId: ep.GetId(), Headers: many})
	wantCode(t, err, codes.InvalidArgument)

	_, err = h.client.GetJob(ctx, &relayv1.GetJobRequest{JobId: "x"})
	wantCode(t, err, codes.InvalidArgument)
	_, err = h.client.GetJob(ctx, &relayv1.GetJobRequest{JobId: uuid.NewString()})
	wantCode(t, err, codes.NotFound)
}

func TestEnqueueIdempotency(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	ep := h.endpoint(t, 0)
	first, err := h.client.EnqueueJob(ctx, &relayv1.EnqueueJobRequest{EndpointId: ep.GetId(), Payload: []byte("a"), IdempotencyKey: "order-1"})
	if err != nil {
		t.Fatal(err)
	}
	again, err := h.client.EnqueueJob(ctx, &relayv1.EnqueueJobRequest{EndpointId: ep.GetId(), Payload: []byte("b"), IdempotencyKey: "order-1"})
	if err != nil {
		t.Fatalf("a repeated key must not be an error: %v", err)
	}
	if again.GetJob().GetId() != first.GetJob().GetId() || again.GetJob().GetIdempotencyKey() != "order-1" {
		t.Fatal("a repeated key must return the existing job")
	}
	if h.enqueued != 1 {
		t.Fatalf("idempotent repeats must not count as enqueued: %d", h.enqueued)
	}
}

// killJob claims a job directly from the store and dead-letters it.
func killJob(t *testing.T, s store.Store) {
	t.Helper()
	ctx := context.Background()
	c, err := s.Claim(ctx, 1, time.Minute)
	if err != nil || len(c) != 1 {
		t.Fatalf("claim: %v %d", err, len(c))
	}
	if _, err := s.Complete(ctx, c[0].Job.ID, c[0].Job.LeaseToken, store.Result{Outcome: store.OutcomeDead, Error: "HTTP 410"}); err != nil {
		t.Fatal(err)
	}
}

func TestDeadLettersAndReplay(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	ep := h.endpoint(t, 1)
	var ids []string
	for range 5 {
		r, err := h.client.EnqueueJob(ctx, &relayv1.EnqueueJobRequest{EndpointId: ep.GetId()})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, r.GetJob().GetId())
		killJob(t, h.store)
	}

	var seen []string
	token := ""
	for page := 0; ; page++ {
		r, err := h.client.ListDeadLetters(ctx, &relayv1.ListDeadLettersRequest{EndpointId: ep.GetId(), PageSize: 2, PageToken: token})
		if err != nil {
			t.Fatal(err)
		}
		for _, j := range r.GetJobs() {
			if j.GetStatus() != relayv1.JobStatus_JOB_STATUS_DEAD || j.GetLastError() != "HTTP 410" {
				t.Fatalf("job: %v", j)
			}
			seen = append(seen, j.GetId())
		}
		token = r.GetNextPageToken()
		if token == "" {
			break
		}
		if page > 5 {
			t.Fatal("pagination does not terminate")
		}
	}
	if len(seen) != 5 {
		t.Fatalf("listed %d dead letters, want 5", len(seen))
	}
	r, err := h.client.ListDeadLetters(ctx, &relayv1.ListDeadLettersRequest{EndpointId: ep.GetId()})
	if err != nil || len(r.GetJobs()) != 5 || r.GetNextPageToken() != "" {
		t.Fatalf("default page size: %v %v", r, err)
	}
	r, err = h.client.ListDeadLetters(ctx, &relayv1.ListDeadLettersRequest{EndpointId: ep.GetId(), PageSize: 10_000})
	if err != nil || len(r.GetJobs()) != 5 {
		t.Fatalf("oversized page is clamped, not rejected: %v", err)
	}

	for _, tc := range []struct {
		req  *relayv1.ListDeadLettersRequest
		code codes.Code
	}{
		{&relayv1.ListDeadLettersRequest{EndpointId: "x"}, codes.InvalidArgument},
		{&relayv1.ListDeadLettersRequest{EndpointId: ep.GetId(), PageSize: -1}, codes.InvalidArgument},
		{&relayv1.ListDeadLettersRequest{EndpointId: ep.GetId(), PageToken: "!!"}, codes.InvalidArgument},
		{&relayv1.ListDeadLettersRequest{EndpointId: ep.GetId(), PageToken: "bm9jb2xvbg"}, codes.InvalidArgument}, // "nocolon"
		{&relayv1.ListDeadLettersRequest{EndpointId: ep.GetId(), PageToken: "eDp5"}, codes.InvalidArgument},       // "x:y"
		{&relayv1.ListDeadLettersRequest{EndpointId: ep.GetId(), PageToken: "MTp1dWlk"}, codes.InvalidArgument},   // "1:uuid"
		{&relayv1.ListDeadLettersRequest{EndpointId: uuid.NewString()}, codes.NotFound},
	} {
		_, err := h.client.ListDeadLetters(ctx, tc.req)
		wantCode(t, err, tc.code)
	}

	rep, err := h.client.ReplayDeadLetter(ctx, &relayv1.ReplayDeadLetterRequest{JobId: ids[0]})
	if err != nil {
		t.Fatal(err)
	}
	if rep.GetJob().GetStatus() != relayv1.JobStatus_JOB_STATUS_PENDING || rep.GetJob().GetAttempts() != 0 {
		t.Fatalf("replayed: %v", rep.GetJob())
	}
	_, err = h.client.ReplayDeadLetter(ctx, &relayv1.ReplayDeadLetterRequest{JobId: ids[0]})
	wantCode(t, err, codes.FailedPrecondition)
	_, err = h.client.ReplayDeadLetter(ctx, &relayv1.ReplayDeadLetterRequest{JobId: uuid.NewString()})
	wantCode(t, err, codes.NotFound)
	_, err = h.client.ReplayDeadLetter(ctx, &relayv1.ReplayDeadLetterRequest{JobId: "x"})
	wantCode(t, err, codes.InvalidArgument)
}

func TestCursorRoundTrip(t *testing.T) {
	c := store.Cursor{UpdatedAt: time.Date(2026, 3, 4, 5, 6, 7, 123456000, time.UTC), ID: uuid.New()}
	got, err := decodeCursor(encodeCursor(c))
	if err != nil || !got.UpdatedAt.Equal(c.UpdatedAt) || got.ID != c.ID {
		t.Fatalf("round trip: %+v %v", got, err)
	}
}

// brokenStore fails every call, to exercise Internal errors and readiness.
type brokenStore struct{ store.Store }

var errBroken = errors.New("database unavailable")

func (brokenStore) CreateEndpoint(context.Context, store.Endpoint) (store.Endpoint, error) {
	return store.Endpoint{}, errBroken
}
func (brokenStore) GetEndpoint(context.Context, uuid.UUID) (store.Endpoint, error) {
	return store.Endpoint{}, errBroken
}
func (brokenStore) Enqueue(context.Context, store.NewJob) (store.Job, bool, error) {
	return store.Job{}, false, errBroken
}
func (brokenStore) GetJob(context.Context, uuid.UUID) (store.Job, error) {
	return store.Job{}, errBroken
}
func (brokenStore) Replay(context.Context, uuid.UUID) (store.Job, error) {
	return store.Job{}, errBroken
}
func (brokenStore) Ping(context.Context) error { return errBroken }

func TestInternalErrorsAreNotLeaked(t *testing.T) {
	s := New(brokenStore{}, slog.New(slog.DiscardHandler), nil)
	ctx := context.Background()
	id := uuid.NewString()
	_, e1 := s.RegisterEndpoint(ctx, &relayv1.RegisterEndpointRequest{Url: "http://x.test", Secret: "k"})
	_, e2 := s.EnqueueJob(ctx, &relayv1.EnqueueJobRequest{EndpointId: id})
	_, e3 := s.GetJob(ctx, &relayv1.GetJobRequest{JobId: id})
	_, e4 := s.ListDeadLetters(ctx, &relayv1.ListDeadLettersRequest{EndpointId: id})
	_, e5 := s.ReplayDeadLetter(ctx, &relayv1.ReplayDeadLetterRequest{JobId: id})
	for i, err := range []error{e1, e2, e3, e4, e5} {
		wantCode(t, err, codes.Internal)
		if strings.Contains(err.Error(), errBroken.Error()) {
			t.Fatalf("call %d leaked the internal error: %v", i, err)
		}
	}
}

func TestHTTPProbes(t *testing.T) {
	m := metrics.New()
	ok := httptest.NewServer(HTTPHandler(memory.New(), m.Handler()))
	defer ok.Close()
	down := httptest.NewServer(HTTPHandler(brokenStore{}, m.Handler()))
	defer down.Close()

	cases := []struct {
		url  string
		code int
	}{
		{ok.URL + "/healthz", http.StatusOK},
		{ok.URL + "/readyz", http.StatusOK},
		{ok.URL + "/metrics", http.StatusOK},
		{down.URL + "/healthz", http.StatusOK},
		{down.URL + "/readyz", http.StatusServiceUnavailable},
	}
	for _, tc := range cases {
		resp, err := http.Get(tc.url) //nolint:noctx // test
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != tc.code {
			t.Errorf("%s: %d, want %d", tc.url, resp.StatusCode, tc.code)
		}
	}
}
