// Package api implements the relay.v1.RelayService gRPC handlers and the
// HTTP health, readiness and metrics endpoints.
package api

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	relayv1 "github.com/NoahNaiman/relay/gen/relay/v1"
	"github.com/NoahNaiman/relay/internal/store"
)

// Limits and defaults for RPC arguments.
const (
	MaxPayloadBytes    = 256 << 10
	DefaultMaxRPS      = 10
	DefaultMaxAttempts = 8
	MaxMaxAttempts     = 25
	DefaultPageSize    = 50
	MaxPageSize        = 500
	maxHeaders         = 50
)

// Server implements relayv1.RelayServiceServer.
type Server struct {
	relayv1.UnimplementedRelayServiceServer
	store    store.Store
	log      *slog.Logger
	enqueued func() // called once per newly created job
}

// New returns a Server. onEnqueue, if non-nil, is called for every newly created job.
func New(s store.Store, log *slog.Logger, onEnqueue func()) *Server {
	if onEnqueue == nil {
		onEnqueue = func() {}
	}
	return &Server{store: s, log: log, enqueued: onEnqueue}
}

// RegisterEndpoint implements relayv1.RelayServiceServer.
func (s *Server) RegisterEndpoint(ctx context.Context, req *relayv1.RegisterEndpointRequest) (*relayv1.RegisterEndpointResponse, error) {
	u, err := url.Parse(req.GetUrl())
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, status.Error(codes.InvalidArgument, "url must be an absolute http or https URL")
	}
	if req.GetSecret() == "" {
		return nil, status.Error(codes.InvalidArgument, "secret is required")
	}
	maxRPS := int(req.GetMaxRps())
	if maxRPS == 0 {
		maxRPS = DefaultMaxRPS
	}
	if maxRPS < 1 {
		return nil, status.Error(codes.InvalidArgument, "max_rps must be >= 1")
	}
	maxAttempts := int(req.GetMaxAttempts())
	if maxAttempts == 0 {
		maxAttempts = DefaultMaxAttempts
	}
	if maxAttempts < 1 || maxAttempts > MaxMaxAttempts {
		return nil, status.Errorf(codes.InvalidArgument, "max_attempts must be between 1 and %d", MaxMaxAttempts)
	}
	e, err := s.store.CreateEndpoint(ctx, store.Endpoint{URL: u.String(), Secret: req.GetSecret(), MaxRPS: maxRPS, MaxAttempts: maxAttempts})
	if err != nil {
		return nil, s.internal(ctx, "create endpoint", err)
	}
	s.log.InfoContext(ctx, "endpoint registered", "endpoint_id", e.ID, "max_rps", maxRPS, "max_attempts", maxAttempts)
	return &relayv1.RegisterEndpointResponse{Endpoint: toEndpoint(e)}, nil
}

// EnqueueJob implements relayv1.RelayServiceServer.
func (s *Server) EnqueueJob(ctx context.Context, req *relayv1.EnqueueJobRequest) (*relayv1.EnqueueJobResponse, error) {
	epID, err := parseID("endpoint_id", req.GetEndpointId())
	if err != nil {
		return nil, err
	}
	if n := len(req.GetPayload()); n > MaxPayloadBytes {
		return nil, status.Errorf(codes.InvalidArgument, "payload is %d bytes; the limit is %d", n, MaxPayloadBytes)
	}
	if err := validateHeaders(req.GetHeaders()); err != nil {
		return nil, err
	}
	j, created, err := s.store.Enqueue(ctx, store.NewJob{
		EndpointID:     epID,
		Payload:        req.GetPayload(),
		Headers:        req.GetHeaders(),
		IdempotencyKey: req.GetIdempotencyKey(),
	})
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "endpoint not found")
	}
	if err != nil {
		return nil, s.internal(ctx, "enqueue", err)
	}
	if created {
		s.enqueued()
	}
	return &relayv1.EnqueueJobResponse{Job: toJob(j)}, nil
}

// GetJob implements relayv1.RelayServiceServer.
func (s *Server) GetJob(ctx context.Context, req *relayv1.GetJobRequest) (*relayv1.GetJobResponse, error) {
	id, err := parseID("job_id", req.GetJobId())
	if err != nil {
		return nil, err
	}
	j, err := s.store.GetJob(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "job not found")
	}
	if err != nil {
		return nil, s.internal(ctx, "get job", err)
	}
	return &relayv1.GetJobResponse{Job: toJob(j)}, nil
}

// ListDeadLetters implements relayv1.RelayServiceServer.
func (s *Server) ListDeadLetters(ctx context.Context, req *relayv1.ListDeadLettersRequest) (*relayv1.ListDeadLettersResponse, error) {
	epID, err := parseID("endpoint_id", req.GetEndpointId())
	if err != nil {
		return nil, err
	}
	size := int(req.GetPageSize())
	switch {
	case size == 0:
		size = DefaultPageSize
	case size < 0:
		return nil, status.Error(codes.InvalidArgument, "page_size must be >= 0")
	case size > MaxPageSize:
		size = MaxPageSize
	}
	var after *store.Cursor
	if tok := req.GetPageToken(); tok != "" {
		c, err := decodeCursor(tok)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid page_token")
		}
		after = &c
	}
	if _, err := s.store.GetEndpoint(ctx, epID); errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "endpoint not found")
	} else if err != nil {
		return nil, s.internal(ctx, "get endpoint", err)
	}
	jobs, next, err := s.store.ListDeadLetters(ctx, epID, size, after)
	if err != nil {
		return nil, s.internal(ctx, "list dead letters", err)
	}
	resp := &relayv1.ListDeadLettersResponse{Jobs: make([]*relayv1.Job, len(jobs))}
	for i, j := range jobs {
		resp.Jobs[i] = toJob(j)
	}
	if next != nil {
		resp.NextPageToken = encodeCursor(*next)
	}
	return resp, nil
}

// ReplayDeadLetter implements relayv1.RelayServiceServer.
func (s *Server) ReplayDeadLetter(ctx context.Context, req *relayv1.ReplayDeadLetterRequest) (*relayv1.ReplayDeadLetterResponse, error) {
	id, err := parseID("job_id", req.GetJobId())
	if err != nil {
		return nil, err
	}
	j, err := s.store.Replay(ctx, id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		return nil, status.Error(codes.NotFound, "job not found")
	case errors.Is(err, store.ErrNotDead):
		return nil, status.Error(codes.FailedPrecondition, "only dead jobs can be replayed")
	case err != nil:
		return nil, s.internal(ctx, "replay", err)
	}
	s.log.InfoContext(ctx, "dead letter replayed", "job_id", id)
	return &relayv1.ReplayDeadLetterResponse{Job: toJob(j)}, nil
}

func (s *Server) internal(ctx context.Context, op string, err error) error {
	s.log.ErrorContext(ctx, op+" failed", "err", err)
	return status.Error(codes.Internal, op+" failed")
}

func parseID(field, v string) (uuid.UUID, error) {
	id, err := uuid.Parse(v)
	if err != nil {
		return uuid.Nil, status.Errorf(codes.InvalidArgument, "%s must be a UUID", field)
	}
	return id, nil
}

// validateHeaders rejects header names that are not RFC 9110 tokens and
// values containing control characters, which would make every attempt fail.
func validateHeaders(h map[string]string) error {
	if len(h) > maxHeaders {
		return status.Errorf(codes.InvalidArgument, "at most %d headers are allowed", maxHeaders)
	}
	for k, v := range h {
		if k == "" || strings.IndexFunc(k, func(r rune) bool { return !isTokenChar(r) }) >= 0 {
			return status.Errorf(codes.InvalidArgument, "invalid header name %q", k)
		}
		if strings.ContainsAny(v, "\r\n\x00") {
			return status.Errorf(codes.InvalidArgument, "invalid value for header %q", k)
		}
	}
	return nil
}

func isTokenChar(r rune) bool {
	if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
		return true
	}
	return strings.ContainsRune("!#$%&'*+-.^_`|~", r)
}

// Page tokens are opaque to clients: base64url("<unix nanos>:<uuid>").
func encodeCursor(c store.Cursor) string {
	raw := strconv.FormatInt(c.UpdatedAt.UnixNano(), 10) + ":" + c.ID.String()
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeCursor(tok string) (store.Cursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		return store.Cursor{}, err
	}
	ts, id, ok := strings.Cut(string(raw), ":")
	if !ok {
		return store.Cursor{}, fmt.Errorf("malformed cursor")
	}
	nanos, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return store.Cursor{}, err
	}
	u, err := uuid.Parse(id)
	if err != nil {
		return store.Cursor{}, err
	}
	return store.Cursor{UpdatedAt: time.Unix(0, nanos).UTC(), ID: u}, nil
}

func toEndpoint(e store.Endpoint) *relayv1.Endpoint {
	return &relayv1.Endpoint{
		Id:          e.ID.String(),
		Url:         e.URL,
		MaxRps:      int32(e.MaxRPS),      //nolint:gosec // validated to small ranges on registration
		MaxAttempts: int32(e.MaxAttempts), //nolint:gosec // validated to 1-25 on registration
		CreatedAt:   timestamppb.New(e.CreatedAt),
	}
}

var statusToProto = map[store.Status]relayv1.JobStatus{
	store.StatusPending:   relayv1.JobStatus_JOB_STATUS_PENDING,
	store.StatusInFlight:  relayv1.JobStatus_JOB_STATUS_IN_FLIGHT,
	store.StatusDelivered: relayv1.JobStatus_JOB_STATUS_DELIVERED,
	store.StatusDead:      relayv1.JobStatus_JOB_STATUS_DEAD,
}

func toJob(j store.Job) *relayv1.Job {
	return &relayv1.Job{
		Id:             j.ID.String(),
		EndpointId:     j.EndpointID.String(),
		Status:         statusToProto[j.Status],
		Attempts:       int32(j.Attempts), //nolint:gosec // bounded by max_attempts (<= 25)
		LastError:      j.LastError,
		NextAttemptAt:  timestamppb.New(j.NextAttemptAt),
		IdempotencyKey: j.IdempotencyKey,
		CreatedAt:      timestamppb.New(j.CreatedAt),
		UpdatedAt:      timestamppb.New(j.UpdatedAt),
	}
}
