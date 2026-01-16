package delivery

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/NoahNaiman/relay/internal/clock"
)

// errInvalidURL marks URL problems that no retry can fix.
var errInvalidURL = errors.New("invalid url")

// IsInvalidURL reports whether err means the endpoint URL itself is unusable.
func IsInvalidURL(err error) bool { return errors.Is(err, errInvalidURL) }

// Request is one delivery attempt.
type Request struct {
	URL     string
	Secret  string
	JobID   string
	Attempt int // 1-based number of this attempt
	Payload []byte
	Headers map[string]string
}

// Response is the raw result of one attempt.
type Response struct {
	StatusCode int // 0 when no response was received
	Header     http.Header
	Err        error
	Duration   time.Duration
}

// Sender performs signed HTTP POSTs.
type Sender struct {
	Client  *http.Client
	Clock   clock.Clock
	Timeout time.Duration
}

// reservedHeaders cannot be overridden by job headers.
var reservedHeaders = map[string]bool{
	HeaderJobID: true, HeaderAttempt: true, HeaderTimestamp: true, HeaderSignature: true,
	"Content-Length": true, "Host": true,
}

// Send delivers one request. It never returns an error itself; failures are
// reported in Response.Err for the classifier.
func (s *Sender) Send(ctx context.Context, r Request) Response {
	start := time.Now()
	u, err := url.Parse(r.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return Response{Err: errInvalidURL}
	}
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.URL, bytes.NewReader(r.Payload))
	if err != nil {
		return Response{Err: errors.Join(errInvalidURL, err)}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "relay/1")
	for k, v := range r.Headers {
		if !reservedHeaders[http.CanonicalHeaderKey(k)] {
			req.Header.Set(k, v)
		}
	}
	Sign(req.Header, r.Secret, r.JobID, r.Attempt, s.Clock.Now(), r.Payload)
	resp, err := s.Client.Do(req)
	if err != nil {
		return Response{Err: simplify(err), Duration: time.Since(start)}
	}
	defer func() { _ = resp.Body.Close() }()
	// Drain a bounded amount so the connection can be reused.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	return Response{StatusCode: resp.StatusCode, Header: resp.Header, Duration: time.Since(start)}
}

// simplify turns verbose transport errors into short, stable messages
// suitable for last_error.
func simplify(err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return errors.New("timeout")
	case errors.Is(err, context.Canceled):
		return errors.New("canceled")
	}
	var ue *url.Error
	if errors.As(err, &ue) {
		msg := ue.Err.Error()
		if i := strings.LastIndex(msg, ": "); i >= 0 && strings.Contains(msg, "dial") {
			msg = "connection error: " + msg[i+2:]
		}
		return errors.New(msg)
	}
	return err
}
