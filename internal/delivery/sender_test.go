package delivery

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/NoahNaiman/relay/internal/clock"
)

func newSender(timeout time.Duration) *Sender {
	return &Sender{Client: &http.Client{}, Clock: clock.Real{}, Timeout: timeout}
}

func TestSendSignsAndPosts(t *testing.T) {
	var (
		gotHeader http.Header
		gotBody   []byte
		gotMethod string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotHeader = r.Method, r.Header.Clone()
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	resp := newSender(time.Second).Send(context.Background(), Request{
		URL: srv.URL, Secret: "k", JobID: "job-1", Attempt: 2, Payload: []byte(`{"x":1}`),
		Headers: map[string]string{"X-Custom": "yes", "relay-signature": "forged", "Content-Type": "application/vnd.test+json"},
	})
	if resp.Err != nil || resp.StatusCode != http.StatusAccepted {
		t.Fatalf("resp = %+v", resp)
	}
	if gotMethod != http.MethodPost || string(gotBody) != `{"x":1}` {
		t.Fatalf("method=%s body=%s", gotMethod, gotBody)
	}
	if gotHeader.Get("X-Custom") != "yes" || gotHeader.Get("Content-Type") != "application/vnd.test+json" {
		t.Fatalf("custom headers not forwarded: %v", gotHeader)
	}
	if gotHeader.Get(HeaderJobID) != "job-1" || gotHeader.Get(HeaderAttempt) != "2" {
		t.Fatalf("relay headers: %v", gotHeader)
	}
	if err := Verify("k", gotHeader, gotBody, time.Now(), 5*time.Minute); err != nil {
		t.Fatalf("receiver-side verification failed (job headers must not override the signature): %v", err)
	}
}

func TestSendFailures(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer slow.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closedURL := "http://" + ln.Addr().String()
	_ = ln.Close()

	cases := []struct {
		name  string
		url   string
		class Class
		msg   string
	}{
		{"timeout", slow.URL, Retryable, "timeout"},
		{"connection refused", closedURL, Retryable, "connection"},
		{"ftp scheme", "ftp://example.test/x", Permanent, "invalid url"},
		{"garbage", "::not a url", Permanent, "invalid url"},
		{"no host", "http://", Permanent, "invalid url"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := newSender(100*time.Millisecond).Send(context.Background(), Request{URL: tc.url, Secret: "k", JobID: "j", Attempt: 1})
			v := Classify(resp.StatusCode, resp.Header, resp.Err)
			if v.Class != tc.class || !strings.Contains(v.Reason, tc.msg) {
				t.Fatalf("got %v %q, want %v containing %q", v.Class, v.Reason, tc.class, tc.msg)
			}
		})
	}
}

func TestSendCanceled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resp := newSender(time.Second).Send(ctx, Request{URL: srv.URL, Secret: "k", JobID: "j", Attempt: 1})
	if resp.Err == nil || resp.Err.Error() != "canceled" {
		t.Fatalf("want canceled, got %v", resp.Err)
	}
}
