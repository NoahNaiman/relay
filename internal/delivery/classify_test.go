package delivery

import (
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestClassify(t *testing.T) {
	ra := func(v string) http.Header { return http.Header{"Retry-After": []string{v}} }
	cases := []struct {
		name       string
		status     int
		header     http.Header
		err        error
		want       Class
		retryAfter time.Duration
	}{
		{"200", 200, nil, nil, Delivered, 0},
		{"204", 204, nil, nil, Delivered, 0},
		{"299", 299, nil, nil, Delivered, 0},
		{"408", 408, nil, nil, Retryable, 0},
		{"429 no header", 429, nil, nil, Retryable, 0},
		{"429 seconds", 429, ra("7"), nil, Retryable, 7 * time.Second},
		{"429 capped", 429, ra("86400"), nil, Retryable, MaxRetryAfter},
		{"429 exactly cap", 429, ra("600"), nil, Retryable, MaxRetryAfter},
		{"429 http-date ignored", 429, ra("Wed, 21 Oct 2026 07:28:00 GMT"), nil, Retryable, 0},
		{"429 negative ignored", 429, ra("-5"), nil, Retryable, 0},
		{"500", 500, nil, nil, Retryable, 0},
		{"503", 503, nil, nil, Retryable, 0},
		{"599", 599, nil, nil, Retryable, 0},
		{"400", 400, nil, nil, Permanent, 0},
		{"401", 401, nil, nil, Permanent, 0},
		{"404", 404, nil, nil, Permanent, 0},
		{"410", 410, nil, nil, Permanent, 0},
		{"422", 422, nil, nil, Permanent, 0},
		{"301 redirect", 301, nil, nil, Permanent, 0},
		{"timeout", 0, nil, errors.New("timeout"), Retryable, 0},
		{"connection refused", 0, nil, errors.New("connection error: connection refused"), Retryable, 0},
		{"invalid url", 0, nil, errInvalidURL, Permanent, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := tc.header
			if h == nil {
				h = http.Header{}
			}
			v := Classify(tc.status, h, tc.err)
			if v.Class != tc.want || v.RetryAfter != tc.retryAfter {
				t.Fatalf("got %v/%v, want %v/%v", v.Class, v.RetryAfter, tc.want, tc.retryAfter)
			}
			if tc.want != Delivered && v.Reason == "" {
				t.Fatal("failures must carry a reason")
			}
		})
	}
}

func TestClassString(t *testing.T) {
	for c, want := range map[Class]string{Delivered: "delivered", Retryable: "retry", Permanent: "dead", 0: "unknown"} {
		if c.String() != want {
			t.Fatalf("%d: %s", c, c.String())
		}
	}
}
