package delivery

import (
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestSignatureKnownVector(t *testing.T) {
	// Computed independently:
	//   printf '1767600000.{"a":1}' | openssl dgst -sha256 -hmac secret
	got := Signature("secret", time.Unix(1767600000, 0), []byte(`{"a":1}`))
	want := "sha256=" + knownHMAC
	if got != want {
		t.Fatalf("got %s\nwant %s", got, want)
	}
}

func TestVerify(t *testing.T) {
	now := time.Unix(1767600000, 0)
	body := []byte(`{"hello":"world"}`)
	signed := func(secret string, ts time.Time) http.Header {
		h := http.Header{}
		Sign(h, secret, "job-1", 3, ts, body)
		return h
	}
	tol := 5 * time.Minute
	cases := []struct {
		name    string
		headers http.Header
		body    []byte
		now     time.Time
		want    error
	}{
		{"valid", signed("k", now), body, now, nil},
		{"valid at past edge", signed("k", now.Add(-tol)), body, now, nil},
		{"valid at future edge", signed("k", now.Add(tol)), body, now, nil},
		{"too old", signed("k", now.Add(-tol-time.Second)), body, now, ErrExpired},
		{"too new", signed("k", now.Add(tol+time.Second)), body, now, ErrExpired},
		{"wrong secret", signed("other", now), body, now, ErrBadSignature},
		{"tampered body", signed("k", now), []byte(`{"hello":"mars"}`), now, ErrBadSignature},
		{"missing headers", http.Header{}, body, now, ErrMissingSignature},
		{"bad timestamp", func() http.Header {
			h := signed("k", now)
			h.Set(HeaderTimestamp, "yesterday")
			return h
		}(), body, now, ErrBadTimestamp},
		{"tampered timestamp", func() http.Header {
			h := signed("k", now)
			h.Set(HeaderTimestamp, "1767600001")
			return h
		}(), body, now, ErrBadSignature},
		{"missing prefix", func() http.Header {
			h := signed("k", now)
			h.Set(HeaderSignature, h.Get(HeaderSignature)[len("sha256="):])
			return h
		}(), body, now, ErrBadSignature},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := Verify("k", tc.headers, tc.body, tc.now, tol); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestSignSetsHeaders(t *testing.T) {
	h := http.Header{}
	Sign(h, "k", "job-9", 2, time.Unix(100, 0), nil)
	if h.Get(HeaderJobID) != "job-9" || h.Get(HeaderAttempt) != "2" || h.Get(HeaderTimestamp) != "100" {
		t.Fatalf("headers: %v", h)
	}
}

const knownHMAC = "75ef4ed709e4ac7108c39bb3714441b8a193aed3c5858f1f729ff4c16ad3a5ad"
