package delivery

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Header names set on every delivery.
const (
	HeaderJobID     = "Relay-Job-Id"
	HeaderAttempt   = "Relay-Attempt"
	HeaderTimestamp = "Relay-Timestamp"
	HeaderSignature = "Relay-Signature"

	signaturePrefix = "sha256="
)

// Verification errors.
var (
	ErrMissingSignature = errors.New("missing signature headers")
	ErrBadTimestamp     = errors.New("malformed timestamp")
	ErrExpired          = errors.New("timestamp outside tolerance")
	ErrBadSignature     = errors.New("signature mismatch")
)

// Signature returns the Relay-Signature header value for a body sent at ts.
func Signature(secret string, ts time.Time, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(ts.Unix(), 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	return signaturePrefix + hex.EncodeToString(mac.Sum(nil))
}

// Sign sets the four Relay-* headers on h.
func Sign(h http.Header, secret, jobID string, attempt int, ts time.Time, body []byte) {
	h.Set(HeaderJobID, jobID)
	h.Set(HeaderAttempt, strconv.Itoa(attempt))
	h.Set(HeaderTimestamp, strconv.FormatInt(ts.Unix(), 10))
	h.Set(HeaderSignature, Signature(secret, ts, body))
}

// Verify checks a delivery's signature in constant time and rejects
// timestamps more than tolerance away from now, which bounds replay attacks.
func Verify(secret string, headers http.Header, body []byte, now time.Time, tolerance time.Duration) error {
	tsRaw := headers.Get(HeaderTimestamp)
	sig := headers.Get(HeaderSignature)
	if tsRaw == "" || sig == "" {
		return ErrMissingSignature
	}
	secs, err := strconv.ParseInt(tsRaw, 10, 64)
	if err != nil {
		return ErrBadTimestamp
	}
	ts := time.Unix(secs, 0)
	if d := now.Sub(ts); d > tolerance || d < -tolerance {
		return ErrExpired
	}
	if !strings.HasPrefix(sig, signaturePrefix) {
		return ErrBadSignature
	}
	if !hmac.Equal([]byte(sig), []byte(Signature(secret, ts, body))) {
		return ErrBadSignature
	}
	return nil
}
