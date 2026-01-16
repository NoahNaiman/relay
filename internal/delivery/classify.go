package delivery

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Class is the outcome category of one delivery attempt.
type Class int

// Classes, in the order of the README's classification table.
const (
	Delivered Class = iota + 1
	Retryable
	Permanent
)

func (c Class) String() string {
	switch c {
	case Delivered:
		return "delivered"
	case Retryable:
		return "retry"
	case Permanent:
		return "dead"
	default:
		return "unknown"
	}
}

// MaxRetryAfter caps how long a Retry-After header can postpone a job.
const MaxRetryAfter = 10 * time.Minute

// Verdict is the classifier's decision for one attempt.
type Verdict struct {
	Class      Class
	RetryAfter time.Duration // > 0 only for a 429 with a valid Retry-After
	Reason     string        // short description, stored as last_error
}

// Classify maps an HTTP status code (0 when there is no response) and a
// transport error to a verdict.
//
//	2xx                                    -> Delivered
//	408, 429, 5xx, timeout, connection err -> Retryable (429 honors Retry-After)
//	other 4xx, other statuses, invalid URL -> Permanent
func Classify(status int, header http.Header, err error) Verdict {
	if err != nil {
		if IsInvalidURL(err) {
			return Verdict{Class: Permanent, Reason: "invalid url: " + err.Error()}
		}
		return Verdict{Class: Retryable, Reason: err.Error()}
	}
	reason := "HTTP " + strconv.Itoa(status)
	switch {
	case status >= 200 && status < 300:
		return Verdict{Class: Delivered}
	case status == http.StatusTooManyRequests:
		return Verdict{Class: Retryable, RetryAfter: parseRetryAfter(header.Get("Retry-After")), Reason: reason}
	case status == http.StatusRequestTimeout, status >= 500 && status < 600:
		return Verdict{Class: Retryable, Reason: reason}
	default:
		return Verdict{Class: Permanent, Reason: reason}
	}
}

// parseRetryAfter accepts delay-seconds only, capped at MaxRetryAfter.
func parseRetryAfter(v string) time.Duration {
	secs, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || secs <= 0 {
		return 0
	}
	if d := time.Duration(secs) * time.Second; d < MaxRetryAfter {
		return d
	}
	return MaxRetryAfter
}
