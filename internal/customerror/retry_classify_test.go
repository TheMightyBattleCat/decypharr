package customerror

import (
	"errors"
	"fmt"
	"testing"
)

// typedRetryable stands in for *nntp.Error: an error that knows whether a
// retry can help, without being a *customerror.Error.
type typedRetryable struct {
	msg       string
	retryable bool
}

func (e *typedRetryable) Error() string     { return e.msg }
func (e *typedRetryable) IsRetryable() bool { return e.retryable }

// A typed error's own answer wins over the text of whatever wraps it, and is
// found anywhere in the chain, an errors.Join included.
func TestRetriableAsksTypedErrorBeforeText(t *testing.T) {
	timeout := &typedRetryable{msg: "NNTP TIMEOUT (code 0): read timed out", retryable: true}
	decode := &typedRetryable{msg: "NNTP YENC_DECODE (code 0): connection reset by peer", retryable: false}

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"bare retryable", timeout, true},
		{"wrapped retryable", fmt.Errorf("fetch segment 12: %w", timeout), true},
		// The wrapper's "4041" used to match the 404 pattern first.
		{"wrapped retryable, wrapper text holds the digits 404", fmt.Errorf("fetch segment 4041: %w", timeout), true},
		{"wrapped retryable, wrapper text is the number 404", fmt.Errorf("re-fetch segment 404: %w", timeout), true},
		// The wrapper's text says "connection reset by peer"; the typed
		// error under it says a retry cannot help.
		{"wrapped not-retryable whose text matches a retriable pattern", fmt.Errorf("fetch segment 12: %w", decode), false},
		{"joined retryable", errors.Join(errors.New("provider a failed"), timeout), true},
	}
	for _, c := range cases {
		if got := IsRetriableError(c.err); got != c.want {
			t.Errorf("%s: IsRetriableError = %v, want %v", c.name, got, c.want)
		}
	}
}

// A status code counts only as a whole number: not inside a segment number,
// a byte count or a duration. The first two messages were seen in production
// logs, where they tripped the mount's read breaker.
func TestPermanentMatchesStatusCodesAsWholeNumbers(t *testing.T) {
	cases := []struct {
		msg  string
		want bool
	}{
		{"segment 41094 still missing after re-fetch", false},
		{"segment 2403 still missing after re-fetch", false},
		{"segment 41195 still missing after re-fetch", false},
		{"read 14040 of 786432 bytes: short article", false},
		{"stalled for 4100ms waiting on the network", false},
		{"HTTP 404 Not Found", true},
		{"unexpected HTTP status: 403", true},
		{"404", true},
		{"status=410, body empty", true},
		{"(401)", true},
		{"402: quota used up", true},
	}
	for _, c := range cases {
		if got := IsPermanentError(errors.New(c.msg)); got != c.want {
			t.Errorf("IsPermanentError(%q) = %v, want %v", c.msg, got, c.want)
		}
	}
}

// With plain text, only a number that is itself one of the five status codes
// still reads as permanent; it took 1.5% of the segment numbers below 100,000
// before.
func TestPermanentSegmentNumberShare(t *testing.T) {
	const n = 100000
	var hits []int
	for i := range n {
		if IsPermanentError(fmt.Errorf("segment %d still missing after re-fetch", i)) {
			hits = append(hits, i)
		}
	}
	if fmt.Sprint(hits) != "[401 402 403 404 410]" {
		t.Errorf("segment numbers read as permanent = %v, want only the five status codes", hits)
	}
}

// An error that says a retry can help is not permanent, whatever number its
// text or its wrapper's carries. "Not retryable" is still not a verdict.
func TestPermanentAsksTypedRetryableBeforeText(t *testing.T) {
	retryable := &typedRetryable{msg: "segment 404 still missing after re-fetch", retryable: true}
	notRetryable := &typedRetryable{msg: "segment 404 could not be decoded", retryable: false}

	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"retryable":         {retryable, false},
		"wrapped retryable": {fmt.Errorf("read 404: %w", retryable), false},
		"joined retryable":  {errors.Join(errors.New("HTTP 404"), retryable), false},
		"not retryable":     {notRetryable, true},
	} {
		if got := IsPermanentError(tc.err); got != tc.want {
			t.Errorf("%s: IsPermanentError(%q) = %v, want %v", name, tc.err, got, tc.want)
		}
	}
}
