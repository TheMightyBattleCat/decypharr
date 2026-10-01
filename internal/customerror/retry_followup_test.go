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

// These tests record how the classifiers behave today. Each case that is
// marked defect asserts the present, wrong answer, so the test fails (and
// must be updated) when the classifier is fixed.
func TestFollowupRetriableWrappedTypedError(t *testing.T) {
	timeout := &typedRetryable{msg: "NNTP TIMEOUT (code 0): read timed out", retryable: true}
	decode := &typedRetryable{msg: "NNTP YENC_DECODE (code 0): connection reset by peer", retryable: false}

	cases := []struct {
		name   string
		err    error
		want   bool
		defect string
	}{
		{name: "bare retryable", err: timeout, want: true},
		// The outer-only type check misses this one, but the unwrap at the
		// end of IsRetriableError reaches the typed error, so the answer is
		// still right.
		{name: "wrapped retryable", err: fmt.Errorf("fetch segment 12: %w", timeout), want: true},
		{
			name:   "wrapped retryable, wrapper text holds the digits 404",
			err:    fmt.Errorf("fetch segment 4041: %w", timeout),
			want:   false,
			defect: "the text patterns run on the wrapper before the typed error is reached",
		},
		{
			name:   "wrapped not-retryable whose text matches a retriable pattern",
			err:    fmt.Errorf("fetch segment 12: %w", decode),
			want:   true,
			defect: "the wrapper's text says 'connection reset by peer', so the typed answer is never asked",
		},
		{
			name:   "joined retryable",
			err:    errors.Join(errors.New("provider a failed"), timeout),
			want:   false,
			defect: "errors.Join has no single Unwrap, so the typed error is never found",
		},
	}
	for _, c := range cases {
		got := IsRetriableError(c.err)
		if got != c.want {
			t.Errorf("%s: IsRetriableError = %v, want %v", c.name, got, c.want)
		}
		if c.defect != "" {
			t.Logf("DEFECT (recorded): %s: %s", c.name, c.defect)
		}
	}
}

// The permanent patterns are plain substrings, so a status code matches
// inside any longer number. The reader's own error text carries a segment
// number, which makes the verdict depend on which segment failed. Both
// messages below were seen in production logs.
func TestFollowupPermanentMatchesDigitsInsideNumbers(t *testing.T) {
	cases := []struct {
		msg    string
		want   bool
		defect bool
	}{
		{msg: "segment 41094 still missing after re-fetch", want: true, defect: true},
		{msg: "segment 2403 still missing after re-fetch", want: true, defect: true},
		{msg: "segment 41195 still missing after re-fetch", want: false},
		{msg: "read 14040 of 786432 bytes: short article", want: true, defect: true},
		{msg: "stalled for 4100ms waiting on the network", want: true, defect: true},
		{msg: "HTTP 404 Not Found", want: true},
	}
	for _, c := range cases {
		got := IsPermanentError(errors.New(c.msg))
		if got != c.want {
			t.Errorf("IsPermanentError(%q) = %v, want %v", c.msg, got, c.want)
		}
		if c.defect {
			t.Logf("DEFECT (recorded): %q counts as permanent only because of the digits in a number", c.msg)
		}
	}
}

// How often a random segment number trips a status-code pattern: the share
// of segment indexes below 100,000 whose "still missing" message is read as
// permanent.
func TestFollowupPermanentSegmentNumberShare(t *testing.T) {
	const n = 100000
	hits := 0
	for i := range n {
		if IsPermanentError(fmt.Errorf("segment %d still missing after re-fetch", i)) {
			hits++
		}
	}
	share := 100 * float64(hits) / n
	t.Logf("%d of %d segment numbers (%.1f%%) make the message permanent", hits, n, share)
	if hits == 0 || hits == n {
		t.Errorf("expected a partial share, got %d of %d", hits, n)
	}
}
