package customerror

import (
	"context"
	"errors"
	"io"
	"net"
	"regexp"
	"strings"
	"syscall"
)

// Error classification for retry logic (inspired by rclone's fserrors)
// Distinguishes transient/retriable errors from permanent failures

// retriableErrorStrings contains error message substrings that indicate retriable errors.
// These catch errors from standard library that aren't exported as typed errors.
var retriableErrorStrings = []string{
	"use of closed network connection",
	"unexpected EOF",
	"connection reset by peer",
	"connection refused",
	"broken pipe",
	"i/o timeout",
	"TLS handshake timeout",
	"no such host",
	"server misbehaving",
	"connection timed out",
	"network is unreachable",
	"no route to host",
	"transport connection broken",
	"http2: client connection lost",
	"http2: server sent GOAWAY",
	"http2: timeout awaiting",
	"stream error:",
	"bad record MAC",
	"server closed idle connection",
	"client connection force closed",
	"context deadline exceeded",
}

// permanentErrorStrings contains error message substrings that indicate non-retriable errors
var permanentErrorStrings = []string{
	"not found",
	"forbidden",
	"unauthorized",
	"payment required",
	"gone",
	"invalid api key",
	"file not exist",
	"no such file",
}

// permanentStatusCode matches the HTTP status codes that mean a retry cannot
// help, as whole numbers only. As plain substrings they matched inside any
// longer number: "segment 41094 still missing" and "stalled for 4100ms" were
// both read as a 410.
var permanentStatusCode = regexp.MustCompile(`(^|[^0-9])(401|402|403|404|410)([^0-9]|$)`)

// selfRetryable is an error that knows whether a retry can help, such as
// *nntp.Error.
type selfRetryable interface {
	error
	IsRetryable() bool
}

// IsRetriableError returns true if the error is likely transient and should be retried.
// This follows rclone's multi-layer error classification strategy.
func IsRetriableError(err error) bool {
	if err == nil {
		return false
	}

	var csError *Error
	if errors.As(err, &csError) {
		if csError.IsPermanent() {
			return false
		}
		// If not permanent, consider retriable
		if csError.IsRetryable() {
			return true
		}
		return false
	}

	// Ask the error itself whether it is retriable. This covers types like
	// *nntp.Error that carry their own retryability knowledge but are not
	// *customerror.Error. We intentionally place this after the *Error check
	// so the explicit permanent/retry flags above always win for our own type.
	// The whole chain is searched, and before any text pattern: a wrapper's
	// text (a segment number, a quoted cause) must not outvote the typed
	// answer underneath it.
	if r, ok := errors.AsType[selfRetryable](err); ok {
		return r.IsRetryable()
	}

	// Check for explicit non-retriable markers first
	if IsPermanentError(err) {
		return false
	}

	// Context deadline exceeded is retriable (timeout)
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// Context canceled is NOT retriable (user initiated)
	if errors.Is(err, context.Canceled) {
		return false
	}

	// io.EOF at expected position is not an error
	// io.ErrUnexpectedEOF during transfer is retriable
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	// io.ErrClosedPipe means the underlying pipe/connection was closed mid-transfer.
	// This is transient (the remote end reset) and should be retried like EPIPE.
	if errors.Is(err, io.ErrClosedPipe) {
		return true
	}

	// Check for net.Error interface (Timeout() and Temporary())
	var netErr net.Error
	if errors.As(err, &netErr) {
		// Timeout errors are always retriable
		if netErr.Timeout() {
			return true
		}
	}

	// Check for specific syscall errors
	if errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ETIMEDOUT) ||
		errors.Is(err, syscall.ENETUNREACH) ||
		errors.Is(err, syscall.EHOSTUNREACH) {
		return true
	}

	// Check error string for known patterns
	errStr := strings.ToLower(err.Error())
	for _, pattern := range retriableErrorStrings {
		if strings.Contains(errStr, strings.ToLower(pattern)) {
			return true
		}
	}

	// Check wrapped errors
	unwrapped := errors.Unwrap(err)
	if unwrapped != nil && !errors.Is(unwrapped, err) {
		return IsRetriableError(unwrapped)
	}

	return false
}

// IsPermanentError returns true if the error should NOT be retried.
// These are typically 4xx HTTP errors or explicit access denials.
func IsPermanentError(err error) bool {
	if err == nil {
		return false
	}

	// An error that says it is permanent is. "Not retryable" is not read as
	// permanent: *nntp.Error reports yEnc decode, protocol and pool errors as
	// not retryable, and a permanent verdict during playback deletes and
	// re-searches the release (see vfs downloaders).
	if p, ok := errors.AsType[interface {
		error
		IsPermanent() bool
	}](err); ok && p.IsPermanent() {
		return true
	}

	// An error that says it only needs a new link, or another try, is not
	// permanent, whatever its text: a debrid link error such as
	// "refresh_failed: ... not found" would otherwise match the patterns
	// below and trip the mount's read breaker before the link is refetched.
	if r, ok := errors.AsType[interface {
		error
		ShouldRefetch() bool
	}](err); ok && r.ShouldRefetch() {
		return false
	}
	if r, ok := errors.AsType[interface {
		error
		ShouldRetry() bool
	}](err); ok && r.ShouldRetry() {
		return false
	}

	// Nor is an error that says a retry can help. Only a yes counts: "not
	// retryable" says nothing about permanence (see above).
	if r, ok := errors.AsType[selfRetryable](err); ok && r.IsRetryable() {
		return false
	}

	errStr := strings.ToLower(err.Error())
	for _, pattern := range permanentErrorStrings {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	return permanentStatusCode.MatchString(errStr)
}
