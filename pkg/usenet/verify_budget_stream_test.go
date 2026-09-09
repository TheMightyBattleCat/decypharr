package usenet

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/usenet/fs/reader"
)

// The metered reader is what actually enforces the cap on the wire, so this
// pins the two properties Stream depends on: it stops once the budget is
// spent, and it stops with the sentinel (not a generic I/O error) so Stream
// can tell "we cut this short" apart from "the article was missing".
func TestMeteredReader_StopsAtBudget(t *testing.T) {
	const total = 1000
	m := &meteredReader{
		inner:  strings.NewReader(strings.Repeat("x", total)),
		budget: reader.NewVerifyBudget(100),
	}

	buf := make([]byte, 64)
	var got int64
	var stopErr error
	for range 100 {
		n, err := m.Read(buf)
		got += int64(n)
		if err != nil {
			stopErr = err
			break
		}
	}

	if !errors.Is(stopErr, reader.ErrVerifyBudgetExhausted) {
		t.Fatalf("expected ErrVerifyBudgetExhausted, got %v", stopErr)
	}
	if !m.budget.Exceeded() {
		t.Fatal("budget should be latched as exceeded")
	}
	// It must stop promptly - within one read buffer of the limit, not after
	// draining the whole source.
	if got > 100+int64(len(buf)) {
		t.Fatalf("read %d bytes, want to stop within one buffer of the 100-byte budget", got)
	}
	if got >= total {
		t.Fatalf("read the entire source (%d bytes) despite the budget", got)
	}
}

// A nil budget must leave the read path exactly as it was before the cap
// existed: read to EOF, no sentinel.
func TestMeteredReader_NilBudgetReadsToEOF(t *testing.T) {
	const total = 500
	m := &meteredReader{inner: strings.NewReader(strings.Repeat("x", total))}

	n, err := io.Copy(io.Discard, m)
	if err != nil {
		t.Fatalf("unexpected error with no budget: %v", err)
	}
	if n != total {
		t.Fatalf("copied %d bytes, want %d", n, total)
	}
	if m.bytes != total {
		t.Fatalf("meter recorded %d bytes, want %d", m.bytes, total)
	}
}
