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

// Once a budget is spent - by the request that crossed the cap, or declared
// spent by the checker - the next range request of the same probe must be
// refused before it reads anything, not after pulling one more buffer.
func TestMeteredReader_RefusesASpentBudgetBeforeReading(t *testing.T) {
	for name, spend := range map[string]func(*reader.VerifyBudget){
		"cut":       func(b *reader.VerifyBudget) { b.Add(101) },
		"exhausted": func(b *reader.VerifyBudget) { b.Exhaust() },
	} {
		t.Run(name, func(t *testing.T) {
			b := reader.NewVerifyBudget(100)
			spend(b)
			src := strings.NewReader(strings.Repeat("x", 1000))
			m := &meteredReader{inner: src, budget: b}

			n, err := m.Read(make([]byte, 64))
			if n != 0 {
				t.Fatalf("a spent budget delivered %d bytes", n)
			}
			if !errors.Is(err, reader.ErrVerifyBudgetExhausted) {
				t.Fatalf("expected ErrVerifyBudgetExhausted, got %v", err)
			}
			if consumed := 1000 - src.Len(); consumed != 0 {
				t.Fatalf("the refused read still pulled %d bytes from the source", consumed)
			}
		})
	}
}

// A range request's reads start small and double to the verification buffer,
// so a request ffprobe abandons after its first megabyte is charged (and
// fetches) about that much, not a whole 4 MiB buffer.
func TestMeteredReader_RampsEachRequestsReads(t *testing.T) {
	b := reader.NewVerifyBudget(1 << 30)
	m := &meteredReader{inner: strings.NewReader(strings.Repeat("x", 32<<20)), budget: b}
	buf := make([]byte, verifyBufferSize)
	var sizes []int
	for range 7 {
		n, err := m.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		sizes = append(sizes, n)
	}
	want := []int{256 << 10, 512 << 10, 1 << 20, 2 << 20, 4 << 20, 4 << 20, 4 << 20}
	for i := range want {
		if sizes[i] != want[i] {
			t.Fatalf("read sizes %v, want %v", sizes, want)
		}
	}
	if got, sum := b.Used(), int64(256<<10+512<<10+1<<20+2<<20+3*(4<<20)); got != sum {
		t.Fatalf("charged %d, want %d", got, sum)
	}

	// A new request (a new meter on the same budget) starts small again.
	m2 := &meteredReader{inner: strings.NewReader(strings.Repeat("x", 8<<20)), budget: b}
	if n, _ := m2.Read(buf); n != verifyFirstRead {
		t.Fatalf("second request's first read %d, want %d", n, verifyFirstRead)
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
