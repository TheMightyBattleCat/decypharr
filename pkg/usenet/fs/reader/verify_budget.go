package reader

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// ErrVerifyBudgetExhausted is returned by a verification read once it has
// delivered VerifyBudget.Limit bytes. It is deliberately not an NNTP or I/O
// error: nothing about it says the file is bad, only that the probe reading it
// stopped being worth the bytes. Stream converts it into a clean end-of-body,
// and the ffprobe checker turns the resulting (truncated, therefore
// untrustworthy) ffprobe verdict into "inconclusive".
var ErrVerifyBudgetExhausted = errors.New("verification read budget exhausted")

// VerifyBudget caps the total bytes one ffprobe verification may pull for a
// single file, across every HTTP range request the probe issues and every pass
// the caller runs.
//
// Why this exists: the decode check asks ffprobe for 15 two-second windows,
// which for a ~1.5 MB/s WEB-DL episode is ~45 MB of video. Measured against a
// complete file - over plain HTTP and over decypharr's own WebDAV, at every
// latency from 0 to 1500 ms - ffprobe consumes 1.4-2.3% of the file, which
// matches that estimate. In production, though, verification reads have been
// observed delivering 87-96% of the file (11.16 GB to reject one 4.25 GB
// grab), ~283 MB per window, with the seeks landing on the right offsets.
// That gap is not understood, and the honest response is to bound it rather
// than to guess at it: the budget makes the worst case a known quantity no
// matter what the demuxer decides to read.
//
// The bound is deliberately loose - roughly an order of magnitude above a
// healthy probe - so a well-behaved read never trips it and only a runaway
// does. Tripping is not a verdict: the caller must treat an exhausted budget
// as inconclusive, never as "broken", because the truncated stream will make
// ffprobe report container errors that say nothing about the file.
//
// The zero value is unusable; construct with NewVerifyBudget. A nil
// *VerifyBudget means "no budget" and every method is a safe no-op, so callers
// that have no size to bound against can pass nil.
type VerifyBudget struct {
	limit    int64
	used     atomic.Int64
	exceeded atomic.Bool

	// Accounting, so one line at the end of a verification can stand in for
	// the per-range-request logging the read path used to emit (ffprobe
	// issues dozens to hundreds of range GETs per probe). Written by Observe
	// from the single metered-read goroutine, read once the probe finishes.
	reads   atomic.Int64 // metered reads that delivered >0 bytes
	waitNs  atomic.Int64 // cumulative time blocked in the underlying reader
	firstAt atomic.Int64 // UnixNano of the first delivered byte, set once
}

// NewVerifyBudget returns a budget of limit bytes, or nil when limit <= 0
// (callers treat nil as unbounded, preserving the pre-budget behaviour for a
// file whose size is unknown).
func NewVerifyBudget(limit int64) *VerifyBudget {
	if limit <= 0 {
		return nil
	}
	return &VerifyBudget{limit: limit}
}

// Add records n bytes delivered and reports whether the budget is still
// within its limit. Once exceeded it stays exceeded (one-way latch), so every
// later range request for the same probe short-circuits immediately rather
// than each spending its own budget. Always true on a nil receiver.
func (b *VerifyBudget) Add(n int64) bool {
	if b == nil {
		return true
	}
	if b.used.Add(n) > b.limit {
		b.exceeded.Store(true)
		return false
	}
	return !b.exceeded.Load()
}

// Observe records that a metered read delivered n bytes after spending
// waited blocked in the underlying reader. Pure accounting for the
// end-of-verification summary line - the cap itself is enforced by Add.
// Safe no-op on a nil receiver or a non-positive n.
func (b *VerifyBudget) Observe(n int64, waited time.Duration) {
	if b == nil || n <= 0 {
		return
	}
	b.reads.Add(1)
	b.waitNs.Add(int64(waited))
	b.firstAt.CompareAndSwap(0, time.Now().UnixNano())
}

// Exceeded reports whether the budget was ever blown. False on nil.
func (b *VerifyBudget) Exceeded() bool {
	return b != nil && b.exceeded.Load()
}

// Reads returns the number of metered reads that delivered bytes. 0 on nil.
func (b *VerifyBudget) Reads() int64 {
	if b == nil {
		return 0
	}
	return b.reads.Load()
}

// Wait returns the cumulative time metered reads spent blocked on the
// underlying fetch. 0 on nil. If this is close to a probe's wall time the
// verification is fetch-bound; well below it, the bottleneck is ffmpeg/CPU.
func (b *VerifyBudget) Wait() time.Duration {
	if b == nil {
		return 0
	}
	return time.Duration(b.waitNs.Load())
}

// Elapsed returns the wall time since the first byte was delivered, or 0 if
// nothing has been read yet (or on nil).
func (b *VerifyBudget) Elapsed() time.Duration {
	if b == nil {
		return 0
	}
	first := b.firstAt.Load()
	if first == 0 {
		return 0
	}
	return time.Since(time.Unix(0, first))
}

// MiBPerSec is the effective delivered-bytes throughput over Elapsed, or 0
// when nothing has been read yet.
func (b *VerifyBudget) MiBPerSec() float64 {
	e := b.Elapsed()
	if b == nil || e <= 0 {
		return 0
	}
	return float64(b.used.Load()) / (1024 * 1024) / e.Seconds()
}

// Used returns the bytes recorded so far. 0 on nil.
func (b *VerifyBudget) Used() int64 {
	if b == nil {
		return 0
	}
	return b.used.Load()
}

// Limit returns the configured cap. 0 on nil.
func (b *VerifyBudget) Limit() int64 {
	if b == nil {
		return 0
	}
	return b.limit
}

// verifyBudgetCtxKey marks a context as carrying a *VerifyBudget.
type verifyBudgetCtxKey struct{}

// ContextWithVerifyBudget attaches b to ctx so the WebDAV read path can meter
// against it. Returns ctx unchanged when b is nil.
func ContextWithVerifyBudget(ctx context.Context, b *VerifyBudget) context.Context {
	if b == nil {
		return ctx
	}
	return context.WithValue(ctx, verifyBudgetCtxKey{}, b)
}

// VerifyBudgetFromContext returns the budget attached by
// ContextWithVerifyBudget, or nil if none is present.
func VerifyBudgetFromContext(ctx context.Context) *VerifyBudget {
	b, _ := ctx.Value(verifyBudgetCtxKey{}).(*VerifyBudget)
	return b
}
