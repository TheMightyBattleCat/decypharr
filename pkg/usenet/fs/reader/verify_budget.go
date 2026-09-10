package reader

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// ErrVerifyBudgetExhausted is returned by a verification read once its budget
// is spent: it has delivered VerifyBudget.Limit bytes, or the checker declared
// it spent (Exhaust). It is deliberately not an NNTP or I/O error: nothing
// about it says the file is bad, only that the probe reading it stopped being
// worth the bytes. Stream converts it into a clean end-of-body, and the ffprobe
// checker decides what the cut means - an inconclusive verdict, or, for the
// seek-detect probe, a file with no usable container index.
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
// Phases: some probes are short on purpose, and their byte cost is itself the
// signal - a seek-detect probe that cannot reach its window inside a small cap
// is reading forward rather than seeking. Those run under a phase: a separate
// budget opened with BeginPhase, which range requests meter against instead of
// this one while it is open (see ForRequest). A phase is independent of its
// parent in both directions - its bytes are not charged to the parent, and the
// parent's limit does not cap it - so sizing one can never starve or distort
// the other.
//
// The zero value is unusable; construct with NewVerifyBudget. A nil
// *VerifyBudget means "no budget" and every method is a safe no-op, so callers
// that have no size to bound against can pass nil.
type VerifyBudget struct {
	limit    int64
	used     atomic.Int64
	exceeded atomic.Bool // delivered bytes passed limit: a byte cut

	// exhausted is set by Exhaust: the verification was declared spent without
	// a byte cut. Kept apart from exceeded so a log line can say which it was.
	exhausted atomic.Bool

	// phase is the budget range requests meter against while one is open, nil
	// otherwise. See BeginPhase.
	phase atomic.Pointer[VerifyBudget]

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

// Add records n bytes delivered and reports whether the budget may still be
// read against. Once the limit is passed it stays exceeded (one-way latch), so
// every later range request for the same probe short-circuits immediately
// rather than each spending its own budget. Also false once Exhaust has been
// called. Always true on a nil receiver.
func (b *VerifyBudget) Add(n int64) bool {
	if b == nil {
		return true
	}
	if b.used.Add(n) > b.limit {
		b.exceeded.Store(true)
		return false
	}
	return !b.Exceeded()
}

// Exhaust marks the budget spent without charging it any bytes: the checker
// has decided this verification must not read again (a bounded scan that
// reached no verdict would only pay for the same bytes on a retry). Latches
// Exceeded, so every retry guard keyed on it stops; Cut stays false. Safe on
// nil.
func (b *VerifyBudget) Exhaust() {
	if b != nil {
		b.exhausted.Store(true)
	}
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

// Exceeded reports whether the budget may no longer be read against: its byte
// limit was passed, or Exhaust was called. False on nil. Every guard that
// downgrades a verdict to inconclusive keys on this.
func (b *VerifyBudget) Exceeded() bool {
	return b != nil && (b.exceeded.Load() || b.exhausted.Load())
}

// Cut reports whether delivered bytes actually passed the limit - the part of
// Exceeded that is a byte cut rather than an Exhaust. False on nil.
func (b *VerifyBudget) Cut() bool {
	return b != nil && b.exceeded.Load()
}

// BeginPhase opens a phase budget of limit bytes and makes it what every range
// request of this verification meters against (ForRequest) until EndPhase. The
// phase is independent of b: its bytes are not charged to b and b's limit does
// not cap it. Phases do not draw on b, so a phase opened on an already-spent b
// starts exhausted - otherwise opening one would let a spent verification read
// again. Returns nil, opening nothing, on a nil receiver or a non-positive
// limit.
func (b *VerifyBudget) BeginPhase(limit int64) *VerifyBudget {
	if b == nil {
		return nil
	}
	p := NewVerifyBudget(limit)
	if p == nil {
		return nil
	}
	if b.Exceeded() {
		p.exhausted.Store(true)
	}
	b.phase.Store(p)
	return p
}

// EndPhase closes p if it is still the open phase. A stale EndPhase - p was
// already replaced by a later BeginPhase - leaves the newer phase open. Safe
// with a nil receiver or a nil p.
func (b *VerifyBudget) EndPhase(p *VerifyBudget) {
	if b == nil || p == nil {
		return
	}
	b.phase.CompareAndSwap(p, nil)
}

// ForRequest returns the budget a range request starting now should meter
// against: the open phase if there is one, else b itself. The read path
// resolves this once per request, so a request keeps metering against the
// budget it started under even if the phase changes mid-body. nil on nil.
func (b *VerifyBudget) ForRequest() *VerifyBudget {
	if b == nil {
		return nil
	}
	if p := b.phase.Load(); p != nil {
		return p
	}
	return b
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
