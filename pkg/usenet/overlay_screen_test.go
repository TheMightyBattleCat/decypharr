package usenet

import (
	"testing"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

func statResults(available map[int]bool, n int) *nntp.BatchStatResult {
	results := make([]nntp.StatResult, n)
	for i := range n {
		ok := available[i]
		results[i] = nntp.StatResult{Available: ok}
		if !ok {
			results[i].Error = &nntp.Error{Type: nntp.ErrorTypeArticleNotFound}
		}
	}
	return &nntp.BatchStatResult{Results: results, TotalCount: n}
}

// TestBuildScreenHypothesisNoFurtherDecay is the acceptance test for the
// screen's dedup: a degraded file whose full-STAT reconfirms only the
// segment already recorded (nothing new missing) must project a dead-set
// IDENTICAL to what's already recorded - not a double-counted one. If this
// ever regresses, Phase 2's auto-regrab would trigger off a fabricated extra
// segment that was never real damage.
func TestBuildScreenHypothesisNoFurtherDecay(t *testing.T) {
	segments := []storage.NZBSegment{
		{MessageID: "<seg0@test>", Bytes: 1000},
		{MessageID: "<seg1@test>", Bytes: 1000},
		{MessageID: "<seg2@test>", Bytes: 1000},
	}
	recorded := []overlay.DeadSegment{
		{Index: 1, MessageID: "<seg1@test>", Bytes: 1000, Status: overlay.StatusPadded},
	}
	// The STAT reconfirms segment 1 missing (still dead) and finds nothing
	// else - segments 0 and 2 are available.
	stat := statResults(map[int]bool{0: true, 1: false, 2: true}, len(segments))

	h := buildScreenHypothesis(recorded, segments, stat)

	if h.newlyMissing != 0 {
		t.Fatalf("newlyMissing = %d, want 0 (the only miss is already recorded)", h.newlyMissing)
	}
	if h.alreadyPadded != 1 || h.alreadyDead != 0 || h.alreadyPatched != 0 {
		t.Fatalf("already-known counts = (dead=%d, padded=%d, patched=%d), want (0, 1, 0)", h.alreadyDead, h.alreadyPadded, h.alreadyPatched)
	}
	if len(h.deadSegments) != len(recorded) {
		t.Fatalf("projected dead-set has %d segments, want %d (identical to the recorded set, no off-by-one)", len(h.deadSegments), len(recorded))
	}
	if h.deadSegments[0] != recorded[0] {
		t.Fatalf("projected dead-set = %+v, want it unchanged from recorded = %+v", h.deadSegments, recorded)
	}

	policy := overlay.DefaultPolicy()
	fe := &overlay.FileEntry{DeadSegments: h.deadSegments}
	if _, ok := overlay.WithinPadCaps(fe, 10_000_000, policy, 0); !ok {
		t.Fatalf("WithinPadCaps on the projected set = false, want true (a file already degraded within caps, with no further decay, must stay degraded)")
	}
}

// TestBuildScreenHypothesisFreshDecay proves the complementary case: a
// genuinely new miss (an index the manifest never recorded) IS counted, and
// counted exactly once, alongside an unrelated already-recorded segment that
// keeps reconfirming missing.
func TestBuildScreenHypothesisFreshDecay(t *testing.T) {
	segments := []storage.NZBSegment{
		{MessageID: "<seg0@test>", Bytes: 1000},
		{MessageID: "<seg1@test>", Bytes: 1000},
		{MessageID: "<seg2@test>", Bytes: 1000},
	}
	recorded := []overlay.DeadSegment{
		{Index: 0, MessageID: "<seg0@test>", Bytes: 1000, Status: overlay.StatusDead},
	}
	// Segment 0 reconfirms missing (already known); segment 2 is a brand new
	// miss; segment 1 is fine.
	stat := statResults(map[int]bool{0: false, 1: true, 2: false}, len(segments))

	h := buildScreenHypothesis(recorded, segments, stat)

	if h.newlyMissing != 1 {
		t.Fatalf("newlyMissing = %d, want 1 (only segment 2 is genuinely new)", h.newlyMissing)
	}
	if len(h.deadSegments) != 2 {
		t.Fatalf("projected dead-set has %d segments, want 2 (recorded segment 0 + newly-found segment 2)", len(h.deadSegments))
	}
	foundNew := false
	for _, d := range h.deadSegments {
		if d.Index == 2 {
			foundNew = true
		}
	}
	if !foundNew {
		t.Fatalf("projected dead-set %+v does not contain the newly-discovered segment 2", h.deadSegments)
	}
}
