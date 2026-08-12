package parser

import (
	"io"
	"testing"

	"github.com/rs/zerolog"
)

// TestResolveVolumeOrder_IdentityIsNoOp proves the extraction of the
// obfuscated-volume-order logic out of parseArchive preserved the original
// no-op guard: when the recovered true order matches posting order exactly
// (the common, non-obfuscated case that already works in production),
// resolveVolumeOrder must return nil so callers skip renumbering entirely.
// This is what keeps both the plain multi-volume RAR path and the 7z path's
// new call site from perturbing archives that already assemble correctly.
func TestResolveVolumeOrder_IdentityIsNoOp(t *testing.T) {
	p := &RARParser{logger: zerolog.New(io.Discard)}

	entries := []volEntry{
		{idx: 0, num: 0, hasNum: true},
		{idx: 1, num: 1, hasNum: true},
		{idx: 2, num: 2, hasNum: true},
	}

	if got := p.resolveVolumeOrder(entries, 3); got != nil {
		t.Fatalf("resolveVolumeOrder = %v, want nil (identity permutation must be a no-op)", got)
	}
}

// TestResolveVolumeOrder_InfersSingleMissingVolumeNumber is a regression test
// for the documented real-world case (comment in resolveVolumeOrder: "62 of
// 63 volumes numbered cleanly, one with the bit unset") where exactly one
// volume's RAR5 main header lacks the volume-number bit — specifically the
// "complete run, hole at the boundary" case: the base archive volume (whose
// main header commonly omits the volume-number field) sits at posting
// position 2 while the two numbered continuation volumes occupy positions 0
// and 1. This logic was extracted verbatim out of parseArchive; it had no
// direct unit test before this change, only indirect production behavior.
func TestResolveVolumeOrder_InfersSingleMissingVolumeNumber(t *testing.T) {
	p := &RARParser{logger: zerolog.New(io.Discard)}

	// Posting order [A, B, C]. A and B are numbered continuation volumes
	// (content positions 1 and 2); C is the unnumbered base volume, posted
	// last instead of first.
	entries := []volEntry{
		{idx: 0, num: 1, hasNum: true}, // A: true content position 1
		{idx: 1, num: 2, hasNum: true}, // B: true content position 2
		{idx: 2, hasNum: false},        // C: unnumbered base volume (the one hole)
	}

	got := p.resolveVolumeOrder(entries, 3)
	if got == nil {
		t.Fatalf("resolveVolumeOrder returned nil; expected the single missing volume number to be inferred as the base volume (minNum-1) and a non-identity order recovered")
	}
	// Inferred hole = minNum-1 = 1-1 = 0, assigned to C (idx 2). Sorted by
	// num: C(num0,idx2), A(num1,idx0), B(num2,idx1) -> volumeOrder = [2,0,1].
	want := []int{2, 0, 1}
	if len(got) != len(want) {
		t.Fatalf("resolveVolumeOrder = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("resolveVolumeOrder = %v, want %v", got, want)
		}
	}
}

// TestResolveVolumeOrder_AmbiguousInputsFallBackToNil proves the safety
// fallback: when volume numbers can't uniquely establish order (duplicates,
// or more than one volume missing its number), resolveVolumeOrder keeps
// posting order (returns nil) rather than risk corrupting an archive that
// currently assembles.
func TestResolveVolumeOrder_AmbiguousInputsFallBackToNil(t *testing.T) {
	p := &RARParser{logger: zerolog.New(io.Discard)}

	t.Run("duplicate volume numbers", func(t *testing.T) {
		entries := []volEntry{
			{idx: 0, num: 0, hasNum: true},
			{idx: 1, num: 0, hasNum: true}, // duplicate
			{idx: 2, num: 1, hasNum: true},
		}
		if got := p.resolveVolumeOrder(entries, 3); got != nil {
			t.Fatalf("resolveVolumeOrder = %v, want nil (duplicate volume numbers are unreliable)", got)
		}
	})

	t.Run("two missing volume numbers", func(t *testing.T) {
		entries := []volEntry{
			{idx: 0, hasNum: false},
			{idx: 1, hasNum: false},
			{idx: 2, num: 0, hasNum: true},
		}
		if got := p.resolveVolumeOrder(entries, 3); got != nil {
			t.Fatalf("resolveVolumeOrder = %v, want nil (more than one hole is too ambiguous to place)", got)
		}
	})
}
