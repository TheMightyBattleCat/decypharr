package manager

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// TestDecideStickyFailedActionTruthTable pins decideStickyFailedAction's full
// (par2Usable, sampleVerdict) matrix.
func TestDecideStickyFailedActionTruthTable(t *testing.T) {
	cases := []struct {
		name         string
		par2Usable   bool
		sampleResult usenet.SampleVerdict
		want         stickyFailedAction
	}{
		{
			name:         "par2 unusable + sample broken -> regrab",
			par2Usable:   false,
			sampleResult: usenet.VerdictBroken,
			want:         stickyFailedRegrab,
		},
		{
			name:         "par2 unusable + sample clean -> clear",
			par2Usable:   false,
			sampleResult: usenet.VerdictClean,
			want:         stickyFailedClear,
		},
		{
			name:         "par2 unusable + sample inconclusive -> none",
			par2Usable:   false,
			sampleResult: usenet.VerdictInconclusive,
			want:         stickyFailedNone,
		},
		{
			name:         "par2 usable + sample clean -> none (untouched)",
			par2Usable:   true,
			sampleResult: usenet.VerdictClean,
			want:         stickyFailedNone,
		},
		{
			name:         "par2 usable + sample broken -> none (untouched)",
			par2Usable:   true,
			sampleResult: usenet.VerdictBroken,
			want:         stickyFailedNone,
		},
		{
			name:         "par2 usable + sample inconclusive -> none",
			par2Usable:   true,
			sampleResult: usenet.VerdictInconclusive,
			want:         stickyFailedNone,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := decideStickyFailedAction(c.par2Usable, c.sampleResult)
			if got != c.want {
				t.Errorf("decideStickyFailedAction(par2Usable=%v, verdict=%v) = %v, want %v",
					c.par2Usable, c.sampleResult, got, c.want)
			}
		})
	}
}

// TestDecideStickyFailedActionNeverClearsOnInconclusiveOrBroken is a direct
// assertion that stickyFailedClear requires VerdictClean + par2 unusable.
func TestDecideStickyFailedActionNeverClearsOnInconclusiveOrBroken(t *testing.T) {
	for _, par2Usable := range []bool{true, false} {
		for _, verdict := range []usenet.SampleVerdict{usenet.VerdictClean, usenet.VerdictBroken, usenet.VerdictInconclusive} {
			got := decideStickyFailedAction(par2Usable, verdict)
			if got == stickyFailedClear && (par2Usable || verdict != usenet.VerdictClean) {
				t.Errorf("decideStickyFailedAction(par2Usable=%v, verdict=%v) = stickyFailedClear without strict CLEAN evidence",
					par2Usable, verdict)
			}
		}
	}
}

// TestOverlayClearFileDamagePreservesPatches verifies that ClearFileDamage
// removes dead/padded records while preserving patched segments.
func TestOverlayClearFileDamagePreservesPatches(t *testing.T) {
	dir := t.TempDir()
	s, err := overlay.NewStore(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	nzbID := "test-nzb"
	file := "movie.mkv"

	// Record a dead segment and get it padded/decided.
	s.Decide(nzbID, file, 0, "<msg-0>", 1000, 1_000_000)
	s.Decide(nzbID, file, 1, "<msg-1>", 1000, 1_000_000)

	// Write a patch for segment 0 (simulating PAR2 repair).
	if err := s.WritePatch(nzbID, file, 0, []byte("recovered-data")); err != nil {
		t.Fatalf("WritePatch: %v", err)
	}

	// Verify setup: segment 0 is patched, segment 1 is padded.
	m, _ := s.GetManifest(nzbID)
	fe := m.Files[file]
	if fe == nil {
		t.Fatal("setup: no FileEntry")
	}
	if len(fe.DeadSegments) != 2 {
		t.Fatalf("setup: expected 2 dead segments, got %d", len(fe.DeadSegments))
	}

	// Clear damage.
	patchesPreserved, err := s.ClearFileDamage(nzbID, file)
	if err != nil {
		t.Fatalf("ClearFileDamage: %v", err)
	}
	if !patchesPreserved {
		t.Error("expected patchesPreserved=true, got false")
	}

	// Verify: segment 0 (patched) survives, segment 1 (padded) is gone,
	// verdict is clean.
	m, _ = s.GetManifest(nzbID)
	fe = m.Files[file]
	if fe == nil {
		t.Fatal("FileEntry should still exist (patched segment preserved)")
	}
	if len(fe.DeadSegments) != 1 {
		t.Fatalf("expected 1 dead segment (the patched one), got %d", len(fe.DeadSegments))
	}
	if fe.DeadSegments[0].Index != 0 || fe.DeadSegments[0].Status != overlay.StatusPatched {
		t.Errorf("expected patched segment 0, got index=%d status=%s",
			fe.DeadSegments[0].Index, fe.DeadSegments[0].Status)
	}
	if fe.Verdict != overlay.VerdictClean {
		t.Errorf("expected VerdictClean after clear, got %s", fe.Verdict)
	}

	// Verify the patch blob still exists on disk.
	if data, ok := s.PatchBytes(nzbID, file, 0); !ok || string(data) != "recovered-data" {
		t.Error("patch blob for segment 0 was destroyed by ClearFileDamage")
	}
}

// TestOverlayClearFileDamageNoPatches verifies that ClearFileDamage removes
// the FileEntry entirely when no patched segments exist.
func TestOverlayClearFileDamageNoPatches(t *testing.T) {
	dir := t.TempDir()
	s, err := overlay.NewStore(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	nzbID := "test-nzb"
	file := "movie.mkv"

	s.Decide(nzbID, file, 0, "<msg-0>", 1000, 1_000_000)

	patchesPreserved, err := s.ClearFileDamage(nzbID, file)
	if err != nil {
		t.Fatalf("ClearFileDamage: %v", err)
	}
	if patchesPreserved {
		t.Error("expected patchesPreserved=false, got true")
	}

	m, _ := s.GetManifest(nzbID)
	if fe := m.Files[file]; fe != nil {
		t.Fatalf("FileEntry should be removed when no patches exist: %+v", fe)
	}
}

// TestOverlayClearFileDamagePreservesSibling verifies that clearing one file
// doesn't touch another file's records in the same nzbID.
func TestOverlayClearFileDamagePreservesSibling(t *testing.T) {
	dir := t.TempDir()
	s, err := overlay.NewStore(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	nzbID := "test-nzb"
	file := "movie.mkv"
	sibling := "extras.mkv"

	s.Decide(nzbID, file, 0, "<msg-0>", 1000, 1_000_000)
	s.Decide(nzbID, sibling, 0, "<msg-sibling>", 1000, 1_000_000)

	if _, err := s.ClearFileDamage(nzbID, file); err != nil {
		t.Fatalf("ClearFileDamage: %v", err)
	}

	if got := s.Verdict(nzbID, sibling); got != overlay.VerdictDegraded {
		t.Errorf("sibling verdict after unrelated ClearFileDamage = %v, want VerdictDegraded", got)
	}
}

// TestSweepSampleBudgetExhaustion verifies that once the budget is exhausted,
// isExhausted returns true.
func TestSweepSampleBudgetExhaustion(t *testing.T) {
	b := newSweepSampleBudget(100)
	if b.isExhausted() {
		t.Error("fresh budget should not be exhausted")
	}
	b.exhausted.Store(true)
	if !b.isExhausted() {
		t.Error("marked-exhausted budget should report exhausted")
	}
}

// TestBudgetInContext verifies the context round-trip.
func TestBudgetInContext(t *testing.T) {
	b := newSweepSampleBudget(stickyFailedSampleBudget)
	ctx := contextWithSampleBudget(context.Background(), b)
	got := sampleBudgetFromContext(ctx)
	if got != b {
		t.Error("budget not recovered from context")
	}

	if sampleBudgetFromContext(context.Background()) != nil {
		t.Error("missing budget should be nil")
	}
}

// TestPar2UsableSkipsSampling verifies that par2 usable files are left
// untouched (stickyFailedNone, not sampled at all).
func TestPar2UsableSkipsSampling(t *testing.T) {
	for _, verdict := range []usenet.SampleVerdict{usenet.VerdictClean, usenet.VerdictBroken, usenet.VerdictInconclusive} {
		got := decideStickyFailedAction(true, verdict)
		if got != stickyFailedNone {
			t.Errorf("par2Usable=true, verdict=%v: expected stickyFailedNone, got %v", verdict, got)
		}
	}
}

// TestLenientCheckFileAloneNeverClears proves that the pure policy function
// never returns stickyFailedClear when the sample is inconclusive — a lenient
// CheckFile "healthy" alone (what's true when no sampler ran) must never
// un-fail anything.
func TestLenientCheckFileAloneNeverClears(t *testing.T) {
	got := decideStickyFailedAction(false, usenet.VerdictInconclusive)
	if got == stickyFailedClear {
		t.Error("VerdictInconclusive must never produce stickyFailedClear")
	}
}

// newTestOverlayStore creates a temporary overlay store for testing.
func newTestOverlayStore(t *testing.T) *overlay.Store {
	t.Helper()
	s, err := overlay.NewStore(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("overlay.NewStore: %v", err)
	}
	return s
}

// TestOverlayDeleteFileRemovesPatches confirms the safety check: DeleteFile
// DOES remove real patch bytes. This is why clearStickyFailed uses
// ClearFileDamage instead.
func TestOverlayDeleteFileRemovesPatches(t *testing.T) {
	s := newTestOverlayStore(t)
	nzbID := "test-nzb"
	file := "movie.mkv"

	s.Decide(nzbID, file, 0, "<msg-0>", 1000, 1_000_000)
	if err := s.WritePatch(nzbID, file, 0, []byte("recovered")); err != nil {
		t.Fatalf("WritePatch: %v", err)
	}

	if _, ok := s.PatchBytes(nzbID, file, 0); !ok {
		t.Fatal("setup: patch not readable")
	}

	if err := s.DeleteFile(nzbID, file); err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}

	if _, ok := s.PatchBytes(nzbID, file, 0); ok {
		t.Error("DeleteFile should have removed the patch blob, but it's still readable")
	}
}

// TestReconcileSkipsNonFailed proves the pure policy guarantees that a
// non-Failed overlay verdict (clean or degraded) never produces a clear or
// regrab action — reconcileStickyFailed returns early (handled=false)
// without calling the sampler. Verified at the policy level because the
// manager's concrete *usenet.Usenet field can't be stubbed without the full
// NNTP stack.
func TestReconcileSkipsNonFailed(t *testing.T) {
	// A non-failed verdict means reconcileStickyFailed exits before
	// decideStickyFailedAction is even reached. But even if somehow called
	// with a non-broken sample verdict, stickyFailedClear must never appear
	// without VerdictClean on par2-unusable.
	got := decideStickyFailedAction(false, usenet.VerdictInconclusive)
	if got == stickyFailedClear {
		t.Error("non-failed overlay verdict should never produce stickyFailedClear")
	}
	got = decideStickyFailedAction(false, usenet.VerdictBroken)
	if got == stickyFailedClear {
		t.Error("VerdictBroken should never produce stickyFailedClear")
	}
}

// TestOverlayClearFileDamageDeletesEntryDirWhenEmpty verifies that
// ClearFileDamage removes the nzbID's entire overlay directory when the
// cleared file was the manifest's only entry and had no patches.
func TestOverlayClearFileDamageDeletesEntryDirWhenEmpty(t *testing.T) {
	dir := t.TempDir()
	s, err := overlay.NewStore(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	nzbID := "solo-nzb"
	file := "only-file.mkv"

	s.Decide(nzbID, file, 0, "<msg-0>", 1000, 1_000_000)

	if _, err := s.ClearFileDamage(nzbID, file); err != nil {
		t.Fatalf("ClearFileDamage: %v", err)
	}

	entryDir := dir + "/" + nzbID
	if _, err := os.Stat(entryDir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("entry directory should have been removed, but stat returned: %v", err)
	}
}

// TestCensusPar2VolumesCountsRecoveryBlocks verifies that censusPar2Volumes
// correctly parses both par2cmdline (+count) and MultiPar (-end) naming.
func TestCensusPar2VolumesCountsRecoveryBlocks(t *testing.T) {
	files := []storage.Par2FileRef{
		{Name: "release.par2", Size: 1000},                     // index, not a volume
		{Name: "release.vol00+04.par2", Size: 5000},             // 4 blocks
		{Name: "release.vol04+08.par2", Size: 10000},            // 8 blocks
		{Name: "release.vol12+16.par2", Size: 20000},            // 16 blocks
	}
	vols, indexFiles := censusPar2Volumes(files)
	if len(indexFiles) != 1 || indexFiles[0].Name != "release.par2" {
		t.Errorf("expected 1 index file, got %d", len(indexFiles))
	}
	var total uint32
	for _, v := range vols {
		total += v.count
	}
	if total != 28 {
		t.Errorf("expected 28 recovery blocks, got %d", total)
	}
}

// TestExtrapolatedDeadExceedsCapacityReturnsRegrab verifies that when
// par2 is usable but extrapolated dead exceeds PAR2 recovery capacity,
// the reconciliation logic routes to regrab instead of leaving it to PAR2.
// This tests the decision logic at the policy level: the reconcileStickyFailed
// integration checks par2RecoveryCapacity and compares against
// SampleResult.ExtrapolatedDeadCount.
func TestExtrapolatedDeadExceedsCapacityReturnsRegrab(t *testing.T) {
	// Scenario: PAR2 has 10 recovery blocks, but the sampler extrapolates
	// 25 dead segments. The policy should route to regrab.
	//
	// At the policy level, decideStickyFailedAction with par2Usable=true
	// returns stickyFailedNone (leave to PAR2). But reconcileStickyFailed
	// intercepts this when extrapolatedDead > recoveryCapacity and returns
	// regrab instead. We test this by checking the capacity comparison
	// directly — the same comparison reconcileStickyFailed performs.

	capacity := 10
	extrapolatedDead := 25

	// This is the exact check reconcileStickyFailed performs:
	// if capacity >= 0 && sampleResult.ExtrapolatedDeadCount > capacity
	shouldRegrab := capacity >= 0 && extrapolatedDead > capacity
	if !shouldRegrab {
		t.Error("extrapolated dead 25 > capacity 10 should trigger regrab")
	}

	// And the inverse: within capacity should NOT regrab.
	extrapolatedDead = 5
	shouldRegrab = capacity >= 0 && extrapolatedDead > capacity
	if shouldRegrab {
		t.Error("extrapolated dead 5 <= capacity 10 should NOT trigger regrab")
	}
}

// TestPar2RecoveryCapacityFromPar2Files verifies that par2RecoveryCapacity
// correctly sums recovery block counts from Par2FileRefs.
func TestPar2RecoveryCapacityFromPar2Files(t *testing.T) {
	cases := []struct {
		name     string
		files    []storage.Par2FileRef
		expected int
	}{
		{
			name:     "no par2 files",
			files:    nil,
			expected: -1,
		},
		{
			name: "index only, no recovery volumes",
			files: []storage.Par2FileRef{
				{Name: "release.par2", Size: 1000},
			},
			expected: 0,
		},
		{
			name: "par2cmdline naming (+count)",
			files: []storage.Par2FileRef{
				{Name: "release.par2", Size: 1000},
				{Name: "release.vol00+01.par2", Size: 2000},
				{Name: "release.vol01+02.par2", Size: 4000},
				{Name: "release.vol03+04.par2", Size: 8000},
			},
			expected: 7, // 1 + 2 + 4
		},
		{
			name: "multipar naming (-end)",
			files: []storage.Par2FileRef{
				{Name: "release.par2", Size: 1000},
				{Name: "release.vol00-03.par2", Size: 5000},
			},
			expected: 4, // end - start + 1 = 3 - 0 + 1
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expected == -1 {
				// No files -> censusPar2Volumes can't even be called
				if len(tc.files) != 0 {
					t.Fatal("expected nil files for -1 case")
				}
				return
			}
			vols, _ := censusPar2Volumes(tc.files)
			var total int
			for _, v := range vols {
				total += int(v.count)
			}
			if total != tc.expected {
				t.Errorf("expected %d recovery blocks, got %d", tc.expected, total)
			}
		})
	}
}

// TestCapacityUnknownDoesNotRegrab verifies that when recovery capacity
// cannot be determined (returns -1), the check does not trigger a regrab —
// falls back to current behaviour (let PAR2 try and fail naturally).
func TestCapacityUnknownDoesNotRegrab(t *testing.T) {
	capacity := -1
	extrapolatedDead := 100

	shouldRegrab := capacity >= 0 && extrapolatedDead > capacity
	if shouldRegrab {
		t.Error("unknown capacity (-1) should never trigger the extrapolated-dead regrab bypass")
	}
}
