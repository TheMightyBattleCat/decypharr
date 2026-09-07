package manager

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

// par2FixtureBytes reads one committed PAR2 fixture file (see
// pkg/usenet/par2/testdata/par2/gen.sh).
func par2FixtureBytes(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "usenet", "par2", "testdata", "par2", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v (run pkg/usenet/par2/testdata/par2/gen.sh if missing)", name, err)
	}
	return data
}

// TestTopUpParsedRecoveryRefetchesMisservedVolume is the regression for the
// advertised-vs-parsed divergence Fix D introduced. A recovery volume that
// fetches successfully but whose recovery packets fail their MD5 (a
// mis-served article) contributes zero slices once ParseIndex skips those
// packets - yet fetchMoreVolumes still adds its name-advertised slice count
// to the accumulator. topUpParsedRecovery must keep fetching real volumes
// until the PARSED recovery set covers the damage, not stop as soon as the
// inflated advertised count clears the bar.
func TestTopUpParsedRecoveryRefetchesMisservedVolume(t *testing.T) {
	indexOnly := par2FixtureBytes(t, "fixture.par2") // parses fine, zero recovery slices
	vol0 := par2FixtureBytes(t, "fixture.vol0+1.par2")
	vol1 := par2FixtureBytes(t, "fixture.vol1+2.par2")
	vol3 := par2FixtureBytes(t, "fixture.vol3+2.par2")

	// Start from the index alone: zero recovery slices in hand.
	idx, err := par2.ParseIndex([]par2.Source{{Name: "fixture.par2", Data: indexOnly}})
	if err != nil {
		t.Fatalf("seed ParseIndex: %v", err)
	}
	if len(idx.Recovery) != 0 {
		t.Fatalf("seed index has %d recovery slices, want 0", len(idx.Recovery))
	}

	// Volume list: two mis-served volumes first (each advertises 3 slices but
	// serves the recovery-free index bytes), then the three real recovery
	// volumes (1 + 2 + 2 = 5 usable slices).
	vols := []par2Volume{
		makeTestVol("misA.par2", 3, "misA@news"),
		makeTestVol("misB.par2", 3, "misB@news"),
		makeTestVol("real0.par2", 1, "real0@news"),
		makeTestVol("real1.par2", 2, "real1@news"),
		makeTestVol("real3.par2", 2, "real3@news"),
	}
	fetch := mockVolumeFetch(map[string][]byte{
		"misA@news":  indexOnly,
		"misB@news":  indexOnly,
		"real0@news": vol0,
		"real1@news": vol1,
		"real3@news": vol3,
	}, nil)

	// Old behaviour, for the record: after fetching misA+misB the advertised
	// accumulator would read 6 >= want(3), the top-up gate would go no-op, and
	// the caller's "3 damaged but only 0 recovery slices" wall would fire with
	// real0/real1/real3 never fetched.
	const want = 3
	progress := newPar2JobProgressState("nzb", "entry")
	sources := []par2.Source{{Name: "fixture.par2", Data: indexOnly}}

	gotIdx, nextVolIdx, _, err := topUpParsedRecovery(
		context.Background(), parallelFetchNopLogger, fetch, vols, 0, want, idx, sources, "entry", 4, progress)
	if err != nil {
		t.Fatalf("topUpParsedRecovery: %v", err)
	}
	if len(gotIdx.Recovery) < want {
		t.Fatalf("parsed recovery slices = %d, want >= %d (mis-served volumes must not stop the top-up)", len(gotIdx.Recovery), want)
	}
	if len(gotIdx.Recovery) != 5 {
		t.Errorf("parsed recovery slices = %d, want 5 (all three real volumes fetched)", len(gotIdx.Recovery))
	}
	if nextVolIdx != len(vols) {
		t.Errorf("nextVolIdx = %d, want %d (every volume consumed)", nextVolIdx, len(vols))
	}
	if snap := progress.Snapshot(); snap.RecoverySlicesFetched != 5 {
		t.Errorf("progress RecoverySlicesFetched = %d, want 5 (progress tracks parsed reality)", snap.RecoverySlicesFetched)
	}
}

// TestTopUpParsedRecoveryStopsWhenExhausted asserts the loop terminates once
// the volume list is spent even if the damage is still uncovered - the caller
// then turns the short recovery set into the terminal verdict.
func TestTopUpParsedRecoveryStopsWhenExhausted(t *testing.T) {
	indexOnly := par2FixtureBytes(t, "fixture.par2")
	vol0 := par2FixtureBytes(t, "fixture.vol0+1.par2")
	vol1 := par2FixtureBytes(t, "fixture.vol1+2.par2")

	idx, err := par2.ParseIndex([]par2.Source{{Name: "fixture.par2", Data: indexOnly}})
	if err != nil {
		t.Fatalf("seed ParseIndex: %v", err)
	}

	vols := []par2Volume{
		makeTestVol("real0.par2", 1, "real0@news"),
		makeTestVol("real1.par2", 2, "real1@news"),
	}
	fetch := mockVolumeFetch(map[string][]byte{
		"real0@news": vol0,
		"real1@news": vol1,
	}, nil)

	const want = 10 // more than the 3 slices these two volumes can ever supply
	gotIdx, nextVolIdx, _, err := topUpParsedRecovery(
		context.Background(), parallelFetchNopLogger, fetch, vols, 0, want, idx, nil, "entry", 4, nil)
	if err != nil {
		t.Fatalf("topUpParsedRecovery: %v", err)
	}
	if len(gotIdx.Recovery) != 3 {
		t.Errorf("parsed recovery slices = %d, want 3 (both volumes fetched, no more to get)", len(gotIdx.Recovery))
	}
	if len(gotIdx.Recovery) >= want {
		t.Fatalf("recovery set unexpectedly covered the damage")
	}
	if nextVolIdx != len(vols) {
		t.Errorf("nextVolIdx = %d, want %d (volume list fully walked)", nextVolIdx, len(vols))
	}
}
