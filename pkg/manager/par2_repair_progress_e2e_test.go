package manager

import (
	"context"
	"crypto/md5"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

// TestRepairProgressEndToEndAgainstRealFixture wires the exact shipped
// pieces this commit adds - concurrentSliceSource and par2JobProgressState -
// together with the real PAR2 engine (par2.Repair) and the real fixture PAR2
// set from pkg/usenet/par2/testdata (shared across a repo symlink-free
// relative path), simulating an artificially-latent "Usenet fetch" so a
// stalled/slow individual article never serializes the whole pass. It proves
// two things runRepair itself is hard to unit test end-to-end without a full
// storage.NZB/usenet.Usenet harness: fetches genuinely overlap up to the
// configured bound (not one-at-a-time), and the live progress snapshot
// reports accurate, monotonically-sane values at each phase.
func TestRepairProgressEndToEndAgainstRealFixture(t *testing.T) {
	const fixtureDir = "../usenet/par2/testdata/par2"
	entries, err := os.ReadDir(fixtureDir)
	if err != nil {
		t.Fatalf("read fixture dir: %v (run pkg/usenet/par2/testdata/par2/gen.sh if missing)", err)
	}
	var sources []par2.Source
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".par2" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(fixtureDir, e.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		sources = append(sources, par2.Source{Name: e.Name(), Data: data})
	}
	if len(sources) == 0 {
		t.Fatalf("no .par2 fixtures found in %s", fixtureDir)
	}

	idx, err := par2.ParseIndex(sources)
	if err != nil {
		t.Fatalf("ParseIndex: %v", err)
	}

	originals := map[string][]byte{}
	for _, name := range []string{"file1.bin", "file2.bin", "file3.bin"} {
		data, err := os.ReadFile(filepath.Join(fixtureDir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		originals[name] = data
	}
	posted := make([]par2.PostedFile, 0, len(originals))
	for name, content := range originals {
		content := content
		posted = append(posted, par2.PostedFile{
			Name:   name,
			Length: int64(len(content)),
			MD5_16k: func() ([16]byte, error) {
				n := len(content)
				if n > 16384 {
					n = 16384
				}
				return md5.Sum(content[:n]), nil
			},
		})
	}
	matches, err := par2.MatchFiles(idx, posted)
	if err != nil {
		t.Fatalf("MatchFiles: %v", err)
	}
	byFile := make(map[[16]byte][]byte, len(matches))
	fileIDByName := make(map[string][16]byte, len(matches))
	for _, m := range matches {
		name := posted[m.PostedIndex].Name
		byFile[m.FileID] = originals[name]
		fileIDByName[name] = m.FileID
	}

	// Damage one slice from each of the three files - same shape as
	// TestGoldenRepair in pkg/usenet/par2, small enough (3) to stay well
	// under this fixture's 5 available recovery slices.
	damagedSet := make(map[int64]struct{})
	var damaged []int64
	for _, name := range []string{"file1.bin", "file2.bin", "file3.bin"} {
		base, err := idx.SliceBase(fileIDByName[name])
		if err != nil {
			t.Fatalf("SliceBase(%s): %v", name, err)
		}
		damaged = append(damaged, base+1)
		damagedSet[base+1] = struct{}{}
	}

	recovery := make([]par2.RecoverySlice, len(damaged))
	for i, ref := range idx.Recovery[:len(damaged)] {
		src := sources[ref.Source].Data
		recovery[i] = par2.RecoverySlice{
			Exponent: ref.Exponent,
			Data:     append([]byte(nil), src[ref.Offset:ref.Offset+ref.Length]...),
		}
	}

	intactOrder := make([]int64, 0, idx.NumSlices()-int64(len(damaged)))
	for s := int64(0); s < idx.NumSlices(); s++ {
		if _, isDamaged := damagedSet[s]; !isDamaged {
			intactOrder = append(intactOrder, s)
		}
	}

	// Simulated per-article Usenet fetch: real bytes from the real fixture
	// files (exactly what a genuine NNTP article body would decode to for
	// this release), with artificial latency standing in for real network
	// RTT/provider load - large enough (15ms) that a fully-sequential pass
	// over ~23 slices (>300ms) would be trivially distinguishable from a
	// concurrency-bounded one in the measured wall time below.
	const simulatedFetchLatency = 15 * time.Millisecond
	const maxConcurrency = 6 // mirrors ProcessingMaxConnections' role, deliberately small so the fixture's ~23 intact slices clearly exceed it
	var inFlight, peakInFlight atomic.Int64
	fetchOne := func(globalIdx int64) ([]byte, error) {
		n := inFlight.Add(1)
		for {
			p := peakInFlight.Load()
			if n <= p || peakInFlight.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(simulatedFetchLatency)
		defer inFlight.Add(-1)

		fileID, local, err := idx.SliceLocation(globalIdx)
		if err != nil {
			return nil, err
		}
		content := byFile[fileID]
		start := local * idx.SliceSize
		buf := make([]byte, idx.SliceSize)
		copy(buf, content[start:min(int64(len(content)), start+idx.SliceSize)])
		return buf, nil
	}

	progress := newPar2JobProgressState("demo-nzb", "demo-entry")
	progress.SetPhase(Par2PhaseFetchingRecovery)
	progress.SetRecoveryVolsFetched(len(sources))
	progress.SetRecoverySlices(len(idx.Recovery), len(damaged))

	progress.SetPhase(Par2PhaseStreamingIntact)
	progress.SetIntactTotal(len(intactOrder))

	var intactRead int64
	intactTotal := int64(len(intactOrder))
	trackedFetch := func(i int64) ([]byte, error) {
		data, err := fetchOne(i)
		progress.AddIntactRead(1)
		if atomic.AddInt64(&intactRead, 1) == intactTotal {
			progress.SetPhase(Par2PhaseSolving)
		}
		return data, err
	}

	ctx := context.Background()
	sliceSource := newConcurrentSliceSource(ctx, intactOrder, maxConcurrency, trackedFetch)

	start := time.Now()
	repaired, err := par2.Repair(idx, damaged, recovery, sliceSource)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	progress.SetPhase(Par2PhaseWriting)

	for _, rs := range repaired {
		trimmed, terr := idx.TrimSlice(rs.Index, rs.Data)
		if terr != nil {
			t.Fatalf("TrimSlice(%d): %v", rs.Index, terr)
		}
		fileID, local, lerr := idx.SliceLocation(rs.Index)
		if lerr != nil {
			t.Fatalf("sliceLocation(%d): %v", rs.Index, lerr)
		}
		original := byFile[fileID]
		sliceStart := local * idx.SliceSize
		sliceEnd := min(sliceStart+int64(len(trimmed)), int64(len(original)))
		want := original[sliceStart:sliceEnd]
		if len(trimmed) != len(want) {
			t.Fatalf("slice %d: reconstructed length %d, want %d", rs.Index, len(trimmed), len(want))
		}
		for i := range want {
			if trimmed[i] != want[i] {
				t.Fatalf("slice %d: byte %d mismatch - reconstructed data does not match the original", rs.Index, i)
			}
		}
	}
	progress.SetPhase(Par2PhaseCompleted)

	snap := progress.Snapshot()
	peak := peakInFlight.Load()

	t.Logf("fetch concurrency: peak %d in-flight fetches (bound was %d), %d intact slices fetched in %s (fully sequential would be >= %s)",
		peak, maxConcurrency, len(intactOrder), elapsed, time.Duration(len(intactOrder))*simulatedFetchLatency)
	t.Logf("final progress snapshot: %+v", snap)

	if peak < 2 {
		t.Errorf("peak in-flight fetches = %d, want > 1 (fetches should genuinely overlap)", peak)
	}
	if peak > maxConcurrency {
		t.Errorf("peak in-flight fetches = %d, want <= %d (concurrency bound violated)", peak, maxConcurrency)
	}
	if elapsed >= time.Duration(len(intactOrder))*simulatedFetchLatency {
		t.Errorf("elapsed %s was not faster than a fully sequential pass (%s) - concurrency had no effect",
			elapsed, time.Duration(len(intactOrder))*simulatedFetchLatency)
	}
	if snap.Phase != Par2PhaseCompleted {
		t.Errorf("final Phase = %q, want %q", snap.Phase, Par2PhaseCompleted)
	}
	if snap.IntactSlicesRead != len(intactOrder) || snap.IntactSlicesTotal != len(intactOrder) {
		t.Errorf("IntactSlicesRead/Total = %d/%d, want %d/%d", snap.IntactSlicesRead, snap.IntactSlicesTotal, len(intactOrder), len(intactOrder))
	}
	// This demo loads every fixture recovery source upfront (unlike
	// runRepair's real incremental fetchMoreVolumes), so "fetched" is the
	// fixture's full available set while "needed" reflects the actual
	// damage count driving the repair.
	if snap.RecoverySlicesFetched != len(idx.Recovery) || snap.RecoverySlicesNeeded != len(damaged) {
		t.Errorf("RecoverySlicesFetched/Needed = %d/%d, want %d/%d", snap.RecoverySlicesFetched, snap.RecoverySlicesNeeded, len(idx.Recovery), len(damaged))
	}
	if snap.LastError != "" {
		t.Errorf("LastError = %q, want empty", snap.LastError)
	}

	fmt.Printf(
		"PAR2 repair progress demo: phase=%s intact_read=%d/%d recovery_slices=%d/%d recovery_vols_fetched=%d peak_concurrent_fetches=%d/%d elapsed=%s (vs %s sequential)\n",
		snap.Phase, snap.IntactSlicesRead, snap.IntactSlicesTotal,
		snap.RecoverySlicesFetched, snap.RecoverySlicesNeeded, snap.RecoveryVolsFetched,
		peak, maxConcurrency, elapsed, time.Duration(len(intactOrder))*simulatedFetchLatency,
	)
}
