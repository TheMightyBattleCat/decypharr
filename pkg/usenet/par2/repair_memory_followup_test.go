package par2

import (
	"bytes"
	"runtime"
	"testing"
)

// How much memory one RepairWith call takes on top of what the caller already
// holds, in units of k x slice size (the figure maxAccumulatorMemory caps).
//
// The caller holds the k recovery slices (1x). RepairWith copies them into
// its accumulators (+1x) and then allocates the k output slices (+1x), so
// the peak is 3x. Two ways to cut it:
//
//   - not copying the recovery slices (accumulate into the caller's buffers):
//     saves 1x, but destroys the caller's recovery data, which a second
//     round reuses. The second half of this test pins that the data is left
//     untouched today.
//   - writing each solved word back over the accumulators: the solve reads
//     word w of every accumulator before it writes word w of any output, so
//     the outputs can share the accumulators' memory. Saves 1x and leaves
//     the caller's data alone.
func TestFollowupRepairMemoryPerCall(t *testing.T) {
	const (
		sliceSize = 64 << 10
		numSlices = 64
		k         = 16
	)
	f := buildBenchFixture(t, sliceSize, numSlices, k)
	src := benchSliceSource{data: f.data}

	before := make([][]byte, k)
	for j, rs := range f.recovery {
		before[j] = bytes.Clone(rs.Data)
	}

	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	out, err := RepairWith(f.idx, f.damaged, f.recovery, src, RepairOptions{MaxUnavailable: -1})
	runtime.ReadMemStats(&m1)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != k {
		t.Fatalf("repaired %d slices, want %d", len(out), k)
	}

	unit := float64(k * sliceSize)
	allocated := float64(m1.TotalAlloc-m0.TotalAlloc) / unit
	t.Logf("allocated inside RepairWith: %.2fx of k x slice (accumulators + outputs); with the caller's recovery data the peak is %.2fx", allocated, allocated+1)
	if allocated < 1.9 || allocated > 2.4 {
		t.Errorf("allocated %.2fx of k x slice, recorded about 2x", allocated)
	}

	for j, rs := range f.recovery {
		if !bytes.Equal(rs.Data, before[j]) {
			t.Fatalf("recovery slice %d was changed by RepairWith; a second round would solve from wrong data", j)
		}
	}

	// What the 3x means at the limits in force (maxAccumulatorMemory caps
	// k x slice, not the peak).
	capGiB := float64(maxAccumulatorMemory) / (1 << 30)
	t.Logf("at the %.0f GiB cap: peak %.0f GiB today, %.0f GiB with outputs written over the accumulators", capGiB, 3*capGiB, 2*capGiB)
	t.Logf("at %d slices of 5 MiB (a typical REMUX set at the slice cap): peak %.1f GiB today, %.1f GiB after", maxRepairSlices, 3*float64(maxRepairSlices)*5/1024, 2*float64(maxRepairSlices)*5/1024)
}
