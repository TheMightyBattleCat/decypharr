package manager

import (
	"slices"
	"testing"
)

func TestDecodeThreadsFor(t *testing.T) {
	cases := []struct{ cpus, workers, want int }{
		{8, 3, 3},  // the production install: ceil(8/3)
		{8, 5, 2},  // default workers
		{8, 8, 1},  // one core per worker
		{16, 3, 4}, // capped at maxDecodeThreads
		{2, 5, 1},  // more workers than cores
		{8, 0, 4},  // workers unset guarded to 1, then capped
		{1, 1, 1},
	}
	for _, c := range cases {
		if got := decodeThreadsFor(c.cpus, c.workers); got != c.want {
			t.Errorf("decodeThreadsFor(%d, %d) = %d, want %d", c.cpus, c.workers, got, c.want)
		}
	}
}

func TestDecodeProbeFlagsThreads(t *testing.T) {
	intervals := []string{"0%+#48", "600%+#48"}

	single := (&ffprobeChecker{decodeThreads: 1}).decodeProbeFlags(intervals)
	if slices.Contains(single, "-threads") {
		t.Fatalf("single-threaded probe passed -threads: %v", single)
	}

	multi := (&ffprobeChecker{decodeThreads: 3}).decodeProbeFlags(intervals)
	i := slices.Index(multi, "-threads")
	if i < 0 || i+1 >= len(multi) || multi[i+1] != "3" {
		t.Fatalf("probe with 3 threads lacks -threads 3: %v", multi)
	}
	// Everything else is unchanged, so the verdict logic reads the same output.
	without := slices.Delete(slices.Clone(multi), i, i+2)
	if !slices.Equal(without, single) {
		t.Fatalf("-threads changed other flags:\n got %v\nwant %v", without, single)
	}
	if j := slices.Index(multi, "-read_intervals"); j < 0 || multi[j+1] != "0%+#48,600%+#48" {
		t.Fatalf("intervals not joined: %v", multi)
	}
}
