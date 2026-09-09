package manager

import (
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// fakeCoverageReader is the dfsCacheCoverageReader seam, so the burst gate can
// be proven without a real DFS mount or usenet client - the same approach the
// reserveBudget gating tests take.
type fakeCoverageReader struct {
	cached, total int64
	ok            bool
	calls         int
}

func (f *fakeCoverageReader) CacheCoverage(entryName, filename string) (int64, int64, time.Time, bool) {
	f.calls++
	return f.cached, f.total, time.Time{}, f.ok
}

// The gate in front of ReadAhead + persistCleanRanges. A file the durable DFS
// cache already holds end to end must report complete, so the next-episode
// walk skips the burst instead of re-downloading the whole file from usenet
// (the reader's SegmentCache is per-reader scratch and does not consult the
// durable cache, so an "already cached" file would otherwise be pulled again).
func TestDurableCacheCompleteGate(t *testing.T) {
	for _, tc := range []struct {
		name          string
		reader        *fakeCoverageReader
		nilReader     bool
		wantComplete  bool
		wantCallCount int
	}{
		{
			name:          "fully cached skips the burst",
			reader:        &fakeCoverageReader{cached: 4432784885, total: 4432784885, ok: true},
			wantComplete:  true,
			wantCallCount: 1,
		},
		{
			name:          "over-reported coverage still counts as complete",
			reader:        &fakeCoverageReader{cached: 4432784900, total: 4432784885, ok: true},
			wantComplete:  true,
			wantCallCount: 1,
		},
		{
			name:          "partially cached still bursts to fill the gaps",
			reader:        &fakeCoverageReader{cached: 2216392442, total: 4432784885, ok: true},
			wantComplete:  false,
			wantCallCount: 1,
		},
		{
			name:          "one byte short still bursts",
			reader:        &fakeCoverageReader{cached: 4432784884, total: 4432784885, ok: true},
			wantComplete:  false,
			wantCallCount: 1,
		},
		{
			name:          "nothing cached bursts cold",
			reader:        &fakeCoverageReader{cached: 0, total: 4432784885, ok: true},
			wantComplete:  false,
			wantCallCount: 1,
		},
		{
			name:          "coverage unknown bursts rather than assuming cached",
			reader:        &fakeCoverageReader{ok: false},
			wantComplete:  false,
			wantCallCount: 1,
		},
		{
			name:          "zero total bursts rather than dividing by zero",
			reader:        &fakeCoverageReader{cached: 0, total: 0, ok: true},
			wantComplete:  false,
			wantCallCount: 1,
		},
		{
			name:         "no coverage seam at all bursts as before",
			nilReader:    true,
			wantComplete: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &Precache{logger: zerolog.Nop()}

			var got bool
			if tc.nilReader {
				got = p.durableCacheComplete(nil, "entry", "file.mkv")
			} else {
				got = p.durableCacheComplete(tc.reader, "entry", "file.mkv")
				if tc.reader.calls != tc.wantCallCount {
					t.Errorf("CacheCoverage called %d times, want %d", tc.reader.calls, tc.wantCallCount)
				}
			}

			if got != tc.wantComplete {
				t.Errorf("durableCacheComplete = %v, want %v", got, tc.wantComplete)
			}
		})
	}
}
