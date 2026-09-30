package manager

import (
	"fmt"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

const coverageIndexSize = 40_000 // fixed packets every PAR2 file carries

// namedSet builds a par2cmdline-style set: an index file plus volumes of
// doubling size holding total recovery slices of sliceSize.
func namedSet(sliceSize int64, total int) []storage.Par2FileRef {
	return namedSetCalled("rel", sliceSize, total)
}

func namedSetCalled(stem string, sliceSize int64, total int) []storage.Par2FileRef {
	files := []storage.Par2FileRef{{Name: stem + ".par2", Size: coverageIndexSize}}
	for start, n := 0, 1; start < total; start, n = start+n, n*2 {
		n = min(n, total-start)
		files = append(files, storage.Par2FileRef{
			Name: fmt.Sprintf("%s.vol%03d+%02d.par2", stem, start, n),
			Size: coverageIndexSize + int64(n)*(sliceSize+par2RecoveryPacketOverhead),
		})
	}
	return files
}

func TestPar2CoverageForNamesTheLimit(t *testing.T) {
	const mib = 1 << 20
	cases := []struct {
		name      string
		sliceSize int64
		posted    int
		wantMax   int
		wantBy    string
	}{
		{"small posting, all usable", mib, 40, 40, "posting"},
		{"REMUX past the slice cap", 10 * mib, 168, par2.MaxRepairSlices, "slice_cap"},
		{"huge slices bound by memory", 32 * mib, 100, par2.MaxRepairSlicesFor(32 * mib), "memory"},
	}
	for _, c := range cases {
		got, ok := Par2CoverageFor(namedSet(c.sliceSize, c.posted))
		if !ok {
			t.Fatalf("%s: no coverage", c.name)
		}
		if got.SliceSize != c.sliceSize || got.RecoverySlicesPosted != c.posted || got.MaxRepairableSlices != c.wantMax || got.LimitedBy != c.wantBy {
			t.Errorf("%s: got %+v, want slice %d, posted %d, max %d, limited by %s", c.name, got, c.sliceSize, c.posted, c.wantMax, c.wantBy)
		}
		if got.MaxRepairableBytes != int64(c.wantMax)*c.sliceSize {
			t.Errorf("%s: max bytes %d, want %d", c.name, got.MaxRepairableBytes, int64(c.wantMax)*c.sliceSize)
		}
	}
}

// A season pack carries one set per episode and a repair uses one of them,
// so the figures are the largest set's, not a total across the pack.
func TestPar2CoverageForSeasonPackUsesOneSet(t *testing.T) {
	const mib = 1 << 20
	files := append(namedSetCalled("show.s01e01", mib, 20), namedSetCalled("show.s01e02", 2*mib, 30)...)
	got, ok := Par2CoverageFor(files)
	if !ok || got.Sets != 2 || got.RecoverySlicesPosted != 30 || got.SliceSize != 2*mib {
		t.Errorf("got %+v ok=%v, want 2 sets and the larger set's 30 slices of 2 MiB", got, ok)
	}
}

// Obfuscated names carry no .volN+M marker: count and slice size come from
// the size steps between the files.
func TestPar2CoverageForObfuscatedNames(t *testing.T) {
	const slice = 2 << 20
	files := []storage.Par2FileRef{{Name: "a1b2c3", Size: coverageIndexSize}}
	for _, n := range []int64{1, 2, 4, 8} {
		files = append(files, storage.Par2FileRef{Name: fmt.Sprintf("x%d", n), Size: coverageIndexSize + n*(slice+par2RecoveryPacketOverhead)})
	}
	got, ok := Par2CoverageFor(files)
	if !ok || got.SliceSize != slice || got.RecoverySlicesPosted != 15 || got.LimitedBy != "posting" {
		t.Errorf("got %+v ok=%v, want slice %d, 15 posted, limited by posting", got, ok, slice)
	}
}

func TestPar2CoverageForNothingToEstimate(t *testing.T) {
	if _, ok := Par2CoverageFor(nil); ok {
		t.Error("no PAR2 files: want no coverage")
	}
	if _, ok := Par2CoverageFor([]storage.Par2FileRef{{Name: "rel.par2", Size: coverageIndexSize}}); ok {
		t.Error("index file only: want no coverage")
	}
}
