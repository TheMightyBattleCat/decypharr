package manager

import (
	"context"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// unnamedPar2File is an obfuscated PAR2 file (no volume marker) of size
// bytes, posted as one article whose message ID is its name.
func unnamedPar2File(name string, size int64) storage.Par2FileRef {
	return storage.Par2FileRef{
		Name:     name,
		Size:     size,
		Segments: []storage.Par2SegmentRef{{MessageID: name, Bytes: size}},
	}
}

// An obfuscated posting's recovery volumes carry no .volN+M marker. Census-ed
// as index files, every one was fetched whole before any gate. They must come
// back as volumes, smallest first, so they are fetched only as far as the
// damage needs.
func TestCensusPar2VolumesSplitsUnnamedPostingBySize(t *testing.T) {
	const mib = 1 << 20
	files := []storage.Par2FileRef{
		unnamedPar2File("c3d4.par2", 40*mib),
		unnamedPar2File("a1b2.par2", 1*mib),
		unnamedPar2File("e5f6.par2", 80*mib),
		unnamedPar2File("0a0b.par2", 10*mib),
		unnamedPar2File("9z8y.par2", 20*mib),
	}
	vols, indexFiles := censusPar2Volumes(files)
	if len(indexFiles) != 1 || indexFiles[0].Name != "a1b2.par2" {
		t.Fatalf("index files = %+v, want only the 1 MiB a1b2.par2", indexFiles)
	}
	if len(vols) != 4 {
		t.Fatalf("got %d volumes, want 4: %+v", len(vols), vols)
	}
	var total uint32
	for i, v := range vols {
		if v.count < 1 {
			t.Errorf("volume %s has count %d, want at least 1", v.ref.Name, v.count)
		}
		if i > 0 && v.ref.Size < vols[i-1].ref.Size {
			t.Errorf("volumes not smallest first: %s after %s", v.ref.Name, vols[i-1].ref.Name)
		}
		total += v.count
	}
	if want := estimateUnnamedRecovery(files); total != want {
		t.Errorf("census total %d slices, estimateUnnamedRecovery says %d", total, want)
	}
}

// A season pack's per-episode index files are similar sizes: all stay index
// files (the multi-set choice needs every one), and only the files at least
// twice the smallest become volumes.
func TestCensusPar2VolumesKeepsSeasonPackIndexes(t *testing.T) {
	files := []storage.Par2FileRef{
		unnamedPar2File("ep1.par2", 40_000),
		unnamedPar2File("ep2.par2", 43_500),
		unnamedPar2File("ep3.par2", 41_200),
		unnamedPar2File("v1.par2", 4_040_000),
		unnamedPar2File("v2.par2", 8_040_000),
	}
	vols, indexFiles := censusPar2Volumes(files)
	if len(indexFiles) != 3 {
		t.Errorf("index files = %+v, want the three episode indexes", indexFiles)
	}
	if len(vols) != 2 || vols[0].ref.Name != "v1.par2" || vols[0].count != 1 || vols[1].count != 2 {
		t.Errorf("volumes = %+v, want v1 (1 slice) then v2 (2 slices)", vols)
	}

	// Only index-sized files: nothing is a volume.
	vols, indexFiles = censusPar2Volumes(files[:3])
	if len(vols) != 0 || len(indexFiles) != 3 {
		t.Errorf("index-only posting: %d volumes, %d index files; want 0 and 3", len(vols), len(indexFiles))
	}
}

// countingPar2Fetch serves every article as zero bytes of its file's size and
// counts the fetches per message ID.
type countingPar2Fetch struct {
	mu    sync.Mutex
	sizes map[string]int64
	calls map[string]int
}

func newCountingPar2Fetch(files []storage.Par2FileRef) *countingPar2Fetch {
	c := &countingPar2Fetch{sizes: map[string]int64{}, calls: map[string]int{}}
	for _, f := range files {
		c.sizes[f.Name] = f.Size
	}
	return c
}

func (c *countingPar2Fetch) fetch(_ context.Context, id string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls[id]++
	return make([]byte, c.sizes[id]), nil
}

// A pass on an obfuscated posting fetches the index file and then volumes
// smallest first, only as many as the damage needs (plus fetchMoreVolumes'
// one slack volume) - never the largest volumes.
func TestUnnamedPostingFetchesOnlyTheVolumesNeeded(t *testing.T) {
	// Same proportions as 1, 10, 20, 40, 80 MiB, in KiB.
	const kib = 1 << 10
	files := []storage.Par2FileRef{
		unnamedPar2File("idx.par2", 1*kib),
		unnamedPar2File("v10.par2", 10*kib),
		unnamedPar2File("v20.par2", 20*kib),
		unnamedPar2File("v40.par2", 40*kib),
		unnamedPar2File("v80.par2", 80*kib),
	}
	f := newCountingPar2Fetch(files)
	vols, indexFiles := censusPar2Volumes(files)
	ctx := context.Background()
	sources, indexErr := fetchPar2IndexFiles(ctx, zerolog.Nop(), f.fetch, indexFiles, "test")
	if indexErr != nil || len(sources) != 1 || sources[0].Name != "idx.par2" {
		t.Fatalf("index fetch: sources %d, err %v; want idx.par2 alone", len(sources), indexErr)
	}

	cases := []struct {
		needed uint32
		want   []string
	}{
		// v10 holds 1 slice: it covers the need, v20 is the slack volume.
		{1, []string{"idx.par2", "v10.par2", "v20.par2"}},
		// v10+v20 hold 3: they cover 2, v40 is the slack volume.
		{2, []string{"idx.par2", "v10.par2", "v20.par2", "v40.par2"}},
	}
	for _, tc := range cases {
		f.calls = map[string]int{"idx.par2": 1}
		next, _, got, _, err := fetchMoreVolumes(ctx, zerolog.Nop(), f.fetch, vols, 0, tc.needed, 0, sources, "test", 4)
		if err != nil {
			t.Fatalf("needed %d: fetchMoreVolumes: %v", tc.needed, err)
		}
		if next != len(tc.want)-1 || len(got) != len(tc.want) {
			t.Errorf("needed %d: next %d, %d sources; want %d volumes", tc.needed, next, len(got), len(tc.want)-1)
		}
		for _, name := range tc.want {
			if f.calls[name] != 1 {
				t.Errorf("needed %d: %s fetched %d times, want once", tc.needed, name, f.calls[name])
			}
		}
		if f.calls["v80.par2"] != 0 {
			t.Errorf("needed %d: the largest volume was fetched", tc.needed)
		}
	}
}

// An index file over par2MaxIndexFileBytes is not fetched whole; the volumes
// carry the same metadata. Skipping it is not a transport failure.
func TestFetchPar2IndexFilesRefusesOversizedIndex(t *testing.T) {
	small := unnamedPar2File("small.par2", 1000)
	huge := unnamedPar2File("huge.par2", par2MaxIndexFileBytes+1)
	f := newCountingPar2Fetch([]storage.Par2FileRef{small})
	sources, indexErr := fetchPar2IndexFiles(context.Background(), zerolog.Nop(), f.fetch, []storage.Par2FileRef{small, huge}, "test")
	if f.calls["huge.par2"] != 0 {
		t.Errorf("oversized index file fetched %d times, want 0", f.calls["huge.par2"])
	}
	if len(sources) != 1 || sources[0].Name != "small.par2" {
		t.Errorf("sources = %d, want small.par2 alone", len(sources))
	}
	if indexErr != nil {
		t.Errorf("indexErr = %v, want nil (a refused file is not a transport failure)", indexErr)
	}
}

// Par2CoverageFor keeps estimating an obfuscated posting from sizes: the
// size-split volumes have no set stem to group by.
func TestPar2CoverageForUnnamedPostingUnchanged(t *testing.T) {
	files := []storage.Par2FileRef{
		{Name: "a8f3e2b1c4d5.par2", Size: 40_000},
		{Name: "0f9e8d7c6b5a.par2", Size: 4_040_000},
		{Name: "1a2b3c4d5e6f.par2", Size: 8_040_000},
		{Name: "9f8e7d6c5b4a.par2", Size: 16_040_000},
	}
	c, ok := Par2CoverageFor(files)
	if !ok {
		t.Fatal("no coverage for an obfuscated posting")
	}
	if c.RecoverySlicesPosted != 7 || c.Sets != 1 {
		t.Errorf("coverage = %+v, want 7 slices posted in 1 set", c)
	}
	if want := int64((4_000_000 - par2RecoveryPacketOverhead) &^ 3); c.SliceSize != want {
		t.Errorf("slice size %d, want %d", c.SliceSize, want)
	}
}
