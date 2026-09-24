package manager

import (
	"context"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

// Two posted copies of one file (the same upload under two subjects, cut into
// different article sizes) share length and MD5-16k. MatchFiles used to pair
// both with the one FileDesc, and runRepair then indexed one copy's article
// geometry with the other's positions: index out of range, a crashed process
// (before recover) and, with the pairing cached, the same on every retry.
func TestDuplicatePostingPairsOneCopy(t *testing.T) {
	const sliceSize = 64
	content := postedContent(256)
	var slices [][]byte
	for off := 0; off < len(content); off += sliceSize {
		slices = append(slices, content[off:off+sliceSize])
	}
	fileID := [16]byte{0x0d}
	idx := buildSingleFileIndexWithIFSC(t, sliceSize, fileID, "v.r04", slices)

	mk := func(name string, art int) storage.PostedFileRef {
		ref := storage.PostedFileRef{Name: name, Size: int64(len(content))}
		for off, i := 0, 0; off < len(content); off, i = off+art, i+1 {
			n := min(art, len(content)-off)
			ref.Segments = append(ref.Segments, storage.Par2SegmentRef{MessageID: name + string(rune('a'+i)), Bytes: int64(n), Real: true})
		}
		return ref
	}
	src := []storage.PostedFileRef{mk("copy-small-articles", 32), mk("copy-big-articles", 64)}
	posted := make([]par2.PostedFile, len(src))
	for i, f := range src {
		posted[i] = par2.PostedFile{Name: f.Name, Length: f.Size, MD5_16k: func() ([16]byte, error) {
			return idx.Files[fileID].MD5_16k, nil
		}}
	}
	matches, _, err := par2.MatchFiles(idx, posted)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 {
		t.Fatalf("MatchFiles paired %d posted files with one FileDesc, want 1", len(matches))
	}
	if cache := par2MatchToCache(src, matches, nil); cache != nil {
		t.Fatal("a partial match set was cached")
	}

	// A duplicate pairing cached before this fix is refused, so matching runs
	// again.
	stale := []storage.Par2MatchRef{{PostedName: src[0].Name, FileID: fileID}, {PostedName: src[1].Name, FileID: fileID}}
	if _, ok := par2MatchFromCache(idx, src, stale); ok {
		t.Fatal("a cached duplicate pairing was accepted")
	}

	// runRepair's range build, as it does it: no panic.
	fetchers := make(map[[16]byte]*postedFileFetcher, len(matches))
	for _, m := range matches {
		fetchers[m.FileID] = newPostedFileFetcher(context.Background(), nil, src[m.PostedIndex], nil, idx.Files[m.FileID].Length, zerolog.Nop())
	}
	for _, m := range matches {
		f := fetchers[m.FileID]
		for i := range src[m.PostedIndex].Segments {
			_ = f.base[i] + f.segSizes[i]
		}
	}
}
