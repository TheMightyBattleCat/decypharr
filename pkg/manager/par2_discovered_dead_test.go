package manager

import (
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

// Articles the pass found gone or corrupt, not already pending, are mapped to
// the reader segment that serves them so runRepair can record and patch them.
func TestDiscoveredDeadRefs(t *testing.T) {
	fileID := [16]byte{0x31}
	nzb := &storage.NZB{
		Par2Source: []storage.PostedFileRef{{Name: "a.r00", Segments: []storage.Par2SegmentRef{
			{MessageID: "<a0>"}, {MessageID: "<a1>"}, {MessageID: "<a2>"}, {MessageID: "<a3>"},
		}}},
		Files: []storage.NZBFile{{Name: "movie.mkv", Segments: []storage.NZBSegment{
			{MessageID: "<a0>", Bytes: 100}, {MessageID: "<a1>", Bytes: 100}, {MessageID: "<a2>", Bytes: 100},
		}}},
	}
	f := &postedFileFetcher{}
	f.badArticles.Store(1, struct{}{}) // new: discovered
	f.badArticles.Store(2, struct{}{}) // already pending
	f.badArticles.Store(3, struct{}{}) // not served by the reader
	matches := []par2.Match{{PostedIndex: 0, FileID: fileID}}
	fetchers := map[[16]byte]*postedFileFetcher{fileID: f}
	ranges := map[string]postedRange{
		"<a1>": {fileID: fileID, start: 100, end: 200},
		"<a2>": {fileID: fileID, start: 200, end: 300},
		"<a3>": {fileID: fileID, start: 300, end: 400},
	}
	known := []par2DeadRef{{file: "movie.mkv", seg: overlay.DeadSegment{Index: 2, MessageID: "<a2>"}}}

	got := discoveredDeadRefs(nzb, matches, fetchers, ranges, known)
	if len(got) != 1 {
		t.Fatalf("got %d refs, want 1: %+v", len(got), got)
	}
	dr := got[0]
	if dr.file != "movie.mkv" || dr.seg.Index != 1 || dr.seg.MessageID != "<a1>" || dr.seg.Bytes != 100 || dr.rng.start != 100 {
		t.Fatalf("ref = %+v", dr)
	}
}
