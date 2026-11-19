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

// P-17: one article served through two reader slots (the end of one archive
// member and the start of the next, or two file records). Every slot is
// returned, so the slices the solve rebuilt are patched into each one, not
// only the slot that was recorded dead.
func TestDiscoveredDeadRefsReturnsEverySlotOfAnArticle(t *testing.T) {
	fileID := [16]byte{0x32}
	nzb := &storage.NZB{
		Par2Source: []storage.PostedFileRef{{Name: "a.r00", Segments: []storage.Par2SegmentRef{
			{MessageID: "<b0>"}, {MessageID: "<b1>"}, {MessageID: "<b2>"},
		}}},
		Files: []storage.NZBFile{
			{Name: "one.mkv", Segments: []storage.NZBSegment{
				{MessageID: "<b0>", Bytes: 100}, {MessageID: "<b1>", Bytes: 40},
			}},
			{Name: "two.mkv", Segments: []storage.NZBSegment{
				{MessageID: "<b1>", Bytes: 60}, {MessageID: "<b2>", Bytes: 100},
			}},
		},
	}
	f := &postedFileFetcher{}
	f.badArticles.Store(2, struct{}{}) // found dead by the pass, served once
	matches := []par2.Match{{PostedIndex: 0, FileID: fileID}}
	fetchers := map[[16]byte]*postedFileFetcher{fileID: f}
	ranges := map[string]postedRange{
		"<b0>": {fileID: fileID, start: 0, end: 100},
		"<b1>": {fileID: fileID, start: 100, end: 200},
		"<b2>": {fileID: fileID, start: 200, end: 300},
	}

	t.Run("recorded article, other slot", func(t *testing.T) {
		known := []par2DeadRef{{file: "one.mkv", seg: overlay.DeadSegment{Index: 1, MessageID: "<b1>", Bytes: 40}}}
		got := discoveredDeadRefs(nzb, matches, fetchers, ranges, known)
		if len(got) != 2 {
			t.Fatalf("got %d refs, want 2 (two.mkv#0 and two.mkv#1): %+v", len(got), got)
		}
		if dr := got[0]; dr.file != "two.mkv" || dr.seg.Index != 0 || dr.seg.MessageID != "<b1>" || dr.seg.Bytes != 60 || dr.rng.start != 100 {
			t.Fatalf("other slot of the recorded article = %+v, want two.mkv#0 <b1> 60 bytes at 100", dr)
		}
		if dr := got[1]; dr.file != "two.mkv" || dr.seg.Index != 1 || dr.seg.MessageID != "<b2>" {
			t.Fatalf("discovered ref = %+v, want two.mkv#1 <b2>", dr)
		}
	})

	t.Run("discovered article, both slots", func(t *testing.T) {
		f.badArticles.Store(1, struct{}{})
		defer f.badArticles.Delete(1)
		got := discoveredDeadRefs(nzb, matches, fetchers, ranges, nil)
		if len(got) != 3 {
			t.Fatalf("got %d refs, want 3: %+v", len(got), got)
		}
		if got[0].file != "one.mkv" || got[0].seg.Index != 1 || got[0].seg.Bytes != 40 ||
			got[1].file != "two.mkv" || got[1].seg.Index != 0 || got[1].seg.Bytes != 60 {
			t.Fatalf("refs for <b1> = %+v, %+v; want one.mkv#1 (40) and two.mkv#0 (60)", got[0], got[1])
		}
	})
}
