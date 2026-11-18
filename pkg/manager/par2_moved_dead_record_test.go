package manager

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// newTestManagerWithNZBRecords is newTestManagerForReap with NZB record
// storage, for code that reloads the record (the heal does before it
// writes). Store the record the test works from with storeNZBRecord.
func newTestManagerWithNZBRecords(t *testing.T) *Manager {
	t.Helper()
	configDir := t.TempDir()
	config.SetConfigPath(configDir)
	strg, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("storage.NewStorage: %v", err)
	}
	t.Cleanup(func() { _ = strg.Close() })
	store, err := overlay.NewStore(filepath.Join(configDir, "usenet", "overlay"), zerolog.Nop())
	if err != nil {
		t.Fatalf("overlay.NewStore: %v", err)
	}
	u, err := usenet.NewWithNZBStorageForTest(store)
	if err != nil {
		t.Fatalf("NewWithNZBStorageForTest: %v", err)
	}
	return &Manager{storage: strg, usenet: u}
}

func storeNZBRecord(t *testing.T, m *Manager, nzb *storage.NZB) {
	t.Helper()
	if err := m.usenet.AddNZBForTest(nzb); err != nil {
		t.Fatalf("store the NZB record: %v", err)
	}
}

// A dead segment was recorded at index 2, and then a volume reorder moved
// its article to index 4 in the NZB record. The record still named index 2,
// where the record now holds another article, so readerPatchWindow refused
// it and the pass ended in writeRecordedPatches' terminal skip - in the
// urgent lane, a re-grab of a release the solve had just repaired. The pass
// now moves the record to the article's new index and patches it there.
func TestPar2PassPatchesADeadRecordAtItsArticlesNewIndex(t *testing.T) {
	servers, fx := newP2eEnv(t, 1)
	srv := servers[0]
	f1 := fx.posted[0]
	const from, to = 2, 4
	if len(f1.ids) <= to || len(f1.payloads[from]) != len(f1.payloads[to]) {
		t.Fatalf("fixture: need articles %d and %d of equal length (have %d articles)", from, to, len(f1.ids))
	}
	nzb := fx.nzb("p2e-dead-record-moved")
	for i := range nzb.Files {
		if nzb.Files[i].Name != f1.name {
			continue
		}
		s := nzb.Files[i].Segments
		s[from].MessageID, s[to].MessageID = s[to].MessageID, s[from].MessageID
		s[from].Number, s[to].Number = s[to].Number, s[from].Number
	}
	h := newP2eHarnessNZB(t, srv, fx, nzb)
	h.recordDead(p2eSeg{f1, from}) // recorded before the reorder: index 2, article ids[2]
	srv.set(f1.ids[from], p2eNotFound)

	o := h.run(nil)
	t.Logf("outcome: %s class=%s err=%q tally=%s overlay=%s %v", o.LogMsg, o.Class, o.Err, o.Tally, o.Overlay, o.OverlayDiff)

	if o.LogMsg != "par2 repair completed" || o.Err != "" {
		t.Fatalf("pass ended %q (class %q, err %q), want completed", o.LogMsg, o.Class, o.Err)
	}
	patch, ok := h.u.OverlayPatchBytes(h.nzbID, f1.name, to)
	if !ok || !bytes.Equal(patch, f1.payloads[from]) {
		t.Fatalf("index %d patch = %d bytes (ok=%v), want the dead article's own %d bytes", to, len(patch), ok, len(f1.payloads[from]))
	}
	man, err := h.u.OverlayManifest(h.nzbID)
	if err != nil || man == nil {
		t.Fatalf("manifest: %v", err)
	}
	if fe := man.Files[f1.name]; fe != nil {
		for _, d := range fe.DeadSegments {
			if d.Index == from {
				t.Fatalf("a record is left at the old index %d: %+v", from, d)
			}
		}
	}
}

// The pre-solve heal fetched a dead article intact, but the NZB record was
// permuted after the pass loaded it: the old index now holds another,
// healthy article. The heal used to write the verified bytes there; it now
// checks a freshly loaded record first, writes nothing and leaves the
// segment to the solve, whose own check ends the pass transient.
func TestHealWritesNoPatchWhenTheRecordMovedUnderIt(t *testing.T) {
	m := newTestManagerWithNZBRecords(t)
	p := &Par2Repair{manager: m, logger: zerolog.Nop()}

	content, articles, ref := estimatedFixture()
	const sliceSize = 40
	var slices [][]byte
	for off := 0; off < len(content); off += sliceSize {
		slices = append(slices, content[off:off+sliceSize])
	}
	fileID := [16]byte{0x07}
	idx := buildSingleFileIndexWithIFSC(t, sliceSize, fileID, "a.r00", slices)
	f := &yencFetcher{articles: articles, calls: map[string]int{}}
	pf := newPostedFileFetcher(context.Background(), f.fetch, ref, nil, int64(len(content)), zerolog.Nop())
	pf.resolveGeometry()

	const nzbID = "nzb-heal-moved"
	readerFile := storage.NZBFile{Name: ref.Name}
	for i, seg := range ref.Segments {
		readerFile.Segments = append(readerFile.Segments, storage.NZBSegment{
			Number: i + 1, MessageID: seg.MessageID, Bytes: pf.segSizes[i],
			StartOffset: pf.base[i], EndOffset: pf.base[i] + pf.segSizes[i] - 1,
		})
	}
	old := &storage.NZB{ID: nzbID, Files: []storage.NZBFile{readerFile}}

	// The stored record, as a reorder left it: articles 1 and 2 swapped.
	moved := &storage.NZB{ID: nzbID, Files: []storage.NZBFile{{Name: ref.Name, Segments: append([]storage.NZBSegment(nil), readerFile.Segments...)}}}
	s := moved.Files[0].Segments
	if s[1].Bytes != s[2].Bytes {
		t.Fatalf("fixture: articles 1 and 2 differ in length (%d, %d)", s[1].Bytes, s[2].Bytes)
	}
	s[1].MessageID, s[2].MessageID = s[2].MessageID, s[1].MessageID
	storeNZBRecord(t, m, moved)

	seg := ref.Segments[1]
	if err := m.usenet.RecordOverlayDead(nzbID, ref.Name, 1, seg.MessageID, pf.segSizes[1]); err != nil {
		t.Fatalf("record dead: %v", err)
	}
	refs := []par2DeadRef{{
		file: ref.Name,
		seg:  overlay.DeadSegment{Index: 1, MessageID: seg.MessageID, Bytes: pf.segSizes[1]},
		rng:  postedRange{fileID: fileID, start: pf.base[1], end: pf.base[1] + pf.segSizes[1]},
	}}

	remaining, err := p.healFetchableDeadSegments(context.Background(), nzbID, "Some.Release", old, idx,
		map[[16]byte]*postedFileFetcher{fileID: pf}, refs, map[string]struct{}{})
	if err != nil {
		t.Fatalf("heal: %v", err)
	}
	if f.calls[seg.MessageID] == 0 {
		t.Fatal("the heal never fetched the article: the test does not reach the write")
	}
	if _, ok := m.usenet.OverlayPatchBytes(nzbID, ref.Name, 1); ok {
		t.Fatal("patched index 1, which the stored record now gives to another article")
	}
	if len(remaining) != 1 || remaining[0].seg.MessageID != seg.MessageID {
		t.Fatalf("remaining = %+v, want the moved segment left to the solve", remaining)
	}
}
