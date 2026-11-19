package manager

import (
	"bytes"
	"context"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// A posted file whose refs are estimates gets its article boundaries from
// its first articles' yEnc headers (resolveGeometry). When the first three
// articles are all gone, the probe used to give up: the file kept estimated
// boundaries, its dead segments could not be placed, and the pass ended
// terminal on the probe's 430 although the damage fit the recovery data.
// Now the pass measures the geometry from a later article the STAT sweep saw
// present, and patches all three.
func TestPar2PassMeasuresGeometryPastThreeDeadHeadArticles(t *testing.T) {
	servers, fx := newP2eEnvSets(t, 1, p2eSet{base: p2eRelease, files: []int{0, 1, 2}, redundancy: 40})
	srv := servers[0]
	h := newP2eHarness(t, srv, fx, "p2e-dead-head-geometry")
	f1 := fx.posted[0]
	if len(f1.ids) < 6 {
		t.Fatalf("fixture: %s has %d articles, want at least 6", f1.name, len(f1.ids))
	}

	// The refs of f1 are estimates, each a few bytes off, as the parser
	// stores them when it could not measure the posting.
	nzb := fx.nzb(h.nzbID)
	ref := &nzb.Par2Source[0]
	if ref.Name != f1.name {
		t.Fatalf("fixture: Par2Source[0] is %s, want %s", ref.Name, f1.name)
	}
	ref.Size = 0
	for i := range ref.Segments {
		ref.Segments[i].Real = false
		ref.Segments[i].Bytes += int64(i%3) - 1
		ref.Size += ref.Segments[i].Bytes
	}
	if err := h.u.AddNZBForTest(nzb); err != nil {
		t.Fatalf("store the estimated record: %v", err)
	}

	dead := []p2eSeg{{f1, 0}, {f1, 1}, {f1, 2}}
	h.recordDead(dead...)
	for _, s := range dead {
		srv.set(s.file.ids[s.idx], p2eNotFound)
	}

	o := h.run(nil)
	t.Logf("outcome: %s class=%s err=%q tally=%s state=%s overlay=%s %v notes=%v", o.LogMsg, o.Class, o.Err, o.Tally, o.State, o.Overlay, o.OverlayDiff, h.notes)

	if o.LogMsg != "par2 repair completed" {
		t.Fatalf("pass ended %q (class %q, err %q); the damage fits the recovery data and the geometry is measurable from a later article", o.LogMsg, o.Class, o.Err)
	}
	if o.Patched != len(dead) || o.StillDead != 0 || o.Mismatch != 0 {
		t.Fatalf("patched %d, still dead %d, mismatched %d; want all %d head articles patched with their real bytes: %v",
			o.Patched, o.StillDead, o.Mismatch, len(dead), o.OverlayDiff)
	}
}

// Ten 60-byte articles recorded as estimates, the first three gone.
func deadHeadFixture() ([]byte, map[string]yencArticle, storage.PostedFileRef) {
	content := postedContent(580)
	articles, ref := uniformPosting(content, 60, []int64{61, 59, 62, 60, 61, 59, 60, 62, 59, 41})
	for _, id := range []string{"<p1>", "<p2>", "<p3>"} {
		delete(articles, id)
	}
	return content, articles, ref
}

// Past a dead head the probe skips the articles the STAT sweep found
// missing and measures from one it saw present; the measured boundaries
// read the right bytes.
func TestResolveGeometryPastDeadHeadSkipsSweptMissing(t *testing.T) {
	content, articles, ref := deadHeadFixture()
	delete(articles, "<p4>")
	delete(articles, "<p5>")
	f := &yencFetcher{articles: articles, calls: map[string]int{}}
	pf := newPostedFileFetcher(context.Background(), f.fetch, ref, nil, int64(len(content)), zerolog.Nop())

	pf.resolveGeometry()
	if pf.exact || !nntp.IsArticleNotFoundError(pf.geometryErr) {
		t.Fatalf("precondition: three dead leading articles leave the geometry unmeasured on a 430 (exact %v, err %v)", pf.exact, pf.geometryErr)
	}
	if !pf.resolveGeometryPastDeadHead(map[string]struct{}{"<p4>": {}, "<p5>": {}}) {
		t.Fatalf("geometry not measured past the dead head: %v", pf.geometryErr)
	}
	if f.calls["<p4>"] != 0 || f.calls["<p5>"] != 0 {
		t.Fatalf("fetched articles the sweep found missing: <p4> %d, <p5> %d", f.calls["<p4>"], f.calls["<p5>"])
	}
	if f.calls["<p6>"] != 1 {
		t.Fatalf("<p6> fetched %d times, want once", f.calls["<p6>"])
	}
	if pf.geometryErr != nil {
		t.Fatalf("geometryErr %v kept after measuring", pf.geometryErr)
	}
	for i := range ref.Segments {
		want := int64(60)
		if i == len(ref.Segments)-1 {
			want = 40
		}
		if pf.base[i] != int64(i)*60 || pf.segSizes[i] != want {
			t.Fatalf("article %d at %d+%d, want %d+%d", i, pf.base[i], pf.segSizes[i], i*60, want)
		}
	}
	got, err := pf.ReadRange(360, int64(len(content))-360)
	if err != nil || !bytes.Equal(got, content[360:]) {
		t.Fatalf("measured geometry misreads: err %v", err)
	}
}

// A later probe that times out makes the miss retryable: the unplaced dead
// segments back off instead of ending terminal on the head's 430s. With
// nothing left to try the 430 stays, and no probe is spent.
func TestResolveGeometryPastDeadHeadTimeoutBacksOff(t *testing.T) {
	content, articles, ref := deadHeadFixture()
	f := &yencFetcher{articles: articles, calls: map[string]int{}}
	timeouts := 0
	fetch := func(ctx context.Context, id string, check usenet.ArticleCheck) ([]byte, error) {
		if _, ok := articles[id]; ok {
			timeouts++
			return nil, &nntp.Error{Type: nntp.ErrorTypeTimeout, Message: "read timeout"}
		}
		return f.fetch(ctx, id, check)
	}
	pf := newPostedFileFetcher(context.Background(), fetch, ref, nil, int64(len(content)), zerolog.Nop())
	pf.resolveGeometry()
	if pf.resolveGeometryPastDeadHead(nil) {
		t.Fatal("measured with every later article timing out")
	}
	if timeouts != geometryProbeArticles {
		t.Fatalf("%d later probes, want %d", timeouts, geometryProbeArticles)
	}
	if pf.geometryErr == nil || nntp.IsArticleNotFoundError(pf.geometryErr) {
		t.Fatalf("geometryErr %v; a timeout must decide over the head's 430s", pf.geometryErr)
	}
	id := [16]byte{0x0c}
	dr := par2DeadRef{file: ref.Name, seg: overlay.DeadSegment{Index: 0, MessageID: ref.Segments[0].MessageID},
		rng: postedRange{fileID: id, start: pf.base[0], end: pf.base[0] + pf.segSizes[0]}}
	if _, err := splitPlaceableDeadRefs(map[[16]byte]*postedFileFetcher{id: pf}, []par2DeadRef{dr}); classifyPar2Failure(err).terminal {
		t.Fatalf("%v classified terminal; a timed-out probe must back off", err)
	}

	// Every later non-final article already known missing: nothing tried.
	pf = newPostedFileFetcher(context.Background(), f.fetch, ref, nil, int64(len(content)), zerolog.Nop())
	pf.resolveGeometry()
	headErr := pf.geometryErr
	all := map[string]struct{}{}
	for _, s := range ref.Segments {
		all[s.MessageID] = struct{}{}
	}
	before := len(f.calls)
	if pf.resolveGeometryPastDeadHead(all) || pf.geometryErr != headErr || len(f.calls) != before {
		t.Fatalf("with every later article missing: exact %v, err %v (was %v), fetches %d -> %d", pf.exact, pf.geometryErr, headErr, before, len(f.calls))
	}
}

// The heal that runs before a failing recovery gate builds its fetchers late,
// with no STAT sweep of its own beyond the pending articles. It used to give
// up on the geometry after three dead leading articles, so a reader-dead
// article that is really intact further on could not be placed and was left
// for parity there is not. Now it measures past the dead head, skipping the
// articles its STAT check found missing, and heals that article.
func TestHealWithoutRecoveryMeasuresGeometryPastDeadHead(t *testing.T) {
	m := newTestManagerWithNZBRecords(t)
	p := &Par2Repair{manager: m, logger: zerolog.Nop()}

	content, articles, ref := deadHeadFixture()
	delete(articles, "<p4>") // also gone, and STAT says so
	const sliceSize = 20
	var slices [][]byte
	for off := 0; off < len(content); off += sliceSize {
		slices = append(slices, content[off:off+sliceSize])
	}
	idx := buildSingleFileIndexWithIFSC(t, sliceSize, [16]byte{0x0d}, ref.Name, slices)

	const nzbID = "nzb-heal-dead-head"
	readerFile := storage.NZBFile{Name: ref.Name}
	for i, seg := range ref.Segments {
		readerFile.Segments = append(readerFile.Segments, storage.NZBSegment{
			Number: i + 1, MessageID: seg.MessageID, Bytes: 60,
		})
	}
	readerFile.Segments[len(readerFile.Segments)-1].Bytes = 40
	nzb := &storage.NZB{ID: nzbID, Files: []storage.NZBFile{readerFile}, Par2Source: []storage.PostedFileRef{ref}}
	storeNZBRecord(t, m, nzb)

	gone := map[string]bool{"<p1>": true, "<p2>": true, "<p3>": true, "<p4>": true}
	pending := map[string][]overlay.DeadSegment{}
	for _, i := range []int{0, 1, 2, 3, 5} {
		seg := ref.Segments[i]
		if err := m.usenet.RecordOverlayDead(nzbID, ref.Name, i, seg.MessageID, 60); err != nil {
			t.Fatalf("record dead %d: %v", i, err)
		}
		pending[ref.Name] = append(pending[ref.Name], overlay.DeadSegment{Index: i, MessageID: seg.MessageID, Bytes: 60})
	}
	stat := func(_ context.Context, ids []string) ([]nntp.StatResult, error) {
		out := make([]nntp.StatResult, len(ids))
		for i, id := range ids {
			out[i] = nntp.StatResult{MessageID: id, Available: !gone[id]}
			if gone[id] {
				out[i].Error = &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Message: "no such article"}
			}
		}
		return out, nil
	}
	f := &yencFetcher{articles: articles, calls: map[string]int{}}

	healed, total := p.healWithoutRecovery(context.Background(), nzbID, "Some.Release", nzb, idx, pending, f.fetch, nil, stat)
	if healed != 1 || total != 5 {
		t.Fatalf("healed %d of %d, want 1 of 5: the intact article past the dead head", healed, total)
	}
	patch, ok := m.usenet.OverlayPatchBytes(nzbID, ref.Name, 5)
	if !ok || !bytes.Equal(patch, content[300:360]) {
		t.Fatalf("segment 5 patch = %d bytes (ok=%v), want its real bytes at the measured offset 300", len(patch), ok)
	}
	for _, i := range []int{0, 1, 2, 3} {
		if _, ok := m.usenet.OverlayPatchBytes(nzbID, ref.Name, i); ok {
			t.Fatalf("segment %d patched although its article is gone", i)
		}
	}
	if f.calls["<p4>"] != 0 {
		t.Fatalf("<p4> fetched %d times; STAT found it missing, so the geometry probe must skip it", f.calls["<p4>"])
	}
}
