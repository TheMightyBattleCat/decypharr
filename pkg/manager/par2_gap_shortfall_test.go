package manager

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

// p2eGapArticleSize cuts the PAR2 volumes small enough that each has a
// middle article (vol00+6 and vol06+6: 6 articles, vol12+5: 5). With the
// default 100 KiB cut a volume has 2 articles, and a 430 on the first or
// last one breaks no packet: the parse resyncs past the head, and a packet
// cut at the tail is too short to be checked.
const p2eGapArticleSize = 20 * 1024

// newP2eGapEnv is newP2eEnv with the PAR2 files cut at p2eGapArticleSize.
func newP2eGapEnv(t *testing.T) (*p2eServer, *p2eFixture) {
	t.Helper()
	p2eStartClock(t)
	fx, err := buildP2eFixture(t.TempDir(), "")
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	for _, f := range fx.par2 {
		for _, id := range f.ids {
			delete(fx.all, id)
		}
		f.ids, f.payloads = nil, nil
		f.cutSized(fx.all, p2eGapArticleSize)
	}
	return p2eServeFixture(t, 1, fx)[0], fx
}

// A recovery shortfall whose only skipped packets are the ones a 430'd
// article of their volume broke is terminal: that recovery data is gone, as
// a wholly 430'd volume's is, and every later pass would fetch the same
// bytes. A skipped packet no gap explains (a wrong body served for a PAR2
// article) still makes it transient.
func TestP2EPar2RecoveryShortfallWithMissingVolumeArticle(t *testing.T) {
	srv, fx := newP2eGapEnv(t)
	f1 := fx.posted[0]
	vols := fx.par2[1:]
	dead := []p2eSeg{{f1, 1}, {f1, 3}}
	for _, v := range vols {
		if len(v.ids) < 5 {
			t.Fatalf("%s cut into %d articles, want a middle one", v.name, len(v.ids))
		}
	}
	// A 430 on the third article of every volume: each volume keeps its
	// paper count (only some of its articles are gone), so the gates let
	// the pass fetch them, and the parsed recovery falls short of the
	// damage.
	midGaps := func(h *p2eHarness) {
		for _, s := range h.dead {
			h.srv.set(s.file.ids[s.idx], p2eNotFound)
		}
		for _, v := range vols {
			h.srv.set(v.ids[2], p2eNotFound)
		}
	}
	cases := []struct {
		name  string
		setup func(h *p2eHarness)
		class string
		msg   string
	}{
		{name: "gaps_only", class: "terminal", setup: midGaps},
		{name: "gaps_and_a_wrong_body", class: "transient", msg: "failed their checksum", setup: func(h *p2eHarness) {
			midGaps(h)
			h.srv.setWrong(fx.file("vol00+").ids[0], fx.file("vol06+").ids[0])
		}},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newP2eHarness(t, srv, fx, fmt.Sprintf("p2e-gap-%02d", i))
			h.recordDead(dead...)
			c.setup(h)
			o := h.run(nil)
			h.report(o)
			p2eFailed(c.class).check(t, h, o)
			if !strings.Contains(o.Err, "recovery slices fetched/available") {
				t.Errorf("err %q, want the recovery shortfall", o.Err)
			}
			if c.class == "terminal" && strings.Contains(o.Err, "(transient") {
				t.Errorf("err %q is tagged transient, want the shortfall terminal", o.Err)
			}
			if c.msg != "" && !strings.Contains(o.Err, c.msg) {
				t.Errorf("err %q, want it to name %q", o.Err, c.msg)
			}
		})
	}
}

// par2UnexplainedSkips on a volume parsed with one article left out: the
// packet across the gap is explained only when the provenance shows the
// article was not served; a source the provenance does not add up to, or a
// skip away from any gap, is counted.
func TestPar2UnexplainedSkipsAt430Gap(t *testing.T) {
	idxData, err := os.ReadFile("testdata/p2e/P2E.Rel.par2")
	if err != nil {
		t.Fatal(err)
	}
	vol, err := os.ReadFile("testdata/p2e/P2E.Rel.vol00+6.par2")
	if err != nil {
		t.Fatal(err)
	}
	const art = p2eGapArticleSize
	ref := storage.Par2FileRef{Name: "v.par2", Size: int64(len(vol))}
	var parts [][]byte
	for off := 0; off < len(vol); off += art {
		id := fmt.Sprintf("v-%d@test", off/art)
		ref.Segments = append(ref.Segments, storage.Par2SegmentRef{MessageID: id, Bytes: int64(min(art, len(vol)-off))})
		parts = append(parts, vol[off:min(off+art, len(vol))])
	}
	const gone = 2
	var cut []byte
	for i, p := range parts {
		if i != gone {
			cut = append(cut, p...)
		}
	}
	files := map[string]storage.Par2FileRef{"v.par2": ref}
	sources := []par2.Source{{Name: "a.par2", Data: idxData}, {Name: "v.par2", Data: cut}}
	idx, err := par2.ParseIndex(context.Background(), sources)
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.SkippedPackets) == 0 {
		t.Fatalf("no packet skipped across the gap: the case does not apply")
	}

	served := func(skip ...int) *par2Provenance {
		pv := newPar2Provenance()
		for i, s := range ref.Segments {
			left := false
			for _, k := range skip {
				left = left || k == i
			}
			if !left {
				pv.record(s.MessageID, "h", len(parts[i]))
			}
		}
		return pv
	}
	if n := par2UnexplainedSkips(idx, sources, files, served(gone)); n != 0 {
		t.Errorf("article %d not served: %d unexplained, want 0 of %d", gone, n, len(idx.SkippedPackets))
	}
	// Every article recorded: the lengths sum past the source, so its gaps
	// are not known and every skip counts.
	if n := par2UnexplainedSkips(idx, sources, files, served()); n != len(idx.SkippedPackets) {
		t.Errorf("no gap recorded: %d unexplained, want %d", n, len(idx.SkippedPackets))
	}
	// A different article missing, with the same total length: the gap sits
	// elsewhere, and the skip is not explained by it.
	other := newPar2Provenance()
	for i, s := range ref.Segments {
		switch i {
		case gone - 1:
			continue
		case gone:
			other.record(s.MessageID, "h", len(parts[gone-1]))
		default:
			other.record(s.MessageID, "h", len(parts[i]))
		}
	}
	if n := par2UnexplainedSkips(idx, sources, files, other); n != len(idx.SkippedPackets) {
		t.Errorf("gap at another article: %d unexplained, want %d", n, len(idx.SkippedPackets))
	}
	if n := par2UnexplainedSkips(idx, sources, nil, served(gone)); n != len(idx.SkippedPackets) {
		t.Errorf("no ref for the source: %d unexplained, want %d", n, len(idx.SkippedPackets))
	}
}
