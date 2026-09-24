package manager

import (
	"context"
	"errors"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// A patch cut from repaired slices at estimated article boundaries is shifted
// (the review's repro stored segment 1 as bytes [51,101) instead of
// [50,100)), and IFSC cannot catch it. runRepair must only patch dead
// segments whose file geometry was measured.
func TestSplitPlaceableDeadRefsRefusesUnmeasuredGeometry(t *testing.T) {
	ctx := context.Background()
	content := postedContent(384)
	// Estimates that sum to the true length but put boundaries 1..5 a byte late.
	articles, ref := uniformPosting(content, 50, []int64{51, 50, 50, 50, 50, 49, 50, 34})
	fileID := [16]byte{0x0b}

	refsFor := func(pf *postedFileFetcher) []par2DeadRef {
		var out []par2DeadRef
		for i := 0; i < 3; i++ {
			seg := ref.Segments[i]
			out = append(out, par2DeadRef{
				file: ref.Name,
				seg:  overlay.DeadSegment{Index: i, MessageID: seg.MessageID, Bytes: 50},
				rng:  postedRange{fileID: fileID, start: pf.base[i], end: pf.base[i] + pf.segSizes[i]},
			})
		}
		return out
	}

	t.Run("dead head articles: nothing placeable, terminal", func(t *testing.T) {
		live := map[string]yencArticle{}
		for id, a := range articles {
			live[id] = a
		}
		for _, id := range []string{"<p1>", "<p2>", "<p3>"} {
			delete(live, id)
		}
		yf := &yencFetcher{articles: live, calls: map[string]int{}}
		pf := newPostedFileFetcher(ctx, yf.fetch, ref, nil, int64(len(content)), zerolog.Nop())
		pf.resolveGeometry()
		if pf.exact {
			t.Fatal("precondition: geometry measured despite dead head articles")
		}
		placeable, err := splitPlaceableDeadRefs(map[[16]byte]*postedFileFetcher{fileID: pf}, refsFor(pf))
		if len(placeable) != 0 {
			t.Fatalf("%d dead segment(s) on estimated geometry would be patched", len(placeable))
		}
		if err == nil {
			t.Fatal("no error for unplaceable segments; the pass would report a full repair")
		}
		if !nntp.IsArticleNotFoundError(err) {
			t.Errorf("error %v lost the 430 cause", err)
		}
		if class := classifyPar2Failure(err); !class.terminal {
			t.Errorf("dead head articles should be terminal, got %+v", class)
		}
	})

	t.Run("head articles time out: transient", func(t *testing.T) {
		timeout := &nntp.Error{Type: nntp.ErrorTypeTimeout, Message: "i/o timeout"}
		fetch := func(context.Context, string, usenet.ArticleCheck) ([]byte, error) { return nil, timeout }
		pf := newPostedFileFetcher(ctx, fetch, ref, nil, int64(len(content)), zerolog.Nop())
		pf.resolveGeometry()
		placeable, err := splitPlaceableDeadRefs(map[[16]byte]*postedFileFetcher{fileID: pf}, refsFor(pf))
		if len(placeable) != 0 || err == nil {
			t.Fatalf("placeable=%d err=%v", len(placeable), err)
		}
		if !errors.Is(err, timeout) {
			t.Errorf("error %v lost the timeout cause", err)
		}
		if class := classifyPar2Failure(err); class.terminal {
			t.Errorf("a timed-out geometry probe classified terminal: %q", class.reason)
		}
	})

	t.Run("measured geometry: all placeable", func(t *testing.T) {
		yf := &yencFetcher{articles: articles, calls: map[string]int{}}
		pf := newPostedFileFetcher(ctx, yf.fetch, ref, nil, int64(len(content)), zerolog.Nop())
		pf.resolveGeometry()
		if !pf.exact {
			t.Fatal("precondition: geometry not measured with every article live")
		}
		refs := refsFor(pf)
		placeable, err := splitPlaceableDeadRefs(map[[16]byte]*postedFileFetcher{fileID: pf}, refs)
		if err != nil || len(placeable) != len(refs) {
			t.Fatalf("placeable=%d/%d err=%v", len(placeable), len(refs), err)
		}
		for _, dr := range placeable {
			if dr.rng.start != int64(dr.seg.Index)*50 {
				t.Errorf("segment %d placed at %d, want %d", dr.seg.Index, dr.rng.start, dr.seg.Index*50)
			}
		}
	})

	t.Run("one-article file: placeable without measuring", func(t *testing.T) {
		one := postedContent(40)
		arts, oneRef := uniformPosting(one, 50, []int64{39})
		delete(arts, "<p1>")
		yf := &yencFetcher{articles: arts, calls: map[string]int{}}
		pf := newPostedFileFetcher(ctx, yf.fetch, oneRef, nil, int64(len(one)), zerolog.Nop())
		pf.resolveGeometry()
		dr := par2DeadRef{file: oneRef.Name, seg: overlay.DeadSegment{Index: 0, MessageID: "<p1>"},
			rng: postedRange{fileID: fileID, start: pf.base[0], end: pf.base[0] + pf.segSizes[0]}}
		placeable, err := splitPlaceableDeadRefs(map[[16]byte]*postedFileFetcher{fileID: pf}, []par2DeadRef{dr})
		if err != nil || len(placeable) != 1 {
			t.Fatalf("placeable=%d err=%v", len(placeable), err)
		}
		if dr.rng.start != 0 || dr.rng.end != int64(len(one)) {
			t.Errorf("one-article range [%d,%d), want [0,%d)", dr.rng.start, dr.rng.end, len(one))
		}
	})

	t.Run("mixed files: measured ones still patched", func(t *testing.T) {
		yf := &yencFetcher{articles: articles, calls: map[string]int{}}
		good := newPostedFileFetcher(ctx, yf.fetch, ref, nil, int64(len(content)), zerolog.Nop())
		good.resolveGeometry()
		bad := newPostedFileFetcher(ctx, nil, ref, nil, int64(len(content)), zerolog.Nop())
		otherID := [16]byte{0x0c}
		refs := refsFor(good)
		refs = append(refs, par2DeadRef{file: "b.r01", seg: overlay.DeadSegment{Index: 0, MessageID: "<q1>"}, rng: postedRange{fileID: otherID}})
		placeable, err := splitPlaceableDeadRefs(map[[16]byte]*postedFileFetcher{fileID: good, otherID: bad}, refs)
		if len(placeable) != 3 || err == nil {
			t.Fatalf("placeable=%d err=%v, want 3 and an error for b.r01", len(placeable), err)
		}
	})
}
