package manager

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// A pass works out every article's place in the file from the NZB record it
// loads at the start, and writes its patches by place. When the record is
// permuted while the pass runs (a volume reorder swapping two equal-size
// articles), the patch for the dead article must not land at its old place,
// which now holds another, healthy article. Before the fix the pass wrote it
// there and marked the file clean; now it writes nothing and ends transient,
// naming the segment that moved. (The overlay record still names the old
// place, so a later pass refuses that segment as before.)
func TestPar2PassWritesNoPatchWhenTheRecordMoved(t *testing.T) {
	servers, fx := newP2eEnv(t, 1)
	srv := servers[0]
	h := newP2eHarness(t, srv, fx, "p2e-record-moved")
	f1 := fx.posted[0]
	if len(f1.payloads[1]) != len(f1.payloads[2]) {
		t.Fatalf("fixture: articles 1 and 2 differ in length (%d, %d); the swap must keep the length check passing", len(f1.payloads[1]), len(f1.payloads[2]))
	}
	h.recordDead(p2eSeg{f1, 1})
	srv.set(f1.ids[1], p2eNotFound)
	srv.slowNS.Store(int64(100 * time.Millisecond))

	swapped := false
	o := h.run(func(done <-chan struct{}) {
		if !h.waitPhase(done, Par2PhaseStreamingIntact) {
			return
		}
		// The pass has its copy of the record. Swap f1's articles 1 and 2
		// in the stored one, offsets and lengths kept.
		nzb := fx.nzb(h.nzbID)
		for i := range nzb.Files {
			if nzb.Files[i].Name != f1.name {
				continue
			}
			s := nzb.Files[i].Segments
			s[1].MessageID, s[2].MessageID = s[2].MessageID, s[1].MessageID
			s[1].Number, s[2].Number = s[2].Number, s[1].Number
		}
		if err := h.u.AddNZBForTest(nzb); err != nil {
			t.Errorf("store the permuted record: %v", err)
			return
		}
		swapped = true
	})
	t.Logf("outcome: %s class=%s err=%q tally=%s overlay=%s %v", o.LogMsg, o.Class, o.Err, o.Tally, o.Overlay, o.OverlayDiff)
	if !swapped {
		t.Fatalf("the pass ended before the record could be permuted (log %q, err %q)", o.LogMsg, o.Err)
	}

	if o.Patched != 0 {
		t.Fatalf("%d patch(es) written against the old layout: %v", o.Patched, o.OverlayDiff)
	}
	want := fmt.Sprintf("segment 1 is %s in the NZB record, not %s", f1.ids[2], f1.ids[1])
	if !strings.Contains(o.Err, want) {
		t.Fatalf("error %q does not name the moved segment (want %q)", o.Err, want)
	}
	if o.Class != "transient" {
		t.Fatalf("class %q, want transient: the record moved under this pass", o.Class)
	}
}
