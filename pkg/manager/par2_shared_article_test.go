package manager

import (
	"bytes"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// P-17: an article served through two reader slots (here a second file record
// carrying part1's segments) is recorded dead through one of them. The pass
// rebuilds its slices once and patches both slots with the posted bytes;
// before, the other slot was left for playback to find dead and for a second
// full pass to rebuild the same slices.
func TestPar2PassPatchesEverySlotOfARebuiltArticle(t *testing.T) {
	servers, fx := newP2eEnv(t, 1)
	h := newP2eHarness(t, servers[0], fx, "p2e-shared-article")
	f1 := fx.posted[0]

	const twin = "P2E.Twin.mkv"
	nzb := fx.nzb(h.nzbID)
	copyRec := nzb.Files[0]
	copyRec.Name = twin
	copyRec.Segments = append(copyRec.Segments[:0:0], nzb.Files[0].Segments...)
	nzb.Files = append(nzb.Files, copyRec)
	if err := h.u.AddNZBForTest(nzb); err != nil {
		t.Fatalf("AddNZBForTest: %v", err)
	}

	h.recordDead(p2eSeg{f1, 1})
	servers[0].set(f1.ids[1], p2eNotFound)

	o := h.run(nil)
	h.report(o)

	if o.LogMsg != "par2 repair completed" || o.Err != "" {
		t.Fatalf("pass ended %q err=%q, want completed", o.LogMsg, o.Err)
	}
	man, err := h.u.OverlayManifest(h.nzbID)
	if err != nil || man == nil {
		t.Fatalf("overlay manifest: %v", err)
	}
	for _, name := range []string{f1.name, twin} {
		fe := man.Files[name]
		if fe == nil {
			t.Fatalf("no overlay record for %s: its slot of the rebuilt article was not patched", name)
		}
		var status overlay.SegmentStatus
		for _, d := range fe.DeadSegments {
			if d.Index == 1 {
				status = d.Status
			}
		}
		if status != overlay.StatusPatched {
			t.Fatalf("%s#1 status = %q, want patched (records %+v)", name, status, fe.DeadSegments)
		}
		data, ok := h.u.OverlayPatchBytes(h.nzbID, name, 1)
		if !ok || !bytes.Equal(data, f1.payloads[1]) {
			t.Fatalf("%s#1 patch ok=%v len=%d, want the posted %d bytes", name, ok, len(data), len(f1.payloads[1]))
		}
	}
}
