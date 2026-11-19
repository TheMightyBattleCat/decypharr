package manager

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// pendingAfterPass re-reads the overlay once a pass has ended. When the
// manifest can't be read it must fall back to the damage read before the
// pass, not report "nothing pending" (which would skip the re-grab of a file
// that is still broken).
func TestPar2PendingAfterPassFallsBackOnReadError(t *testing.T) {
	servers, fx := newP2eEnv(t, 1)
	h := newP2eHarness(t, servers[0], fx, "p2e-pending-after")
	f1, f2 := fx.posted[0], fx.posted[1]
	h.recordDead(p2eSeg{f1, 1}, p2eSeg{f2, 2})

	prePass := map[string][]overlay.DeadSegment{
		f1.name:     {{Index: 1, MessageID: f1.ids[1]}},
		"other.mkv": {{Index: 4, MessageID: "<pre-pass>"}},
	}

	// Readable manifest: the re-read wins over the pre-pass map.
	got := h.p.pendingAfterPass(h.nzbID, p2eRelease, prePass)
	if len(got) != 2 || len(got[f1.name]) != 1 || len(got[f2.name]) != 1 || got["other.mkv"] != nil {
		t.Fatalf("readable manifest: pendingAfterPass = %+v, want the overlay's own %s#1 and %s#2", got, f1.name, f2.name)
	}

	// Unreadable manifest: the pre-pass map comes back unchanged.
	manifest := filepath.Join(config.GetMainPath(), "usenet", "overlay", h.nzbID, "manifest.json")
	if _, err := os.Stat(manifest); err != nil {
		t.Fatalf("manifest not where expected: %v", err)
	}
	if err := os.WriteFile(manifest, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := h.u.OverlayPendingRepair(h.nzbID); err == nil {
		t.Fatalf("OverlayPendingRepair read a corrupt manifest without error; the test no longer reaches the fallback")
	}
	got = h.p.pendingAfterPass(h.nzbID, p2eRelease, prePass)
	if len(got) != len(prePass) || len(got[f1.name]) != 1 || len(got["other.mkv"]) != 1 || got["other.mkv"][0].MessageID != "<pre-pass>" {
		t.Fatalf("corrupt manifest: pendingAfterPass = %+v, want the pre-pass map %+v", got, prePass)
	}
}
