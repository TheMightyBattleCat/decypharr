package manager

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// A file PAR2 repaired completely kept its Failed verdict (WritePatch never
// un-fails), and with nothing pending par2Usable is false, so the playback
// policy chose a re-grab of a file that plays fine. handleAutoDamage must
// clear the verdict and take no action instead.
func TestHandleAutoDamageDoesNotRegrabFullyPatchedFailedFile(t *testing.T) {
	_, repair := newTestPar2Repair(t)
	m := repair.manager

	cfg := config.Get()
	prevPar2 := cfg.Repair.Par2Repair
	off := false
	cfg.Repair.Par2Repair = &off // the quadrant par2Usable=false lands in
	t.Cleanup(func() { cfg.Repair.Par2Repair = prevPar2 })

	store, err := overlay.NewStore(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("overlay.NewStore: %v", err)
	}
	m.usenet = usenet.NewWithOverlayForTest(store)

	const nzbID, name, file = "nzb-repaired", "My.Show.S01E01", "My.Show.S01E01.mkv"
	if err := m.storage.AddOrUpdate(&storage.Entry{
		InfoHash: nzbID,
		Name:     name,
		Protocol: config.ProtocolNZB,
		Files: map[string]*storage.File{
			file: {Name: file, InfoHash: nzbID, Size: 1 << 20, AddedOn: time.Now()},
		},
	}); err != nil {
		t.Fatalf("AddOrUpdate: %v", err)
	}

	store.SetPolicy(overlay.Policy{MaxRunSegments: 1, MaxTotalSegments: 1, MaxByteRatio: 1.0})
	_, _ = store.Decide(nzbID, file, 0, "<a@test>", 1024, 1<<20, 0)
	if _, v := store.Decide(nzbID, file, 2, "<b@test>", 1024, 1<<20, 0); v != overlay.VerdictFailed {
		t.Fatalf("setup: verdict = %v, want failed", v)
	}
	patch := bytes.Repeat([]byte{1}, 1024)
	for _, seg := range []int{0, 2} {
		if err := store.WritePatch(nzbID, file, seg, patch); err != nil {
			t.Fatalf("WritePatch %d: %v", seg, err)
		}
	}
	if decideAutoRepairAction(RepairSourcePlayback, false, store.Verdict(nzbID, file)) != autoActionRegrab {
		t.Fatalf("setup: the policy no longer picks a re-grab for this state; the test proves nothing")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := repair.handleAutoDamage(ctx, RepairSourcePlayback, name, file, 0)
	if err != nil {
		t.Fatalf("handleAutoDamage: %v", err)
	}
	if out.regrab || out.acted {
		t.Fatalf("handleAutoDamage = %+v, want no action for a fully patched file", out)
	}
	if v := store.Verdict(nzbID, file); v != overlay.VerdictClean {
		t.Fatalf("verdict after handleAutoDamage = %v, want clean", v)
	}
	if got := store.PatchedSegments(nzbID, file); len(got) != 2 {
		t.Fatalf("patched segments = %v, want both kept", got)
	}
}
