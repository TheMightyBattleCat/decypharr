package manager

// What a client is served after a PAR2 pass: the reader, on a 430 for a
// patched article, must return the patch (the posted bytes), with playback
// padding on or off.

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// readBack reads the whole of f through the real reader and checks it equals
// the posted bytes. Every recorded article still answers 430, so the patched
// ranges can only come from the overlay.
func (h *p2eHarness) readBack(f *p2eFile) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	got := make([]byte, len(f.data))
	n, err := h.u.ReadCachedAt(ctx, h.nzbID, f.name, got, 0)
	if err != nil || n != len(f.data) {
		h.t.Fatalf("read %s: %d of %d bytes, err %v", short(f.name), n, len(f.data), err)
	}
	if !bytes.Equal(got, f.data) {
		at := firstDiff(got, f.data)
		h.t.Fatalf("%s served bytes differ from the posted file at offset %d (article %d)", short(f.name), at, at/p2eArticleSize)
	}
}

// Padding on: playback recorded the damage, the pass patched it, and a read
// of the whole file returns the posted bytes.
func TestP2EPar2PatchedRangeReadBack(t *testing.T) {
	servers, fx := newP2eEnv(t, 1)
	h := newP2eHarness(t, servers[0], fx, "p2e-readback")
	f1, f2 := fx.posted[0], fx.posted[1]
	h.recordDead(p2eSeg{f1, 1}, p2eSeg{f2, 3})
	for _, s := range h.dead {
		h.srv.set(s.file.ids[s.idx], p2eNotFound)
	}
	o := h.run(nil)
	h.report(o)
	p2eCompleted(2).check(t, h, o)
	h.readBack(f1)
	h.readBack(f2)
}

// Padding off: playback records nothing, so the damage comes from the
// sweep's probe (recordDeadSegments). The pass patches it, and the reader
// still serves the patch although it zero-fills nothing.
func TestP2EPar2PaddingOffSweepThenPass(t *testing.T) {
	servers, fx := newP2eEnv(t, 1)
	cfg := config.Get()
	oldPadding := cfg.Repair.PlaybackPadding
	off := false
	cfg.Repair.PlaybackPadding = &off
	t.Cleanup(func() { cfg.Repair.PlaybackPadding = oldPadding })
	cfg.Usenet.AvailabilitySamplePercent = 100 // the probe asks about every article (cfg.Usenet is restored by newP2eEnv)

	h := newP2eHarness(t, servers[0], fx, "p2e-padding-off")
	f1 := fx.posted[0]
	h.srv.set(f1.ids[1], p2eNotFound)
	h.srv.set(f1.ids[3], p2eNotFound)
	h.dead = []p2eSeg{{f1, 1}, {f1, 3}}

	entry, err := h.p.manager.storage.Get(h.nzbID)
	if err != nil || entry == nil {
		t.Fatalf("entry: %v", err)
	}
	h.warmup() // the probe's first connection, as h.run does before a pass
	verdict, _ := h.repair.recordDeadSegments(context.Background(), entry, f1.name)
	if verdict == overlay.VerdictClean {
		t.Fatalf("sweep probe recorded no damage (verdict %q)", verdict)
	}
	pend, err := h.u.OverlayPendingRepair(h.nzbID)
	if err != nil || len(pend[f1.name]) == 0 {
		t.Fatalf("pending after the sweep probe = %v (err %v), want part1's dead articles", pend, err)
	}

	o := h.run(nil)
	h.report(o)
	p2eCompleted(len(pend[f1.name])).check(t, h, o)
	h.readBack(f1)
}
