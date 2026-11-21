package manager

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// brokenHealthsFor is one broken entry whose only broken file is
// "dead.mkv" of release nzbID, owned by radarr movie 1.
func brokenHealthsFor(nzbID string) *xsync.Map[string, *storage.EntryHealth] {
	h := &storage.EntryHealth{
		EntryName: "Entry",
		Status:    storage.HealthBroken,
		FileCount: 2, // not fully broken: the entry itself is kept
		BrokenFiles: []storage.BrokenFile{
			{FileName: "dead.mkv", Reason: "usenet_segment_missing", ArrName: "radarr", MediaID: 1, ArrFileID: 101, InfoHash: nzbID},
		},
	}
	h.BrokenCount = len(h.BrokenFiles)
	healths := xsync.NewMap[string, *storage.EntryHealth]()
	healths.Store("Entry", h)
	return healths
}

// repairBroken (FixBroken and the stop-schedule pass) heals a release only
// while holding its re-grab claim. With a re-grab already holding it, the
// entry is left to that re-grab: no Arr call, and the claim is untouched.
// Without one, the heal runs and leaves no claim behind.
func TestRepairBrokenHoldsTheReleasesRegrabClaim(t *testing.T) {
	for _, automatic := range []bool{false, true} {
		name := "FixBroken"
		if automatic {
			name = "stop-schedule pass"
		}
		t.Run(name+": a held re-grab claim defers the heal", func(t *testing.T) {
			fake := &fakeRadarr{historyID: map[string]int{"1": 9}}
			srv := httptest.NewServer(fake)
			defer srv.Close()
			repair := newTestRepairForFix(t)
			repair.manager.arr.AddOrUpdate(arr.New("radarr", srv.URL, "token", false, nil, "", ""))
			if !repair.handlers.TryAcquire("nzb-dead", handlerRegrab) {
				t.Fatal("setup: claim refused")
			}

			repair.repairBroken(context.Background(), &storage.RepairRun{}, brokenHealthsFor("nzb-dead"), automatic)

			if n := fake.count("POST /api/v3/history/failed/"); n != 0 {
				t.Fatalf("blocklist calls %d under another re-grab's claim, want 0 (calls %v)", n, fake.calls)
			}
			if n := fake.count("DELETE "); n != 0 {
				t.Fatalf("delete calls %d under another re-grab's claim, want 0 (calls %v)", n, fake.calls)
			}
			kind, terminal, exists := repair.handlers.State("nzb-dead")
			if !exists || terminal || kind != handlerRegrab {
				t.Fatalf("claim after the pass: kind=%v terminal=%v exists=%v, want the other re-grab's claim kept", kind, terminal, exists)
			}
		})
		t.Run(name+": no other claim, healed and released", func(t *testing.T) {
			fake := &fakeRadarr{historyID: map[string]int{"1": 9}}
			srv := httptest.NewServer(fake)
			defer srv.Close()
			repair := newTestRepairForFix(t)
			repair.manager.arr.AddOrUpdate(arr.New("radarr", srv.URL, "token", false, nil, "", ""))

			repair.repairBroken(context.Background(), &storage.RepairRun{}, brokenHealthsFor("nzb-dead"), automatic)

			if n := fake.count("POST /api/v3/history/failed/9"); n != 1 {
				t.Fatalf("blocklist calls %d, want the heal to run once (calls %v)", n, fake.calls)
			}
			if _, _, exists := repair.handlers.State("nzb-dead"); exists {
				t.Fatal("re-grab claim left behind after the heal")
			}
		})
	}
}

// A PAR2 terminal state reclaims the stuck claim, as the sweep's own
// routeAutoRepair does, so FixBroken still heals a release PAR2 gave up on.
func TestRepairBrokenReclaimsAPar2TerminalClaim(t *testing.T) {
	fake := &fakeRadarr{historyID: map[string]int{"1": 9}}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	repair := newTestRepairForFix(t)
	repair.manager.arr.AddOrUpdate(arr.New("radarr", srv.URL, "token", false, nil, "", ""))
	repair.handlers.MarkTerminal("nzb-dead")
	if err := repair.manager.storage.SavePar2RepairState(&storage.Par2RepairState{NzbID: "nzb-dead", Terminal: true}); err != nil {
		t.Fatalf("SavePar2RepairState: %v", err)
	}

	repair.repairBroken(context.Background(), &storage.RepairRun{}, brokenHealthsFor("nzb-dead"), false)

	if n := fake.count("POST /api/v3/history/failed/9"); n != 1 {
		t.Fatalf("blocklist calls %d, want the heal to run once (calls %v)", n, fake.calls)
	}
}

// The sticky-failed re-grab takes the release's re-grab claim before
// healing. With a re-grab already holding it, nothing is re-grabbed (the
// playback-repair path is never entered: no cooldown is armed), the file is
// not listed broken for a second, unclaimed heal, and the claim is kept.
func TestRegrabStickyFailedHoldsTheReleasesRegrabClaim(t *testing.T) {
	entry := &storage.Entry{InfoHash: "nzb-sticky", Name: "Sticky.Entry", Protocol: config.ProtocolNZB, Files: map[string]*storage.File{
		"dead.mkv": {Name: "dead.mkv", InfoHash: "nzb-sticky", Size: 1},
	}}
	ctx := contextWithStickyRegrabAutoRepair(context.Background(), true)

	t.Run("held claim: no re-grab", func(t *testing.T) {
		repair := newTestRepairForFix(t)
		if !repair.handlers.TryAcquire("nzb-sticky", handlerRegrab) {
			t.Fatal("setup: claim refused")
		}
		res := repair.regrabStickyFailed(ctx, &candidate{name: entry.Name}, entry, "dead.mkv", fileResult{healthy: true})

		if _, armed := repair.lastPlaybackRepair[normalizeCooldownKey(entry.Name)]; armed {
			t.Fatal("playback re-grab entered under another re-grab's claim")
		}
		if res.broken {
			t.Fatal("file listed broken: the run would heal it a second time, unclaimed")
		}
		kind, terminal, exists := repair.handlers.State("nzb-sticky")
		if !exists || terminal || kind != handlerRegrab {
			t.Fatalf("claim after: kind=%v terminal=%v exists=%v, want the other re-grab's claim kept", kind, terminal, exists)
		}
	})
	t.Run("no other claim: re-grab attempted, claim released", func(t *testing.T) {
		repair := newTestRepairForFix(t)
		repair.regrabStickyFailed(ctx, &candidate{name: entry.Name}, entry, "dead.mkv", fileResult{healthy: true})

		if _, armed := repair.lastPlaybackRepair[normalizeCooldownKey(entry.Name)]; !armed {
			t.Fatal("playback re-grab not entered with no other claim")
		}
		if _, _, exists := repair.handlers.State("nzb-sticky"); exists {
			t.Fatal("re-grab claim left behind")
		}
	})
}
