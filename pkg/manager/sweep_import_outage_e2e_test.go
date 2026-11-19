package manager

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

// The sweep's probe-and-heal and the import availability gate, run against
// the p2e fake news server in each way it can fail. Only articles the server
// answers 430 for may lead to a recorded dead segment, a delete in the Arr or
// a re-grab. A server that refuses connections, never answers, or answers
// every command with a quota error is an outage: the file is not judged
// broken, nothing is recorded in the overlay, and the Arr is not called.

// outageMode is one way the single provider behaves for the probed file.
type outageMode struct {
	name    string
	set     func(srv *p2eServer, f *p2eFile)
	real430 bool // the server says the articles are gone
	slow    bool // the client gives up after about 60 s
}

func outageModes() []outageMode {
	every := func(b p2eBehavior) func(*p2eServer, *p2eFile) {
		return func(srv *p2eServer, f *p2eFile) {
			for _, id := range f.ids {
				srv.set(id, b)
			}
		}
	}
	return []outageMode{
		{name: "healthy", set: func(*p2eServer, *p2eFile) {}},
		{name: "all_430", set: every(p2eNotFound), real430: true},
		{name: "refuse_connections", set: func(srv *p2eServer, _ *p2eFile) {
			srv.refuse.Store(true)
			srv.dropConns()
		}},
		{name: "stall", set: every(p2eStallAll), slow: true},
		{name: "quota", set: func(srv *p2eServer, _ *p2eFile) { srv.quotaAll.Store(true) }},
	}
}

// countingArr starts a fake Arr that answers 404 to everything and counts
// the calls it gets.
func countingArr(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.NotFound(w, nil)
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

// pendingFor returns the dead segments the overlay holds for file.
func pendingFor(t *testing.T, h *p2eHarness, file string) int {
	t.Helper()
	pend, err := h.u.OverlayPendingRepair(h.nzbID)
	if err != nil {
		t.Fatalf("OverlayPendingRepair: %v", err)
	}
	return len(pend[file])
}

func TestSweepProbeAndHealUnderFailingNewsServer(t *testing.T) {
	for _, mode := range outageModes() {
		t.Run(mode.name, func(t *testing.T) {
			if mode.slow && testing.Short() {
				t.Skip("waits for the NNTP read timeout (~60 s)")
			}
			servers, fx := newP2eEnv(t, 1)
			h := newP2eHarness(t, servers[0], fx, "sweep-outage-"+mode.name)
			// The heal deletes the entry after the Arr step: it needs both.
			h.repair.regrabGuard = newRegrabGuard()
			h.p.manager.entry = NewEntryCache(h.p.manager)
			f := fx.posted[0]

			arrSrv, arrCalls := countingArr(t)
			h.p.manager.arr.AddOrUpdate(arr.New("radarr", arrSrv.URL, "token", false, nil, "", ""))

			// The entry as the Arr knows it, so a broken verdict reaches the
			// Arr delete + blocklist + re-search.
			c := &candidate{
				name: p2eRelease,
				item: &storage.EntryItem{Name: p2eRelease, Files: map[string]*storage.File{
					f.name: {Name: f.name, InfoHash: h.nzbID, Size: int64(len(f.data))},
				}},
				arrName: "radarr",
				arrKind: storage.ArrKindRadarr,
				contentMap: map[string]arr.ContentFile{
					f.name: {Id: 7, FileId: 70, Name: f.name, Path: "/movies/" + f.name, Size: int64(len(f.data))},
				},
			}

			// An earlier sweep found it healthy: an outage must not move
			// LastOKAt.
			lastOK := time.Now().Add(-48 * time.Hour).Truncate(time.Second)
			if err := h.p.manager.storage.SaveEntryHealth(&storage.EntryHealth{
				EntryName: p2eRelease, Status: storage.HealthHealthy, FileCount: 1,
				LastCheckedAt: lastOK, LastOKAt: lastOK, NextCheckDueAt: lastOK.Add(24 * time.Hour),
				DecodeVerifiedAt: lastOK, DecodeVerifiedFingerprint: "fp",
			}); err != nil {
				t.Fatalf("SaveEntryHealth: %v", err)
			}

			mode.set(servers[0], f)

			run := &storage.RepairRun{ID: "outage-run"}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			ctx = h.repair.withSweepBrake(ctx, run)
			started := time.Now()
			if err := h.repair.probeAndHealCandidates(ctx, run, map[string]*candidate{p2eRelease: c}, []string{p2eRelease},
				newHealCache(), RepairRunOptions{}, true, &supersessionContext{}); err != nil {
				t.Fatalf("probeAndHealCandidates: %v", err)
			}
			took := time.Since(started)

			pending := pendingFor(t, h, f.name)
			_, entryErr := h.p.manager.storage.Get(h.nzbID)
			t.Logf("%s: stats=%+v pending=%d arr_calls=%d healed=%v entry_err=%v took=%s server=%+v",
				mode.name, run.Stats, pending, arrCalls.Load(), run.WasHealed(p2eRelease), entryErr, took, servers[0].counts())

			if run.Stats.Probed != 1 {
				t.Errorf("probed %d entries, want 1", run.Stats.Probed)
			}
			if mode.real430 {
				// Broken, healed through the Arr, then the entry is deleted
				// (its health record and overlay go with it).
				if run.Stats.Broken != 1 {
					t.Errorf("%d broken, want 1", run.Stats.Broken)
				}
				if !run.WasHealed(p2eRelease) || arrCalls.Load() == 0 {
					t.Errorf("healed=%v arr calls=%d, want the broken entry healed through the Arr", run.WasHealed(p2eRelease), arrCalls.Load())
				}
				if entryErr == nil {
					t.Errorf("entry still stored after the heal, want it deleted")
				}
				return
			}
			health, _ := h.p.manager.storage.GetEntryHealth(p2eRelease)
			if health == nil {
				t.Fatalf("no health record written")
			}
			if health.Status == storage.HealthBroken || health.BrokenCount != 0 {
				t.Errorf("status %s with %d broken file(s) (%q), want not broken: the server never said an article is gone",
					health.Status, health.BrokenCount, health.FailureReason)
			}
			if run.Stats.Broken != 0 {
				t.Errorf("%d broken, want 0", run.Stats.Broken)
			}
			if pending != 0 {
				t.Errorf("%d dead segment(s) recorded in the overlay, want none", pending)
			}
			if run.WasHealed(p2eRelease) || arrCalls.Load() != 0 {
				t.Errorf("healed=%v arr calls=%d, want the Arr left alone", run.WasHealed(p2eRelease), arrCalls.Load())
			}
			if entryErr != nil {
				t.Errorf("entry gone after the sweep (%v), want it kept", entryErr)
			}
			if mode.name == "healthy" {
				if health.Status != storage.HealthHealthy || !health.LastOKAt.After(lastOK) || health.Dirty {
					t.Errorf("status=%s last_ok=%s dirty=%v, want healthy, LastOKAt moved on, not dirty",
						health.Status, health.LastOKAt, health.Dirty)
				}
				return
			}
			// No news server answered: nothing was verified.
			if health.Status != storage.HealthUnknown || run.Stats.Unknown != 1 {
				t.Errorf("status %s (run unknown=%d), want unknown: no server answered the check", health.Status, run.Stats.Unknown)
			}
			if !health.LastOKAt.Equal(lastOK) {
				t.Errorf("LastOKAt %s, want it left at %s", health.LastOKAt, lastOK)
			}
			if !health.DecodeVerifiedAt.Equal(lastOK) {
				t.Errorf("decode stamp %s, want it kept: an outage verifies nothing either way", health.DecodeVerifiedAt)
			}
			if !health.Dirty || !health.IsDue(time.Now(), 24*time.Hour) {
				t.Errorf("dirty=%v (%q) due=%v, want the entry dirty and due for the next sweep",
					health.Dirty, health.DirtyReason, health.IsDue(time.Now(), 24*time.Hour))
			}
		})
	}
}

func TestImportAvailabilityGateUnderFailingNewsServer(t *testing.T) {
	for _, mode := range outageModes() {
		t.Run(mode.name, func(t *testing.T) {
			if mode.slow && testing.Short() {
				t.Skip("waits for the NNTP read timeout (~60 s)")
			}
			servers, fx := newP2eEnv(t, 1)
			h := newP2eHarness(t, servers[0], fx, "import-outage-"+mode.name)
			f := fx.posted[0]

			// An Arr for the entry's category: the gate's re-grab starts by
			// asking it for the grab's history.
			arrSrv, arrCalls := countingArr(t)
			h.p.manager.arr.AddOrUpdate(arr.New("p2e", arrSrv.URL, "token", false, nil, "", ""))

			entry := &storage.Entry{
				InfoHash: h.nzbID, Name: p2eRelease, Protocol: config.ProtocolNZB, Category: "p2e",
				Files: map[string]*storage.File{
					f.name: {Name: f.name, InfoHash: h.nzbID, Size: int64(len(f.data))},
				},
			}
			d := &Downloader{manager: h.p.manager, logger: zerolog.Nop()}

			mode.set(servers[0], f)

			started := time.Now()
			gateErr := d.importAvailabilityGate(entry)
			took := time.Since(started)
			pending := pendingFor(t, h, f.name)
			t.Logf("%s: gate=%v pending=%d arr_calls=%d took=%s server=%+v",
				mode.name, gateErr, pending, arrCalls.Load(), took, servers[0].counts())

			// The post-parse check fails open on an outage too, while the
			// sweep's CheckFile reports it as unchecked. Skipped for the
			// stall: another 60 s for the same STAT path.
			if !mode.slow {
				segs := make([]storage.NZBSegment, len(f.ids))
				for i, id := range f.ids {
					segs[i] = storage.NZBSegment{Number: i + 1, MessageID: id}
				}
				postErr := h.u.CheckFileAvailability(context.Background(), &storage.NZBFile{Name: f.name, Segments: segs}, 100)
				sweepErr := h.u.CheckFile(context.Background(), h.nzbID, f.name)
				switch {
				case mode.real430:
					if !errors.Is(postErr, customerror.UsenetSegmentMissingError) {
						t.Errorf("post-parse check = %v, want segment missing", postErr)
					}
				case mode.name == "healthy":
					if postErr != nil || sweepErr != nil {
						t.Errorf("post-parse check = %v, CheckFile = %v, want both nil", postErr, sweepErr)
					}
				default:
					if postErr != nil {
						t.Errorf("post-parse check = %v, want nil: it fails open when no server answers", postErr)
					}
					if !errors.Is(sweepErr, usenet.ErrAvailabilityUnchecked) {
						t.Errorf("CheckFile = %v, want usenet.ErrAvailabilityUnchecked", sweepErr)
					}
				}
			}

			if mode.real430 {
				if gateErr == nil {
					t.Errorf("gate passed the import, want it rejected")
				}
				if pending == 0 {
					t.Errorf("no dead segment recorded in the overlay for %s", f.name)
				}
				if arrCalls.Load() == 0 {
					t.Errorf("the Arr was not asked: no re-grab started")
				}
				return
			}
			if gateErr != nil {
				t.Errorf("gate rejected the import (%v), want it passed: the server never said an article is gone", gateErr)
			}
			if pending != 0 {
				t.Errorf("%d dead segment(s) recorded in the overlay, want none", pending)
			}
			if arrCalls.Load() != 0 {
				t.Errorf("the Arr got %d call(s), want none: no re-grab", arrCalls.Load())
			}
		})
	}
}

// An entry with one file no news server answered for and one that passed its
// decode check stays healthy, but is not stamped decode-verified or OK now:
// the next sweep must read the unchecked file, not skip its decode windows on
// a fresh stamp.
func TestProbeEntryPartlyUncheckedNotDecodeStamped(t *testing.T) {
	repair := newTestRepairForClaims(t)
	results := map[string]fileResult{
		"a.mkv": {name: "a.mkv", infoHash: "nzb-a", protocol: config.ProtocolNZB, healthy: true, decodeConclusive: true, decodeCoverage: decodeCoverageFull},
		"b.mkv": {name: "b.mkv", infoHash: "nzb-a", protocol: config.ProtocolNZB, reason: reasonUsenetUnchecked},
	}
	repair.probeFileFn = func(_ context.Context, _ *candidate, name string, _ RepairRunOptions, _ bool) fileResult {
		return results[name]
	}
	const name = "Partly Unchecked"
	lastOK := time.Now().Add(-48 * time.Hour).Truncate(time.Second)
	if err := repair.manager.storage.SaveEntryHealth(&storage.EntryHealth{
		EntryName: name, Status: storage.HealthHealthy, FileCount: 2, LastCheckedAt: lastOK, LastOKAt: lastOK,
	}); err != nil {
		t.Fatalf("SaveEntryHealth: %v", err)
	}
	c := &candidate{name: name, item: &storage.EntryItem{Name: name, Files: map[string]*storage.File{
		"a.mkv": {Name: "a.mkv", InfoHash: "nzb-a"},
		"b.mkv": {Name: "b.mkv", InfoHash: "nzb-a"},
	}}}

	// ForceDecodeVerification: decode windows run whatever the config says.
	h, _ := repair.probeEntry(context.Background(), "run", c, newHealCache(), RepairRunOptions{ForceDecodeVerification: true}, false, &supersessionContext{})
	if h == nil {
		t.Fatal("no health record")
	}
	if h.Status != storage.HealthHealthy {
		t.Errorf("status %s, want healthy: the other file passed", h.Status)
	}
	if !h.DecodeVerifiedAt.IsZero() || h.DecodeVerifiedFingerprint != "" {
		t.Errorf("decode stamped at %s (%q), want none: b.mkv was never read", h.DecodeVerifiedAt, h.DecodeVerifiedFingerprint)
	}
	if !h.LastOKAt.Equal(lastOK) {
		t.Errorf("LastOKAt %s, want it left at %s", h.LastOKAt, lastOK)
	}
	if !h.Dirty || !h.IsDue(time.Now(), 24*time.Hour) {
		t.Errorf("dirty=%v due=%v, want the entry dirty and due for the next sweep", h.Dirty, h.IsDue(time.Now(), 24*time.Hour))
	}
}
