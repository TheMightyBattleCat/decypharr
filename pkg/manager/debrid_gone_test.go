package manager

import (
	"context"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"

	"github.com/sirrobot01/decypharr/internal/config"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// fakeDebridClient stands in for a configured debrid; none of its methods run.
type fakeDebridClient struct{ debrid.Client }

func onlyRealDebrid(name string) bool { return name == "realdebrid" }

func torrentEntry(hash, name, active string, providers ...string) *storage.Entry {
	e := &storage.Entry{
		Protocol:       config.ProtocolTorrent,
		InfoHash:       hash,
		Name:           name,
		ActiveProvider: active,
		Providers:      map[string]*storage.ProviderEntry{},
		Files: map[string]*storage.File{
			name + ".mkv": {Name: name + ".mkv", Size: 1000, InfoHash: hash},
		},
	}
	for _, p := range providers {
		e.Providers[p] = &storage.ProviderEntry{Provider: p, ID: "id-" + p}
	}
	return e
}

func TestDebridGoneReason(t *testing.T) {
	bad := torrentEntry("h4", "Bad", "realdebrid", "realdebrid")
	bad.Bad = true
	nzb := torrentEntry("h6", "Usenet", "")
	nzb.Protocol = config.ProtocolNZB
	nzb.Bad = true
	cases := []struct {
		name  string
		entry *storage.Entry
		want  string
	}{
		{"on a configured debrid", torrentEntry("h1", "RD", "realdebrid", "realdebrid"), ""},
		{"TorBox, not configured", torrentEntry("h2", "TB", "torbox", "torbox"), reasonDebridNotConfigured},
		{"TorBox active, RealDebrid placement: re-insert can move it", torrentEntry("h3", "Both", "torbox", "torbox", "realdebrid"), ""},
		{"re-insertion gave up", bad, reasonDebridUnavailable},
		{"no placement at all", torrentEntry("h5", "None", ""), reasonDebridNotConfigured},
		{"Usenet entries are never debrid-gone", nzb, ""},
		{"nil", nil, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := debridGoneReason(c.entry, onlyRealDebrid); got != c.want {
				t.Fatalf("got %q, want %q", got, c.want)
			}
		})
	}
}

// The debrid-gone reasons blocklist (the release is unusable) and skip
// re-insertion; the import-fault reasons keep the release.
func TestDebridGoneReasonsBlocklistAndSkipReinsert(t *testing.T) {
	for _, r := range []string{reasonDebridNotConfigured, reasonDebridUnavailable} {
		if keepReleaseReason(r) {
			t.Errorf("%s must blocklist the release", r)
		}
		if !noReinsertReason(r) {
			t.Errorf("%s must skip re-insertion", r)
		}
	}
	for _, r := range []string{reasonSplicedVolumes, reasonMissingVolume, "hoster_unavailable", "missing_provider_link", ""} {
		if noReinsertReason(r) {
			t.Errorf("%q must still try re-insertion", r)
		}
	}
}

func newTestRepairWithRealDebrid(t *testing.T) *Repair {
	t.Helper()
	repair := newTestRepairForFix(t)
	repair.manager.clients = xsync.NewMap[string, debrid.Client]()
	repair.manager.clients.Store("realdebrid", fakeDebridClient{})
	return repair
}

func TestProbeTorrentFileDebridGoneWithoutAskingADebrid(t *testing.T) {
	repair := newTestRepairWithRealDebrid(t)
	e := torrentEntry("h2", "TB", "torbox", "torbox")
	res := repair.probeTorrentFile(context.Background(), e, e.Files["TB.mkv"], "TB.mkv", fileResult{name: "TB.mkv"}, RepairRunOptions{})
	if !res.broken || res.healthy || res.reason != reasonDebridNotConfigured {
		t.Fatalf("got %+v, want broken with %s", res, reasonDebridNotConfigured)
	}
}

func TestFindAndFixDebridGone(t *testing.T) {
	repair := newTestRepairWithRealDebrid(t)
	bad := torrentEntry("h4", "Bad", "realdebrid", "realdebrid")
	bad.Bad = true
	for _, e := range []*storage.Entry{
		torrentEntry("h1", "RD", "realdebrid", "realdebrid"),
		torrentEntry("h2", "TB1", "torbox", "torbox"),
		torrentEntry("h3", "TB2", "torbox", "torbox"),
		bad,
	} {
		if err := repair.manager.storage.AddOrUpdate(e); err != nil {
			t.Fatalf("AddOrUpdate: %v", err)
		}
	}

	found, err := repair.FindDebridGone()
	if err != nil {
		t.Fatal(err)
	}
	if found.Total != 3 || found.ByReason[reasonDebridNotConfigured] != 2 || found.ByReason[reasonDebridUnavailable] != 1 {
		t.Fatalf("found %+v, want 2 not configured + 1 unavailable", found)
	}
	if found.Entries[0].Name != "Bad" || found.Entries[1].Name != "TB1" {
		t.Fatalf("entries not sorted by name: %+v", found.Entries)
	}

	fixed, err := repair.FixDebridGone(context.Background(), 2)
	if err != nil {
		t.Fatalf("FixDebridGone: %v", err)
	}
	repair.runWG.Wait()
	if len(fixed.Entries) != 2 || fixed.Run == nil {
		t.Fatalf("fixed %+v, want 2 entries and a run", fixed)
	}
	for name, reason := range map[string]string{"Bad": reasonDebridUnavailable, "TB1": reasonDebridNotConfigured} {
		h, err := repair.manager.storage.GetEntryHealth(name)
		if err != nil || h == nil {
			t.Fatalf("%s: no health (%v)", name, err)
		}
		if h.Status != storage.HealthBroken || h.FailureReason != reason || len(h.BrokenFiles) != 1 ||
			h.BrokenFiles[0].InfoHash == "" || h.BrokenFiles[0].Reason != reason || h.FileCount != 1 {
			t.Fatalf("%s: health %+v", name, h)
		}
	}
	if h, _ := repair.manager.storage.GetEntryHealth("TB2"); h != nil && h.Status == storage.HealthBroken {
		t.Fatal("TB2 was past the limit but got marked broken")
	}
	if h, _ := repair.manager.storage.GetEntryHealth("RD"); h != nil && h.Status == storage.HealthBroken {
		t.Fatal("a servable RealDebrid entry got marked broken")
	}

	// The next batch moves on: the two marked entries wait for Fix broken.
	again, err := repair.FindDebridGone()
	if err != nil {
		t.Fatal(err)
	}
	if again.Total != 3 || again.AlreadyBroken != 2 || len(again.Entries) != 1 || again.Entries[0].Name != "TB2" {
		t.Fatalf("second listing %+v, want TB2 next and 2 already broken", again)
	}
}

// An entry whose file another copy already serves (the Arr re-grabbed it) is
// not re-grabbed again.
func TestFindDebridGoneSkipsSupersededEntries(t *testing.T) {
	repair := newTestRepairWithRealDebrid(t)
	old := torrentEntry("h2", "Show.S01E01", "torbox", "torbox")
	replacement := torrentEntry("h9", "Show.S01E01", "realdebrid", "realdebrid")
	replacement.AddedOn = time.Now()
	replacement.Files["Show.S01E01.mkv"].AddedOn = replacement.AddedOn
	for _, e := range []*storage.Entry{old, replacement} {
		if err := repair.manager.storage.AddOrUpdate(e); err != nil {
			t.Fatalf("AddOrUpdate: %v", err)
		}
	}
	found, err := repair.FindDebridGone()
	if err != nil {
		t.Fatal(err)
	}
	if found.Total != 1 || found.Superseded != 1 || len(found.Entries) != 0 {
		t.Fatalf("found %+v, want the old TorBox copy counted as superseded", found)
	}
}
