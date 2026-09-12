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
	repair.manager.config = config.Get()
	repair.manager.entry = NewEntryCache(repair.manager)
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

// refsTo builds a reference set pointing at every file of entries, served by
// that entry's own InfoHash.
func refsTo(entries ...*storage.Entry) map[string]map[string]string {
	refs := map[string]map[string]string{}
	for _, e := range entries {
		files := refs[e.GetFolder()]
		if files == nil {
			files = map[string]string{}
			refs[e.GetFolder()] = files
		}
		for name := range e.Files {
			files[name] = e.InfoHash
		}
	}
	return refs
}

func addEntries(t *testing.T, repair *Repair, entries ...*storage.Entry) {
	t.Helper()
	for _, e := range entries {
		if err := repair.manager.storage.AddOrUpdate(e); err != nil {
			t.Fatalf("AddOrUpdate: %v", err)
		}
	}
}

func TestDebridGoneAction(t *testing.T) {
	e := torrentEntry("h2", "TB", "torbox", "torbox")
	other := torrentEntry("h9", "TB", "realdebrid", "realdebrid")
	unknown := map[string]map[string]string{"TB": {"TB.mkv": ""}}
	none := map[string]map[string]string{}
	cases := []struct {
		name string
		refs debridGoneRefs
		want string
	}{
		{"a repair Arr points at it", debridGoneRefs{repair: refsTo(e), skipRepair: none}, debridGoneRegrab},
		{"no Arr points at it", debridGoneRefs{repair: none, skipRepair: none}, debridGoneOrphan},
		{"the Arr's slot is served by a same-named replacement", debridGoneRefs{repair: refsTo(other), skipRepair: none}, debridGoneOrphan},
		{"only a skip_repair Arr points at it", debridGoneRefs{repair: none, skipRepair: refsTo(e)}, debridGoneKeep},
		{"slot InfoHash unknown", debridGoneRefs{repair: unknown, skipRepair: none}, debridGoneKeep},
		{"both kinds of Arr point at it", debridGoneRefs{repair: refsTo(e), skipRepair: refsTo(e)}, debridGoneRegrab},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := debridGoneAction(e, "TB", c.refs); got != c.want {
				t.Fatalf("got %q, want %q", got, c.want)
			}
		})
	}
}

func TestFindAndFixDebridGone(t *testing.T) {
	repair := newTestRepairWithRealDebrid(t)
	rd := torrentEntry("h1", "RD", "realdebrid", "realdebrid")
	tb1 := torrentEntry("h2", "TB1", "torbox", "torbox")
	tb2 := torrentEntry("h3", "TB2", "torbox", "torbox")
	orphan := torrentEntry("h5", "Orphan", "torbox", "torbox")
	kept := torrentEntry("h6", "Kept", "torbox", "torbox")
	invalid := &storage.Entry{Protocol: config.ProtocolTorrent, Name: "."}
	bad := torrentEntry("h4", "Bad", "realdebrid", "realdebrid")
	bad.Bad = true
	addEntries(t, repair, rd, tb1, tb2, orphan, kept, bad, invalid)
	refs := debridGoneRefs{repair: refsTo(rd, tb1, tb2, bad), skipRepair: refsTo(kept)}

	found, err := repair.findDebridGone(refs)
	if err != nil {
		t.Fatal(err)
	}
	if found.Total != 5 || found.ByReason[reasonDebridNotConfigured] != 4 || found.ByReason[reasonDebridUnavailable] != 1 ||
		found.Orphaned != 1 || found.Kept != 1 || found.Regrab != 3 || found.Invalid != 1 {
		t.Fatalf("found %+v, want 1 orphan, 1 kept, 3 to re-grab, 1 invalid", found)
	}
	if len(found.Entries) != 3 || found.Entries[0].Name != "Bad" || found.Entries[1].Name != "TB1" {
		t.Fatalf("re-grab entries not sorted by name: %+v", found.Entries)
	}
	if len(found.Orphans) != 1 || found.Orphans[0].Name != "Orphan" {
		t.Fatalf("orphans %+v, want Orphan", found.Orphans)
	}

	// Without delete, orphans stay.
	fixed, err := repair.fixDebridGone(context.Background(), refs, DebridGoneFixOptions{Limit: 2})
	if err != nil {
		t.Fatalf("fixDebridGone: %v", err)
	}
	repair.runWG.Wait()
	if fixed.Deleted != 0 || fixed.Marked != 2 || len(fixed.Entries) != 2 || fixed.Run == nil {
		t.Fatalf("fixed %+v, want 2 marked, none deleted and a run", fixed)
	}
	if e, err := repair.manager.GetEntry("h5"); err != nil || e == nil {
		t.Fatalf("orphan deleted without delete set: %v", err)
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
	for _, name := range []string{"TB2", "RD", "Kept", "Orphan"} {
		if h, _ := repair.manager.storage.GetEntryHealth(name); h != nil && h.Status == storage.HealthBroken {
			t.Fatalf("%s got marked broken", name)
		}
	}

	// With delete, the orphan goes and the next batch moves on.
	fixed, err = repair.fixDebridGone(context.Background(), refs, DebridGoneFixOptions{Limit: 2, Delete: true})
	if err != nil {
		t.Fatalf("fixDebridGone with delete: %v", err)
	}
	repair.runWG.Wait()
	if fixed.Deleted != 1 || fixed.Marked != 1 || fixed.Entries[0].Name != "TB2" {
		t.Fatalf("fixed %+v, want Orphan deleted and TB2 marked", fixed)
	}
	if e, err := repair.manager.GetEntry("h5"); err == nil && e != nil {
		t.Fatal("orphan not deleted")
	}
	for _, hash := range []string{"h1", "h6"} {
		if e, err := repair.manager.GetEntry(hash); err != nil || e == nil {
			t.Fatalf("%s deleted: %v", hash, err)
		}
	}

	again, err := repair.findDebridGone(refs)
	if err != nil {
		t.Fatal(err)
	}
	if again.Total != 4 || again.AlreadyBroken != 3 || again.Kept != 1 || again.Orphaned != 0 || len(again.Entries) != 0 {
		t.Fatalf("third listing %+v, want 3 already broken and 1 kept", again)
	}
	if _, err := repair.fixDebridGone(context.Background(), refs, DebridGoneFixOptions{Delete: true}); err == nil {
		t.Fatal("nothing left to do, want an error")
	}
}

// A dead copy whose slot a same-named replacement serves is deleted, and the
// delete leaves the replacement's file in place.
func TestFixDebridGoneDeletesReplacedCopyOnly(t *testing.T) {
	repair := newTestRepairWithRealDebrid(t)
	old := torrentEntry("h2", "Show.S01E01", "torbox", "torbox")
	replacement := torrentEntry("h9", "Show.S01E01", "realdebrid", "realdebrid")
	replacement.AddedOn = time.Now()
	replacement.Files["Show.S01E01.mkv"].AddedOn = replacement.AddedOn
	addEntries(t, repair, old, replacement)
	refs := debridGoneRefs{repair: refsTo(replacement), skipRepair: map[string]map[string]string{}}

	fixed, err := repair.fixDebridGone(context.Background(), refs, DebridGoneFixOptions{Delete: true})
	if err != nil {
		t.Fatalf("fixDebridGone: %v", err)
	}
	if fixed.Total != 1 || fixed.Orphaned != 1 || fixed.Deleted != 1 || fixed.Marked != 0 || fixed.Run != nil {
		t.Fatalf("fixed %+v, want the old TorBox copy deleted and nothing re-grabbed", fixed)
	}
	if e, err := repair.manager.GetEntry("h9"); err != nil || e == nil {
		t.Fatalf("replacement deleted: %v", err)
	}
	item, err := repair.manager.GetEntryItem("Show.S01E01")
	if err != nil || item == nil || item.Files["Show.S01E01.mkv"] == nil || item.Files["Show.S01E01.mkv"].InfoHash != "h9" {
		t.Fatalf("replacement's file gone from the item: %+v, %v", item, err)
	}
}

// Without an Arr library to check against nothing is deleted or marked.
func TestFixDebridGoneNeedsTheArrLibraries(t *testing.T) {
	repair := newTestRepairWithRealDebrid(t)
	addEntries(t, repair, torrentEntry("h2", "TB", "torbox", "torbox"))
	if _, err := repair.FindDebridGone(context.Background()); err == nil {
		t.Fatal("FindDebridGone with no Arrs: want an error")
	}
	if _, err := repair.FixDebridGone(context.Background(), DebridGoneFixOptions{Delete: true}); err == nil {
		t.Fatal("FixDebridGone with no Arrs: want an error")
	}
	if e, err := repair.manager.GetEntry("h2"); err != nil || e == nil {
		t.Fatalf("entry deleted: %v", err)
	}
	if h, _ := repair.manager.storage.GetEntryHealth("TB"); h != nil && h.Status == storage.HealthBroken {
		t.Fatalf("entry marked: %+v", h)
	}
}
