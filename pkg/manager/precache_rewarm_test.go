package manager

import (
	"context"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func withPrecacheReadAhead(t *testing.T) {
	t.Helper()
	cfg := config.Get()
	prev := cfg.Repair.PrecacheReadAhead
	on := true
	cfg.Repair.PrecacheReadAhead = &on
	t.Cleanup(func() { cfg.Repair.PrecacheReadAhead = prev })
}

// A re-grabbed pre-cached episode is warmed again once Sonarr serves it
// from a new entry - not while it still resolves to the old grab.
func TestRewarmDueBurstsReplacementOnceImported(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	withPrecacheReadAhead(t)
	p := NewPrecache(&Manager{})
	ref := walkIdentity{arr: &arr.Arr{Name: "sonarr"}, seriesName: "Show", seriesId: 7, seasonNumber: 4, episodeNumber: 3}
	p.storeReadiness(EpisodeReadiness{InfoHash: "old", Filename: "e03.mkv", SegmentsPending: 5})
	p.addRewarm(ref, "old", "old:e03.mkv")

	var lookedUp []int
	p.rewarmLookup = func(_ context.Context, r walkIdentity) (arr.NextEpisodeInfo, bool, error) {
		lookedUp = append(lookedUp, r.episodeNumber)
		return arr.NextEpisodeInfo{EpisodeNumber: r.episodeNumber, HasFile: true, Path: "/lib/e03.mkv"}, true, nil
	}
	current := "old"
	p.rewarmResolve = func(arr.NextEpisodeInfo) (*storage.Entry, string, bool) {
		return &storage.Entry{InfoHash: current, Name: "Show.S04E03"}, "e03.mkv", true
	}
	var bursts []walkIdentity
	p.rewarmBurst = func(_ context.Context, r walkIdentity, _ arr.NextEpisodeInfo) episodeStep {
		bursts = append(bursts, r)
		return stepBurst
	}

	p.rewarmDue()
	if len(bursts) != 0 {
		t.Fatal("burst while Sonarr still serves the old grab")
	}
	if len(p.rewarm) != 1 {
		t.Fatal("target dropped before its replacement arrived")
	}

	current = "new"
	p.rewarmDue()
	if len(bursts) != 1 || bursts[0].episodeNumber != 3 {
		t.Fatalf("bursts = %+v, want one burst of episode 3", bursts)
	}
	if len(p.rewarm) != 0 {
		t.Fatal("target kept after warming its replacement")
	}
	if _, ok := p.readiness["old:e03.mkv"]; ok {
		t.Fatal("the old grab's readiness row was kept")
	}
	if len(lookedUp) != 2 || lookedUp[0] != 3 {
		t.Fatalf("looked up %v, want episode 3 twice", lookedUp)
	}
}

// A burst deferred by budget or bandwidth must leave the target queued, or
// the replacement is never warmed.
func TestRewarmDueKeepsDeferredTarget(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	withPrecacheReadAhead(t)
	p := NewPrecache(&Manager{})
	ref := walkIdentity{arr: &arr.Arr{Name: "sonarr"}, seriesId: 2, seasonNumber: 1, episodeNumber: 5}
	p.storeReadiness(EpisodeReadiness{InfoHash: "old", Filename: "e05.mkv", SegmentsPending: 1})
	p.addRewarm(ref, "old", "old:e05.mkv")
	p.rewarmLookup = func(context.Context, walkIdentity) (arr.NextEpisodeInfo, bool, error) {
		return arr.NextEpisodeInfo{HasFile: true}, true, nil
	}
	p.rewarmResolve = func(arr.NextEpisodeInfo) (*storage.Entry, string, bool) {
		return &storage.Entry{InfoHash: "new"}, "e05.mkv", true
	}
	step := stepDeferred
	p.rewarmBurst = func(context.Context, walkIdentity, arr.NextEpisodeInfo) episodeStep { return step }

	p.rewarmDue()
	if len(p.rewarm) != 1 {
		t.Fatal("deferred re-warm dropped")
	}
	if _, ok := p.readiness["old:e05.mkv"]; !ok {
		t.Fatal("old row dropped before the replacement was warmed")
	}
	step = stepBurst
	p.rewarmDue()
	if len(p.rewarm) != 0 {
		t.Fatal("target kept after the replacement was warmed")
	}
}

func TestRewarmDueForgetsExpiredTargets(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	withPrecacheReadAhead(t)
	p := NewPrecache(&Manager{})
	ref := walkIdentity{arr: &arr.Arr{Name: "sonarr"}, seriesId: 1, seasonNumber: 1, episodeNumber: 1}
	p.addRewarm(ref, "old", "old:f")
	p.rewarm[rewarmKey(ref)].until = time.Now().Add(-time.Second)
	p.rewarmLookup = func(context.Context, walkIdentity) (arr.NextEpisodeInfo, bool, error) {
		t.Fatal("looked up an expired target")
		return arr.NextEpisodeInfo{}, false, nil
	}
	p.rewarmDue()
	if len(p.rewarm) != 0 {
		t.Fatal("expired target kept")
	}
}

// A PAR2 pass that lands after the readiness wait gave up must still settle
// the row; it used to say "still damaged" until restart.
func TestOnPar2RepairedSettlesStillDamagedRow(t *testing.T) {
	m, _ := newTestManagerForReap(t)
	if err := m.storage.AddOrUpdate(&storage.Entry{
		InfoHash: "h1", Name: "Show.S01E02", Protocol: config.ProtocolNZB,
		Files: map[string]*storage.File{"e02.mkv": {Name: "e02.mkv", InfoHash: "h1", Size: 100, AddedOn: time.Now()}},
	}); err != nil {
		t.Fatal(err)
	}
	p := NewPrecache(m)
	ctx, cancel := context.WithCancel(context.Background())
	p.ctx, p.cancel = ctx, cancel
	t.Cleanup(cancel) // ends the delayed persist goroutine

	p.storeReadiness(EpisodeReadiness{InfoHash: "h1", Filename: "e02.mkv", SegmentsPending: 3})
	p.storeReadiness(EpisodeReadiness{InfoHash: "other", Filename: "x.mkv", SegmentsPending: 2})

	p.OnPar2Repaired("h1")

	got := p.readiness["h1:e02.mkv"]
	if got.SegmentsPending != 0 || got.SegmentsRepaired != 3 {
		t.Fatalf("row after repair = %+v, want 3 repaired, 0 pending", got)
	}
	if other := p.readiness["other:x.mkv"]; other.SegmentsPending != 2 {
		t.Fatalf("another release's row changed: %+v", other)
	}
}

func TestRefreshReadinessDamageDropsReplacedEntry(t *testing.T) {
	m, _ := newTestManagerForReap(t)
	if err := m.storage.AddOrUpdate(&storage.Entry{InfoHash: "live", Name: "Show", Protocol: config.ProtocolNZB,
		Files: map[string]*storage.File{"f.mkv": {Name: "f.mkv", InfoHash: "live", Size: 1, AddedOn: time.Now()}}}); err != nil {
		t.Fatal(err)
	}
	p := NewPrecache(m)

	gone := EpisodeReadiness{InfoHash: "deleted", Filename: "f.mkv", Clean: true}
	if p.refreshReadinessDamage(&gone) {
		t.Fatal("row of a deleted entry kept")
	}
	damaged := EpisodeReadiness{InfoHash: "live", Filename: "f.mkv", SegmentsPending: 4}
	if !p.refreshReadinessDamage(&damaged) {
		t.Fatal("row of a live entry dropped")
	}
	if damaged.SegmentsPending != 0 || damaged.SegmentsRepaired != 4 {
		t.Fatalf("damaged row = %+v, want refreshed to 0 pending (overlay has none)", damaged)
	}
}
