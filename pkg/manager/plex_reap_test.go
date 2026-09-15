package manager

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
)

// fakePlex serves the subset of Plex's API the reaper uses, over one movie
// section and one item with a live and an old version.
type fakePlex struct {
	t         *testing.T
	dir       string
	live      string
	stale     string
	mu        sync.Mutex
	deletedAt int64 // old version's deletedAt; 0 = Plex hasn't noticed yet
	gone      bool  // old version removed by a DELETE
	deletes   []string
	refresh   []string
	playing   bool
	// shared adds a second section over the same folder that holds no
	// matching item (live: TEST Movies over the Movie Archive folder).
	shared bool
}

func (p *fakePlex) itemJSON() map[string]any {
	media := []map[string]any{
		{"id": 411255, "Part": []map[string]any{{"id": 1, "file": p.live, "size": 10}}},
	}
	if !p.gone {
		old := map[string]any{"id": 409490, "Part": []map[string]any{{"id": 2, "file": p.stale, "size": 5}}}
		if p.deletedAt > 0 {
			old["deletedAt"] = p.deletedAt
		}
		media = append(media, old)
	}
	return map[string]any{
		"ratingKey": "100", "type": "movie", "title": "Movie", "year": 2026,
		// Both keys, as Plex sends them: a case-insensitive decoder would
		// fold "guid" onto the Guid list without an exact field.
		"guid": "plex://movie/abc", "Guid": []map[string]any{{"id": "imdb://tt1"}, {"id": "tmdb://55"}},
		"Media": media,
	}
}

func (p *fakePlex) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("X-Plex-Token") != "tok" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	write := func(v any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	}
	path := r.URL.Path
	switch {
	case r.Method == http.MethodGet && path == "/library/sections":
		dirs := []map[string]any{
			{"key": "36", "type": "movie", "title": "Movies HD", "Location": []map[string]any{{"id": 67, "path": p.dir}}},
		}
		if p.shared {
			dirs = append(dirs, map[string]any{"key": "42", "type": "movie", "title": "TEST Movies", "Location": []map[string]any{{"id": 70, "path": p.dir}}})
		}
		write(map[string]any{"MediaContainer": map[string]any{"Directory": dirs}})
	case r.Method == http.MethodGet && (path == "/library/sections/36/refresh" || path == "/library/sections/42/refresh"):
		p.refresh = append(p.refresh, r.URL.Query().Get("path"))
	case r.Method == http.MethodGet && path == "/library/sections/42/all":
		write(map[string]any{"MediaContainer": map[string]any{"size": 0, "totalSize": 0}})
	case r.Method == http.MethodGet && path == "/library/sections/36/all":
		write(map[string]any{"MediaContainer": map[string]any{"size": 1, "totalSize": 1, "Metadata": []any{p.itemJSON()}}})
	case r.Method == http.MethodGet && path == "/library/metadata/100":
		write(map[string]any{"MediaContainer": map[string]any{"Metadata": []any{p.itemJSON()}}})
	case r.Method == http.MethodGet && path == "/status/sessions":
		var md []any
		if p.playing {
			md = append(md, map[string]any{"ratingKey": "100"})
		}
		write(map[string]any{"MediaContainer": map[string]any{"Metadata": md}})
	case r.Method == http.MethodDelete && strings.HasPrefix(path, "/library/metadata/100/media/"):
		id := strings.TrimPrefix(path, "/library/metadata/100/media/")
		p.deletes = append(p.deletes, id)
		if id == "409490" {
			p.gone = true
		}
	default:
		p.t.Errorf("fake plex: unexpected %s %s", r.Method, r.URL)
		w.WriteHeader(http.StatusNotFound)
	}
}

type reapHarness struct {
	plex   *fakePlex
	reaper *PlexReaper
	clock  time.Time
	mode   config.PlexReapMode
}

func newReapHarness(t *testing.T) *reapHarness {
	t.Helper()
	dir := t.TempDir()
	folder := filepath.Join(dir, "Movie (2026)")
	if err := os.MkdirAll(folder, 0o755); err != nil {
		t.Fatal(err)
	}
	fp := &fakePlex{
		t:         t,
		dir:       dir,
		live:      filepath.Join(folder, "Movie (2026) [Bluray-1080p]-knives.mkv"),
		stale:     filepath.Join(folder, "Movie (2026) [WEBDL-1080p]-playWEB.mkv"),
		deletedAt: 1789454321,
	}
	if err := os.WriteFile(fp.live, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	plexSrv := httptest.NewServer(fp)
	t.Cleanup(plexSrv.Close)

	radarr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/api/v3/movie" && r.URL.Query().Get("tmdbId") == "55":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 7, "title": "Movie", "path": folder, "movieFile": map[string]any{"path": fp.live}}})
		case r.URL.Path == "/api/v3/movie":
			_, _ = w.Write([]byte("[]"))
		case r.URL.Path == "/api/v3/movie/7":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 7, "title": "Movie"})
		case r.URL.Path == "/api/v3/moviefile":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"path": fp.live}})
		default:
			t.Errorf("fake radarr: unexpected %s", r.URL)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(radarr.Close)

	h := &reapHarness{plex: fp, clock: time.Date(2026, 9, 15, 6, 38, 40, 0, time.UTC), mode: config.PlexReapOn}
	lib := &plexLibrary{
		client: plexSrv.Client(),
		cfg: func() config.PlexConfig {
			return config.PlexConfig{URL: plexSrv.URL + "/", Token: "tok", ReapMode: h.mode}
		},
	}
	arrs := []*arr.Arr{arr.New("radarr", radarr.URL, "key", false, nil, "", "manual")}
	h.reaper = &PlexReaper{
		logger:   zerolog.Nop(),
		library:  lib,
		resolver: newReapArrResolver(func() []*arr.Arr { return arrs }, lib),
		jobsPath: filepath.Join(dir, "jobs.json"),
		now:      func() time.Time { return h.clock },
		env:      defaultReapEnv,
		jobs:     make(map[string]*plexReapJob),
	}
	return h
}

func (h *reapHarness) tick(d time.Duration) {
	h.clock = h.clock.Add(d)
	h.reaper.runDue(context.Background())
}

func (h *reapHarness) upgrade() {
	h.reaper.Enqueue(PlexReapNotice{
		Source: ReapSourceUpgrade, StalePaths: []string{h.plex.stale}, CurrentPath: h.plex.live, TitleHint: "Movie",
	})
}

func lastDecision(t *testing.T, r *PlexReaper) PlexReapDecision {
	t.Helper()
	st := r.Status()
	if len(st.Decisions) == 0 {
		t.Fatal("no decisions recorded")
	}
	return st.Decisions[0]
}

func TestPlexReaper_UpgradeReapsOldVersion(t *testing.T) {
	h := newReapHarness(t)
	h.upgrade()

	h.tick(plexReapScanSettle) // folder scan requested
	if len(h.plex.refresh) != 1 || h.plex.refresh[0] != filepath.Dir(h.plex.live) {
		t.Fatalf("refresh = %v, want the movie folder once", h.plex.refresh)
	}
	if len(h.plex.deletes) != 0 {
		t.Fatal("deleted before the scan settled")
	}

	h.tick(plexReapScanSettle)
	if len(h.plex.deletes) != 1 || h.plex.deletes[0] != "409490" {
		t.Fatalf("deletes = %v, want [409490]", h.plex.deletes)
	}
	d := lastDecision(t, h.reaper)
	if d.Status != "reaped" || d.RatingKey != "100" || d.Title != "Movie (2026)" {
		t.Fatalf("decision = %+v", d)
	}
	if n := len(h.reaper.Status().Pending); n != 0 {
		t.Fatalf("pending = %d, want 0", n)
	}
}

func TestPlexReaper_DryRunOnlyReports(t *testing.T) {
	h := newReapHarness(t)
	h.mode = config.PlexReapDryRun
	h.upgrade()
	h.tick(plexReapScanSettle)
	h.tick(plexReapScanSettle)
	if len(h.plex.deletes) != 0 {
		t.Fatalf("dry run deleted %v", h.plex.deletes)
	}
	if d := lastDecision(t, h.reaper); d.Status != "would_reap" || len(d.MediaIDs) != 1 || d.MediaIDs[0] != 409490 {
		t.Fatalf("decision = %+v, want would_reap 409490", d)
	}
}

func TestPlexReaper_OffIgnoresNotices(t *testing.T) {
	h := newReapHarness(t)
	h.mode = config.PlexReapOff
	h.upgrade()
	if n := len(h.reaper.Status().Pending); n != 0 {
		t.Fatalf("pending = %d while off, want 0", n)
	}
}

func TestPlexReaper_WaitsForPlexThenReaps(t *testing.T) {
	h := newReapHarness(t)
	h.plex.deletedAt = 0 // Plex hasn't rescanned yet
	h.upgrade()
	h.tick(plexReapScanSettle)
	h.tick(plexReapScanSettle)
	if len(h.plex.deletes) != 0 {
		t.Fatal("reaped a version Plex still shows as available")
	}
	st := h.reaper.Status()
	if len(st.Pending) != 1 || st.Pending[0].LastReason != reasonNotMarkedYet {
		t.Fatalf("pending = %+v, want one job waiting on %s", st.Pending, reasonNotMarkedYet)
	}

	h.plex.deletedAt = 1789454321
	h.tick(time.Hour)
	if len(h.plex.deletes) != 1 {
		t.Fatalf("deletes = %v after Plex marked it, want one", h.plex.deletes)
	}
}

func TestPlexReaper_PlayingDefers(t *testing.T) {
	h := newReapHarness(t)
	h.plex.playing = true
	h.upgrade()
	h.tick(plexReapScanSettle)
	h.tick(plexReapScanSettle)
	if len(h.plex.deletes) != 0 {
		t.Fatal("reaped while the item was playing")
	}
	h.plex.playing = false
	h.tick(time.Hour)
	if len(h.plex.deletes) != 1 {
		t.Fatalf("deletes = %v after playback stopped, want one", h.plex.deletes)
	}
}

func TestPlexReaper_GivesUpEventually(t *testing.T) {
	h := newReapHarness(t)
	h.plex.deletedAt = 0
	h.upgrade()
	h.tick(plexReapScanSettle)
	for i := 0; i < plexReapEventMaxTries+2; i++ {
		h.tick(24 * time.Hour)
	}
	if n := len(h.reaper.Status().Pending); n != 0 {
		t.Fatalf("pending = %d, want the job given up", n)
	}
	if d := lastDecision(t, h.reaper); d.Status != "gave_up" {
		t.Fatalf("decision = %+v, want gave_up", d)
	}
}

func TestPlexReaper_RepairJobWaitsAndNudge(t *testing.T) {
	h := newReapHarness(t)
	h.reaper.Enqueue(PlexReapNotice{Source: ReapSourceRepair, StalePaths: []string{h.plex.stale}, ArrName: "radarr", MediaID: 7})
	h.tick(plexReapScanSettle)
	if len(h.plex.refresh) != 0 {
		t.Fatal("repair job ran before its first gap")
	}
	h.reaper.NudgeFolder(filepath.Dir(h.plex.stale))
	h.tick(plexReapScanSettle) // scan
	h.tick(plexReapScanSettle) // evaluate
	if len(h.plex.deletes) != 1 {
		t.Fatalf("deletes = %v, want one after the import nudge", h.plex.deletes)
	}
	if d := lastDecision(t, h.reaper); d.Source != ReapSourceRepair || d.Status != "reaped" {
		t.Fatalf("decision = %+v", d)
	}
}

// A keep-release re-grab lands at the same path: Plex never shows a stale
// version, the Arr holds the old path again, and the job must finish rather
// than wait out its 72 h - also when a second section shares the folder.
func TestPlexReaper_RepairSamePathFinishesAcrossSharedSections(t *testing.T) {
	h := newReapHarness(t)
	h.plex.shared = true
	h.plex.gone = true // only the live version exists
	h.reaper.Enqueue(PlexReapNotice{Source: ReapSourceRepair, StalePaths: []string{h.plex.live}, ArrName: "radarr", MediaID: 7})
	h.tick(plexReapRepairFirstGap) // scan both sections
	if len(h.plex.refresh) != 2 {
		t.Fatalf("refresh = %v, want both sections", h.plex.refresh)
	}
	h.tick(plexReapScanSettle)
	if n := len(h.reaper.Status().Pending); n != 0 {
		t.Fatalf("pending = %d, want the job finished", n)
	}
	d := lastDecision(t, h.reaper)
	if d.Status != reapSkipped || d.Reason != reasonSamePath {
		t.Fatalf("decision = %+v, want skipped/%s", d, reasonSamePath)
	}
	if len(h.plex.deletes) != 0 {
		t.Fatalf("deleted %v", h.plex.deletes)
	}
}

func TestPlexReaper_JobsPersist(t *testing.T) {
	h := newReapHarness(t)
	h.upgrade()
	h.upgrade() // duplicate delivery merges
	again := &PlexReaper{jobsPath: h.reaper.jobsPath, jobs: make(map[string]*plexReapJob), logger: zerolog.Nop()}
	again.load()
	if len(again.jobs) != 1 {
		t.Fatalf("reloaded %d jobs, want 1", len(again.jobs))
	}
}

func TestPlexReaper_InPlaceReplacementIgnored(t *testing.T) {
	h := newReapHarness(t)
	h.reaper.Enqueue(PlexReapNotice{Source: ReapSourceUpgrade, StalePaths: []string{h.plex.live}, CurrentPath: h.plex.live})
	if n := len(h.reaper.Status().Pending); n != 0 {
		t.Fatalf("pending = %d for a same-path replacement, want 0", n)
	}
}

func TestPlexReaper_BacklogScanAndApply(t *testing.T) {
	h := newReapHarness(t)
	h.mode = config.PlexReapDryRun // apply is explicit, so it acts even in dry run
	if err := h.reaper.runScan(context.Background()); err != nil {
		t.Fatal(err)
	}
	st := h.reaper.Status()
	if len(st.Scan.Candidates) != 1 || st.Scan.Candidates[0].Status != reapReapable {
		t.Fatalf("candidates = %+v, want one reapable", st.Scan.Candidates)
	}
	if len(h.plex.deletes) != 0 {
		t.Fatal("scan deleted something")
	}
	res := h.reaper.ApplyScan(context.Background(), nil)
	if res.Reaped != 1 || len(h.plex.deletes) != 1 {
		t.Fatalf("apply = %+v deletes=%v, want one reaped", res, h.plex.deletes)
	}
	if c := h.reaper.Status().Scan.Candidates[0]; c.Status != "reaped" {
		t.Fatalf("candidate after apply = %+v", c)
	}
}

func TestPlexReaper_ApplyRechecksDisk(t *testing.T) {
	h := newReapHarness(t)
	if err := h.reaper.runScan(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The old file reappears between scan and apply: never delete it.
	if err := os.WriteFile(h.plex.stale, []byte("back"), 0o644); err != nil {
		t.Fatal(err)
	}
	res := h.reaper.ApplyScan(context.Background(), nil)
	if res.Reaped != 0 || len(h.plex.deletes) != 0 {
		t.Fatalf("apply = %+v deletes=%v, want nothing removed", res, h.plex.deletes)
	}
}
