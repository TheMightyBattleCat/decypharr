package manager

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// fakeRadarrWithFile serves one movie whose file is a symlink to
// <mount>/<entry folder>/<file>, the way an Arr library points into the mount.
func fakeRadarrWithFile(t *testing.T, libDir, mountDir string, e *storage.Entry, file string) string {
	t.Helper()
	target := filepath.Join(mountDir, e.GetFolder(), file)
	link := filepath.Join(libDir, e.GetFolder()+".mkv")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/api/v3/movie" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{{
			"id": 1, "title": e.Name, "movieFile": map[string]any{"id": 11, "path": link},
		}})
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func oldNZBEntry(hash, name string) *storage.Entry {
	e := torrentEntry(hash, name, "")
	e.Protocol = config.ProtocolNZB
	e.AddedOn = time.Now().Add(-30 * 24 * time.Hour)
	e.UpdatedAt = e.AddedOn
	for _, f := range e.Files {
		f.AddedOn = e.AddedOn
	}
	return e
}

// A download only an Arr with skip_repair uses is still in use: it is in the
// reference set, the stale-NZB check does not count it as stale, and its
// broken record is not treated as superseded.
func TestReferenceSetIncludesSkipRepairArrs(t *testing.T) {
	repair := newTestRepairWithRealDebrid(t)
	libDir, mountDir := t.TempDir(), t.TempDir()
	used := oldNZBEntry("nzb-used", "Used.Movie.2026")
	onlySkip := oldNZBEntry("nzb-skip", "Skip.Movie.2026")
	unused := oldNZBEntry("nzb-unused", "Unused.Movie.2026")
	addEntries(t, repair, used, onlySkip, unused)

	repair.manager.arr.AddOrUpdate(arr.New("radarr", fakeRadarrWithFile(t, libDir, mountDir, used, "Used.Movie.2026.mkv"), "key", false, nil, "", ""))
	repair.manager.arr.AddOrUpdate(arr.New("radarr-4k", fakeRadarrWithFile(t, libDir, mountDir, onlySkip, "Skip.Movie.2026.mkv"), "key", true, nil, "", ""))

	ctx := context.Background()
	refs, err := repair.buildArrReferencedSet(ctx)
	if err != nil {
		t.Fatalf("buildArrReferencedSet: %v", err)
	}
	if got := refs[onlySkip.GetFolder()]["Skip.Movie.2026.mkv"]; got != onlySkip.InfoHash {
		t.Errorf("reference set %v: skip-repair Arr's file maps to %q, want %q", refs, got, onlySkip.InfoHash)
	}

	// staleNZBSkipReason is the "is it used" test both the preview and the
	// delete-time re-check apply. AddOrUpdate stamps UpdatedAt, so judge two
	// days on, past staleNZBMinAge.
	later := time.Now().Add(2 * staleNZBMinAge)
	for _, tc := range []struct {
		e    *storage.Entry
		want string
	}{
		{used, "now referenced by an Arr"},
		{onlySkip, "now referenced by an Arr"},
		{unused, ""},
	} {
		if got := staleNZBSkipReason(tc.e, refs, later); got != tc.want {
			t.Errorf("%s: stale skip reason %q, want %q", tc.e.Name, got, tc.want)
		}
	}

	h := &storage.EntryHealth{
		EntryName: onlySkip.GetFolder(),
		Status:    storage.HealthBroken,
		BrokenFiles: []storage.BrokenFile{{
			EntryName: onlySkip.GetFolder(), FileName: "Skip.Movie.2026.mkv", InfoHash: onlySkip.InfoHash,
		}},
	}
	healths := xsync.NewMap[string, *storage.EntryHealth]()
	healths.Store(h.EntryName, h)
	repair.filterSupersededHealths(ctx, healths)
	got, ok := healths.Load(h.EntryName)
	if !ok || len(got.BrokenFiles) != 1 {
		t.Errorf("broken record after supersession check: %+v (present=%v), want it kept with its file", got, ok)
	}
}
