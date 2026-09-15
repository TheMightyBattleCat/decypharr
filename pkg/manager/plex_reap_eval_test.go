package manager

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// reapFixture lays out a library folder with a present live file and a
// missing old file, and returns an item whose two versions point at them.
type reapFixture struct {
	dir     string
	live    string
	stale   string
	item    *plexItem
	owned   bool
	refs    map[string]bool
	arrErr  error
	playing bool
	arrHits int
}

func newReapFixture(t *testing.T) *reapFixture {
	t.Helper()
	dir := t.TempDir()
	f := &reapFixture{
		dir:   dir,
		live:  filepath.Join(dir, "Movie (2026) [Bluray-1080p]-knives.mkv"),
		stale: filepath.Join(dir, "Movie (2026) [WEBDL-1080p]-playWEB.mkv"),
	}
	if err := os.WriteFile(f.live, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	f.item = &plexItem{
		RatingKey: "342193",
		Type:      "movie",
		Title:     "Movie",
		Media: []plexMedia{
			{ID: 411255, Part: []plexPart{{ID: 1, File: f.live}}},
			{ID: 409490, DeletedAt: 1789454321, Part: []plexPart{{ID: 2, File: f.stale}}},
		},
	}
	f.owned = true
	f.refs = map[string]bool{f.live: true}
	return f
}

func (f *reapFixture) env() reapEnv {
	return reapEnv{
		lstat: os.Lstat,
		stat:  os.Stat,
		arr: func() (bool, map[string]bool, error) {
			f.arrHits++
			return f.owned, f.refs, f.arrErr
		},
		playing: func() (bool, error) { return f.playing, nil },
	}
}

func TestEvaluateReap_Reapable(t *testing.T) {
	f := newReapFixture(t)
	v := evaluateReap(f.item, nil, f.env())
	if v.Status != reapReapable {
		t.Fatalf("status = %s (%s %s), want reapable", v.Status, v.Reason, v.Detail)
	}
	if len(v.Stale) != 1 || v.Stale[0].ID != 409490 {
		t.Fatalf("stale = %+v, want only media 409490", v.Stale)
	}
}

func TestEvaluateReap_Guards(t *testing.T) {
	cases := []struct {
		name       string
		mutate     func(t *testing.T, f *reapFixture)
		wantStatus string
		wantReason string
		wantNoArr  bool // the Arr lookup must not have run
	}{
		{
			name: "kept second version is not unavailable",
			mutate: func(t *testing.T, f *reapFixture) {
				f.item.Media[1].DeletedAt = 0
			},
			wantStatus: reapSkipped, wantReason: reasonNoStale, wantNoArr: true,
		},
		{
			name: "every version unavailable",
			mutate: func(t *testing.T, f *reapFixture) {
				f.item.Media[0].DeletedAt = 1
			},
			wantStatus: reapSkipped, wantReason: reasonAllUnavailable, wantNoArr: true,
		},
		{
			name: "live file missing (mount down)",
			mutate: func(t *testing.T, f *reapFixture) {
				_ = os.Remove(f.live)
			},
			wantStatus: reapWaiting, wantReason: reasonLiveUnreadable, wantNoArr: true,
		},
		{
			name: "old file still on disk",
			mutate: func(t *testing.T, f *reapFixture) {
				if err := os.WriteFile(f.stale, []byte("y"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			wantStatus: reapSkipped, wantReason: reasonStaleOnDisk, wantNoArr: true,
		},
		{
			name: "old path is a dangling symlink",
			mutate: func(t *testing.T, f *reapFixture) {
				if err := os.Symlink(filepath.Join(f.dir, "gone-target.mkv"), f.stale); err != nil {
					t.Fatal(err)
				}
			},
			wantStatus: reapSkipped, wantReason: reasonStaleOnDisk, wantNoArr: true,
		},
		{
			name:       "no Arr tracks the title",
			mutate:     func(t *testing.T, f *reapFixture) { f.owned = false },
			wantStatus: reapSkipped, wantReason: reasonNoArrOwner,
		},
		{
			name:       "Arr still references the old file",
			mutate:     func(t *testing.T, f *reapFixture) { f.refs[f.stale] = true },
			wantStatus: reapSkipped, wantReason: reasonArrReferences,
		},
		{
			name:       "Arr's current file not a live Plex version yet",
			mutate:     func(t *testing.T, f *reapFixture) { f.refs = map[string]bool{"/elsewhere.mkv": true} },
			wantStatus: reapWaiting, wantReason: reasonArrNotLive,
		},
		{
			name:       "Arr lookup failed",
			mutate:     func(t *testing.T, f *reapFixture) { f.arrErr = errors.New("radarr down") },
			wantStatus: reapWaiting, wantReason: reasonLookupFailed,
		},
		{
			name:       "playing now",
			mutate:     func(t *testing.T, f *reapFixture) { f.playing = true },
			wantStatus: reapWaiting, wantReason: reasonPlaying,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newReapFixture(t)
			tc.mutate(t, f)
			v := evaluateReap(f.item, nil, f.env())
			if v.Status != tc.wantStatus || v.Reason != tc.wantReason {
				t.Fatalf("got %s/%s (%s), want %s/%s", v.Status, v.Reason, v.Detail, tc.wantStatus, tc.wantReason)
			}
			if tc.wantNoArr && f.arrHits != 0 {
				t.Fatalf("Arr lookup ran %d times before a cheaper guard failed", f.arrHits)
			}
		})
	}
}

func TestEvaluateReap_Targets(t *testing.T) {
	t.Run("restricts to the named old file", func(t *testing.T) {
		f := newReapFixture(t)
		other := filepath.Join(f.dir, "Movie (2026) [HDTV]-other.mkv")
		f.item.Media = append(f.item.Media, plexMedia{ID: 5, DeletedAt: 1, Part: []plexPart{{File: other}}})
		v := evaluateReap(f.item, []string{f.stale}, f.env())
		if v.Status != reapReapable || len(v.Stale) != 1 || v.Stale[0].ID != 409490 {
			t.Fatalf("got %s/%s stale=%+v, want only 409490", v.Status, v.Reason, v.Stale)
		}
	})
	t.Run("old file still live in Plex waits", func(t *testing.T) {
		f := newReapFixture(t)
		f.item.Media[1].DeletedAt = 0
		v := evaluateReap(f.item, []string{f.stale}, f.env())
		if v.Status != reapWaiting || v.Reason != reasonNotMarkedYet {
			t.Fatalf("got %s/%s, want waiting/%s", v.Status, v.Reason, reasonNotMarkedYet)
		}
	})
	t.Run("re-imported at the same path", func(t *testing.T) {
		f := newReapFixture(t)
		f.item.Media = f.item.Media[:1]
		v := evaluateReap(f.item, []string{f.live}, f.env())
		if v.Status != reapSkipped || v.Reason != reasonSamePath {
			t.Fatalf("got %s/%s, want skipped/%s", v.Status, v.Reason, reasonSamePath)
		}
	})
	t.Run("old file not in this item", func(t *testing.T) {
		f := newReapFixture(t)
		v := evaluateReap(f.item, []string{"/somewhere/else.mkv"}, f.env())
		if v.Status != reapSkipped || v.Reason != reasonTargetNotInItem {
			t.Fatalf("got %s/%s, want skipped/%s", v.Status, v.Reason, reasonTargetNotInItem)
		}
	})
}

func TestExternalID(t *testing.T) {
	it := &plexItem{GUID: "plex://movie/abc"}
	it.Guids = append(it.Guids, struct {
		ID string `json:"id"`
	}{ID: "imdb://tt00000017"}, struct {
		ID string `json:"id"`
	}{ID: "tmdb://1234"})
	if got := externalID(it, "tmdb"); got != 1234 {
		t.Fatalf("tmdb = %d, want 1234", got)
	}
	legacy := &plexItem{GUID: "com.plexapp.agents.thetvdb://81189?lang=en"}
	if got := externalID(legacy, "tvdb"); got != 81189 {
		t.Fatalf("legacy tvdb = %d, want 81189", got)
	}
}

func TestSectionsForPath(t *testing.T) {
	sections := []plexSection{
		{Key: "37", Type: "movie", Locations: []string{"/lib/movie_arch"}},
		{Key: "42", Type: "movie", Locations: []string{"/lib/movie_arch"}},
		{Key: "36", Type: "movie", Locations: []string{"/lib/MoviesHD"}},
		{Key: "27", Type: "artist", Locations: []string{"/lib"}},
		{Key: "9", Type: "movie", Locations: []string{"/lib/Movies"}},
	}
	got := sectionsForPath(sections, "/lib/MoviesHD/X (2026)/X.mkv")
	if len(got) != 1 || got[0].Key != "36" {
		t.Fatalf("MoviesHD path -> %+v, want only 36 (not the /lib/Movies prefix lookalike)", got)
	}
	if got := sectionsForPath(sections, "/lib/movie_arch/Y/Y.mkv"); len(got) != 2 {
		t.Fatalf("shared folder -> %d sections, want both", len(got))
	}
}
