package manager

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirrobot01/decypharr/pkg/arr"
)

// reapArrListTTL bounds how long the full movie/series lists used as the
// folder-match fallback are reused. Radarr's full list is tens of MB, so it
// is only fetched when an item has no usable external id.
const reapArrListTTL = 30 * time.Minute

// reapArrResolver answers "which Arr owns this Plex item, and which files
// does it hold for it" across every distinct configured Arr host.
type reapArrResolver struct {
	arrs    func() []*arr.Arr
	library *plexLibrary

	mu        sync.Mutex
	types     map[string]arr.Type // arr name -> type, for Arrs whose name/host didn't say
	movies    map[string]reapArrList[arr.Movie]
	series    map[string]reapArrList[arr.Series]
	showTvdb  map[string]int // show ratingKey -> tvdb id (0 = none)
	showPaths map[string][]string
}

type reapArrList[T any] struct {
	at    time.Time
	items []T
}

func newReapArrResolver(arrs func() []*arr.Arr, library *plexLibrary) *reapArrResolver {
	return &reapArrResolver{
		arrs:      arrs,
		library:   library,
		types:     make(map[string]arr.Type),
		movies:    make(map[string]reapArrList[arr.Movie]),
		series:    make(map[string]reapArrList[arr.Series]),
		showTvdb:  make(map[string]int),
		showPaths: make(map[string][]string),
	}
}

// arrType returns a's type, asking the Arr itself when its name and host
// don't say (inferType only looks for "sonarr"/"radarr" in them).
func (r *reapArrResolver) arrType(ctx context.Context, a *arr.Arr) arr.Type {
	if a.Type == arr.Sonarr || a.Type == arr.Radarr {
		return a.Type
	}
	r.mu.Lock()
	t, ok := r.types[a.Name]
	r.mu.Unlock()
	if ok {
		return t
	}
	var status struct {
		AppName string `json:"appName"`
	}
	t = arr.Others
	if resp, err := a.RequestCtx(ctx, http.MethodGet, "api/v3/system/status", nil, &status); err == nil && resp.StatusCode == http.StatusOK {
		switch strings.ToLower(status.AppName) {
		case "sonarr":
			t = arr.Sonarr
		case "radarr":
			t = arr.Radarr
		}
		r.mu.Lock()
		r.types[a.Name] = t
		r.mu.Unlock()
	}
	return t
}

// externalID extracts an id from Plex guids for scheme ("tmdb", "tvdb"),
// accepting both the new agents' Guid list ("tmdb://123") and the legacy
// agents' item guid ("com.plexapp.agents.themoviedb://123?lang=en").
func externalID(it *plexItem, scheme string) int {
	for _, g := range it.Guids {
		if rest, ok := strings.CutPrefix(g.ID, scheme+"://"); ok {
			if n, err := strconv.Atoi(rest); err == nil {
				return n
			}
		}
	}
	legacy := map[string]string{"tmdb": "com.plexapp.agents.themoviedb://", "tvdb": "com.plexapp.agents.thetvdb://"}[scheme]
	if rest, ok := strings.CutPrefix(it.GUID, legacy); ok {
		rest, _, _ = strings.Cut(rest, "?")
		rest, _, _ = strings.Cut(rest, "/")
		if n, err := strconv.Atoi(rest); err == nil {
			return n
		}
	}
	return 0
}

// itemFiles returns every file of every version of it.
func itemFiles(it *plexItem) []string {
	var files []string
	for _, m := range it.Media {
		files = append(files, mediaFiles(m)...)
	}
	return files
}

func anyUnder(files []string, dir string) bool {
	if dir == "" {
		return false
	}
	for _, f := range files {
		if pathUnder(f, dir) {
			return true
		}
	}
	return false
}

// lookup resolves ownership for item in a section of sectionType. An error
// from any Arr of the relevant type fails the whole lookup: a title can only
// be declared unreferenced when every Arr has answered.
func (r *reapArrResolver) lookup(ctx context.Context, item *plexItem, sectionType string) (bool, map[string]bool, error) {
	files := itemFiles(item)
	refs := make(map[string]bool)
	owned := false

	want := arr.Radarr
	if sectionType == "show" {
		want = arr.Sonarr
	}

	var tvdb int
	var showPaths []string
	if want == arr.Sonarr {
		var err error
		tvdb, showPaths, err = r.showInfo(ctx, item)
		if err != nil {
			return false, nil, err
		}
	}

	for _, a := range r.arrs() {
		if r.arrType(ctx, a) != want {
			continue
		}
		var paths []string
		var err error
		if want == arr.Radarr {
			paths, owned, err = r.radarrRefs(ctx, a, item, files, owned)
		} else {
			paths, owned, err = r.sonarrRefs(ctx, a, tvdb, files, showPaths, owned)
		}
		if err != nil {
			return false, nil, fmt.Errorf("%s: %w", a.Name, err)
		}
		for _, p := range paths {
			refs[filepath.Clean(p)] = true
		}
	}
	return owned, refs, nil
}

func (r *reapArrResolver) radarrRefs(ctx context.Context, a *arr.Arr, item *plexItem, files []string, owned bool) ([]string, bool, error) {
	var owners []arr.Movie
	if tmdb := externalID(item, "tmdb"); tmdb > 0 {
		movies, err := a.MoviesByTmdbID(ctx, tmdb)
		if err != nil {
			return nil, owned, err
		}
		for _, mv := range movies {
			if anyUnder(files, mv.Path) {
				owners = append(owners, mv)
			}
		}
	}
	if len(owners) == 0 {
		all, err := r.allMovies(ctx, a)
		if err != nil {
			return nil, owned, err
		}
		for _, mv := range all {
			if anyUnder(files, mv.Path) {
				owners = append(owners, mv)
			}
		}
	}
	var paths []string
	for _, mv := range owners {
		owned = true
		p, err := a.MovieFilePaths(ctx, mv.Id)
		if err != nil {
			return nil, owned, err
		}
		paths = append(paths, p...)
	}
	return paths, owned, nil
}

func (r *reapArrResolver) sonarrRefs(ctx context.Context, a *arr.Arr, tvdb int, files, showPaths []string, owned bool) ([]string, bool, error) {
	matches := func(s arr.Series) bool {
		if s.Path == "" {
			return false
		}
		if anyUnder(files, s.Path) {
			return true
		}
		for _, sp := range showPaths {
			if filepath.Clean(sp) == filepath.Clean(s.Path) {
				return true
			}
		}
		return false
	}
	var owners []arr.Series
	if tvdb > 0 {
		series, err := a.SeriesByTvdbID(ctx, tvdb)
		if err != nil {
			return nil, owned, err
		}
		for _, s := range series {
			if matches(s) {
				owners = append(owners, s)
			}
		}
	}
	if len(owners) == 0 {
		all, err := r.allSeries(ctx, a)
		if err != nil {
			return nil, owned, err
		}
		for _, s := range all {
			if matches(s) {
				owners = append(owners, s)
			}
		}
	}
	var paths []string
	for _, s := range owners {
		owned = true
		p, err := a.EpisodeFilePaths(ctx, s.Id)
		if err != nil {
			return nil, owned, err
		}
		paths = append(paths, p...)
	}
	return paths, owned, nil
}

// showInfo returns an episode's show tvdb id and folders, cached per show.
func (r *reapArrResolver) showInfo(ctx context.Context, item *plexItem) (int, []string, error) {
	key := string(item.GrandparentRatingKey)
	if key == "" {
		return 0, nil, nil
	}
	r.mu.Lock()
	tvdb, ok := r.showTvdb[key]
	paths := r.showPaths[key]
	r.mu.Unlock()
	if ok {
		return tvdb, paths, nil
	}
	show, err := r.library.Metadata(ctx, key)
	if err != nil {
		return 0, nil, err
	}
	tvdb = externalID(show, "tvdb")
	for _, l := range show.Location {
		paths = append(paths, l.Path)
	}
	r.mu.Lock()
	r.showTvdb[key] = tvdb
	r.showPaths[key] = paths
	r.mu.Unlock()
	return tvdb, paths, nil
}

func (r *reapArrResolver) allMovies(ctx context.Context, a *arr.Arr) ([]arr.Movie, error) {
	r.mu.Lock()
	c, ok := r.movies[a.Name]
	r.mu.Unlock()
	if ok && time.Since(c.at) < reapArrListTTL {
		return c.items, nil
	}
	items, err := a.AllMovies(ctx)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.movies[a.Name] = reapArrList[arr.Movie]{at: time.Now(), items: items}
	r.mu.Unlock()
	return items, nil
}

func (r *reapArrResolver) allSeries(ctx context.Context, a *arr.Arr) ([]arr.Series, error) {
	r.mu.Lock()
	c, ok := r.series[a.Name]
	r.mu.Unlock()
	if ok && time.Since(c.at) < reapArrListTTL {
		return c.items, nil
	}
	items, err := a.AllSeries(ctx)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.series[a.Name] = reapArrList[arr.Series]{at: time.Now(), items: items}
	r.mu.Unlock()
	return items, nil
}

// resetShowCache drops cached show ids/folders (start of a backlog scan).
func (r *reapArrResolver) resetShowCache() {
	r.mu.Lock()
	r.showTvdb = make(map[string]int)
	r.showPaths = make(map[string][]string)
	r.mu.Unlock()
}
