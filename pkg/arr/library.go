package arr

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

// Series is the subset of a Sonarr series record the library lookups need.
type Series struct {
	Id     int    `json:"id"`
	Title  string `json:"title"`
	Path   string `json:"path"`
	TvdbId int    `json:"tvdbId"`
}

// getList issues a GET and decodes a JSON list, turning a non-2xx status
// into an error (RequestCtx itself only errors on transport failures).
func (a *Arr) getList(ctx context.Context, endpoint string, out any) error {
	resp, err := a.RequestCtx(ctx, http.MethodGet, endpoint, nil, out)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s: %s", endpoint, resp.Status)
	}
	return nil
}

// MoviesByTmdbID returns the Radarr movies (normally zero or one) with the
// given TMDB id, including their folder and current file path.
func (a *Arr) MoviesByTmdbID(ctx context.Context, tmdbID int) ([]Movie, error) {
	var movies []Movie
	err := a.getList(ctx, fmt.Sprintf("api/v3/movie?tmdbId=%d", tmdbID), &movies)
	return movies, err
}

// AllMovies returns every Radarr movie. Large (tens of MB on a big
// library) - callers cache it.
func (a *Arr) AllMovies(ctx context.Context) ([]Movie, error) {
	var movies []Movie
	err := a.getList(ctx, "api/v3/movie", &movies)
	return movies, err
}

// MovieFilePaths returns the paths of every file Radarr holds for movieID.
func (a *Arr) MovieFilePaths(ctx context.Context, movieID int) ([]string, error) {
	var files []struct {
		Path string `json:"path"`
	}
	if err := a.getList(ctx, fmt.Sprintf("api/v3/moviefile?movieId=%d", movieID), &files); err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(files))
	for _, f := range files {
		if f.Path != "" {
			paths = append(paths, f.Path)
		}
	}
	return paths, nil
}

// SeriesByTvdbID returns the Sonarr series (normally zero or one) with the
// given TVDB id.
func (a *Arr) SeriesByTvdbID(ctx context.Context, tvdbID int) ([]Series, error) {
	var series []Series
	err := a.getList(ctx, fmt.Sprintf("api/v3/series?tvdbId=%d", tvdbID), &series)
	return series, err
}

// AllSeries returns every Sonarr series.
func (a *Arr) AllSeries(ctx context.Context) ([]Series, error) {
	var series []Series
	err := a.getList(ctx, "api/v3/series", &series)
	return series, err
}

// EpisodeFilePaths returns the paths of every episode file Sonarr holds for
// seriesID.
func (a *Arr) EpisodeFilePaths(ctx context.Context, seriesID int) ([]string, error) {
	var files []seriesFile
	if err := a.getList(ctx, fmt.Sprintf("api/v3/episodefile?seriesId=%d", seriesID), &files); err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(files))
	for _, f := range files {
		if f.Path != "" {
			paths = append(paths, f.Path)
		}
	}
	return paths, nil
}

// hostKey normalizes an Arr host for identity comparison: two configured
// Arrs whose hosts differ only by scheme case or a trailing slash are the
// same instance.
func hostKey(host string) string {
	return strings.TrimRight(strings.ToLower(strings.TrimSpace(host)), "/")
}

// DistinctByHost returns one configured Arr per distinct host, so callers
// that query "every Arr" don't hit the same instance twice when it is
// configured under two names (e.g. radarr and bh-radarr on one host). The
// first name in sorted order wins, so the choice is stable.
func (s *Storage) DistinctByHost() []*Arr {
	all := s.GetAll()
	byHost := make(map[string]*Arr, len(all))
	for _, a := range all {
		if a == nil || a.Host == "" || a.Token == "" {
			continue
		}
		k := hostKey(a.Host)
		if cur, ok := byHost[k]; !ok || a.Name < cur.Name {
			byHost[k] = a
		}
	}
	out := make([]*Arr, 0, len(byHost))
	for _, a := range byHost {
		out = append(out, a)
	}
	return out
}
