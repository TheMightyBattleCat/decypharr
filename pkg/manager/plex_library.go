package manager

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	json "github.com/bytedance/sonic"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
)

// plexLibraryTimeout bounds one Plex library request. Section listings of a
// large TV library take several seconds (13k episodes ~6 s live), so this is
// far looser than plexSessionFetchTimeout.
const plexLibraryTimeout = 60 * time.Second

// flexString decodes a JSON value that may be a string or a bare number.
// Plex sends ratingKey/key as quoted strings today, but mixes forms
// field-by-field (see flexInt64), so identifiers are decoded tolerantly.
type flexString string

func (f *flexString) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || string(b) == "null" {
		*f = ""
		return nil
	}
	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		*f = flexString(s)
		return nil
	}
	*f = flexString(string(b))
	return nil
}

// plexPart is one file of a Plex media version.
type plexPart struct {
	ID   flexInt64 `json:"id"`
	File string    `json:"file"`
	Size flexInt64 `json:"size"`
}

// plexMedia is one version of a Plex item. DeletedAt is set (unix seconds)
// once a scan found the version's file gone - the "Unavailable" badge.
// Verified live 2026-09-15: id is a bare number, deletedAt a bare number.
type plexMedia struct {
	ID        flexInt64  `json:"id"`
	DeletedAt flexInt64  `json:"deletedAt"`
	Part      []plexPart `json:"Part"`
}

// plexItem is a movie, show or episode as returned by Plex's library
// endpoints. GUID (lowercase "guid", a plex:// string) and Guids ("Guid",
// the external id list) must both be declared: the decoder matches keys
// case-insensitively, so without the exact "guid" field the string would be
// folded onto the list field and fail the whole response.
type plexItem struct {
	RatingKey            flexString `json:"ratingKey"`
	Type                 string     `json:"type"`
	Title                string     `json:"title"`
	GrandparentTitle     string     `json:"grandparentTitle"`
	GrandparentRatingKey flexString `json:"grandparentRatingKey"`
	ParentIndex          flexInt64  `json:"parentIndex"`
	Index                flexInt64  `json:"index"`
	Year                 flexInt64  `json:"year"`
	GUID                 string     `json:"guid"`
	Guids                []struct {
		ID string `json:"id"`
	} `json:"Guid"`
	Location []struct {
		Path string `json:"path"`
	} `json:"Location"`
	Media []plexMedia `json:"Media"`
}

// displayTitle is "Show S01E02 Title" for an episode, "Title (Year)" for a
// movie.
func (it *plexItem) displayTitle() string {
	if it.Type == "episode" {
		return fmt.Sprintf("%s S%02dE%02d %s", it.GrandparentTitle, it.ParentIndex, it.Index, it.Title)
	}
	if it.Year > 0 {
		return fmt.Sprintf("%s (%d)", it.Title, it.Year)
	}
	return it.Title
}

type plexItemsResponse struct {
	MediaContainer struct {
		Size      int        `json:"size"`
		TotalSize int        `json:"totalSize"`
		Metadata  []plexItem `json:"Metadata"`
	} `json:"MediaContainer"`
}

// plexSection is one library section and its folders.
type plexSection struct {
	Key       string
	Type      string // "movie", "show", ...
	Title     string
	Locations []string
}

type plexSectionsResponse struct {
	MediaContainer struct {
		Directory []struct {
			Key      flexString `json:"key"`
			Type     string     `json:"type"`
			Title    string     `json:"title"`
			Location []struct {
				Path string `json:"path"`
			} `json:"Location"`
		} `json:"Directory"`
	} `json:"MediaContainer"`
}

// plexLibrary talks to Plex's library API for the stale-version reaper.
type plexLibrary struct {
	client *http.Client
	// cfg returns the live Plex config; a func so tests can point it at an
	// httptest server without touching the global config.
	cfg func() config.PlexConfig
}

func newPlexLibrary() *plexLibrary {
	return &plexLibrary{
		client: &http.Client{Timeout: plexLibraryTimeout},
		cfg:    func() config.PlexConfig { return config.Get().Plex },
	}
}

// do issues an authenticated request and, when out is non-nil, decodes a
// JSON body. headers are extra request headers (container paging).
func (p *plexLibrary) do(ctx context.Context, method, path string, out any, headers map[string]string) error {
	cfg := p.cfg()
	if !cfg.Enabled() {
		return fmt.Errorf("plex not configured")
	}
	u, err := utils.JoinURL(cfg.URL, path)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, plexLibraryTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Plex-Token", cfg.Token)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("plex: %s %s: status %d", method, path, resp.StatusCode)
	}
	if out == nil {
		return nil
	}
	return json.ConfigDefault.NewDecoder(resp.Body).Decode(out)
}

// Sections lists the library sections.
func (p *plexLibrary) Sections(ctx context.Context) ([]plexSection, error) {
	var r plexSectionsResponse
	if err := p.do(ctx, http.MethodGet, "/library/sections", &r, nil); err != nil {
		return nil, err
	}
	out := make([]plexSection, 0, len(r.MediaContainer.Directory))
	for _, d := range r.MediaContainer.Directory {
		s := plexSection{Key: string(d.Key), Type: d.Type, Title: d.Title}
		for _, l := range d.Location {
			if l.Path != "" {
				s.Locations = append(s.Locations, filepath.Clean(l.Path))
			}
		}
		out = append(out, s)
	}
	return out, nil
}

// pathUnder reports whether p is dir or inside it.
func pathUnder(p, dir string) bool {
	p, dir = filepath.Clean(p), filepath.Clean(dir)
	return p == dir || strings.HasPrefix(p, dir+string(filepath.Separator))
}

// sectionsForPath returns every movie/show section with a folder containing
// path, longest folder first. More than one is normal: two sections may
// share a folder (e.g. a test section over an archive's folder).
func sectionsForPath(sections []plexSection, path string) []plexSection {
	type hit struct {
		s   plexSection
		len int
	}
	var hits []hit
	for _, s := range sections {
		if s.Type != "movie" && s.Type != "show" {
			continue
		}
		best := -1
		for _, loc := range s.Locations {
			if pathUnder(path, loc) && len(loc) > best {
				best = len(loc)
			}
		}
		if best >= 0 {
			hits = append(hits, hit{s, best})
		}
	}
	for i := 1; i < len(hits); i++ {
		for j := i; j > 0 && hits[j].len > hits[j-1].len; j-- {
			hits[j], hits[j-1] = hits[j-1], hits[j]
		}
	}
	out := make([]plexSection, len(hits))
	for i, h := range hits {
		out[i] = h.s
	}
	return out
}

// RefreshPath asks Plex to scan one folder of a section.
func (p *plexLibrary) RefreshPath(ctx context.Context, sectionKey, dir string) error {
	q := url.Values{}
	q.Set("path", dir)
	return p.do(ctx, http.MethodGet, "/library/sections/"+url.PathEscape(sectionKey)+"/refresh?"+q.Encode(), nil, nil)
}

// Metadata fetches one item with its versions and external ids.
func (p *plexLibrary) Metadata(ctx context.Context, ratingKey string) (*plexItem, error) {
	var r plexItemsResponse
	if err := p.do(ctx, http.MethodGet, "/library/metadata/"+url.PathEscape(ratingKey)+"?includeGuids=1", &r, nil); err != nil {
		return nil, err
	}
	if len(r.MediaContainer.Metadata) == 0 {
		return nil, fmt.Errorf("plex: item %s not found", ratingKey)
	}
	return &r.MediaContainer.Metadata[0], nil
}

// searchSection lists items of plexType (1 movie, 2 show, 4 episode) in a
// section matching filter (e.g. title=..., show.title=...). Plex silently
// ignores an unknown filter and returns the whole section - live,
// grandparentTitle= on episodes did exactly that - so only filters verified
// live are used by callers.
func (p *plexLibrary) searchSection(ctx context.Context, sectionKey string, plexType int, filter url.Values) ([]plexItem, error) {
	q := url.Values{}
	for k, v := range filter {
		q[k] = v
	}
	q.Set("type", strconv.Itoa(plexType))
	q.Set("includeGuids", "1")
	var r plexItemsResponse
	if err := p.do(ctx, http.MethodGet, "/library/sections/"+url.PathEscape(sectionKey)+"/all?"+q.Encode(), &r, nil); err != nil {
		return nil, err
	}
	return r.MediaContainer.Metadata, nil
}

// ListPage returns one page of a section's items of plexType, and the total.
func (p *plexLibrary) ListPage(ctx context.Context, sectionKey string, plexType, start, size int) ([]plexItem, int, error) {
	q := url.Values{}
	q.Set("type", strconv.Itoa(plexType))
	q.Set("includeGuids", "1")
	var r plexItemsResponse
	headers := map[string]string{
		"X-Plex-Container-Start": strconv.Itoa(start),
		"X-Plex-Container-Size":  strconv.Itoa(size),
	}
	if err := p.do(ctx, http.MethodGet, "/library/sections/"+url.PathEscape(sectionKey)+"/all?"+q.Encode(), &r, headers); err != nil {
		return nil, 0, err
	}
	total := r.MediaContainer.TotalSize
	if total == 0 {
		total = start + len(r.MediaContainer.Metadata)
	}
	return r.MediaContainer.Metadata, total, nil
}

// itemHasFile reports whether any version of it has a part at path.
func itemHasFile(it *plexItem, path string) bool {
	path = filepath.Clean(path)
	for _, m := range it.Media {
		for _, pt := range m.Part {
			if pt.File != "" && filepath.Clean(pt.File) == path {
				return true
			}
		}
	}
	return false
}

// FindItemByFile finds the movie or episode in a section that has a version
// whose file is one of paths. titleHint (the Arr's movie or series title)
// narrows the search to a title filter first; when that finds nothing and
// walk is true the section is paged through, so a title that differs between
// the Arr and Plex still resolves (a large TV section is several seconds, so
// callers ration walks). Returns nil, nil when no item has any of the paths.
func (p *plexLibrary) FindItemByFile(ctx context.Context, section plexSection, titleHint string, paths []string, walk bool) (*plexItem, error) {
	match := func(items []plexItem) *plexItem {
		for i := range items {
			for _, path := range paths {
				if itemHasFile(&items[i], path) {
					return &items[i]
				}
			}
		}
		return nil
	}

	switch section.Type {
	case "movie":
		if titleHint != "" {
			items, err := p.searchSection(ctx, section.Key, 1, url.Values{"title": {titleHint}})
			if err != nil {
				return nil, err
			}
			if it := match(items); it != nil {
				return it, nil
			}
		}
		if !walk {
			return nil, nil
		}
		return p.scanSection(ctx, section.Key, 1, match)
	case "show":
		if titleHint != "" {
			items, err := p.searchSection(ctx, section.Key, 4, url.Values{"show.title": {titleHint}})
			if err != nil {
				return nil, err
			}
			if it := match(items); it != nil {
				return it, nil
			}
		}
		if !walk {
			return nil, nil
		}
		return p.scanSection(ctx, section.Key, 4, match)
	}
	return nil, nil
}

// plexScanPageSize is the page size for whole-section walks.
const plexScanPageSize = 1000

// scanSection pages through a section until match returns an item.
func (p *plexLibrary) scanSection(ctx context.Context, sectionKey string, plexType int, match func([]plexItem) *plexItem) (*plexItem, error) {
	for start := 0; ; start += plexScanPageSize {
		items, total, err := p.ListPage(ctx, sectionKey, plexType, start, plexScanPageSize)
		if err != nil {
			return nil, err
		}
		if it := match(items); it != nil {
			return it, nil
		}
		if len(items) == 0 || start+len(items) >= total {
			return nil, nil
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
}

// PlayingRatingKeys returns the ratingKeys of items in any Plex session
// (playing, paused or buffering - a paused viewer still holds the item).
func (p *plexLibrary) PlayingRatingKeys(ctx context.Context) (map[string]bool, error) {
	var r plexSessionsResponse
	if err := p.do(ctx, http.MethodGet, "/status/sessions", &r, nil); err != nil {
		return nil, err
	}
	keys := make(map[string]bool, len(r.MediaContainer.Metadata))
	for _, m := range r.MediaContainer.Metadata {
		if m.RatingKey != "" {
			keys[m.RatingKey] = true
		}
	}
	return keys, nil
}

// DeleteMedia removes one version of an item. Plex also deletes the
// version's files from disk if they still exist, so callers must have
// confirmed they are gone. Needs "Allow media deletion" on the server.
// Endpoint verified live 2026-09-15 (Young Harrington, media 409490: 200,
// only the other version left).
func (p *plexLibrary) DeleteMedia(ctx context.Context, ratingKey string, mediaID int64) error {
	return p.do(ctx, http.MethodDelete, fmt.Sprintf("/library/metadata/%s/media/%d", url.PathEscape(ratingKey), mediaID), nil, nil)
}
