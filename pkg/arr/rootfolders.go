package arr

import (
	"context"
	"fmt"
	"net/http"
	"path"
)

// RootFolder is one root folder an Arr manages, as the Arr names it (in the
// Arr's own path namespace).
type RootFolder struct {
	ID   int    `json:"id"`
	Path string `json:"path"`
}

// MediaFile is one file an Arr has imported: an episode file for Sonarr, a
// movie file for Radarr. Path is in the Arr's own namespace.
type MediaFile struct {
	Path string `json:"path"`
	// FileID is the episodefile / moviefile id.
	FileID int `json:"fileId"`
	// ItemID is the series or movie id.
	ItemID int    `json:"itemId"`
	Title  string `json:"title,omitempty"`
}

func (a *Arr) getJSON(ctx context.Context, endpoint string, res any) error {
	resp, err := a.RequestCtx(ctx, http.MethodGet, endpoint, nil, res)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s: %s returned %d", a.Name, endpoint, resp.StatusCode)
	}
	return nil
}

// RootFolders lists the Arr's root folders.
func (a *Arr) RootFolders(ctx context.Context) ([]RootFolder, error) {
	var roots []RootFolder
	if err := a.getJSON(ctx, "api/v3/rootfolder", &roots); err != nil {
		return nil, err
	}
	return roots, nil
}

// RescanSeries asks Sonarr to re-read a series' folder.
func (a *Arr) RescanSeries(ctx context.Context, seriesID int) error {
	return a.command(ctx, map[string]any{"name": "RescanSeries", "seriesId": seriesID})
}

// RescanMovie asks Radarr to re-read a movie's folder.
func (a *Arr) RescanMovie(ctx context.Context, movieID int) error {
	return a.command(ctx, map[string]any{"name": "RescanMovie", "movieIds": []int{movieID}})
}

func (a *Arr) command(ctx context.Context, payload map[string]any) error {
	resp, err := a.RequestCtx(ctx, http.MethodPost, "api/v3/command", payload, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s: command %v returned %d", a.Name, payload["name"], resp.StatusCode)
	}
	return nil
}

// EpisodeFiles lists every episode file Sonarr knows, with its series.
func (a *Arr) EpisodeFiles(ctx context.Context) ([]MediaFile, error) {
	var series []struct {
		ID    int    `json:"id"`
		Title string `json:"title"`
	}
	if err := a.getJSON(ctx, "api/v3/series", &series); err != nil {
		return nil, err
	}
	var out []MediaFile
	for _, s := range series {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var files []struct {
			ID   int    `json:"id"`
			Path string `json:"path"`
		}
		if err := a.getJSON(ctx, fmt.Sprintf("api/v3/episodefile?seriesId=%d", s.ID), &files); err != nil {
			return nil, err
		}
		for _, f := range files {
			if f.Path == "" {
				continue
			}
			out = append(out, MediaFile{Path: f.Path, FileID: f.ID, ItemID: s.ID, Title: s.Title})
		}
	}
	return out, nil
}

// MovieFiles lists every movie file Radarr knows, with its movie.
func (a *Arr) MovieFiles(ctx context.Context) ([]MediaFile, error) {
	var movies []Movie
	if err := a.getJSON(ctx, "api/v3/movie", &movies); err != nil {
		return nil, err
	}
	var out []MediaFile
	for _, m := range movies {
		p := m.MovieFile.Path
		if p == "" && m.MovieFile.RelativePath != "" && m.Path != "" {
			p = path.Join(m.Path, m.MovieFile.RelativePath)
		}
		if p == "" || m.MovieFile.Id == 0 {
			continue
		}
		out = append(out, MediaFile{Path: p, FileID: m.MovieFile.Id, ItemID: m.Id, Title: m.Title})
	}
	return out, nil
}
