package arr

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRootFoldersAndFiles(t *testing.T) {
	var commands []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v3/rootfolder":
			_, _ = w.Write([]byte(`[{"id":1,"path":"/tv"},{"id":2,"path":"/tv2"}]`))
		case r.URL.Path == "/api/v3/series":
			_, _ = w.Write([]byte(`[{"id":7,"title":"Show"},{"id":8,"title":"Other"}]`))
		case r.URL.Path == "/api/v3/episodefile" && r.URL.Query().Get("seriesId") == "7":
			_, _ = w.Write([]byte(`[{"id":70,"path":"/tv/Show/S01E01.mkv"},{"id":71,"path":""}]`))
		case r.URL.Path == "/api/v3/episodefile":
			_, _ = w.Write([]byte(`[]`))
		case r.URL.Path == "/api/v3/movie":
			_, _ = w.Write([]byte(`[{"id":3,"title":"Film","path":"/movies/Film","movieFile":{"id":30,"relativePath":"Film.mkv"}},
				{"id":4,"title":"None","path":"/movies/None"},
				{"id":5,"title":"Full","path":"/movies/Full","movieFile":{"id":50,"path":"/movies/Full/Full.mkv"}}]`))
		case r.URL.Path == "/api/v3/command" && r.Method == http.MethodPost:
			var m map[string]any
			_ = json.NewDecoder(r.Body).Decode(&m)
			commands = append(commands, m)
			w.WriteHeader(http.StatusCreated)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	a := New("sonarr", srv.URL, "k", false, nil, "", "manual")
	ctx := context.Background()

	roots, err := a.RootFolders(ctx)
	if err != nil || len(roots) != 2 || roots[1].Path != "/tv2" {
		t.Fatalf("roots = %v, %v", roots, err)
	}
	eps, err := a.EpisodeFiles(ctx)
	if err != nil || len(eps) != 1 || eps[0] != (MediaFile{Path: "/tv/Show/S01E01.mkv", FileID: 70, ItemID: 7, Title: "Show"}) {
		t.Fatalf("episode files = %v, %v", eps, err)
	}
	movies, err := a.MovieFiles(ctx)
	if err != nil || len(movies) != 2 || movies[0].Path != "/movies/Film/Film.mkv" || movies[1].FileID != 50 || movies[1].ItemID != 5 {
		t.Fatalf("movie files = %v, %v", movies, err)
	}
	if err := a.RescanSeries(ctx, 7); err != nil {
		t.Fatal(err)
	}
	if err := a.RescanMovie(ctx, 3); err != nil {
		t.Fatal(err)
	}
	if len(commands) != 2 || commands[0]["name"] != "RescanSeries" || commands[0]["seriesId"] != float64(7) ||
		commands[1]["name"] != "RescanMovie" {
		t.Fatalf("commands = %v", commands)
	}
}

func TestRootFoldersErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no", http.StatusUnauthorized)
	}))
	defer srv.Close()
	a := New("sonarr", srv.URL, "k", false, nil, "", "manual")
	if _, err := a.RootFolders(context.Background()); err == nil {
		t.Fatal("want an error for a 401")
	}
}
