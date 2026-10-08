package arr

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
)

func manualImportServer(t *testing.T, candidates string, commands *[]map[string]any) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v3/manualimport":
			_, _ = w.Write([]byte(candidates))
		case r.URL.Path == "/api/v3/command" && r.Method == http.MethodPost:
			var m map[string]any
			_ = json.NewDecoder(r.Body).Decode(&m)
			*commands = append(*commands, m)
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{}`))
		default:
			http.NotFound(w, r)
		}
	}))
}

func TestImportRadarrSendsMovieAndKeepsQuality(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	var commands []map[string]any
	srv := manualImportServer(t, `[{"path":"/dl/Film.mkv","folderName":"Film","movie":{"id":9},
		"quality":{"quality":{"id":7,"name":"Bluray-1080p"},"revision":{"version":1},"future":true}}]`, &commands)
	defer srv.Close()
	a := New("radarr", srv.URL, "k", false, nil, "", "manual")

	if _, err := a.Import("abc"); err != nil {
		t.Fatalf("Import: %v", err)
	}
	if len(commands) != 1 {
		t.Fatalf("commands = %d, want 1", len(commands))
	}
	file := commands[0]["files"].([]any)[0].(map[string]any)
	if file["movieId"] != float64(9) {
		t.Errorf("movieId = %v, want 9", file["movieId"])
	}
	for _, key := range []string{"seriesId", "seasonNumber", "episodeIds", "releaseType"} {
		if _, ok := file[key]; ok {
			t.Errorf("%s was sent for a movie", key)
		}
	}
	if file["quality"].(map[string]any)["future"] != true {
		t.Errorf("quality was not passed through unchanged: %v", file["quality"])
	}
}

func TestImportRefusesCandidatesItCannotSubmit(t *testing.T) {
	for name, candidates := range map[string]string{
		"nothing found": `[]`,
		"no movie":      `[{"path":"/dl/Film.mkv","quality":{"quality":{"id":7}}}]`,
		"no quality":    `[{"path":"/dl/Film.mkv","movie":{"id":9}}]`,
	} {
		t.Run(name, func(t *testing.T) {
			config.SetConfigPath(t.TempDir())
			var commands []map[string]any
			srv := manualImportServer(t, candidates, &commands)
			defer srv.Close()
			a := New("radarr", srv.URL, "k", false, nil, "", "manual")

			if _, err := a.Import("abc"); err == nil {
				t.Fatal("Import succeeded, want an error")
			}
			if len(commands) != 0 {
				t.Fatalf("a command was submitted: %v", commands)
			}
		})
	}
}
