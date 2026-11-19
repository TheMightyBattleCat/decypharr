package arr

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
)

// A season pack has one grab record per episode: the broken episode's own
// record is preferred, any record of the grab otherwise, and 0 when the Arr
// has none for the download ID.
func TestFindGrabHistoryIDForDownload(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	records := map[string][]HistoryRecord{
		"pack": {
			{ID: 11, DownloadID: "PACK", EpisodeID: 101},
			{ID: 12, DownloadID: "PACK", EpisodeID: 102},
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v3/history" || r.URL.Query().Get("eventType") != "1" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(HistorySchema{Records: records[r.URL.Query().Get("downloadId")]})
	}))
	defer srv.Close()
	a := New("sonarr", srv.URL, "token", false, nil, "", "")
	a.Type = Sonarr

	for _, c := range []struct {
		name     string
		download string
		episode  int
		want     int
	}{
		{"episode's own record", "pack", 102, 12},
		{"episode not in the grab: any record of it", "pack", 999, 11},
		{"no record for the download", "other", 101, 0},
		{"no download ID", "", 101, 0},
	} {
		if got := a.FindGrabHistoryIDForDownload(c.download, c.episode); got != c.want {
			t.Errorf("%s: got %d, want %d", c.name, got, c.want)
		}
	}
}
