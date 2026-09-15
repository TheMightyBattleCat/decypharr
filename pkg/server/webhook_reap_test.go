package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	json "github.com/bytedance/sonic"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

// radarrUpgradePayload mirrors Radarr's WebhookImportPayload for the
// Young Harrington upgrade seen live on 2026-09-15 (camelCase fields,
// deletedFiles holding the replaced file).
const radarrUpgradePayload = `{
  "movie": {"id": 5440, "title": "Young Harrington", "year": 2026, "folderPath": "/data/media/MoviesHD/Young Harrington (2026) {imdb-tt00000017}", "tmdbId": 1234, "imdbId": "tt00000017"},
  "remoteMovie": {"tmdbId": 1234, "title": "Young Harrington"},
  "movieFile": {"id": 99, "relativePath": "Young Harrington (2026) {imdb-tt00000017} [Bluray-1080p][AC3 5.1][x264]-knives.mkv", "path": "/data/media/MoviesHD/Young Harrington (2026) {imdb-tt00000017}/Young Harrington (2026) {imdb-tt00000017} [Bluray-1080p][AC3 5.1][x264]-knives.mkv", "quality": "Bluray-1080p", "size": 13898381950, "sceneName": "young.harrington.2026.1080p.bluray.x264-knives"},
  "isUpgrade": true,
  "downloadClient": "decyphar - nzb - new",
  "downloadClientType": "SABnzbd",
  "downloadId": "76c09b18-dc9b-4efa-8366-2d885b0707b8",
  "deletedFiles": [{"id": 98, "relativePath": "Young Harrington (2026) {imdb-tt00000017} [AMZN][WEBDL-1080p][EAC3 5.1][h264]-playWEB.mkv", "path": "/data/media/MoviesHD/Young Harrington (2026) {imdb-tt00000017}/Young Harrington (2026) {imdb-tt00000017} [AMZN][WEBDL-1080p][EAC3 5.1][h264]-playWEB.mkv", "quality": "WEBDL-1080p", "size": 7644809933}],
  "eventType": "Download",
  "instanceName": "Radarr",
  "applicationUrl": ""
}`

const sonarrRenamePayload = `{
  "series": {"id": 524, "title": "3 Moon Paradox", "path": "/data/media/TV Shows_new/3 Moon Paradox", "tvdbId": 100002},
  "renamedEpisodeFiles": [
    {"id": 1, "relativePath": "Season 1/3 Moon Paradox - S01E01 - Countdown WEBDL-1080p.mkv", "path": "/data/media/TV Shows_new/3 Moon Paradox/Season 1/3 Moon Paradox - S01E01 - Countdown WEBDL-1080p.mkv", "previousRelativePath": "Season 1/3 Moon Paradox - S01E01 - Countdown WEBRip-1080p Proper.mkv", "previousPath": "/data/media/TV Shows_new/3 Moon Paradox/Season 1/3 Moon Paradox - S01E01 - Countdown WEBRip-1080p Proper.mkv"},
    {"id": 2, "relativePath": "Season 1/x.mkv", "path": "/same.mkv", "previousPath": "/same.mkv"}
  ],
  "eventType": "Rename",
  "instanceName": "Sonarr"
}`

func decodePayload(t *testing.T, body string) arrWebhookPayload {
	t.Helper()
	var p arrWebhookPayload
	if err := json.ConfigDefault.Unmarshal([]byte(body), &p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestReapNotices_RadarrUpgrade(t *testing.T) {
	p := decodePayload(t, radarrUpgradePayload)
	notices, nudge := p.reapNotices()
	if len(notices) != 1 {
		t.Fatalf("notices = %+v, want 1", notices)
	}
	n := notices[0]
	if n.Source != manager.ReapSourceUpgrade || len(n.StalePaths) != 1 ||
		!strings.HasSuffix(n.StalePaths[0], "-playWEB.mkv") || !strings.HasSuffix(n.CurrentPath, "-knives.mkv") ||
		n.TitleHint != "Young Harrington" || n.MediaID != 5440 {
		t.Fatalf("notice = %+v", n)
	}
	if nudge != "/data/media/MoviesHD/Young Harrington (2026) {imdb-tt00000017}" {
		t.Fatalf("nudge = %q, want the movie folder", nudge)
	}
}

func TestReapNotices_ImportWithoutUpgradeOnlyNudges(t *testing.T) {
	p := decodePayload(t, strings.Replace(radarrUpgradePayload, `"isUpgrade": true`, `"isUpgrade": false`, 1))
	notices, nudge := p.reapNotices()
	if len(notices) != 0 || nudge == "" {
		t.Fatalf("notices=%+v nudge=%q, want no notices and a nudge", notices, nudge)
	}
}

func TestReapNotices_SonarrRename(t *testing.T) {
	p := decodePayload(t, sonarrRenamePayload)
	notices, nudge := p.reapNotices()
	if len(notices) != 1 {
		t.Fatalf("notices = %+v, want 1 (the unchanged file skipped)", notices)
	}
	if n := notices[0]; n.Source != manager.ReapSourceRename || !strings.HasSuffix(n.StalePaths[0], "WEBRip-1080p Proper.mkv") || n.MediaID != 524 {
		t.Fatalf("notice = %+v", n)
	}
	if nudge != "" {
		t.Fatalf("rename nudged %q", nudge)
	}
}

// A Download/Rename event must be accepted even with no reaper wired (nil
// manager), and a file delete must not reach the teardown while it is off.
func TestHandleArrWebhook_ReapEventsAndTeardownGate(t *testing.T) {
	s := newTestServerForWebhook()
	config.Get().WebhookToken = ""
	config.Get().ArrWebhookTeardown = false
	for _, body := range []string{radarrUpgradePayload, sonarrRenamePayload, `{"eventType":"MovieFileDelete","deleteReason":"upgrade","movieFile":{"path":"/x.mkv"}}`} {
		req := httptest.NewRequest(http.MethodPost, "/webhooks/arr", strings.NewReader(body))
		rec := httptest.NewRecorder()
		s.handleArrWebhook(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d for %.40s", rec.Code, body)
		}
	}
}
