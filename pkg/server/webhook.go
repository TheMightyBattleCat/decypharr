package server

import (
	"cmp"
	"net/http"
	"path/filepath"
	"strings"

	json "github.com/bytedance/sonic"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

// handleTautulli handles webhooks from Tautulli. When the payload includes a
// tvdb/tmdb id (or a generic media_id), the repair system runs a targeted
// recheck against that specific media — the v2 equivalent of v1's
// "media-id-scoped repair job". When no media id is supplied the webhook
// falls back to a full manual sweep.
func (s *Server) handleTautulli(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload struct {
		Topic   string `json:"topic"`
		Arr     string `json:"arr,omitempty"`
		MediaID string `json:"media_id,omitempty"`
		TvdbID  string `json:"tvdb_id,omitempty"`
		TmdbID  string `json:"tmdb_id,omitempty"`
		Fix     bool   `json:"fix,omitempty"`
	}
	if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&payload); err != nil {
		s.logger.Error().Err(err).Msg("Failed to parse webhook body")
		http.Error(w, "Failed to parse webhook body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if payload.Topic != "tautulli" {
		http.Error(w, "Invalid topic", http.StatusBadRequest)
		return
	}

	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}

	mediaID := strings.TrimSpace(cmp.Or(payload.MediaID, payload.TmdbID, payload.TvdbID))
	if mediaID == "" {
		// No targeting → fall back to a full sweep.
		if _, err := svc.RunNow(manager.RepairRunOptions{}); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}

	run, err := svc.RecheckMedia(s.manager.Context(), strings.TrimSpace(payload.Arr), mediaID, payload.Fix)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "already running") {
			status = http.StatusConflict
		}
		http.Error(w, err.Error(), status)
		return
	}

	if run != nil {
		s.logger.Info().
			Str("run_id", run.ID).
			Str("arr", payload.Arr).
			Str("media_id", mediaID).
			Bool("fix", payload.Fix).
			Msg("Tautulli webhook: media recheck triggered")
	}
	w.WriteHeader(http.StatusOK)
}

// arrWebhookFile is the shape of Sonarr's episodeFile / Radarr's movieFile
// block. Only the fields the cleanup path needs are decoded; everything
// else in the real payload (quality, media info, ids, ...) is ignored.
type arrWebhookFile struct {
	RelativePath string `json:"relativePath,omitempty"`
	Path         string `json:"path,omitempty"`
}

// arrWebhookPayload covers both Sonarr's and Radarr's webhook notification
// shape with one struct: the two Arrs use different field names
// (episodeFile/series vs movieFile/movie) for the same concepts, so both are
// declared as optional and file()/rootPath() pick whichever is present.
type arrWebhookPayload struct {
	EventType    string `json:"eventType"`
	InstanceName string `json:"instanceName,omitempty"`
	DownloadID   string `json:"downloadId,omitempty"`
	DeleteReason string `json:"deleteReason,omitempty"`

	Series *struct {
		Path string `json:"path,omitempty"`
	} `json:"series,omitempty"`
	Movie *struct {
		FolderPath string `json:"folderPath,omitempty"`
	} `json:"movie,omitempty"`

	EpisodeFile *arrWebhookFile `json:"episodeFile,omitempty"`
	MovieFile   *arrWebhookFile `json:"movieFile,omitempty"`
}

// file returns whichever of episodeFile/movieFile the payload carries.
func (p *arrWebhookPayload) file() *arrWebhookFile {
	if p.EpisodeFile != nil {
		return p.EpisodeFile
	}
	return p.MovieFile
}

// arrWebhookWarnOnce logs the unauthenticated-endpoint warning at most once
// per process, the first time the endpoint is actually hit with no token
// configured - not at startup, since config can change without a restart via
// /api/config, and re-checking here always reflects the current value.
var arrWebhookWarnedUnauthenticated bool

// handleRefreshWebhookToken generates a new Arr webhook token and saves it,
// for the settings page's Generate/Refresh Token action.
func (s *Server) handleRefreshWebhookToken(w http.ResponseWriter, _ *http.Request) {
	token, err := s.refreshWebhookToken()
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to refresh Arr webhook token")
		http.Error(w, "Failed to refresh token: "+err.Error(), http.StatusInternalServerError)
		return
	}

	utils.JSONResponse(w, map[string]any{
		"token":   token,
		"message": "Webhook token refreshed successfully",
	}, http.StatusOK)
}

// refreshWebhookToken generates a new Arr webhook token and saves it.
// Unlike the API token (stored in a separate auth file), WebhookToken lives
// directly on the main Config struct, so this loads the live config, sets
// only the token field, and saves immediately - deliberately not routing
// through handleUpdateConfig's whole-config decode/save path, which would
// reintroduce the same round-trip risk this field was already preserved
// against.
func (s *Server) refreshWebhookToken() (string, error) {
	token, err := s.generateAPIToken()
	if err != nil {
		return "", err
	}

	cfg := config.Get()
	cfg.WebhookToken = token
	if err := cfg.Save(); err != nil {
		return "", err
	}

	return token, nil
}

// handleArrWebhook handles Sonarr/Radarr's built-in Webhook notification.
// When a file is deleted or replaced by an upgrade, the matching usenet
// download is torn down immediately instead of waiting for the next
// scheduled sweep to infer it - see Manager.HandleArrWebhookCleanup for the
// actual cleanup and its idempotency/usenet-only guarantees.
//
// This endpoint is entirely optional: Sonarr/Radarr have no obligation to
// call it, and decypharr's existing scheduled cleanup is completely
// unaffected by whether it's configured. Like handleTautulli, it is
// registered outside the auth-required route group (see server.go) since
// Sonarr/Radarr's Webhook connection type can't supply decypharr's normal
// session/API-token auth - only an optional ?token= query parameter, checked
// here rather than by the shared authMiddleware.
func (s *Server) handleArrWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if token := config.Get().WebhookToken; token != "" {
		if r.URL.Query().Get("token") != token {
			http.Error(w, "Invalid or missing token", http.StatusUnauthorized)
			return
		}
	} else if !arrWebhookWarnedUnauthenticated {
		arrWebhookWarnedUnauthenticated = true
		s.logger.Warn().Msg("Arr webhook: no webhook_token configured - /webhooks/arr accepts any request")
	}

	var payload arrWebhookPayload
	if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&payload); err != nil {
		s.logger.Error().Err(err).Msg("Arr webhook: failed to parse payload")
		http.Error(w, "Failed to parse webhook body: "+err.Error(), http.StatusBadRequest)
		return
	}

	switch payload.EventType {
	case "Test":
		// Sonarr/Radarr send this from the "Test" button in the UI and
		// refuse to save the notification unless it gets a 200 back.
		w.WriteHeader(http.StatusOK)
		return
	case "MovieFileDelete", "EpisodeFileDelete":
		// Handled below.
	default:
		// Out of scope for v1 (MovieDelete/SeriesDelete carry no downloadId
		// or single file to act on; Download/Rename/etc. aren't cleanup
		// events at all). The scheduled sweep covers these regardless.
		s.logger.Debug().Str("event_type", payload.EventType).Msg("Arr webhook: event type not handled, ignoring")
		w.WriteHeader(http.StatusOK)
		return
	}

	var fileName string
	if f := payload.file(); f != nil {
		fileName = filepath.Base(cmp.Or(f.RelativePath, f.Path))
	}

	action, entryName, matchedBy := s.manager.HandleArrWebhookCleanup(manager.ArrWebhookEvent{
		DownloadID:   payload.DownloadID,
		DeleteReason: payload.DeleteReason,
		FileName:     fileName,
	})

	matchedByLog := matchedBy
	if matchedBy == "downloadId" {
		matchedByLog = payload.DownloadID
	} else if matchedBy == "filename" {
		matchedByLog = "path-matched"
	}

	s.logger.Info().
		Str("arr_event_type", payload.EventType).
		Str("arr_delete_reason", payload.DeleteReason).
		Str("arr_matched_by", matchedByLog).
		Str("arr_entry_name", entryName).
		Str("arr_file_name", fileName).
		Str("arr_action", action).
		Msg("Arr webhook: processed")

	w.WriteHeader(http.StatusOK)
}
