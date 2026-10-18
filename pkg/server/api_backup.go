package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/backup"
)

// backupService is the backup service the handlers call. It is never nil:
// until the real service is installed it answers ErrNotImplemented.
func (s *Server) backupService() backup.Service {
	return s.manager.Backup()
}

// preAction takes a snapshot before a risky action (spec: "Automatic
// snapshot before risky actions"). It returns backup.ErrPreActionRefused when
// the action's setting says to refuse after a failed snapshot; any other
// failure is logged and returned for information, and the action carries on.
func (s *Server) preAction(ctx context.Context, action string) error {
	id, err := backup.PreAction(ctx, s.backupService(), action)
	if err != nil {
		if errors.Is(err, backup.ErrPreActionRefused) {
			s.logger.Error().Err(err).Str("action", action).Msg("Backup: pre-action snapshot failed, refusing the action")
		} else {
			s.logger.Error().Err(err).Str("action", action).Msg("Backup: pre-action snapshot failed, continuing with the action")
		}
		return err
	}
	if id != "" {
		s.logger.Info().Str("action", action).Str("snapshot", id).Msg("Backup: pre-action snapshot taken")
	}
	return nil
}

// preActionRefused runs preAction and, when the action is refused, answers
// 409 and reports true so the handler stops. Other errors do not stop it.
func (s *Server) preActionRefused(w http.ResponseWriter, r *http.Request, action string) bool {
	err := s.preAction(r.Context(), action)
	if !errors.Is(err, backup.ErrPreActionRefused) {
		return false
	}
	utils.JSONResponse(w, map[string]string{
		"error": "The action was not run because the safety snapshot before it failed: " + err.Error(),
	}, http.StatusConflict)
	return true
}

// writeBackupError maps a backup error to a status code.
func (s *Server) writeBackupError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, backup.ErrNotImplemented):
		status = http.StatusNotImplemented
	case errors.Is(err, backup.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, backup.ErrInvalid):
		status = http.StatusBadRequest
	case errors.Is(err, backup.ErrBusy), errors.Is(err, backup.ErrNotConfigured):
		status = http.StatusConflict
	default:
		s.logger.Error().Err(err).Msg("Backup request failed")
	}
	utils.JSONResponse(w, map[string]string{"error": err.Error()}, status)
}

// backupReply writes v as JSON, or the error.
func (s *Server) backupReply(w http.ResponseWriter, v any, err error) {
	if err != nil {
		s.writeBackupError(w, err)
		return
	}
	utils.JSONResponse(w, v, http.StatusOK)
}

// decodeBackupBody decodes an optional JSON body into v. An empty body is
// fine; a malformed one answers 400 and returns false.
func (s *Server) decodeBackupBody(w http.ResponseWriter, r *http.Request, v any) bool {
	if r.Body == nil || r.ContentLength == 0 {
		return true
	}
	if err := json.NewDecoder(r.Body).Decode(v); err != nil && !errors.Is(err, io.EOF) {
		utils.JSONResponse(w, map[string]string{"error": "invalid request body: " + err.Error()}, http.StatusBadRequest)
		return false
	}
	return true
}

func (s *Server) handleBackupStatus(w http.ResponseWriter, r *http.Request) {
	res, err := s.backupService().Status(r.Context())
	s.backupReply(w, res, err)
}

// handleBackupReleaseHold ends the post-restore hold. The Service contract has
// no such method, so the real service is reached through backup.HoldReleaser.
func (s *Server) handleBackupReleaseHold(w http.ResponseWriter, r *http.Request) {
	rel, ok := s.backupService().(backup.HoldReleaser)
	if !ok {
		s.writeBackupError(w, backup.ErrNotImplemented)
		return
	}
	if err := rel.ReleaseHold(); err != nil {
		s.writeBackupError(w, err)
		return
	}
	utils.JSONResponse(w, map[string]bool{"post_restore_hold": rel.HoldActive()}, http.StatusOK)
}

func (s *Server) handleBackupListSnapshots(w http.ResponseWriter, r *http.Request) {
	res, err := s.backupService().ListSnapshots(r.Context())
	if err == nil && res == nil {
		res = []backup.SnapshotInfo{}
	}
	s.backupReply(w, res, err)
}

func (s *Server) handleBackupGetSnapshot(w http.ResponseWriter, r *http.Request) {
	res, err := s.backupService().GetSnapshot(r.Context(), chi.URLParam(r, "id"))
	s.backupReply(w, res, err)
}

func (s *Server) handleBackupRunJob(w http.ResponseWriter, r *http.Request) {
	res, err := s.backupService().RunJob(r.Context(), chi.URLParam(r, "name"))
	if err == nil {
		utils.JSONResponse(w, res, http.StatusAccepted)
		return
	}
	s.writeBackupError(w, err)
}

func (s *Server) handleBackupVerify(w http.ResponseWriter, r *http.Request) {
	full := r.URL.Query().Get("full") == "true"
	res, err := s.backupService().VerifySnapshot(r.Context(), chi.URLParam(r, "id"), full)
	s.backupReply(w, res, err)
}

func (s *Server) handleBackupDownload(w http.ResponseWriter, r *http.Request) {
	comp := backup.Component(r.URL.Query().Get("component"))
	if comp != backup.ComponentDB && comp != backup.ComponentConfig && comp != backup.ComponentLinks {
		s.writeBackupError(w, fmt.Errorf("%w: component must be db, config or links", backup.ErrInvalid))
		return
	}
	res, err := s.backupService().Download(r.Context(), chi.URLParam(r, "id"), comp)
	if err != nil {
		s.writeBackupError(w, err)
		return
	}
	if res.Body == nil {
		s.writeBackupError(w, backup.ErrNotFound)
		return
	}
	defer res.Body.Close()
	if res.ContentType == "" {
		res.ContentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", res.ContentType)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", res.Filename))
	_, _ = io.Copy(w, res.Body)
}

func (s *Server) handleBackupDeleteSnapshot(w http.ResponseWriter, r *http.Request) {
	if err := s.backupService().DeleteSnapshot(r.Context(), chi.URLParam(r, "id")); err != nil {
		s.writeBackupError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleBackupRestoreDB(w http.ResponseWriter, r *http.Request) {
	res, err := s.backupService().RestoreDB(r.Context(), chi.URLParam(r, "id"))
	s.backupReply(w, res, err)
}

func (s *Server) handleBackupRestoreNZB(w http.ResponseWriter, r *http.Request) {
	var req backup.RestoreNZBRequest
	if !s.decodeBackupBody(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.Entry) == "" {
		s.writeBackupError(w, fmt.Errorf("%w: entry is required", backup.ErrInvalid))
		return
	}
	res, err := s.backupService().RestoreNZB(r.Context(), chi.URLParam(r, "id"), req)
	s.backupReply(w, res, err)
}

func (s *Server) handleBackupLinksPlan(w http.ResponseWriter, r *http.Request) {
	var req backup.LinksPlanRequest
	if !s.decodeBackupBody(w, r, &req) {
		return
	}
	res, err := s.backupService().PlanLinksRestore(r.Context(), chi.URLParam(r, "id"), req)
	s.backupReply(w, res, err)
}

func (s *Server) handleBackupLinksApply(w http.ResponseWriter, r *http.Request) {
	var req backup.LinksApplyRequest
	if !s.decodeBackupBody(w, r, &req) {
		return
	}
	if req.PlanID == "" {
		s.writeBackupError(w, fmt.Errorf("%w: plan_id is required", backup.ErrInvalid))
		return
	}
	res, err := s.backupService().ApplyLinksRestore(r.Context(), chi.URLParam(r, "id"), req)
	s.backupReply(w, res, err)
}

func (s *Server) handleBackupConfigHistory(w http.ResponseWriter, r *http.Request) {
	res, err := s.backupService().ListConfigHistory(r.Context())
	if err == nil && res == nil {
		res = []backup.ConfigVersion{}
	}
	s.backupReply(w, res, err)
}

func (s *Server) handleBackupConfigDiff(w http.ResponseWriter, r *http.Request) {
	against := r.URL.Query().Get("against")
	if against == "" {
		against = "previous"
	}
	res, err := s.backupService().DiffConfigHistory(r.Context(), chi.URLParam(r, "id"), against)
	s.backupReply(w, res, err)
}

func (s *Server) handleBackupConfigRevert(w http.ResponseWriter, r *http.Request) {
	res, err := s.backupService().RevertConfig(r.Context(), chi.URLParam(r, "id"))
	if err == nil && res.RestartRequired {
		// The same restart a settings save makes when it changes a field that
		// needs one.
		go s.Restart()
	}
	s.backupReply(w, res, err)
}

func (s *Server) handleBackupListRemoved(w http.ResponseWriter, r *http.Request) {
	q := backup.RemovedQuery{
		Name:     r.URL.Query().Get("name"),
		Category: r.URL.Query().Get("category"),
	}
	for _, p := range []struct {
		key string
		dst *time.Time
	}{{"since", &q.Since}, {"until", &q.Until}} {
		if v := r.URL.Query().Get(p.key); v != "" {
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				s.writeBackupError(w, fmt.Errorf("%w: %s must be an RFC 3339 time", backup.ErrInvalid, p.key))
				return
			}
			*p.dst = t
		}
	}
	res, err := s.backupService().ListRemoved(r.Context(), q)
	if err == nil && res == nil {
		res = []backup.RemovedEntry{}
	}
	s.backupReply(w, res, err)
}

func (s *Server) handleBackupRestoreRemoved(w http.ResponseWriter, r *http.Request) {
	var req backup.RestoreRemovedRequest
	if !s.decodeBackupBody(w, r, &req) {
		return
	}
	res, err := s.backupService().RestoreRemoved(r.Context(), chi.URLParam(r, "key"), req)
	s.backupReply(w, res, err)
}

func (s *Server) handleBackupRetentionPreview(w http.ResponseWriter, r *http.Request) {
	var req backup.RetentionPreviewRequest
	if !s.decodeBackupBody(w, r, &req) {
		return
	}
	res, err := s.backupService().RetentionPreview(r.Context(), req)
	s.backupReply(w, res, err)
}

// scheduleCheckResponse is the reply of POST /api/backups/schedule/check.
type scheduleCheckResponse struct {
	Valid bool        `json:"valid"`
	Error string      `json:"error,omitempty"`
	Next  []time.Time `json:"next,omitempty"`
}

// handleBackupScheduleCheck validates a schedule string ({"schedule": "..."})
// and returns its next three run times, for the Settings page's live check.
// It needs no service, only the parser the scheduler uses.
func (s *Server) handleBackupScheduleCheck(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Schedule string `json:"schedule"`
	}
	if !s.decodeBackupBody(w, r, &req) {
		return
	}
	next, err := utils.NextScheduleRuns(req.Schedule, time.Now(), 3)
	if err != nil {
		utils.JSONResponse(w, scheduleCheckResponse{Error: err.Error()}, http.StatusOK)
		return
	}
	utils.JSONResponse(w, scheduleCheckResponse{Valid: true, Next: next}, http.StatusOK)
}
