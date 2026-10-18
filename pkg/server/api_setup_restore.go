package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/backup"
)

type setupRestoreRequest struct {
	// Dir is the backup folder.
	Dir        string `json:"dir"`
	SnapshotID string `json:"snapshot_id,omitempty"`
}

type setupRestoreResponse struct {
	Snapshots       []backup.SnapshotInfo `json:"snapshots,omitempty"`
	SnapshotID      string                `json:"snapshot_id,omitempty"`
	Restored        map[string]int        `json:"restored,omitempty"`
	Warnings        []string              `json:"warnings,omitempty"`
	RestartRequired bool                  `json:"restart_required,omitempty"`
}

// setupRestoreOpen checks the wizard is still open and opens the repository.
// Setup pages are public, so every restore call is refused once setup is done.
func (s *Server) setupRestoreOpen(w http.ResponseWriter, r *http.Request) (*setupRestoreRequest, *backup.FileRepo, bool) {
	if err := config.Get().SetupComplete(); err == nil {
		http.Error(w, "forbidden", http.StatusForbidden)
		return nil, nil, false
	}
	var req setupRestoreRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
		s.sendSetupError(w, "Invalid request format", err)
		return nil, nil, false
	}
	req.Dir = strings.TrimSpace(req.Dir)
	if req.Dir == "" {
		s.sendSetupError(w, "Backup folder is required", nil)
		return nil, nil, false
	}
	repo, err := backup.OpenExistingRepo(req.Dir)
	if err != nil {
		s.sendSetupError(w, "Could not open the backup folder", err)
		return nil, nil, false
	}
	return &req, repo, true
}

func writeSetupJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// setupRestoreListHandler lists the snapshots in a backup folder.
func (s *Server) setupRestoreListHandler(w http.ResponseWriter, r *http.Request) {
	_, repo, ok := s.setupRestoreOpen(w, r)
	if !ok {
		return
	}
	list, err := repo.ListSnapshots(r.Context())
	if err != nil {
		s.sendSetupError(w, "Could not list snapshots", err)
		return
	}
	sort.Slice(list, func(i, j int) bool { return list[i].CreatedAt.After(list[j].CreatedAt) })
	writeSetupJSON(w, setupRestoreResponse{Snapshots: list})
}

// setupRestoreHandler restores config, state, hearsay key, NZB records and
// overlay into the data folder and stages the database; the swap happens at
// the next start, which also turns the post-restore hold on.
func (s *Server) setupRestoreHandler(w http.ResponseWriter, r *http.Request) {
	if err := config.Get().SetupComplete(); err == nil {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if !setupRestoreMu.TryLock() {
		http.Error(w, "another restore is in progress", http.StatusConflict)
		return
	}
	defer setupRestoreMu.Unlock()
	req, repo, ok := s.setupRestoreOpen(w, r)
	if !ok {
		return
	}
	id, err := backup.ResolveSnapshotID(r.Context(), repo, req.SnapshotID)
	if err != nil {
		s.sendSetupError(w, "Could not pick a snapshot", err)
		return
	}
	rep, err := backup.RestoreToDataDir(r.Context(), repo, backup.RestoreOptions{
		SnapshotID: id, DataDir: config.GetMainPath(),
	})
	if err != nil {
		s.sendSetupError(w, "Restore failed", err)
		return
	}
	out := setupRestoreResponse{SnapshotID: rep.SnapshotID, Warnings: rep.Warnings, Restored: map[string]int{}, RestartRequired: true}
	for c, n := range rep.Restored {
		out.Restored[string(c)] = n
	}
	writeSetupJSON(w, out)
}

// setupUploadLimit caps an uploaded archive. A variable so tests can lower it.
var setupUploadLimit int64 = 16 << 30

// setupRestoreMu serialises the folder restore and the upload restore, so
// two requests cannot both pass the pending-restore check and write into the
// data folder together.
var setupRestoreMu sync.Mutex

// setupRestoreUploadHandler restores downloaded .tar.zst archives (the
// multipart field "file", repeated for each archive) into the data folder in
// one request, the database archive first and only once every archive has
// been checked. Each upload is streamed to a temporary file inside the data
// folder, never held in memory, and the files are removed afterwards. As with the folder restore, a database in the archive is only
// staged; the swap happens at the next start.
func (s *Server) setupRestoreUploadHandler(w http.ResponseWriter, r *http.Request) {
	if err := config.Get().SetupComplete(); err == nil {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if !setupRestoreMu.TryLock() {
		http.Error(w, "another restore is in progress", http.StatusConflict)
		return
	}
	defer setupRestoreMu.Unlock()

	r.Body = http.MaxBytesReader(w, r.Body, setupUploadLimit)
	mr, err := r.MultipartReader()
	if err != nil {
		s.sendSetupError(w, "Invalid upload", err)
		return
	}
	dataDir := config.GetMainPath()
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		s.sendSetupError(w, "Could not prepare the data folder", err)
		return
	}
	var files []string
	defer func() {
		for _, f := range files {
			_ = os.Remove(f)
		}
	}()
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			s.sendUploadError(w, "Upload failed", err)
			return
		}
		if part.FormName() != "file" {
			_ = part.Close()
			continue
		}
		tmp, err := os.CreateTemp(dataDir, ".setup-upload-*.tar.zst")
		if err != nil {
			s.sendSetupError(w, "Could not store the upload", err)
			return
		}
		files = append(files, tmp.Name())
		_, cerr := io.Copy(tmp, part)
		if err := tmp.Close(); cerr == nil {
			cerr = err
		}
		if cerr != nil {
			s.sendUploadError(w, "Upload failed", cerr)
			return
		}
	}
	if len(files) == 0 {
		s.sendSetupError(w, "Choose a backup file to upload", nil)
		return
	}

	rep, err := backup.RestoreFromArchiveFiles(r.Context(), files, backup.ArchiveRestoreOptions{DataDir: dataDir})
	if err != nil {
		s.sendSetupError(w, "Restore failed", err)
		return
	}
	out := setupRestoreResponse{SnapshotID: rep.SnapshotID, Warnings: rep.Warnings, Restored: map[string]int{}, RestartRequired: true}
	for c, n := range rep.Restored {
		out.Restored[string(c)] = n
	}
	writeSetupJSON(w, out)
}

func (s *Server) sendUploadError(w http.ResponseWriter, message string, err error) {
	var tooBig *http.MaxBytesError
	if errors.As(err, &tooBig) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		_ = json.NewEncoder(w).Encode(SetupWizardResponse{Error: "That file is larger than the upload limit"})
		return
	}
	s.sendSetupError(w, message, err)
}
