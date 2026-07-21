// Handlers for the overlay management GUI: introspection (read-only) and
// action (write) endpoints over the playback-padding/PAR2-patch state
// tracked by pkg/usenet/overlay. Mirrors the existing repair API's patterns
// in api.go (error handling, JSON shapes, chi registration) - see
// handleGetRepairConfig / handleListRepairRuns / handleFixBroken and
// friends.
package server

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	json "github.com/bytedance/sonic"

	"github.com/go-chi/chi/v5"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// OverlaySegmentRun is one contiguous run of same-status segment indices
// (dead, padded, or patched), for rendering a segment-state sparkline
// without the client needing to walk every individual segment.
type OverlaySegmentRun struct {
	Start  int    `json:"start"`
	End    int    `json:"end"`    // inclusive
	Status string `json:"status"` // dead | padded | patched
}

// OverlayRepairStatus is a file's current PAR2-repair pipeline status, as
// derived from the live worker queue (queued/running) and, failing that, the
// most recent persisted Par2RepairAttempt for its NZB.
type OverlayRepairStatus string

const (
	OverlayRepairNone        OverlayRepairStatus = "none"
	OverlayRepairQueued      OverlayRepairStatus = "queued"
	OverlayRepairRunning     OverlayRepairStatus = "running"
	OverlayRepairCompleted   OverlayRepairStatus = "completed"
	OverlayRepairFailed      OverlayRepairStatus = "failed"
	OverlayRepairUnavailable OverlayRepairStatus = "unavailable"
)

// OverlayFile summarizes one logical file's overlay damage/repair state for
// the overlay management GUI.
type OverlayFile struct {
	Entry string `json:"entry"` // entry (release) name
	NzbID string `json:"nzb_id"`
	File  string `json:"file"`

	Verdict string `json:"verdict"` // clean | degraded | failed

	DeadSegments    int `json:"dead_segments"`
	PaddedSegments  int `json:"padded_segments"`
	PatchedSegments int `json:"patched_segments"`
	TotalSegments   int `json:"total_segments,omitempty"`

	DamageByteRatio float64 `json:"damage_byte_ratio"`

	RepairStatus       OverlayRepairStatus `json:"repair_status"`
	RepairStatusReason string              `json:"repair_status_reason,omitempty"`

	Repairable          bool   `json:"repairable"`
	NotRepairableReason string `json:"not_repairable_reason,omitempty"`

	PatchBytes    int64 `json:"patch_bytes"`
	Par2MetaBytes int64 `json:"par2_metadata_bytes"`

	// SegmentRuns covers every recorded segment (dead, padded, AND patched -
	// unlike DeadSegments/PaddedSegments/PatchedSegments' plain counts, this
	// carries position), for a client-side sparkline over the segment space.
	SegmentRuns []OverlaySegmentRun `json:"segment_runs,omitempty"`
}

// segmentRuns collapses segments (sorted by Index, per overlay.Store's
// sortDeadSegments invariant) into contiguous same-status runs - a status
// change (even between adjacent indices) starts a new run, so a sparkline
// can color dead/padded/patched distinctly.
func segmentRuns(segments []overlay.DeadSegment) []OverlaySegmentRun {
	var runs []OverlaySegmentRun
	for _, d := range segments {
		status := string(d.Status)
		if n := len(runs); n > 0 && runs[n-1].Status == status && runs[n-1].End == d.Index-1 {
			runs[n-1].End = d.Index
			continue
		}
		runs = append(runs, OverlaySegmentRun{Start: d.Index, End: d.Index, Status: status})
	}
	return runs
}

// par2MetadataBytes sums the real, decoded sizes of every PAR2-related file
// retained for nzb (index/recovery volumes plus the posted-file layout PAR2
// protects) - real sizes, not estimates, per the NZB parser's Par2Files/
// Par2Source fields.
func par2MetadataBytes(nzb *storage.NZB) int64 {
	if nzb == nil {
		return 0
	}
	var total int64
	for _, f := range nzb.Par2Files {
		total += f.Size
	}
	for _, f := range nzb.Par2Source {
		total += f.Size
	}
	return total
}

// handleListOverlayFiles returns every file with recorded overlay state
// (dead segments and/or patches), across every entry.
func (s *Server) handleListOverlayFiles(w http.ResponseWriter, r *http.Request) {
	u := s.manager.Usenet()
	out := make([]OverlayFile, 0)
	if u == nil {
		utils.JSONResponse(w, out, http.StatusOK)
		return
	}

	nzbIDs, err := u.OverlayListNZBIDs()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	par2Repair := s.manager.Par2Repair()

	for _, nzbID := range nzbIDs {
		// An entry can be deleted while its overlay state lingers on disk
		// (best-effort cleanup elsewhere) - hide those rather than showing
		// damage for something that no longer exists, same convention as
		// handleListEntryHealth's EntryNameHasBackingEntry filter.
		entry, err := s.manager.GetEntry(nzbID)
		if err != nil || entry == nil {
			continue
		}
		manifest, err := u.OverlayManifest(nzbID)
		if err != nil || manifest == nil || len(manifest.Files) == 0 {
			continue
		}

		nzb, _ := u.GetNZB(nzbID)
		metaBytes := par2MetadataBytes(nzb)

		repairable, notRepairableReason := false, "par2 repair worker unavailable"
		if par2Repair != nil {
			repairable, notRepairableReason = par2Repair.Availability(nzbID)
		}
		running := par2Repair != nil && par2Repair.IsRunning(nzbID)
		queued := !running && par2Repair != nil && par2Repair.IsQueued(nzbID)
		var lastAttempt *storage.Par2RepairAttempt
		if !running && !queued {
			lastAttempt, _ = s.manager.Storage().LatestPar2RepairAttemptForNzb(nzbID)
		}

		for file, fe := range manifest.Files {
			if fe == nil {
				continue
			}
			of := OverlayFile{
				Entry:   entry.Name,
				NzbID:   nzbID,
				File:    file,
				Verdict: string(fe.Verdict),
			}

			var fileSize int64
			if nzb != nil {
				if nf := nzb.GetFileByName(file); nf != nil {
					of.TotalSegments = len(nf.Segments)
					fileSize = nf.Size
				}
			}

			var damageBytes int64
			for _, d := range fe.DeadSegments {
				switch d.Status {
				case overlay.StatusDead:
					of.DeadSegments++
					damageBytes += d.Bytes
				case overlay.StatusPadded:
					of.PaddedSegments++
					damageBytes += d.Bytes
				case overlay.StatusPatched:
					of.PatchedSegments++
				}
			}
			if fileSize > 0 {
				of.DamageByteRatio = float64(damageBytes) / float64(fileSize)
			}
			of.SegmentRuns = segmentRuns(fe.DeadSegments)
			of.PatchBytes = u.OverlayFilePatchBytes(nzbID, file, fe)
			of.Par2MetaBytes = metaBytes

			pending := of.DeadSegments+of.PaddedSegments > 0
			of.Repairable = pending && repairable
			if pending && !repairable {
				of.NotRepairableReason = notRepairableReason
			}

			switch {
			case running:
				of.RepairStatus = OverlayRepairRunning
			case queued:
				of.RepairStatus = OverlayRepairQueued
			case pending && !repairable:
				of.RepairStatus = OverlayRepairUnavailable
				of.RepairStatusReason = notRepairableReason
			case lastAttempt != nil && lastAttempt.Outcome == storage.Par2RepairOutcomeCompleted && !pending:
				of.RepairStatus = OverlayRepairCompleted
			case lastAttempt != nil && pending && (lastAttempt.Outcome == storage.Par2RepairOutcomeFailed || lastAttempt.Outcome == storage.Par2RepairOutcomeUnavailable):
				of.RepairStatus = OverlayRepairFailed
				of.RepairStatusReason = lastAttempt.FailReason
			default:
				of.RepairStatus = OverlayRepairNone
			}

			out = append(out, of)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Entry != out[j].Entry {
			return out[i].Entry < out[j].Entry
		}
		return out[i].File < out[j].File
	})
	utils.JSONResponse(w, out, http.StatusOK)
}

// OverlayEntryDiskUsage is one entry's overlay disk-usage breakdown.
type OverlayEntryDiskUsage struct {
	Entry         string `json:"entry"`
	NzbID         string `json:"nzb_id"`
	FileCount     int    `json:"file_count"`
	PatchBytes    int64  `json:"patch_bytes"`
	ManifestBytes int64  `json:"manifest_bytes"`
	Par2MetaBytes int64  `json:"par2_metadata_bytes"`
}

// OverlayDiskUsageResponse is the aggregate overlay disk-usage report.
type OverlayDiskUsageResponse struct {
	TotalPatchBytes     int64                   `json:"total_patch_bytes"`
	TotalManifestBytes  int64                   `json:"total_manifest_bytes"`
	TotalPar2MetaBytes  int64                   `json:"total_par2_metadata_bytes"`
	FileCountsByVerdict map[string]int          `json:"file_counts_by_verdict"`
	Entries             []OverlayEntryDiskUsage `json:"entries"`
}

// handleOverlayDiskUsage aggregates real on-disk overlay/PAR2-metadata usage
// across every entry with overlay state.
func (s *Server) handleOverlayDiskUsage(w http.ResponseWriter, r *http.Request) {
	resp := OverlayDiskUsageResponse{
		FileCountsByVerdict: make(map[string]int),
		Entries:             make([]OverlayEntryDiskUsage, 0),
	}

	u := s.manager.Usenet()
	if u == nil {
		utils.JSONResponse(w, resp, http.StatusOK)
		return
	}

	nzbIDs, err := u.OverlayListNZBIDs()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for _, nzbID := range nzbIDs {
		entry, err := s.manager.GetEntry(nzbID)
		if err != nil || entry == nil {
			continue
		}
		patchBytes, manifestBytes, err := u.OverlayDiskUsage(nzbID)
		if err != nil {
			continue
		}
		manifest, _ := u.OverlayManifest(nzbID)
		fileCount := 0
		if manifest != nil {
			fileCount = len(manifest.Files)
			for _, fe := range manifest.Files {
				if fe != nil {
					resp.FileCountsByVerdict[string(fe.Verdict)]++
				}
			}
		}
		nzb, _ := u.GetNZB(nzbID)
		metaBytes := par2MetadataBytes(nzb)

		resp.Entries = append(resp.Entries, OverlayEntryDiskUsage{
			Entry:         entry.Name,
			NzbID:         nzbID,
			FileCount:     fileCount,
			PatchBytes:    patchBytes,
			ManifestBytes: manifestBytes,
			Par2MetaBytes: metaBytes,
		})
		resp.TotalPatchBytes += patchBytes
		resp.TotalManifestBytes += manifestBytes
		resp.TotalPar2MetaBytes += metaBytes
	}

	sort.Slice(resp.Entries, func(i, j int) bool { return resp.Entries[i].Entry < resp.Entries[j].Entry })
	utils.JSONResponse(w, resp, http.StatusOK)
}

// handleListPar2RepairHistory returns every persisted PAR2 repair attempt,
// newest first - mirrors handleListRepairRuns.
func (s *Server) handleListPar2RepairHistory(w http.ResponseWriter, r *http.Request) {
	attempts, err := s.manager.Storage().ListPar2RepairAttempts()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	utils.JSONResponse(w, attempts, http.StatusOK)
}

// handleGetPar2RepairHistoryEntry returns one persisted PAR2 repair attempt
// by ID - mirrors handleGetRepairRun.
func (s *Server) handleGetPar2RepairHistoryEntry(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		http.Error(w, "No id provided", http.StatusBadRequest)
		return
	}
	attempt, err := s.manager.Storage().GetPar2RepairAttempt(id)
	if err != nil {
		http.Error(w, "Attempt not found", http.StatusNotFound)
		return
	}
	utils.JSONResponse(w, attempt, http.StatusOK)
}

// handleClearPar2RepairHistory deletes every persisted PAR2 repair attempt -
// mirrors handleClearRepairRuns.
func (s *Server) handleClearPar2RepairHistory(w http.ResponseWriter, r *http.Request) {
	if err := s.manager.Storage().ClearPar2RepairAttempts(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// overlayFileRequest is the common body shape for every overlay action
// handler below: which file, in which entry, to act on.
type overlayFileRequest struct {
	Entry string `json:"entry"`
	File  string `json:"file"`
}

func decodeOverlayFileRequest(r *http.Request) (overlayFileRequest, error) {
	var req overlayFileRequest
	if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
		return req, err
	}
	req.Entry = strings.TrimSpace(req.Entry)
	req.File = strings.TrimSpace(req.File)
	return req, nil
}

// resolveOverlayEntry looks up the storage.Entry backing (entry, file) -
// same resolution GetEntryByName already provides elsewhere - so every
// action handler below can key overlay/PAR2 calls off entry.InfoHash (the
// nzbID overlay state is stored under) from the entry/file names the GUI
// shows.
func (s *Server) resolveOverlayEntry(req overlayFileRequest) (*storage.Entry, error) {
	if req.Entry == "" || req.File == "" {
		return nil, fmt.Errorf("entry and file are required")
	}
	return s.manager.GetEntryByName(req.Entry, req.File)
}

// handleOverlayRepairNow enqueues an immediate PAR2 repair pass for one
// file's entry, bypassing the repair sweep's StopSchedule window (a manual,
// user-initiated repair runs now) but not any other gate - see
// Par2Repair.RunNow.
func (s *Server) handleOverlayRepairNow(w http.ResponseWriter, r *http.Request) {
	req, err := decodeOverlayFileRequest(r)
	if err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	entry, err := s.resolveOverlayEntry(req)
	if err != nil || entry == nil {
		http.Error(w, "Entry not found", http.StatusNotFound)
		return
	}

	par2Repair := s.manager.Par2Repair()
	if par2Repair == nil {
		http.Error(w, "PAR2 repair worker not available", http.StatusServiceUnavailable)
		return
	}
	if repairable, reason := par2Repair.Availability(entry.InfoHash); !repairable {
		utils.JSONResponse(w, map[string]any{"status": "unavailable", "reason": reason}, http.StatusOK)
		return
	}
	if err := par2Repair.RunNow(entry.InfoHash); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	utils.JSONResponse(w, map[string]string{"status": "queued"}, http.StatusOK)
}

// handleOverlayVerify re-checks a patched file's bytes against the PAR2
// FileDesc's whole-file MD5 for its posted file - see Par2Repair.Verify.
func (s *Server) handleOverlayVerify(w http.ResponseWriter, r *http.Request) {
	req, err := decodeOverlayFileRequest(r)
	if err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	entry, err := s.resolveOverlayEntry(req)
	if err != nil || entry == nil {
		http.Error(w, "Entry not found", http.StatusNotFound)
		return
	}

	par2Repair := s.manager.Par2Repair()
	if par2Repair == nil {
		http.Error(w, "PAR2 repair worker not available", http.StatusServiceUnavailable)
		return
	}
	pass, reason, err := par2Repair.Verify(r.Context(), entry.InfoHash, req.File)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	utils.JSONResponse(w, map[string]any{"pass": pass, "reason": reason}, http.StatusOK)
}

// handleOverlayReclaim deletes a file's overlay patches + manifest record
// (see overlay.Store.DeleteFile) WITHOUT re-searching - for reclaiming disk
// on files that are fine now. Refuses while a PAR2 repair is in-flight for
// the entry, so a running pass never has its target overlay state pulled out
// from under it.
func (s *Server) handleOverlayReclaim(w http.ResponseWriter, r *http.Request) {
	req, err := decodeOverlayFileRequest(r)
	if err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	entry, err := s.resolveOverlayEntry(req)
	if err != nil || entry == nil {
		http.Error(w, "Entry not found", http.StatusNotFound)
		return
	}

	if par2Repair := s.manager.Par2Repair(); par2Repair != nil &&
		(par2Repair.IsRunning(entry.InfoHash) || par2Repair.IsQueued(entry.InfoHash)) {
		http.Error(w, "a par2 repair is in-flight for this entry", http.StatusConflict)
		return
	}

	u := s.manager.Usenet()
	if u == nil {
		http.Error(w, "Usenet client not available", http.StatusServiceUnavailable)
		return
	}
	if err := u.OverlayDeleteFile(entry.InfoHash, req.File); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	utils.JSONResponse(w, map[string]string{"status": "reclaimed"}, http.StatusOK)
}

// handleOverlayResearch is the "give up, get a clean copy" action: it clears
// this file's overlay state, then blocklists the current release and
// re-searches via the Arr so a non-broken copy replaces it - reusing the
// EXACT blocklist+SearchMissing flow the repair sweep's playback-failure
// path already uses (Repair.RepairPlaybackFileNow -> repairArrFiles), rather
// than reimplementing any of it here.
func (s *Server) handleOverlayResearch(w http.ResponseWriter, r *http.Request) {
	req, err := decodeOverlayFileRequest(r)
	if err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	entry, err := s.resolveOverlayEntry(req)
	if err != nil || entry == nil {
		http.Error(w, "Entry not found", http.StatusNotFound)
		return
	}

	if u := s.manager.Usenet(); u != nil {
		if err := u.OverlayDeleteFile(entry.InfoHash, req.File); err != nil {
			s.logger.Warn().Err(err).Str("entry", req.Entry).Str("file", req.File).Msg("overlay research: failed to clear overlay state")
		}
	}

	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	if err := svc.RepairPlaybackFileNow(s.manager.Context(), entry.Name, req.File); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	utils.JSONResponse(w, map[string]string{"status": "researching"}, http.StatusOK)
}
