// Handlers for the overlay management GUI: introspection (read-only) and
// action (write) endpoints over the playback-padding/PAR2-patch state
// tracked by pkg/usenet/overlay. Mirrors the existing repair API's patterns
// in api.go (error handling, JSON shapes, chi registration) - see
// handleGetRepairConfig / handleListRepairRuns / handleFixBroken and
// friends.
package server

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	json "github.com/bytedance/sonic"

	"github.com/go-chi/chi/v5"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/manager"
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
	// OverlayRepairUnrepairable marks a file the automatic PAR2 path has
	// stopped retrying after a terminal failure (see classifyPar2Failure) -
	// distinct from "unavailable" (a preflight Availability() check failed
	// without ever attempting a repair): this means a repair was actually
	// attempted and failed for a reason retrying can't fix. Manual "repair
	// now" still works.
	OverlayRepairUnrepairable OverlayRepairStatus = "unrepairable"
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

	DamageByteRatio  float64 `json:"damage_byte_ratio"`
	CoverageFraction float64 `json:"coverage_fraction,omitempty"`

	RepairStatus       OverlayRepairStatus `json:"repair_status"`
	RepairStatusReason string              `json:"repair_status_reason,omitempty"`

	Repairable          bool   `json:"repairable"`
	NotRepairableReason string `json:"not_repairable_reason,omitempty"`

	// Par2Terminal mirrors storage.Par2RepairState.Terminal: true means the
	// automatic path has given up re-enqueuing this file (see
	// Par2Repair.par2ShouldAutoEnqueue) after a failure classifyPar2Failure
	// judged unfixable by retrying. Repairable/manual "repair now" are
	// UNAFFECTED by this - a manual retry always overrides and re-evaluates
	// from scratch, per Par2Repair.RunNow bypassing the same gate.
	Par2Terminal     bool       `json:"par2_terminal,omitempty"`
	Par2AttemptCount int        `json:"par2_attempt_count,omitempty"`
	Par2LastError    string     `json:"par2_last_error,omitempty"`
	Par2NextRetryAt  *time.Time `json:"par2_next_retry_at,omitempty"`

	// PatchBytes and Par2RetainedMetaBytes are real on-disk sizes; their sum,
	// OverlayDiskBytes, is what this file's overlay state actually costs
	// locally. Par2MetaBytes (deprecated alias of Par2ProtectedReleaseBytes,
	// kept only so an already-built frontend bundle doesn't silently show
	// zeros - prefer ProtectedReleaseBytes) is NOT a disk figure at all: it's
	// the declared size of the release PAR2 protects on the remote Usenet
	// server, purely informational.
	PatchBytes                int64 `json:"patch_bytes"`
	Par2RetainedMetaBytes     int64 `json:"par2_retained_meta_bytes"`
	OverlayDiskBytes          int64 `json:"overlay_disk_bytes"`
	Par2MetaBytes             int64 `json:"par2_metadata_bytes"`
	Par2ProtectedReleaseBytes int64 `json:"protected_release_bytes"`

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

// par2ProtectedReleaseBytes sums the DECLARED sizes (real, decoded, per the
// NZB parser's Par2Files/Par2Source fields - not the NZB's encoded/posted
// byte count) of every PAR2-related file for nzb: the index/recovery
// volumes plus the posted-file layout PAR2 protects. This describes what
// PAR2 covers on the remote Usenet server - informational only. It is NOT a
// measure of anything stored on THIS machine's disk: none of those bytes
// are fetched or retained locally until a repair pass actually runs and
// writes a patch (see par2RetainedMetaBytes for the real local cost of
// retaining the Par2Files/Par2Source records themselves).
func par2ProtectedReleaseBytes(nzb *storage.NZB) int64 {
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

// par2RetainedMetaBytes measures the REAL on-disk cost of retaining nzb's
// PAR2 metadata: the serialized size of just the Par2Files/Par2Source
// structures (message IDs plus declared sizes - small, regardless of how
// large the release those message IDs point at happens to be), not the
// segment byte payloads those structures describe. This is what actually
// occupies local disk space; par2ProtectedReleaseBytes never does, until a
// repair pass writes an overlay patch.
func par2RetainedMetaBytes(nzb *storage.NZB) int64 {
	if nzb == nil || (len(nzb.Par2Files) == 0 && len(nzb.Par2Source) == 0) {
		return 0
	}
	data, err := json.Marshal(struct {
		Par2Files  []storage.Par2FileRef   `json:"par2_files,omitempty"`
		Par2Source []storage.PostedFileRef `json:"par2_source,omitempty"`
	}{nzb.Par2Files, nzb.Par2Source})
	if err != nil {
		return 0
	}
	return int64(len(data))
}

// overlayArrRefs builds the Arr reference set used to tell a current overlay
// record apart from an orphan (see overlayFileIsOrphan) - best-effort: nil
// means it couldn't be built this call (Arr outage, zero eligible Arrs, no
// repair service), in which case every orphan check below is skipped rather
// than guessed at, exactly like ClearSuperseded's own callers already treat
// a failed reference-set build.
func (s *Server) overlayArrRefs(ctx context.Context) map[string]map[string]string {
	svc := s.manager.Repair()
	if svc == nil {
		return nil
	}
	refs, err := svc.BuildArrReferencedSet(ctx)
	if err != nil {
		s.logger.Debug().Err(err).Msg("overlay: failed to build Arr reference set; skipping orphan filter")
		return nil
	}
	return refs
}

// overlayFileIsOrphan reports whether (entryName, file)'s overlay record no
// longer corresponds to the file's current, Arr-owned copy: either nothing
// references that slot anymore, or something does but it's now backed by a
// DIFFERENT nzbID (a re-grabbed twin) - see manager.FileSuperseded, the same
// primitive the supersession/stale-NZB cleanup already use. refs == nil (no
// reference set available this call) always reports false: never guess an
// overlay record is an orphan without a reference set to check it against.
func overlayFileIsOrphan(refs map[string]map[string]string, entryName, file, nzbID string) bool {
	if refs == nil {
		return false
	}
	return manager.FileSuperseded(refs, entryName, file, nzbID)
}

// handleListOverlayFiles returns every file with recorded overlay state
// (dead segments and/or patches), across every entry - excluding records
// whose entry no longer exists, or whose specific file slot has been
// superseded by a re-grabbed twin (see overlayFileIsOrphan). Those are
// "ghosts" of a repair or supersession that already happened; use
// handleOverlayGCOrphans to reap the backlog already on disk.
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
	refs := s.overlayArrRefs(r.Context())

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
		protectedBytes := par2ProtectedReleaseBytes(nzb)
		retainedMetaBytes := par2RetainedMetaBytes(nzb)

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
		repairState, _ := s.manager.Storage().GetPar2RepairState(nzbID)

		for file, fe := range manifest.Files {
			if fe == nil {
				continue
			}
			if overlayFileIsOrphan(refs, entry.Name, file, nzbID) {
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
			of.CoverageFraction = fe.CoverageFraction
			of.SegmentRuns = segmentRuns(fe.DeadSegments)
			of.PatchBytes = u.OverlayFilePatchBytes(nzbID, file, fe)
			of.Par2RetainedMetaBytes = retainedMetaBytes
			of.OverlayDiskBytes = of.PatchBytes + retainedMetaBytes
			of.Par2ProtectedReleaseBytes = protectedBytes
			of.Par2MetaBytes = protectedBytes // deprecated alias, see field doc

			pending := of.DeadSegments+of.PaddedSegments > 0
			of.Repairable = pending && repairable
			if pending && !repairable {
				of.NotRepairableReason = notRepairableReason
			}
			if repairState != nil {
				of.Par2Terminal = repairState.Terminal
				of.Par2AttemptCount = repairState.AttemptCount
				of.Par2LastError = repairState.LastError
				if !repairState.NextRetryAt.IsZero() {
					t := repairState.NextRetryAt
					of.Par2NextRetryAt = &t
				}
			}

			switch {
			case running:
				of.RepairStatus = OverlayRepairRunning
			case queued:
				of.RepairStatus = OverlayRepairQueued
			case pending && repairState != nil && repairState.Terminal:
				of.RepairStatus = OverlayRepairUnrepairable
				of.RepairStatusReason = repairState.TerminalReason
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
// PatchBytes, ManifestBytes, and Par2RetainedMetaBytes are real on-disk
// sizes; OverlayDiskBytes (their sum) is what this entry's overlay state
// actually costs locally. ProtectedReleaseBytes is informational only - the
// declared size of the release PAR2 protects on the remote Usenet server,
// NOT a local disk figure. Par2MetaBytes is a deprecated alias of
// ProtectedReleaseBytes, kept only so an already-built frontend bundle
// doesn't silently show zeros.
type OverlayEntryDiskUsage struct {
	Entry                 string `json:"entry"`
	NzbID                 string `json:"nzb_id"`
	FileCount             int    `json:"file_count"`
	PatchBytes            int64  `json:"patch_bytes"`
	ManifestBytes         int64  `json:"manifest_bytes"`
	Par2RetainedMetaBytes int64  `json:"par2_retained_meta_bytes"`
	OverlayDiskBytes      int64  `json:"overlay_disk_bytes"`
	ProtectedReleaseBytes int64  `json:"protected_release_bytes"`
	Par2MetaBytes         int64  `json:"par2_metadata_bytes"`
}

// OverlayDiskUsageResponse is the aggregate overlay disk-usage report. See
// OverlayEntryDiskUsage for what each figure means; TotalOverlayDiskBytes is
// the true local cost, TotalProtectedReleaseBytes is informational only.
type OverlayDiskUsageResponse struct {
	TotalPatchBytes            int64                   `json:"total_patch_bytes"`
	TotalManifestBytes         int64                   `json:"total_manifest_bytes"`
	TotalPar2RetainedMetaBytes int64                   `json:"total_par2_retained_meta_bytes"`
	TotalOverlayDiskBytes      int64                   `json:"total_overlay_disk_bytes"`
	TotalProtectedReleaseBytes int64                   `json:"total_protected_release_bytes"`
	TotalPar2MetaBytes         int64                   `json:"total_par2_metadata_bytes"`
	FileCountsByVerdict        map[string]int          `json:"file_counts_by_verdict"`
	Entries                    []OverlayEntryDiskUsage `json:"entries"`
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
		protectedBytes := par2ProtectedReleaseBytes(nzb)
		retainedMetaBytes := par2RetainedMetaBytes(nzb)
		diskBytes := patchBytes + manifestBytes + retainedMetaBytes

		resp.Entries = append(resp.Entries, OverlayEntryDiskUsage{
			Entry:                 entry.Name,
			NzbID:                 nzbID,
			FileCount:             fileCount,
			PatchBytes:            patchBytes,
			ManifestBytes:         manifestBytes,
			Par2RetainedMetaBytes: retainedMetaBytes,
			OverlayDiskBytes:      diskBytes,
			ProtectedReleaseBytes: protectedBytes,
			Par2MetaBytes:         protectedBytes, // deprecated alias, see type doc
		})
		resp.TotalPatchBytes += patchBytes
		resp.TotalManifestBytes += manifestBytes
		resp.TotalPar2RetainedMetaBytes += retainedMetaBytes
		resp.TotalOverlayDiskBytes += diskBytes
		resp.TotalProtectedReleaseBytes += protectedBytes
		resp.TotalPar2MetaBytes += protectedBytes
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

// handleOverlayRepairProgress returns live, in-memory progress for the most
// recent PAR2 repair pass this process has run (or is running) for a file's
// entry - phase, recovery/intact-slice counts, cache vs. usenet bytes
// fetched, and the last error, if any. See manager.Par2Repair.Progress /
// par2JobProgressState. Distinct from repair-history: this is a live,
// per-second view of an IN-FLIGHT job, gone on process restart, whereas
// repair-history is the persisted, terminal-outcome record. GET with query
// params (not the shared overlayFileRequest JSON body decoder) since this is
// a pure read with no body.
func (s *Server) handleOverlayRepairProgress(w http.ResponseWriter, r *http.Request) {
	req := overlayFileRequest{
		Entry: strings.TrimSpace(r.URL.Query().Get("entry")),
		File:  strings.TrimSpace(r.URL.Query().Get("file")),
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
	progress, ok := par2Repair.Progress(entry.InfoHash)
	if !ok {
		utils.JSONResponse(w, map[string]any{"status": "no_job"}, http.StatusOK)
		return
	}
	utils.JSONResponse(w, progress, http.StatusOK)
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
	// The overlay record is gone, but the file could still be sitting in
	// the permanent-failure cache from before it was reclaimed - un-poison
	// it so the next read re-verifies from scratch instead of
	// short-circuiting on a stale cause.
	u.ClearFailedFile(entry.InfoHash, req.File)
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
		// Manual research always overrides: the re-grab this triggers must
		// not have its own eventual playback short-circuited by a stale
		// permanent-failure record left over from the release being
		// replaced.
		u.ClearFailedFile(entry.InfoHash, req.File)
	}

	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	// Manual override: proceed regardless of whatever the automatic
	// auto-repair policy currently claims for this entry - including a
	// terminal mark left by a PAR2 pass that gave up - and clear that claim
	// once this one-shot action has run its course. See
	// Repair.HandlePlaybackFailure for the automatic policy this bypasses.
	svc.ClaimManualAutoRepairOverride(entry.InfoHash)
	defer svc.ReleaseManualAutoRepairOverride(entry.InfoHash)
	if err := svc.RepairPlaybackFileNow(s.manager.Context(), entry.Name, req.File); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	utils.JSONResponse(w, map[string]string{"status": "researching"}, http.StatusOK)
}

// OverlayGCResult summarizes one handleOverlayGCOrphans pass.
type OverlayGCResult struct {
	Checked        int  `json:"checked"`         // overlay file records examined, across every entry
	DeletedEntries int  `json:"deleted_entries"` // whole nzbIDs removed (backing entry no longer exists)
	DeletedFiles   int  `json:"deleted_files"`   // individual file records removed (superseded by a re-grabbed twin, or unreferenced)
	RefsAvailable  bool `json:"refs_available"`  // whether the Arr reference set was available this pass - false means only the "entry gone entirely" check ran
}

// OverlayOrphanCount is a cheap, non-destructive preview of what
// handleOverlayGCOrphans would reap, so the GUI can decide whether to show
// its "clean up" button at all without committing to running it.
type OverlayOrphanCount struct {
	Checked       int  `json:"checked"`
	OrphanEntries int  `json:"orphan_entries"`
	OrphanFiles   int  `json:"orphan_files"`
	RefsAvailable bool `json:"refs_available"`
}

// overlayOrphanScan walks every overlay record and either reaps (execute
// true) or merely counts (execute false) whichever ones are orphaned - see
// overlayFileIsOrphan. Shared by handleOverlayGCOrphans (the actual GC pass)
// and handleOverlayOrphanCount (a read-only preview of the same walk).
func (s *Server) overlayOrphanScan(ctx context.Context, execute bool) (OverlayGCResult, error) {
	result := OverlayGCResult{}
	u := s.manager.Usenet()
	if u == nil {
		return result, nil
	}

	nzbIDs, err := u.OverlayListNZBIDs()
	if err != nil {
		return result, err
	}

	refs := s.overlayArrRefs(ctx)
	result.RefsAvailable = refs != nil

	for _, nzbID := range nzbIDs {
		entry, err := s.manager.GetEntry(nzbID)
		if err != nil || entry == nil {
			if manifest, merr := u.OverlayManifest(nzbID); merr == nil && manifest != nil {
				result.Checked += len(manifest.Files)
			} else {
				result.Checked++
			}
			if execute {
				if derr := u.OverlayDeleteEntry(nzbID); derr != nil {
					s.logger.Warn().Err(derr).Str("nzb_id", nzbID).Msg("overlay gc: failed to delete orphaned entry")
					continue
				}
			}
			result.DeletedEntries++
			continue
		}

		if refs == nil {
			continue
		}
		manifest, err := u.OverlayManifest(nzbID)
		if err != nil || manifest == nil {
			continue
		}
		for file := range manifest.Files {
			result.Checked++
			if !overlayFileIsOrphan(refs, entry.Name, file, nzbID) {
				continue
			}
			if execute {
				if derr := u.OverlayDeleteFile(nzbID, file); derr != nil {
					s.logger.Warn().Err(derr).Str("entry", entry.Name).Str("file", file).Msg("overlay gc: failed to delete orphaned file record")
					continue
				}
			}
			result.DeletedFiles++
		}
	}

	return result, nil
}

// handleOverlayGCOrphans deletes every overlay record that no longer
// corresponds to a live, Arr-owned file - the same two checks
// handleListOverlayFiles already applies to hide them, applied here to
// actually reclaim the backlog already on disk from before that filter
// existed: overlay.DeleteEntry for a whole nzbID whose backing entry is gone
// entirely, overlay.DeleteFile for an individual file superseded by a
// re-grabbed twin under the same (entry, file) slot. A manual "clean up
// ghosts" action - never runs automatically.
func (s *Server) handleOverlayGCOrphans(w http.ResponseWriter, r *http.Request) {
	result, err := s.overlayOrphanScan(r.Context(), true)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	utils.JSONResponse(w, result, http.StatusOK)
}

// handleOverlayOrphanCount is a read-only preview of handleOverlayGCOrphans:
// how many orphaned records are sitting on disk right now, without deleting
// anything. Lets the GUI show/hide its "clean up N orphaned records" button
// without running the (mutating) GC pass just to find out.
func (s *Server) handleOverlayOrphanCount(w http.ResponseWriter, r *http.Request) {
	scan, err := s.overlayOrphanScan(r.Context(), false)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	utils.JSONResponse(w, OverlayOrphanCount{
		Checked:       scan.Checked,
		OrphanEntries: scan.DeletedEntries,
		OrphanFiles:   scan.DeletedFiles,
		RefsAvailable: scan.RefsAvailable,
	}, http.StatusOK)
}
