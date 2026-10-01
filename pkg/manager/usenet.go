package manager

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/logger"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/hearsay"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/parser"
)

// AddNewNZB persists an NZB and returns as soon as it enters the active-download queue.
func (m *Manager) AddNewNZB(ctx context.Context, req *ImportRequest) (string, error) {
	if m.usenet == nil {
		return "", fmt.Errorf("usenet not configured")
	}
	if req == nil || len(req.NZBContent) == 0 {
		return "", fmt.Errorf("NZB content is empty")
	}
	if req.Arr == nil {
		return "", fmt.Errorf("arr is required")
	}

	m.logger.Info().
		Str("name", req.Name).
		Str("category", req.Arr.Name).
		Str(logger.FieldStatus, logger.StatusStart).
		Str(logger.FieldNote, req.Arr.Name).
		Msg("Adding new NZB to usenet")

	stagedPath, err := m.usenet.StageNZB(req.Id, req.NZBContent)
	if err != nil {
		return "", err
	}
	req.NZBContent = nil

	entry := &storage.Entry{
		InfoHash:         req.Id,
		Name:             req.Name,
		OriginalFilename: req.Name,
		Protocol:         config.ProtocolNZB,
		Magnet:           stagedPath,
		Category:         req.Arr.Name,
		SavePath:         filepath.Join(req.DownloadFolder, req.Arr.Name),
		Status:           debridTypes.TorrentStatusQueued,
		State:            storage.EntryStateDownloading,
		Progress:         0,
		Action:           req.Action,
		CallbackURL:      req.CallBackUrl,
		SkipMultiSeason:  req.SkipMultiSeason,
		CreatedAt:        time.Now(),
		UpdatedAt:        time.Now(),
		AddedOn:          time.Now(),
		Providers:        make(map[string]*storage.ProviderEntry),
		Files:            make(map[string]*storage.File),
		Tags:             []string{},
	}

	entry.ContentPath = entry.DownloadPath()
	if err := m.queue.Add(entry); err != nil {
		m.usenet.RemoveStagedNZB(stagedPath)
		return "", fmt.Errorf("failed to add nzb to queue: %w", err)
	}

	req.Status = "queued"
	job := NewJob(JobTypeNZB, req)
	job.ID = entry.InfoHash
	job.Entry = entry
	if err := m.SubmitJob(job); err != nil {
		m.usenet.RemoveStagedNZB(stagedPath)
		entry.MarkAsError(err)
		_ = m.queue.Update(entry)
		return "", fmt.Errorf("failed to queue NZB: %w", err)
	}
	return req.Id, nil
}

const (
	// nzbParseMaxAttempts bounds how often a queued NZB is parsed before a
	// parse error fails the download. A failed download is what the Arr
	// blocklists, so a transient provider error must not end a good release
	// on its first try.
	nzbParseMaxAttempts = 3
	// nzbParseRetryDelay is the wait before the second parse; it doubles for
	// each later one.
	nzbParseRetryDelay = 30 * time.Second
)

// errJobSettled tells processJob that the job already recorded its outcome
// (failed the entry, or scheduled itself again), so there is nothing left to
// log, mark or wait for.
var errJobSettled = errors.New("job outcome already recorded")

// rejectDamagedNZB fails a queued NZB whose parse found the release confirmed
// unavailable (missing segments, or the negative cache already knows this
// exact content is dead). The failed queue entry is what the Arr sees in the
// SABnzbd history, so it blocklists the release and searches again. Without a
// tracked failed item it has nothing to blocklist and retries the identical
// grab forever (observed: FLUX/ETHEL/Kitsune/RAWR cycling every ~60s).
func (m *Manager) rejectDamagedNZB(job *Job, cause error) {
	entry := job.Entry
	entry.MarkAsError(cause)
	if err := m.queue.Update(entry); err != nil {
		m.logger.Error().Err(err).Str("name", entry.Name).Msg("Failed to record a rejected NZB")
	}
	m.logger.Warn().
		Str("name", entry.Name).
		Str("category", entry.Category).
		Err(cause).
		Msg("NZB rejected: release confirmed unavailable; queued as failed so the Arr blocklists and re-searches")
}

// retryNZBParse schedules another parse of a queued NZB after a parse error
// that may pass (a provider or connection error), until nzbParseMaxAttempts.
// It reports false when the error should fail the download now: the release
// is confirmed dead, the NZB itself is invalid, the attempts are used up, or
// the manager is stopping.
func (m *Manager) retryNZBParse(ctx context.Context, job *Job, err error) bool {
	if ctx.Err() != nil || errors.Is(err, parser.ErrReleaseUnavailable) || errors.Is(err, usenet.ErrInvalidNZB) {
		return false
	}
	if job.ParseAttempts+1 >= nzbParseMaxAttempts {
		return false
	}
	job.ParseAttempts++
	delay := nzbParseRetryDelay << (job.ParseAttempts - 1)
	m.logger.Warn().
		Err(err).
		Str("name", job.Entry.Name).
		Int("attempt", job.ParseAttempts).
		Dur("retry_in", delay).
		Msg("NZB parse failed; parsing it again before failing the download")
	m.jobQueue.Retry(job, delay)
	return true
}

func (m *Manager) processNZBJob(ctx context.Context, job *Job) error {
	if job == nil || job.Entry == nil {
		return fmt.Errorf("invalid NZB job")
	}
	if _, err := m.queue.GetTorrent(job.Entry.InfoHash); err != nil {
		return nil
	}
	if job.NZBMeta == nil {
		if job.Request == nil {
			return fmt.Errorf("NZB job has no request or processing payload")
		}
		content, err := os.ReadFile(job.Entry.Magnet)
		if err != nil {
			return fmt.Errorf("read staged NZB: %w", err)
		}
		meta, groups, err := m.usenet.ParseWithID(ctx, job.Entry.InfoHash, job.Request.Name, content, job.Request.Arr.Name)
		if err != nil {
			if errors.Is(err, parser.ErrReleaseUnavailable) {
				// A missing article at the parse stat is a definitive
				// availability result: record and share it, so the next
				// encounter of this post - ours or another operator's -
				// rejects without NNTP work. The parser hands back its
				// groups only for that failure; the other unavailable
				// verdicts carry none, so nothing is reported for them.
				if m.hearsay != nil {
					m.hearsay.ReportNZB(hearsay.NZBSubjectFromGroups(groups), false)
				}
				m.rejectDamagedNZB(job, err)
				return errJobSettled
			}
			if m.retryNZBParse(ctx, job, err) {
				return errJobSettled
			}
			return fmt.Errorf("usenet parse failed: %w", err)
		}

		// Own truth or a strong network consensus that the segments are
		// gone means the availability check is doomed. Upstream fails the
		// add itself; here the NZB is parsed after it was queued, so the
		// queued entry is failed the way a dead release is, and the Arr
		// blocklists it and searches again. No report follows a
		// rejection - nothing was actually checked.
		if m.hearsay != nil && m.hearsay.NZBClaimedIncomplete(hearsay.NZBSubjectFromGroups(groups)) {
			m.rejectDamagedNZB(job, fmt.Errorf("hearsay claims segments missing on every configured backbone: %w", parser.ErrReleaseUnavailable))
			return errJobSettled
		}

		m.usenet.RemoveStagedNZB(job.Entry.Magnet)
		job.Entry.Magnet = ""
		job.NZBMeta = meta
		job.NZBGroups = groups
		job.Entry.Name = meta.Name
		job.Entry.OriginalFilename = meta.Name
		job.Entry.Size = meta.TotalSize
		job.Entry.Bytes = meta.TotalSize
		job.Entry.Status = debridTypes.TorrentStatusDownloading
		job.Entry.ActiveProvider = "usenet"
		_ = job.Entry.AddUsenetProvider(meta)
		if err := m.queue.Update(job.Entry); err != nil {
			return fmt.Errorf("update queued NZB: %w", err)
		}
	}
	if job.Request != nil {
		job.Request.Status = "started"
	}
	return m.processNewNzb(ctx, job.Entry, job.NZBMeta, job.NZBGroups)
}

func (m *Manager) processNZB(ctx context.Context, entry *storage.Entry, metadata *storage.NZB) error {
	// Add files using logical streamable files
	for _, file := range metadata.Files {
		tFile := &storage.File{
			Name:     file.Name,
			Size:     file.Size,
			InfoHash: entry.InfoHash,
			AddedOn:  entry.AddedOn,
		}
		entry.Files[file.Name] = tFile
	}
	// Mark as complete
	if placement := entry.GetActiveProvider(); placement != nil {
		now := time.Now()
		placement.DownloadedAt = &now
		placement.Progress = 1.0
	}
	entry.Size = metadata.TotalSize
	entry.Progress = 1.0
	entry.UpdatedAt = time.Now()
	_ = m.queue.Update(entry)

	if len(entry.Files) == 0 {
		return fmt.Errorf("nzb has no files")
	}

	m.startDownloadTask(func() { m.processAction(entry) })
	return nil
}

// processNewNzb processes a new NZB entry after it has been added to the usenet client
func (m *Manager) processNewNzb(parentCtx context.Context, entry *storage.Entry, metadata *storage.NZB, groups map[string]*parser.FileGroup) error {
	// Create context with timeout for processing
	ctx, cancel := context.WithTimeout(parentCtx, m.usenetTimeout)
	defer cancel()

	// Derive the content identifier from the parsed groups. Process is
	// what fills in metadata.Files, so hashing the NZB here would
	// always see an empty list and produce no subject at all.
	var hearsaySubject string
	if m.hearsay != nil {
		hearsaySubject = hearsay.NZBSubjectFromGroups(groups)
	}

	updatedNZB, err := m.usenet.Process(ctx, metadata, groups)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return fmt.Errorf("usenet processing timed out after %s: %w", m.usenetTimeout, err)
		}
		if errors.Is(err, customerror.UsenetSegmentMissingError) {
			m.hearsay.ReportNZB(hearsaySubject, false)
		}
		return fmt.Errorf("failed to process nzb: %w", err)
	}
	m.hearsay.ReportNZB(hearsaySubject, true)

	metadata = updatedNZB
	return m.processNZB(ctx, entry, metadata)
}

// HasUsenet returns true if usenet is configured
func (m *Manager) HasUsenet() bool {
	return m.usenet != nil
}

// UsenetStats returns usenet client statistics
func (m *Manager) UsenetStats() map[string]any {
	if m.usenet == nil {
		return nil
	}
	return m.usenet.Stats()
}

// SpeedTestRequest represents a speed test request payload
type SpeedTestRequest struct {
	Protocol string `json:"protocol"` // "nntp" or "debrid"
	Provider string `json:"provider"` // provider host/identifier
}

// SpeedTestResponse represents a speed test result
type SpeedTestResponse struct {
	Provider  string  `json:"provider"`
	Protocol  string  `json:"protocol"`
	SpeedMBps float64 `json:"speed_mbps"`
	LatencyMs int64   `json:"latency_ms"`
	BytesRead int64   `json:"bytes_read"`
	TestedAt  string  `json:"tested_at"`
	Error     string  `json:"error,omitempty"`
}

// SpeedTest runs a speed test for a specific provider based on protocol
func (m *Manager) SpeedTest(ctx context.Context, req SpeedTestRequest) SpeedTestResponse {
	switch req.Protocol {
	case "nntp":
		if m.usenet == nil {
			return SpeedTestResponse{
				Provider: req.Provider,
				Protocol: req.Protocol,
				Error:    "usenet not configured",
			}
		}
		result := m.usenet.SpeedTest(ctx, req.Provider)
		return SpeedTestResponse{
			Provider:  result.Provider,
			Protocol:  req.Protocol,
			SpeedMBps: result.SpeedMBps,
			LatencyMs: result.LatencyMs,
			BytesRead: result.BytesRead,
			TestedAt:  result.TestedAt.Format("2006-01-02T15:04:05Z07:00"),
			Error:     result.Error,
		}
	case "debrid":
		// Look up debrid client by provider name
		client, exists := m.clients.Load(req.Provider)
		if !exists {
			return SpeedTestResponse{
				Provider: req.Provider,
				Protocol: req.Protocol,
				Error:    "debrid provider not found: " + req.Provider,
			}
		}
		result := client.SpeedTest(ctx)

		// Store the result for persistence (so it shows up in stats)
		if result.Error == "" {
			m.debridSpeedTestResults.Store(req.Provider, result)
		}

		return SpeedTestResponse{
			Provider:  result.Provider,
			Protocol:  req.Protocol,
			SpeedMBps: result.SpeedMBps,
			LatencyMs: result.LatencyMs,
			BytesRead: result.BytesRead,
			TestedAt:  result.TestedAt.Format("2006-01-02T15:04:05Z07:00"),
			Error:     result.Error,
		}
	default:
		return SpeedTestResponse{
			Provider: req.Provider,
			Protocol: req.Protocol,
			Error:    "unknown protocol: " + req.Protocol,
		}
	}
}

func (m *Manager) syncNZBs(ctx context.Context) error {
	if m.usenet == nil {
		return nil
	}

	m.nzbSyncMu.Lock()
	defer m.nzbSyncMu.Unlock()

	pendingNZBs, err := m.usenet.ClaimNewNZBs()
	if err != nil {
		return fmt.Errorf("failed to claim new NZBs from usenet client: %w", err)
	}

	for _, pending := range pendingNZBs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		req := NewNZBRequest(
			pending.Name,
			config.Get().DownloadFolder,
			pending.Content,
			m.arr.GetOrCreate(""),
			config.DownloadActionNone,
			"",
			ImportTypeWatch,
			false,
		)
		if _, err := m.AddNewNZB(ctx, req); err != nil {
			m.logger.Error().Err(err).Str("name", pending.Name).Msg("Failed to queue watched NZB")
			continue
		}
		m.usenet.RemoveClaimedNZB(pending.Path)
	}
	return nil
}
