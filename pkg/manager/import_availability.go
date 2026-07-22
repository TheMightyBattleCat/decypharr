package manager

import (
	"context"
	"fmt"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// importAvailabilityGate probes a newly-downloaded NZB entry's video files
// for confirmed-missing segments (a genuine 430 on every provider) BEFORE
// the entry is reported complete to Sonarr/Radarr. The existing ffprobe
// import gate (see ffprobeImportGate) only validates container structure -
// it reads through the shared padding path, so a segment that's dead but
// zero-filled still produces a container ffprobe parses as healthy. This
// gate reuses the sweep's own BatchStat-backed availability primitive
// (Usenet.CheckFileDetailed, gated through the NNTP client's repair pool) to
// find that damage directly, independent of padding.
//
// Any confirmed-missing segment is recorded to the overlay store (exactly
// like the repair sweep's recordDeadSegments) and routed through the
// coordinated auto-repair policy (decideAutoRepairAction, source=import) -
// the same decision function the repair sweep and playback-failure
// escalation use, so import-time detection is never pad-and-forget: PAR2
// repair when enabled (the entry still completes import degraded-but-
// self-healing - playback padding covers any interim access until the PAR2
// pass lands), otherwise the grab is blocklisted and re-searched via the Arr
// and the file is NOT imported. Claims the handler registry before acting
// (via queueImportPar2/RegrabImportGrab), so a concurrent sweep/PAR2 pass
// for the same nzbID is never double-queued or double-re-grabbed.
func (d *Downloader) importAvailabilityGate(entry *storage.Entry) (err error) {
	defer func() {
		if r := recover(); r != nil {
			d.logger.Warn().Interface("panic", r).Str("entry", entry.Name).
				Msg("Import: availability gate panicked; proceeding without validation")
			err = nil
		}
	}()

	if entry.Protocol != config.ProtocolNZB || d.manager.usenet == nil {
		return nil
	}
	if !config.Get().Repair.ImportAvailabilityCheckEnabled() {
		return nil
	}

	ctx := d.manager.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	for _, file := range entry.GetActiveFiles() {
		if ctx.Err() != nil {
			// Cancellation (shutdown) is inconclusive, never a rejection.
			return nil
		}
		if file == nil || file.Size < ffprobeImportMinSize || !config.IsVideoFile(file.Name) {
			continue
		}

		missing, cerr := d.manager.usenet.CheckFileDetailed(ctx, entry.InfoHash, file.Name)
		if cerr != nil {
			// Non-fatal: a probe error (connection issues) doesn't mean the
			// file is broken, just that this check couldn't run.
			d.logger.Debug().Err(cerr).Str("entry", entry.Name).Str("file", file.Name).
				Msg("Import: availability check failed to run; skipping")
			continue
		}
		if len(missing) == 0 {
			continue
		}

		for _, seg := range missing {
			if rerr := d.manager.usenet.RecordOverlayDead(entry.InfoHash, file.Name, seg.Index, seg.MessageID, seg.Bytes); rerr != nil {
				d.logger.Debug().Err(rerr).Str("entry", entry.Name).Str("file", file.Name).Int("segment", seg.Index).
					Msg("Import: failed to record dead segment in overlay")
			}
		}
		verdict := d.manager.usenet.OverlayVerdict(entry.InfoHash, file.Name)
		par2Enabled := config.Get().Repair.Par2RepairEnabled()

		switch decideAutoRepairAction(RepairSourceImport, par2Enabled, verdict) {
		case autoActionQueuePar2:
			d.logger.Warn().
				Str("entry", entry.Name).
				Str("file", file.Name).
				Int("missing_segments", len(missing)).
				Msg("Import: confirmed-missing segment(s); queuing PAR2 repair, import proceeds")
			d.queueImportPar2(entry, file.Name, len(missing))
		default: // autoActionRegrab - source=import never returns autoActionNone.
			d.logger.Warn().
				Str("entry", entry.Name).
				Str("file", file.Name).
				Int("missing_segments", len(missing)).
				Msg("Import: confirmed-missing segment(s), not PAR2-repairable; blocklisting and rejecting import")
			if rerr := d.manager.Repair().RegrabImportGrab(ctx, entry, file.Name, "usenet_segment_missing"); rerr != nil {
				d.logger.Warn().Err(rerr).Str("entry", entry.Name).Str("file", file.Name).
					Msg("Import: failed to blocklist + re-search via Arr")
			}
			return fmt.Errorf("import availability check: file %q has confirmed-missing segments", file.Name)
		}
	}
	return nil
}

// queueImportPar2 hands entry/name to the PAR2 worker via AutoEnqueue, which
// claims the handler registry itself and applies Par2RepairMode's own
// mode/threshold gating (manual: never auto-queues; auto_threshold: only
// once this file's dead segment count reaches Par2RepairMinSegments) - the
// same gate the repair sweep's queuePar2FromSweep and the reader's own
// padding-triggered EnqueueRepair path use, so a file left below threshold
// stays exactly as padding-during-playback would leave it rather than being
// queued regardless.
func (d *Downloader) queueImportPar2(entry *storage.Entry, name string, missingSegments int) {
	if d.manager.par2Repair == nil {
		return
	}
	d.manager.par2Repair.AutoEnqueue(entry.InfoHash, missingSegments)
}
