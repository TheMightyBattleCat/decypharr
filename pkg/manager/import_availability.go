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
// like the repair sweep's recordDeadSegments). PAR2 repair, when enabled,
// gets first refusal - the entry still completes import degraded-but-
// self-healing, since playback padding covers any interim access until the
// PAR2 pass lands. Otherwise the grab is blocklisted and re-searched via the
// Arr and the file is NOT imported.
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

		if d.tryQueueImportPar2(entry, file.Name, len(missing)) {
			continue
		}

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
	return nil
}

// tryQueueImportPar2 hands entry/name to the PAR2 worker when repair is
// enabled, via the same AutoEnqueue mode/threshold gating (and handler-
// registry claim) the repair sweep's own queuePar2FromSweep uses. Returns
// false (nothing queued) when PAR2 repair is disabled outright - the caller
// falls back to re-grabbing via the Arr in that case.
func (d *Downloader) tryQueueImportPar2(entry *storage.Entry, name string, missingSegments int) bool {
	if !config.Get().Repair.Par2RepairEnabled() {
		return false
	}
	if d.manager.par2Repair == nil || d.manager.usenet == nil {
		return false
	}
	d.logger.Warn().
		Str("entry", entry.Name).
		Str("file", name).
		Int("missing_segments", missingSegments).
		Msg("Import: confirmed-missing segment(s); queuing PAR2 repair, import proceeds")
	// AutoEnqueue claims the handler registry itself (see Par2Repair.enqueue)
	// and applies Par2RepairMode's own mode/threshold gating - a no-op call
	// here (mode=manual, or below auto_threshold's minimum) is not a
	// rejection: the entry still isn't handed to the caller's regrab path,
	// since a below-threshold amount of damage is exactly what padding
	// during playback is meant to absorb.
	d.manager.par2Repair.AutoEnqueue(entry.InfoHash, missingSegments)
	return true
}
