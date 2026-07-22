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
// like the repair sweep's recordDeadSegments) and unconditionally re-grabbed
// via RegrabImportGrab (blocklist + reject the import) - decideAutoRepairAction
// with source=import always resolves damage to autoActionRegrab regardless of
// the PAR2 toggle, since PAR2 is a playback-only mechanism: at import the DFS
// cache is cold (nothing has played yet), so a PAR2 pass would fetch the
// entire release from Usenet instead of the cache-warm slices it relies on to
// be cheap. Import either completes with a complete file or re-grabs - never
// degraded-but-self-healing, never queued for PAR2. Claims the handler
// registry before acting (via RegrabImportGrab), so a concurrent sweep pass
// for the same nzbID is never double-re-grabbed.
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
		// decideAutoRepairAction(RepairSourceImport, ...) always resolves
		// damage to autoActionRegrab, regardless of the PAR2 toggle - PAR2 is
		// a playback-only mechanism (see decideAutoRepairAction's doc
		// comment). Re-grab unconditionally rather than consult the policy
		// for a result that's already known.
		d.logger.Warn().
			Str("entry", entry.Name).
			Str("file", file.Name).
			Int("missing_segments", len(missing)).
			Msg("Import: confirmed-missing segment(s); blocklisting and rejecting import")
		if rerr := d.manager.Repair().RegrabImportGrab(ctx, entry, file.Name, "usenet_segment_missing"); rerr != nil {
			d.logger.Warn().Err(rerr).Str("entry", entry.Name).Str("file", file.Name).
				Msg("Import: failed to blocklist + re-search via Arr")
		}
		return fmt.Errorf("import availability check: file %q has confirmed-missing segments", file.Name)
	}
	return nil
}
