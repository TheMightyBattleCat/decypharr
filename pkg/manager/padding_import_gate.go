package manager

import (
	"context"
	"fmt"
	"strings"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// paddingImportGate rejects a newly-completed NZB download if any of its
// files has a segment the overlay store recorded as confirmed-missing (a 430
// across every provider) while reading it in during download - whether that
// segment ended up zero-filled (overlay.StatusPadded, within the padding
// caps) or left unresolved (overlay.StatusDead, over caps or a
// non-video-container file, which fails the read outright rather than
// padding it - see overlay.Store.Decide). Both are confirmed data loss the
// ffprobe gate can miss - the damage can land outside the windows ffprobe
// actually reads, and a timed-out or inconclusive ffprobe check lets the
// import proceed regardless - so this runs independently, after
// ffprobeImportGate, using the same reject+regrab path (RegrabImportGrab)
// ffprobe rejections use.
//
// The segment counts come straight from the overlay store via
// OverlayPendingRepair, which already returns every non-patched (dead or
// padded) segment - there's no separate "430" record to also check, since a
// 430 is the only thing that ever gets recorded to the overlay in the first
// place (see the overlay package doc comment). By the time a download
// completes, every confirmed-dead segment has already been decided (padded
// or failed), so there's no race with segments still being fetched.
//
// Only applies to NZB entries with an active usenet backend; torrents have no
// overlay store. Like the other import gates, the whole function is wrapped
// in a recover(): a bug here must never take down completeEntry.
func (d *Downloader) paddingImportGate(entry *storage.Entry) (err error) {
	defer func() {
		if r := recover(); r != nil {
			d.logger.Warn().Interface("panic", r).Str("entry", entry.Name).
				Msg("Import: padding gate panicked; proceeding without validation")
			err = nil
		}
	}()

	if entry.Protocol != config.ProtocolNZB || d.manager.usenet == nil {
		return nil
	}

	ctx := d.manager.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	pending, perr := d.manager.usenet.OverlayPendingRepair(entryNZBID(entry))
	if perr != nil {
		d.logger.Debug().Err(perr).Str("entry", entry.Name).
			Msg("Import: padding check failed to run; skipping")
		return nil
	}
	if len(pending) == 0 {
		return nil
	}

	for _, file := range entry.GetActiveFiles() {
		if ctx.Err() != nil {
			// Cancellation (shutdown) is inconclusive, never a rejection.
			return nil
		}
		if file == nil || file.Size < ffprobeImportMinSize || !config.IsVideoFile(file.Name) {
			continue
		}
		segs := pending[file.Name]
		if len(segs) == 0 {
			continue
		}

		// Both statuses originate from the same 430 - split them only to keep
		// the reason/log diagnostic: StatusDead means the segment failed the
		// read outright (over caps / not paddable), StatusPadded means it was
		// zero-filled instead.
		var padded, unavailable int
		for _, seg := range segs {
			if seg.Status == overlay.StatusPadded {
				padded++
			} else {
				unavailable++
			}
		}

		var parts []string
		if padded > 0 {
			parts = append(parts, fmt.Sprintf("%d padded", padded))
		}
		if unavailable > 0 {
			parts = append(parts, fmt.Sprintf("%d unavailable (430)", unavailable))
		}
		reason := fmt.Sprintf("%s segment(s) during download", strings.Join(parts, ", "))

		d.logger.Warn().Str("entry", entry.Name).Str("file", file.Name).
			Int("padded_segments", padded).Int("unavailable_segments", unavailable).
			Msg("Import: rejecting — segments were dead/padded during download")
		if rerr := d.manager.Repair().RegrabImportGrab(ctx, entry, file.Name, reason); rerr != nil {
			d.logger.Warn().Err(rerr).Str("entry", entry.Name).Str("file", file.Name).
				Msg("Import: failed to blocklist + re-search via Arr")
		}
		return fmt.Errorf("padding import check: %s", reason)
	}
	return nil
}

// statImportGate STATs every segment of each qualifying video file in a
// newly-completed NZB import and rejects the import if any segment is
// confirmed dead (a 430 across every provider, per
// nntp.IsArticleNotFoundError).
//
// This closes the gap paddingImportGate structurally can't: paddingImportGate
// only sees segments that were actually fetched (and padded) during the
// streaming import window - a small fraction of a large file - so a dead
// segment anywhere in the unread remainder sails straight through. The STAT
// census reads headers only, covers the whole file, and completes in a few
// seconds.
//
// Runs between importAvailabilityGate and ffprobeImportGate, using the same
// reject+regrab path (RegrabImportGrab) the other gates use. Inconclusive
// checks - a STAT transport error, or shutdown mid-sweep - fail open; only a
// positive dead-segment finding rejects. Only applies to NZB entries with an
// active usenet backend. Wrapped in recover() like its sibling gates: a bug
// here must never take down completeEntry.
func (d *Downloader) statImportGate(entry *storage.Entry) (err error) {
	defer func() {
		if r := recover(); r != nil {
			d.logger.Warn().Interface("panic", r).Str("entry", entry.Name).
				Msg("Import: STAT census gate panicked; proceeding without validation")
			err = nil
		}
	}()

	if entry.Protocol != config.ProtocolNZB || d.manager.usenet == nil {
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

		dead, total, herr := d.manager.usenet.StatFileHealth(ctx, fileNZBID(entry, file), file.Name)
		if herr != nil {
			d.logger.Debug().Err(herr).Str("entry", entry.Name).Str("file", file.Name).
				Msg("Import: STAT census error, treating as inconclusive")
			continue
		}
		if dead > 0 {
			reason := fmt.Sprintf("%d dead segment(s) confirmed by STAT census (of %d)", dead, total)
			d.logger.Warn().
				Str("entry", entry.Name).Str("file", file.Name).
				Int("dead_segments", dead).Int("total_segments", total).
				Msg("Import: STAT census found dead segments; rejecting")
			if rerr := d.manager.Repair().RegrabImportGrab(ctx, entry, file.Name, reason); rerr != nil {
				d.logger.Warn().Err(rerr).Str("entry", entry.Name).Str("file", file.Name).
					Msg("Import: failed to blocklist + re-search via Arr")
			}
			return fmt.Errorf("stat import check: %d dead segment(s) in %s", dead, file.Name)
		}
		d.logger.Debug().Str("entry", entry.Name).Str("file", file.Name).
			Int("segments_checked", total).
			Msg("Import: STAT census passed")
	}
	return nil
}
