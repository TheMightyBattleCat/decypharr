package manager

import (
	"context"
	"errors"
	"io"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// dfsCacheRangeWriter is satisfied by the DFS mount's manager.MountManager
// implementation (pkg/mount/dfs.Manager) - the write-side mirror of
// dfsCacheRangeReader (see par2_cache_source.go for why this is a
// type-asserted interface rather than a direct import: pkg/mount/dfs
// already imports this package via *manager.Manager, so a direct import
// back would cycle). A failed assertion (rclone mode, no mount, mount not
// ready) just means there's nowhere to durably persist pre-cached bytes
// this run - the ephemeral per-stream SegmentCache still serves the
// read-ahead/damage-detection pass exactly as before this existed.
type dfsCacheRangeWriter interface {
	// WriteCachedRange durably writes p at [off, off+len(p)) into filename's
	// cache item under entryName, creating it (sized by fileSize) if it
	// doesn't exist yet. Pure disk write: no fetch, no padding, no Stream,
	// no Downloaders - callers must only ever pass bytes already known
	// correct (see Precache.persistCleanRanges).
	WriteCachedRange(entryName, filename string, fileSize int64, p []byte, off int64) error
}

// cacheWriter resolves the DFS write seam via the same MountManager()
// type-assertion dfsCacheRangeReader uses. nil (not an error) when no DFS
// mount is available - callers treat that as "nothing to durably write to
// this run" and fall back to their pre-existing behavior.
func (p *Precache) cacheWriter() dfsCacheRangeWriter {
	mgr := p.manager.MountManager()
	if mgr == nil {
		return nil
	}
	writer, _ := mgr.(dfsCacheRangeWriter)
	return writer
}

// precacheNZBSource narrows *usenet.Usenet to what persistDurableRanges
// needs. Satisfied implicitly (Go structural interfaces) with no changes to
// the usenet package; exists purely so tests can substitute a fake instead
// of a real NNTP client/overlay store.
type precacheNZBSource interface {
	GetNZB(id string) (*storage.NZB, error)
	OverlayPendingRepair(nzoID string) (map[string][]overlay.DeadSegment, error)
	ReadCachedAt(ctx context.Context, nzoID, filename string, p []byte, off int64) (int, error)
}

// persistDurableRanges durably writes every CLEAN segment of filename
// (under infoHash) into the DFS cache via writer - the pure orchestration
// behind Precache.persistCleanRanges, factored out so it's testable without
// a real usenet client.
//
// Per-range state: a segment absent from src's overlay pending-repair set
// is CLEAN (or, on a later call after repair, REPAIRED) and gets read back
// via ReadCachedAt and written durably here. A segment PRESENT in the
// pending-repair set is PENDING-REPAIR and is skipped entirely - not
// written as zero-fill/padding, not marked any other way, simply absent
// from the durable entry - so a playhead reaching it before repair lands
// always falls through to the normal live fetch path (see
// pkg/usenet/fs/reader's handleConfirmedMissing) instead of ever reading
// persisted padding as final. Calling this again after a segment's damage
// is repaired (see Precache.recordReadiness) picks it up on that later
// call - the same function carries a segment PENDING-REPAIR -> REPAIRED
// with no separate code path or on-disk marker.
//
// Lock-eligibility note for the follow-up block that adds retention
// locking: only bytes this function actually writes - CLEAN or REPAIRED -
// are ever durably present at all, so only those can ever become
// lock-eligible. A PENDING-REPAIR range is never durably present, so there
// is nothing here for a future lock to accidentally protect.
//
// No-op if writer is nil (DFS write seam unavailable this run - rclone
// mode, no mount, mount not ready) or the NZB/file/segments can't be
// resolved. Best-effort throughout: a read or write failure for one
// segment just skips it, never aborts the rest. A cancelled/expired ctx
// (playhead moved on) stops the segment walk cleanly at the next
// iteration - whatever's already durably written stays, the rest is left
// for a later pass - and the summary log carries aborted=true.
func persistDurableRanges(ctx context.Context, src precacheNZBSource, writer dfsCacheRangeWriter, entryName, infoHash, filename string, fileSize int64, log zerolog.Logger) {
	if writer == nil || src == nil {
		return
	}
	nzb, err := src.GetNZB(infoHash)
	if err != nil {
		return
	}
	file := nzb.GetFileByName(filename)
	if file == nil {
		return
	}

	dead := make(map[int]bool)
	if pending, err := src.OverlayPendingRepair(infoHash); err == nil {
		for _, seg := range pending[filename] {
			dead[seg.Index] = true
		}
	}

	// Read failures are split three ways so the summary line distinguishes a
	// benign "not all fetched yet" from a real dead article:
	//   shortReads  - ReadCachedAt returned no error but fewer bytes than the
	//                 segment's recorded size (partial fetch / near-EOF). The
	//                 range simply isn't fully warm yet; a later pass gets it.
	//   deadReads   - the backing article is confirmed missing (430) or
	//                 shorter than posted. This segment raced ahead of the
	//                 overlay's pending-repair map; it's real damage, not a
	//                 timing artefact, and escalates the summary to WARN.
	//   readErrors  - any other read error (I/O, cancellation surfacing here).
	var bytesWritten, segmentsWritten, writeFailures, segmentsSeen int64
	var shortReads, deadReads, readErrors int64
	firstByteZero := false
	aborted := false

	var buf []byte
	for idx, seg := range file.Segments {
		if ctx.Err() != nil {
			// Playhead moved on (or the run was cancelled) before we finished
			// walking this file's segments. Stop here rather than burning read
			// budget on bytes nobody's waiting for - a later pass picks up
			// whatever's left, exactly like a per-segment read failure would.
			aborted = true
			break
		}
		segmentsSeen++
		if dead[idx] {
			continue // PENDING-REPAIR - leave absent, never persisted as padding
		}
		start, end := seg.StartOffset, seg.EndOffset+1 // EndOffset is inclusive
		if start < 0 || end <= start || end > fileSize {
			continue
		}
		size := end - start
		if int64(cap(buf)) < size {
			buf = make([]byte, size)
		}
		buf = buf[:size]
		n, err := src.ReadCachedAt(ctx, infoHash, filename, buf, start)
		if err != nil || int64(n) != size {
			switch {
			case err == nil:
				shortReads++
			case errors.Is(err, io.ErrUnexpectedEOF):
				// The reader stopped at a segment stored shorter than its
				// slot and handed back the contiguous prefix (see
				// StreamingReader.readFromCache). Same benign accounting as a
				// nil-error short read - it is a hole, not a dead article -
				// but it arrives as an explicit signal rather than as a count
				// that silently disagrees with the buffer.
				shortReads++
			case nntp.IsArticleNotFoundError(err) || errors.Is(err, ErrSegmentShort):
				deadReads++
			default:
				readErrors++
			}
			log.Debug().Err(err).Str("entry", entryName).Str("file", filename).Int("segment", idx).
				Int64("offset", start).Int("got", n).Int64("want", size).
				Msg("next-episode pre-cache: durable read incomplete")
			continue // not actually available right now - a later pass picks it up
		}
		if err := writer.WriteCachedRange(entryName, filename, fileSize, buf, start); err != nil {
			writeFailures++
			log.Debug().Err(err).Str("entry", entryName).Str("file", filename).Int("segment", idx).
				Msg("next-episode pre-cache: durable write failed")
			continue
		}
		bytesWritten += size
		segmentsWritten++
		if start == 0 {
			// Front-of-file zero-fill canary: burst zero-fill is already fixed
			// upstream, so this is a belt-and-suspenders alarm, not the
			// primary guard against it.
			firstByteZero = len(buf) > 0 && buf[0] == 0x00
		}
	}

	// A benign short read (shortReads) is an expected "not warm yet" state and
	// stays at INFO; only genuine damage (deadReads), write failures, or the
	// zero-fill canary escalate to WARN.
	evt := log.Info()
	if firstByteZero || writeFailures > 0 || deadReads > 0 || readErrors > 0 {
		evt = log.Warn()
	}
	evt.Str("entry", entryName).Str("file", filename).
		Int64("bytes", bytesWritten).Int64("segments", segmentsWritten).
		Int64("segmentsSeen", segmentsSeen).Int("segmentsTotal", len(file.Segments)).
		Int64("writeFailures", writeFailures).
		Int64("shortReads", shortReads).Int64("deadReads", deadReads).Int64("readErrors", readErrors).
		Bool("firstByteZero", firstByteZero).Bool("aborted", aborted).
		Msg("durable persist complete")
}

// persistCleanRanges is the real-world entry point for persistDurableRanges,
// resolving the write seam and usenet source from the live manager.
func (p *Precache) persistCleanRanges(ctx context.Context, entry *storage.Entry, filename string, fileSize int64) {
	if p.manager.usenet == nil {
		return
	}
	writer := p.cacheWriter()
	if writer == nil {
		return
	}
	persistDurableRanges(ctx, p.manager.usenet, writer, entry.Name, entry.InfoHash, filename, fileSize, p.logger)
}
