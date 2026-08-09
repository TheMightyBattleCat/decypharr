package manager

import (
	"context"

	"github.com/rs/zerolog"
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
// segment just skips it, never aborts the rest.
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

	var bytesWritten, segmentsWritten, writeFailures, readFailures int64
	firstByteZero := false

	var buf []byte
	for idx, seg := range file.Segments {
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
			readFailures++
			log.Debug().Err(err).Str("entry", entryName).Str("file", filename).Int("segment", idx).Int64("offset", start).
				Msg("next-episode pre-cache: durable read failed")
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

	evt := log.Info()
	if firstByteZero || writeFailures > 0 || readFailures > 0 {
		evt = log.Warn()
	}
	evt.Str("entry", entryName).Str("file", filename).
		Int64("bytes", bytesWritten).Int64("segments", segmentsWritten).
		Int64("writeFailures", writeFailures).Int64("readFailures", readFailures).
		Bool("firstByteZero", firstByteZero).
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
