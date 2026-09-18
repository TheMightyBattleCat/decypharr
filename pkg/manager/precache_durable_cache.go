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

// dfsCacheRangePresence is satisfied by the same DFS MountManager as
// dfsCacheRangeWriter: whether a byte range is already durably cached,
// answered from metadata without reading or fetching anything.
type dfsCacheRangePresence interface {
	HasCachedRange(entryName, filename string, off, length int64) bool
}

// cachePresence resolves the presence seam like cacheWriter; nil when no DFS
// mount is available, and then nothing is skipped as already cached.
func (p *Precache) cachePresence() dfsCacheRangePresence {
	mgr := p.manager.MountManager()
	if mgr == nil {
		return nil
	}
	have, _ := mgr.(dfsCacheRangePresence)
	return have
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
	persistDurableSpan(ctx, src, writer, nil, entryName, infoHash, filename, fileSize, 0, fileSize, log)
}

// persistDurableSpan is persistDurableRanges over the segments that start in
// [lo, hi), skipping any the durable cache already holds when have is set:
// reading one back goes through the reader, which re-downloads a segment its
// scratch cache has already evicted only for WriteAtNoOverwrite to discard it.
func persistDurableSpan(ctx context.Context, src precacheNZBSource, writer dfsCacheRangeWriter, have dfsCacheRangePresence, entryName, infoHash, filename string, fileSize, lo, hi int64, log zerolog.Logger) {
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
	st := persistSegments(ctx, src, writer, have, entryName, infoHash, filename, file, fileSize, lo, hi, log)
	st.log(log, entryName, filename, len(file.Segments), "durable persist complete")
}

// persistStats counts what a persist pass did, summed across the chunks of a
// burst.
//
// Read failures are split three ways so the summary line distinguishes a
// benign "not all fetched yet" from a real dead article:
//
//	shortReads - ReadCachedAt returned no error but fewer bytes than the
//	             segment's recorded size (partial fetch / near-EOF). The
//	             range simply isn't fully warm yet; a later pass gets it.
//	deadReads  - the backing article is confirmed missing (430) or shorter
//	             than posted. This segment raced ahead of the overlay's
//	             pending-repair map; it's real damage, not a timing
//	             artefact, and escalates the summary to WARN.
//	readErrors - any other read error (I/O, cancellation surfacing here).
type persistStats struct {
	bytesWritten, segmentsWritten, writeFailures, segmentsSeen int64
	shortReads, deadReads, readErrors, alreadyCached           int64
	firstByteZero, aborted                                     bool
}

func (s *persistStats) add(o persistStats) {
	s.bytesWritten += o.bytesWritten
	s.segmentsWritten += o.segmentsWritten
	s.writeFailures += o.writeFailures
	s.segmentsSeen += o.segmentsSeen
	s.shortReads += o.shortReads
	s.deadReads += o.deadReads
	s.readErrors += o.readErrors
	s.alreadyCached += o.alreadyCached
	s.firstByteZero = s.firstByteZero || o.firstByteZero
	s.aborted = s.aborted || o.aborted
}

// log writes the summary line. A benign short read (shortReads) is an
// expected "not warm yet" state and stays at INFO; only genuine damage
// (deadReads), write failures, or the zero-fill canary escalate to WARN.
func (s persistStats) log(log zerolog.Logger, entryName, filename string, segmentsTotal int, msg string) {
	evt := log.Info()
	if s.firstByteZero || s.writeFailures > 0 || s.deadReads > 0 || s.readErrors > 0 {
		evt = log.Warn()
	}
	evt.Str("entry", entryName).Str("file", filename).
		Int64("bytes", s.bytesWritten).Int64("segments", s.segmentsWritten).
		Int64("segmentsSeen", s.segmentsSeen).Int("segmentsTotal", segmentsTotal).
		Int64("alreadyCached", s.alreadyCached).
		Int64("writeFailures", s.writeFailures).
		Int64("shortReads", s.shortReads).Int64("deadReads", s.deadReads).Int64("readErrors", s.readErrors).
		Bool("firstByteZero", s.firstByteZero).Bool("aborted", s.aborted).
		Msg(msg)
}

// persistSegments copies file's CLEAN segments that start in [lo, hi) from
// the reader into the durable cache - the walk behind persistDurableSpan and
// burstToDurable. The pending-repair set is read on every call, so a burst
// persisting chunk by chunk sees damage its latest chunk surfaced.
func persistSegments(ctx context.Context, src precacheNZBSource, writer dfsCacheRangeWriter, have dfsCacheRangePresence, entryName, infoHash, filename string, file *storage.NZBFile, fileSize, lo, hi int64, log zerolog.Logger) persistStats {
	var st persistStats
	dead := make(map[int]bool)
	if pending, err := src.OverlayPendingRepair(infoHash); err == nil {
		for _, seg := range pending[filename] {
			dead[seg.Index] = true
		}
	}

	var buf []byte
	for idx, seg := range file.Segments {
		start, end := seg.StartOffset, seg.EndOffset+1 // EndOffset is inclusive
		if start < lo || start >= hi {
			continue
		}
		if ctx.Err() != nil {
			// Playhead moved on (or the run was cancelled) before we finished
			// walking this file's segments. Stop here rather than burning read
			// budget on bytes nobody's waiting for - a later pass picks up
			// whatever's left, exactly like a per-segment read failure would.
			st.aborted = true
			break
		}
		st.segmentsSeen++
		if dead[idx] {
			continue // PENDING-REPAIR - leave absent, never persisted as padding
		}
		if start < 0 || end <= start || end > fileSize {
			continue
		}
		size := end - start
		if have != nil && have.HasCachedRange(entryName, filename, start, size) {
			st.alreadyCached++
			continue
		}
		if int64(cap(buf)) < size {
			buf = make([]byte, size)
		}
		buf = buf[:size]
		n, err := src.ReadCachedAt(ctx, infoHash, filename, buf, start)
		if err != nil || int64(n) != size {
			switch {
			case err == nil:
				st.shortReads++
			case errors.Is(err, io.ErrUnexpectedEOF):
				// The reader stopped at a segment stored shorter than its
				// slot and handed back the contiguous prefix (see
				// StreamingReader.readFromCache). Same benign accounting as a
				// nil-error short read - it is a hole, not a dead article -
				// but it arrives as an explicit signal rather than as a count
				// that silently disagrees with the buffer.
				st.shortReads++
			case nntp.IsArticleNotFoundError(err) || errors.Is(err, ErrSegmentShort):
				st.deadReads++
			default:
				st.readErrors++
			}
			log.Debug().Err(err).Str("entry", entryName).Str("file", filename).Int("segment", idx).
				Int64("offset", start).Int("got", n).Int64("want", size).
				Msg("pre-cache: durable read incomplete")
			continue // not actually available right now - a later pass picks it up
		}
		if err := writer.WriteCachedRange(entryName, filename, fileSize, buf, start); err != nil {
			st.writeFailures++
			log.Debug().Err(err).Str("entry", entryName).Str("file", filename).Int("segment", idx).
				Msg("pre-cache: durable write failed")
			continue
		}
		st.bytesWritten += size
		st.segmentsWritten++
		if start == 0 {
			// Front-of-file zero-fill canary: burst zero-fill is already fixed
			// upstream, so this is a belt-and-suspenders alarm, not the
			// primary guard against it.
			st.firstByteZero = len(buf) > 0 && buf[0] == 0x00
		}
	}
	return st
}

// persistCleanRanges is the real-world entry point for persistDurableRanges,
// resolving the write seam and usenet source from the live manager. Segments
// the durable cache already holds are skipped, so the pass after a repair
// reads back only what the repair changed.
func (p *Precache) persistCleanRanges(ctx context.Context, entry *storage.Entry, filename string, fileSize int64) {
	if p.manager.usenet == nil {
		return
	}
	writer := p.cacheWriter()
	if writer == nil {
		return
	}
	persistDurableSpan(ctx, p.manager.usenet, writer, p.cachePresence(), entry.Name, entry.InfoHash, filename, fileSize, 0, fileSize, p.logger)
}

// durableBurstChunk is how much a burst fetches before copying it into the
// durable cache. The reader's scratch SegmentCache evicts past 256 MB
// (reader.DefaultConfig().MaxDisk) and also carries the playing file's own
// prefetch window, so a chunk has to fit well inside it. Before this a burst
// fetched the whole file first: Rocky III's in-playback read-ahead on
// 2026-09-18 fetched 10.8 GB and kept none of it, and a next-episode burst
// fetched everything but its last ~256 MB twice, once for the burst and
// again for the persist walk reading it back.
const durableBurstChunk = 96 << 20

// burstResult is what burstToDurable did.
type burstResult struct {
	fetched       int64 // bytes of chunks fetched over NNTP
	skipped       int64 // bytes of chunks the durable cache already held
	segmentsTotal int   // segments in the file; 0 when nothing was persisted
	persist       persistStats
}

// burstToDurable fetches [from, fileSize) of filename at concurrency and
// copies each durableBurstChunk into the durable DFS cache while it is still
// in the reader's scratch cache. A chunk the durable cache already holds is
// not fetched at all. Segments the overlay holds for repair are not
// persisted, so a zero-filled dead article never becomes durable data (see
// persistDurableRanges). With no DFS write seam (rclone mode, no mount) it
// is the plain read-ahead it replaced.
//
// Returns the first ctx error; chunks persisted before it stay durable. A
// dead article is not an error here, as with ReadAhead.
func (p *Precache) burstToDurable(ctx context.Context, entry *storage.Entry, filename string, from, fileSize int64, concurrency int) (burstResult, error) {
	writer := p.cacheWriter()
	if writer == nil {
		return burstResult{fetched: max(fileSize-from, 0)},
			p.manager.usenet.ReadAhead(ctx, entry.InfoHash, filename, from, concurrency)
	}
	return burstChunks(ctx, p.manager.usenet, writer, p.cachePresence(), entry.Name, entry.InfoHash, filename,
		from, fileSize, concurrency, durableBurstChunk, p.logger)
}

// burstSource is what burstChunks needs from *usenet.Usenet.
type burstSource interface {
	precacheNZBSource
	ReadAhead(ctx context.Context, nzoID, filename string, from int64, concurrency int) error
	ReadAheadRange(ctx context.Context, nzoID, filename string, off, length int64, concurrency int) error
}

// burstChunks is burstToDurable with its seams and chunk size passed in, so
// it can be tested without a usenet client or DFS mount.
func burstChunks(ctx context.Context, src burstSource, writer dfsCacheRangeWriter, have dfsCacheRangePresence, entryName, infoHash, filename string, from, fileSize int64, concurrency int, chunk int64, log zerolog.Logger) (burstResult, error) {
	var res burstResult
	from = max(from, 0)
	var file *storage.NZBFile
	if nzb, err := src.GetNZB(infoHash); err == nil {
		file = nzb.GetFileByName(filename)
	}
	if file == nil {
		res.fetched = max(fileSize-from, 0)
		return res, src.ReadAhead(ctx, infoHash, filename, from, concurrency)
	}
	res.segmentsTotal = len(file.Segments)

	// Segments are persisted by where they start, so the first chunk also
	// takes the segment `from` falls inside.
	first := from
	for _, seg := range file.Segments {
		if seg.StartOffset <= from && from <= seg.EndOffset {
			first = seg.StartOffset
			break
		}
	}
	for off := from; off < fileSize; off += chunk {
		n := min(chunk, fileSize-off)
		if have != nil && have.HasCachedRange(entryName, filename, off, n) {
			res.skipped += n
			continue
		}
		err := src.ReadAheadRange(ctx, infoHash, filename, off, n, concurrency)
		res.fetched += n
		lo := off
		if off == from {
			lo = first
		}
		res.persist.add(persistSegments(ctx, src, writer, have, entryName, infoHash, filename, file, fileSize, lo, off+n, log))
		if err != nil {
			return res, err
		}
	}
	return res, nil
}
