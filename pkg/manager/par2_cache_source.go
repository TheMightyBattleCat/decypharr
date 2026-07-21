package manager

import (
	"sync/atomic"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// dfsCacheRangeReader is satisfied by the DFS mount's manager.MountManager
// implementation (pkg/mount/dfs.Manager) - narrowed to just the read this
// package needs. Checked via a type assertion against
// Manager.MountManager() rather than an import, since pkg/mount/dfs already
// imports this package (via *manager.Manager) and a direct import back
// would cycle. A failed assertion (rclone mode, no mount, mount not ready)
// just means no cache source is available this run; the existing Usenet
// fetch path is always the fallback.
type dfsCacheRangeReader interface {
	// PeekCachedRange reads [off, off+len(p)) of filename's cache item under
	// entryName directly from local disk if already fully cached, WITHOUT
	// triggering a download. False means nothing to read from here.
	PeekCachedRange(entryName, filename string, p []byte, off int64) bool
}

// cacheMapEntry records one extracted, uncompressed (IsStored) file's claim
// on part of one posted article's decoded body: the article-local byte
// range [DataStart, DataEnd) that article contributes to the extracted
// output, and where in that output file (OutputStart) it lands. Built once
// per repair pass by buildCacheSegmentMap.
type cacheMapEntry struct {
	fileName    string
	dataStart   int64 // inclusive, offset within the article's decoded body
	dataEnd     int64 // exclusive
	outputStart int64 // corresponding offset in the extracted output file
}

// buildCacheSegmentMap indexes every retained, uncompressed (IsStored)
// extracted file's segments by MessageID, for mapping a posted article's
// decoded-body byte range onto the local DFS cache's copy of the extracted
// output file it was unpacked into. A RAW posted file (never archived) and
// a STORE-mode RAR member both go through this same mapping - the
// difference between them is just whether SegmentDataStart happens to be 0
// for every segment (raw: no header to skip) or not (store-mode: header
// bytes precede the useful data within some articles).
//
// A compressed archive member is deliberately excluded: its extracted bytes
// have no direct byte-for-byte relationship to the posted article bytes (a
// real decompression pass produced them), so there is nothing here to map -
// and per the overlay's IsVideoContainer/PAR2 design, a compressed member
// isn't a repair candidate in the first place. Those callers always fall
// through to the existing Usenet fetch path.
func buildCacheSegmentMap(nzb *storage.NZB) map[string][]cacheMapEntry {
	out := make(map[string][]cacheMapEntry)
	for _, f := range nzb.Files {
		if f.IsDeleted || !f.IsStored {
			continue
		}
		for _, seg := range f.Segments {
			out[seg.MessageID] = append(out[seg.MessageID], cacheMapEntry{
				fileName:    f.Name,
				dataStart:   seg.SegmentDataStart,
				dataEnd:     seg.SegmentDataStart + seg.Bytes,
				outputStart: seg.StartOffset,
			})
		}
	}
	return out
}

// outputByteRange is a [start, end) byte range within one extracted file's
// output space.
type outputByteRange struct {
	start, end int64
}

// buildDeadOutputRanges maps pending's overlay dead/padded segments - the
// unknowns this whole repair pass exists to solve for - onto their output
// byte ranges in each affected extracted file, keyed by filename.
// overlay.DeadSegment.Index is the index of the segment WITHIN that file's
// own NZBFile.Segments (see pkg/usenet/fs/reader.NewSegmentMetaSlice, which
// preserves that same indexing when it's what recorded the segment as dead
// in the first place), so it's looked up directly. These ranges are
// EXCLUDED from cache-sourcing everywhere, not just for the file(s) this
// particular call is patching: on disk they read back as the padding
// feature's zero-fill, never real bytes, regardless of which repair pass
// happens to be looking at them right now.
func buildDeadOutputRanges(nzb *storage.NZB, pending map[string][]overlay.DeadSegment) map[string][]outputByteRange {
	out := make(map[string][]outputByteRange, len(pending))
	for file, segs := range pending {
		nf := nzb.GetFileByName(file)
		if nf == nil {
			continue
		}
		for _, d := range segs {
			if d.Index < 0 || d.Index >= len(nf.Segments) {
				continue
			}
			seg := nf.Segments[d.Index]
			out[file] = append(out[file], outputByteRange{start: seg.StartOffset, end: seg.EndOffset + 1})
		}
	}
	return out
}

func outputRangeOverlapsAny(rs []outputByteRange, start, end int64) bool {
	for _, r := range rs {
		if start < r.end && end > r.start {
			return true
		}
	}
	return false
}

// cacheSlicedSource resolves whether a posted article's decoded-body byte
// range is already on local disk via the DFS mount cache, for the streaming
// repair pass to source intact slices from instead of Usenet. A nil
// *cacheSlicedSource (or one with a nil reader) always misses - every
// caller falls back to fetching, matching pre-cache-sourcing behavior
// exactly.
type cacheSlicedSource struct {
	reader      dfsCacheRangeReader
	entryName   string
	byMessageID map[string][]cacheMapEntry
	deadRanges  map[string][]outputByteRange
	cacheBytes  *int64
	progress    *par2JobProgressState // nil-safe; live cache_bytes for the progress API
}

// readCached returns exactly n bytes if [dataStart, dataStart+n) of
// messageID's decoded article body is entirely covered by one cache-mapped
// extracted file's segment, that output range is not part of the overlay's
// current dead/padded set for that file (never trust cache bytes for the
// unknowns being solved for - see buildDeadOutputRanges), and the DFS cache
// actually has those output bytes present right now. ok=false for any
// other reason at all - the caller must fetch from Usenet instead.
func (s *cacheSlicedSource) readCached(messageID string, dataStart, n int64) ([]byte, bool) {
	if s == nil || s.reader == nil || n <= 0 {
		return nil, false
	}
	dataEnd := dataStart + n
	for _, m := range s.byMessageID[messageID] {
		if m.fileName == "" || dataStart < m.dataStart || dataEnd > m.dataEnd {
			continue // no mapping for this article, or doesn't fully cover our sub-range
		}
		outStart := m.outputStart + (dataStart - m.dataStart)
		outEnd := outStart + n
		if outputRangeOverlapsAny(s.deadRanges[m.fileName], outStart, outEnd) {
			continue // overlaps a dead/padded range - that's zeros on disk, not real data
		}
		buf := make([]byte, n)
		if !s.reader.PeekCachedRange(s.entryName, m.fileName, buf, outStart) {
			continue // not (fully) present in the cache right now
		}
		if s.cacheBytes != nil {
			atomic.AddInt64(s.cacheBytes, n)
		}
		s.progress.AddCacheBytes(n)
		return buf, true
	}
	return nil, false
}
