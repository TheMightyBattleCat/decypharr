package manager

import (
	"strconv"
	"strings"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// dfsCachePadForgetter is satisfied by the DFS mount MountManager (same
// type-assertion seam as dfsCacheRangeForgetter). A failed assertion means
// there is no durable cache holding marks.
type dfsCachePadForgetter interface {
	// ForgetPaddedRanges forgets every range of filename's cache item
	// under entryName that was marked as zero-fill when written, and drops
	// the marks. Returns the bytes forgotten.
	ForgetPaddedRanges(entryName, filename string) int64
}

// OverlayPadRanges returns the [start, end) byte ranges of entry's filename
// within [off, end) that the overlay records as dead or padded: the bytes a
// stream of that range served as zero-fill rather than the article. The DFS
// cache marks them in its sidecar (vfs CacheItem.markPads) so they can be
// forgotten once the records are gone. Encrypted files get the CBC widening
// a PAR2 forget uses (buildDeadOutputRanges). nil for anything but a usenet
// entry, and for a file with no such records; the NZB is only read when the
// file has some.
func (m *Manager) OverlayPadRanges(entry *storage.Entry, filename string, off, end int64) [][2]int64 {
	if m == nil || m.usenet == nil || entry == nil || entry.Protocol != config.ProtocolNZB || end <= off {
		return nil
	}
	nzbID := entry.InfoHash
	if !m.usenet.OverlayEntryExists(nzbID) {
		return nil
	}
	mf, err := m.usenet.OverlayManifest(nzbID)
	if err != nil || mf == nil {
		return nil
	}
	fe := mf.Files[filename]
	if fe == nil {
		return nil
	}
	var segs []overlay.DeadSegment
	var sig strings.Builder
	for _, d := range fe.DeadSegments {
		if d.Status != overlay.StatusPatched {
			segs = append(segs, d)
			sig.WriteString(strconv.Itoa(d.Index))
			sig.WriteByte(',')
		}
	}
	if len(segs) == 0 {
		return nil
	}
	// The ranges only change with the record set, so they are kept per
	// file and rebuilt (one NZB decode) only when the set differs.
	key := nzbID + "\x00" + filename
	var rs []outputByteRange
	if c, ok := m.padRangeCache.Load(key); ok && c.(padRangeCacheEntry).sig == sig.String() {
		rs = c.(padRangeCacheEntry).ranges
	} else {
		nzb, err := m.usenet.GetNZB(nzbID)
		if err != nil || nzb == nil {
			return nil
		}
		rs = buildDeadOutputRanges(nzb, map[string][]overlay.DeadSegment{filename: segs})[filename]
		m.padRangeCache.Store(key, padRangeCacheEntry{sig: sig.String(), ranges: rs})
	}
	var out [][2]int64
	for _, r := range rs {
		s, e := max(r.start, off), min(r.end, end)
		if s < e {
			out = append(out, [2]int64{s, e})
		}
	}
	return out
}

// padRangeCacheEntry is one file's entry in Manager.padRangeCache: its pad
// output ranges and the record set (segment indices) they were built from.
type padRangeCacheEntry struct {
	sig    string
	ranges []outputByteRange
}

// onOverlayDamageCleared is the overlay store's damage-cleared notifier: an
// overlay reap, reclaim, sticky-failed clear or entry delete removed these
// dead/padded records, the only note of where a viewer was served
// zero-fill. While the NZB record still loads, the cached copies of those
// segments are invalidated as a PAR2 repair does (live reader first, then
// the DFS ranges, then once more after a delay). Then every range the DFS
// cache marked as zero-fill for the file is forgotten, which also covers
// pads the records no longer describe. An entry whose record is already
// gone resolves no cache folder and is left alone: its sidecars carry its
// InfoHash, so a later grab under the same name discards them anyway.
func (m *Manager) onOverlayDamageCleared(nzbID string, removed map[string][]overlay.DeadSegment) {
	if m == nil || m.usenet == nil || len(removed) == 0 {
		return
	}
	for file := range removed {
		m.padRangeCache.Delete(nzbID + "\x00" + file)
	}
	entry, err := m.GetEntry(nzbID)
	if err != nil || entry == nil {
		return
	}
	if m.par2Repair != nil {
		if nzb, nerr := m.usenet.GetNZB(nzbID); nerr == nil && nzb != nil {
			m.par2Repair.invalidateCachedRanges(nzb, nzbID, entry.Name, removed)
		}
	}
	mgr := m.MountManager()
	if mgr == nil {
		return
	}
	fw, ok := mgr.(dfsCachePadForgetter)
	if !ok {
		return
	}
	folder := entry.GetFolder()
	if folder == "" || folder == "." {
		folder = entry.Name
	}
	for file := range removed {
		if n := fw.ForgetPaddedRanges(folder, file); n > 0 {
			m.logger.Info().Str("entry", entry.Name).Str("file", file).Int64("bytes", n).
				Msg("Forgot cached zero-fill after its overlay damage records were removed")
		}
	}
}
