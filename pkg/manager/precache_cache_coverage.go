package manager

import (
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// dfsCacheCoverageReader is satisfied by the DFS mount's manager.MountManager
// implementation (pkg/mount/dfs.Manager) - the same type-assertion bridge
// pattern dfsCacheRangeReader/dfsCacheRangeWriter use (see
// par2_cache_source.go / precache_durable_cache.go) rather than growing the
// shared MountManager interface with a method rclone mode and the stub can't
// answer. A failed assertion (rclone mode, no mount, mount not ready) just
// means there's no coverage figure available right now - callers keep
// whatever was last known.
type dfsCacheCoverageReader interface {
	// CacheCoverage returns filename's cache coverage under entryName:
	// cached bytes against the file's total declared size, plus the cache
	// item's last write time (modTime). ok=false means neither a live
	// in-memory cache item nor an on-disk metadata sidecar has an answer.
	CacheCoverage(entryName, filename string) (cached, total int64, modTime time.Time, ok bool)
}

// cacheCoverageReader resolves the DFS cache-coverage seam via the same
// MountManager() type-assertion dfsCacheRangeReader/dfsCacheRangeWriter use.
// nil when no DFS mount is available this run.
func (p *Precache) cacheCoverageReader() dfsCacheCoverageReader {
	mgr := p.manager.MountManager()
	if mgr == nil {
		return nil
	}
	reader, _ := mgr.(dfsCacheCoverageReader)
	return reader
}

// refreshCacheCoverage queries reader for r's live cache coverage and, if
// available, updates r's CachedBytes/TotalBytes/CacheCoverage. Leaves r
// untouched when reader is nil or has nothing to say right now - a figure
// already known (from a prior live query or populateFromCache's disk scan)
// must never regress to zero just because the live source is momentarily
// silent (item not loaded in memory this instant, metadata sidecar mid-write).
func (p *Precache) refreshCacheCoverage(reader dfsCacheCoverageReader, r *EpisodeReadiness) {
	if reader == nil {
		return
	}
	if !p.ownsCacheName(r.EntryName, r.Filename, r.InfoHash) {
		return
	}
	cached, total, _, ok := reader.CacheCoverage(r.EntryName, r.Filename)
	if !ok || total <= 0 {
		return
	}
	r.CachedBytes = cached
	r.TotalBytes = total
	r.CacheCoverage = float64(cached) / float64(total)
}

// populateFromCache is the "Tier 1" restart fix: on the first call (see
// Precache.Summary's cachePopulateOnce), walk every usenet-backed storage
// entry's media files and check whether the DFS cache already has bytes for
// it from a prior run - if so, seed a readiness row for it (ReadyAt taken
// from the cache item's own last-write time, since this wasn't pre-cached
// by this process), so the precache table isn't empty again after every
// restart. Best-effort and silent throughout: any entry/file this can't
// resolve is simply skipped, never surfaced as an error.
func (p *Precache) populateFromCache() {
	reader := p.cacheCoverageReader()
	if reader == nil || p.manager.usenet == nil {
		return
	}
	st := p.manager.Storage()
	if st == nil {
		return
	}

	_ = st.ForEach(func(entry *storage.Entry) error {
		if entry.Protocol != config.ProtocolNZB {
			return nil
		}
		for filename := range entry.Files {
			if !utils.IsMediaFile(filename) {
				continue
			}
			if !p.ownsCacheName(entry.Name, filename, entry.InfoHash) {
				continue
			}
			cached, total, modTime, ok := reader.CacheCoverage(entry.Name, filename)
			if !ok || cached <= 0 || total <= 0 {
				continue
			}

			key := entry.InfoHash + ":" + filename
			pc, ok := p.overlayPendingCount(entry, filename)
			clean := ok && pc == 0
			segPending := 0
			if ok && pc > 0 {
				segPending = pc
			}
			p.readinessMu.Lock()
			_, exists := p.readiness[key]
			if !exists {
				p.readiness[key] = EpisodeReadiness{
					EntryName:       entry.Name,
					InfoHash:        entry.InfoHash,
					Filename:        filename,
					ReadyAt:         modTime,
					CachedBytes:     cached,
					TotalBytes:      total,
					CacheCoverage:   float64(cached) / float64(total),
					Clean:           clean,
					SegmentsPending: segPending,
				}
			}
			p.readinessMu.Unlock()
		}
		return nil
	})
}

// ownsCacheName reports whether the entry stored under infoHash is the grab
// now served as filename under entryName. The DFS cache is keyed by that
// name, so its coverage is the current owner's: a same-name twin (an old
// grab still in storage, e.g. waiting on a deferred delete) got a readiness
// row showing the owner's cached bytes as its own. Files are compared, not
// entries: a season split out of a multi-season NZB has its own InfoHash
// while its files carry the NZB's, the same as the name index's. True when
// either side can't be resolved, so a row is never dropped on a lookup
// failure alone.
func (p *Precache) ownsCacheName(entryName, filename, infoHash string) bool {
	st := p.manager.Storage()
	if st == nil || infoHash == "" {
		return true
	}
	item, err := st.GetEntryItem(entryName)
	if err != nil || item == nil {
		return true
	}
	served := item.Files[filename]
	if served == nil || served.InfoHash == "" {
		return true
	}
	entry, err := p.manager.GetEntry(infoHash)
	if err != nil || entry == nil {
		return true
	}
	return fileNZBID(entry, entry.Files[filename]) == served.InfoHash
}

// overlayIsClean reports whether filename under entry currently has no
// overlay-pending-repair damage - the same signal recordReadiness's
// pendingCount uses, reused here so a disk-discovered readiness row's Clean
// flag means the same thing a live one's does.
func (p *Precache) overlayIsClean(entry *storage.Entry, filename string) bool {
	c, ok := p.overlayPendingCount(entry, filename)
	return ok && c == 0
}

// overlayPendingCount returns filename's still-damaged segment count under
// entry per the overlay's pending-repair map, and ok=false if the overlay
// lookup itself failed (as opposed to a genuine zero-damage answer) - so
// callers can tell "known clean" apart from "unknown" instead of collapsing
// both into false/0.
func (p *Precache) overlayPendingCount(entry *storage.Entry, filename string) (int, bool) {
	pending, err := p.manager.usenet.OverlayPendingRepair(entry.InfoHash)
	if err != nil {
		return 0, false
	}
	return len(pending[filename]), true
}

// durableCacheComplete reports whether the durable DFS cache already holds
// filename under entryName end to end, i.e. whether a next-episode burst has
// anything left to fetch.
//
// This is the gate in front of ReadAhead + persistCleanRanges, and it is the
// difference between "re-verify" and "re-download". The burst reads through
// the usenet reader's SegmentCache, which is per-reader scratch under
// Usenet.DiskBufferPath that is removed when the reader closes - it is not the
// durable cache and does not consult it. So on a reader opened after a restart
// every segment is StateEmpty and SegmentFetcher.doFetch pulls the article
// down again over NNTP, even for a file the DFS cache already holds in full.
//
// Split out as a plain function of the coverage seam so the decision is
// testable without a real DFS mount or usenet client, matching the gating
// tests around reserveBudget.
func (p *Precache) durableCacheComplete(reader dfsCacheCoverageReader, entryName, filename string) bool {
	if reader == nil {
		// No coverage seam (rclone mode, no mount, mount not ready). Nothing
		// can be proven cached, so let the burst run as before.
		return false
	}
	cached, total, _, ok := reader.CacheCoverage(entryName, filename)
	if !ok || total <= 0 {
		return false
	}
	switch {
	case cached >= total:
		p.logger.Info().Str("entry", entryName).Str("file", filename).
			Int64("cachedBytes", cached).Int64("totalBytes", total).
			Msg("next-episode precache: already fully cached; skipping burst")
		return true
	case cached > 0:
		p.logger.Info().Str("entry", entryName).Str("file", filename).
			Int64("cachedBytes", cached).Int64("totalBytes", total).
			Float64("coverage", float64(cached)/float64(total)).
			Msg("next-episode precache: partially cached; burst will fill the gaps")
	default:
		p.logger.Debug().Str("entry", entryName).Str("file", filename).
			Msg("next-episode precache: nothing cached yet; cold burst")
	}
	return false
}
