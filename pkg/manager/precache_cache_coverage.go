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
			cached, total, modTime, ok := reader.CacheCoverage(entry.Name, filename)
			if !ok || cached <= 0 || total <= 0 {
				continue
			}

			key := entry.InfoHash + ":" + filename
			p.readinessMu.Lock()
			_, exists := p.readiness[key]
			if !exists {
				p.readiness[key] = EpisodeReadiness{
					EntryName:     entry.Name,
					Filename:      filename,
					ReadyAt:       modTime,
					CachedBytes:   cached,
					TotalBytes:    total,
					CacheCoverage: float64(cached) / float64(total),
					Clean:         p.overlayIsClean(entry, filename),
				}
			}
			p.readinessMu.Unlock()
		}
		return nil
	})
}

// overlayIsClean reports whether filename under entry currently has no
// overlay-pending-repair damage - the same signal recordReadiness's
// pendingCount uses, reused here so a disk-discovered readiness row's Clean
// flag means the same thing a live one's does.
func (p *Precache) overlayIsClean(entry *storage.Entry, filename string) bool {
	pending, err := p.manager.usenet.OverlayPendingRepair(entry.InfoHash)
	if err != nil {
		return false
	}
	return len(pending[filename]) == 0
}
