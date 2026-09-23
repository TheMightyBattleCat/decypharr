package manager

import "time"

// watchedEvictWindow is how long a watched pre-cached episode waits for its
// last handle to close before its eviction is dropped.
const watchedEvictWindow = 24 * time.Hour

// dfsCacheFileEvictor is satisfied by the DFS mount's MountManager: remove
// one file's cached bytes unless it is open (see dfsCacheRangeWriter for why
// this is type-asserted rather than imported).
type dfsCacheFileEvictor interface {
	EvictCachedFile(entryName, filename string) (freed int64, ok bool)
}

// watchedEvict is a watched pre-cached episode whose DFS cache is still to
// be removed.
type watchedEvict struct {
	infoHash, entryName, filename string
	until                         time.Time
}

// queueWatchedEvict schedules the DFS cache of a watched pre-cached episode
// for removal and tries once now. evictIfWatched runs at 90% of the file,
// while the viewer still holds it, so the first try usually fails; the
// maintenance tick retries (evictWatchedDue). "Evict after watched" used to
// drop only the usenet reader's scratch buffer and release the budget: the
// episode's durable DFS cache - the actual disk footprint - stayed.
func (p *Precache) queueWatchedEvict(infoHash, entryName, filename string) {
	p.evictMu.Lock()
	if p.evictQueue == nil {
		p.evictQueue = make(map[string]*watchedEvict)
	}
	p.evictQueue[infoHash+":"+filename] = &watchedEvict{
		infoHash: infoHash, entryName: entryName, filename: filename,
		until: time.Now().Add(watchedEvictWindow),
	}
	p.evictMu.Unlock()
	p.evictWatchedDue()
}

// evictWatchedDue tries every queued eviction once, dropping those that
// succeed or have expired. A file a burst is writing is left for later.
func (p *Precache) evictWatchedDue() {
	ev, _ := p.manager.MountManager().(dfsCacheFileEvictor)
	now := time.Now()
	p.evictMu.Lock()
	due := make([]*watchedEvict, 0, len(p.evictQueue))
	for k, w := range p.evictQueue {
		if now.After(w.until) || ev == nil {
			delete(p.evictQueue, k)
			continue
		}
		due = append(due, w)
	}
	p.evictMu.Unlock()

	for _, w := range due {
		if p.InflightHas(w.infoHash, w.filename) {
			continue
		}
		freed, ok := ev.EvictCachedFile(w.entryName, w.filename)
		if !ok {
			continue // still open
		}
		p.evictMu.Lock()
		delete(p.evictQueue, w.infoHash+":"+w.filename)
		p.evictMu.Unlock()
		p.dropReadiness(w.infoHash + ":" + w.filename)
		p.logger.Info().Str("entry", w.entryName).Str("file", w.filename).Int64("bytes_freed", freed).
			Msg("evicted watched pre-cached episode from the DFS cache")
	}
}
