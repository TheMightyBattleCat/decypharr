// Sonarr next-episode pre-cache: once a playing episode crosses the
// read-ahead threshold (see precache.go), resolve and burst-download the
// next episode(s) in the same series ahead of time, then repair them if
// damaged - so by the time playback actually reaches them, they're already
// intact on disk. Movies have no "next episode"; read-ahead (precache.go) is
// their entire path here.
package manager

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/notifications"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

const (
	// precacheNextEpisodeTimeout bounds one episode's step of the forward
	// walk (burst-download + repair-wait). Per episode rather than per walk:
	// a whole-season walk at ~4 min an episode outlasted a shared 45 minutes.
	precacheNextEpisodeTimeout = 45 * time.Minute

	// precacheWalkResolveTimeout bounds resolving the playing episode's
	// Sonarr coordinate, which scans every Sonarr instance's media.
	precacheWalkResolveTimeout = 5 * time.Minute

	// precacheRepairWaitTimeout bounds how long awaitReadiness waits for
	// the repair it handed an episode's damage to before recording the
	// episode as still-damaged. The wait runs beside the walk, not in it,
	// so it can cover a queued urgent PAR2 pass on a large release; a pass
	// that lands later still updates the row (see OnPar2Repaired).
	precacheRepairWaitTimeout  = 30 * time.Minute
	precacheRepairPollInterval = 5 * time.Second
)

// EpisodeReadiness is the outcome of a next-episode pre-cache pass, surfaced
// to notifications/GUI (see Commit D).
type EpisodeReadiness struct {
	EntryName        string    `json:"entry_name"`
	InfoHash         string    `json:"info_hash"` // paired with Filename, forms the "infoHash:filename" key SetKeyPaused/keyPaused use
	Filename         string    `json:"filename"`
	ReadyAt          time.Time `json:"ready_at"`
	Clean            bool      `json:"clean"`             // no damage found at all
	SegmentsRepaired int       `json:"segments_repaired"` // > 0 only when damage was found AND fully repaired before the wait timed out
	SegmentsPending  int       `json:"segments_pending"`  // still-damaged segments left when the wait gave up (0 if clean or fully repaired)

	// Paused reports whether this specific (info_hash,filename) is
	// currently held back from starting a new burst - either individually
	// (SetKeyPaused) or via the global pause (SetPaused). Computed fresh in
	// Summary(), not stored alongside the rest of the row.
	Paused bool `json:"paused"`

	// CachedBytes/TotalBytes/CacheCoverage are a live snapshot of the DFS
	// cache's coverage for this file - refreshed on every Summary() call
	// (see Precache.refreshCacheCoverage), not just set once at pre-cache
	// time, so the GUI bar tracks eviction/re-caching too. Zero until the
	// first successful CacheCoverage query for this row.
	CachedBytes   int64   `json:"cached_bytes"`
	TotalBytes    int64   `json:"total_bytes"`
	CacheCoverage float64 `json:"cache_coverage"` // 0.0-1.0
}

// walkIdentity is the Sonarr coordinate a forward walk needs: which Arr to
// ask, which series, and the season/episode to step forward FROM. It is
// resolved once, before the walk starts, so the walk never has to resolve a
// decypharr entry again - by the time a damaged burst finishes, its entry may
// have been deleted by its own re-grab.
type walkIdentity struct {
	arr *arr.Arr

	// seriesName is the decypharr entry name the walk originally started
	// from, carried for log context only - never used to resolve anything,
	// so it staying valid does not matter.
	seriesName string

	seriesId      int
	seasonNumber  int
	episodeNumber int
}

// forwardWalk is one running walk's window. target is the last episode
// number to warm, or -1 for the rest of the season. Guarded by
// Precache.walksMu.
type forwardWalk struct {
	target int
}

// walkTarget is the last episode number a walk from episode `from` warms:
// -1 (season end) when ahead is negative, otherwise from+ahead.
func walkTarget(from, ahead int) int {
	if ahead < 0 {
		return -1
	}
	return from + ahead
}

// extend widens w's window to cover target: -1 (season end) wins, otherwise
// the larger episode number.
func (w *forwardWalk) extend(target int) {
	if w.target == -1 || target == -1 {
		w.target = -1
		return
	}
	w.target = max(w.target, target)
}

// covers reports whether episode sits inside w's window.
func (w *forwardWalk) covers(episode int) bool {
	return w.target == -1 || episode <= w.target
}

func walkKey(ident walkIdentity) string {
	return fmt.Sprintf("%s|%d|%d", ident.arr.Name, ident.seriesId, ident.seasonNumber)
}

// maybePrecacheNextEpisodes resolves the Sonarr series/episode context for a
// playing (entry, filename) and, if found, walks forward
// config.Precache.EpisodesAhead episodes (or to the season's end):
// burst-downloading (and repairing) each one that's already grabbed, or
// triggering a targeted Sonarr search for the first one that isn't. No-op for
// movies (no Sonarr context resolves), when the depth is 0, or when arrs are
// unreachable.
//
// Called when playback of a file crosses the threshold: from readAhead once
// its burst ends, or directly when that file was already burst-cached by an
// earlier walk (see tryMarkWalked). Playing each episode therefore moves the
// window forward by one; the walk no longer restarts itself after every
// burst, which is what made any depth behave as "whole season".
func (p *Precache) maybePrecacheNextEpisodes(entry *storage.Entry, filename string) {
	// Next-episode precache only applies to series. Resolve the Arr this entry
	// came from and bail unless it's a Sonarr instance - a movie (Radarr) has no
	// next episode, and running the lookup for one wastes a call against every
	// Sonarr instance just to fail the filename match.
	if a := p.manager.arr.GetOrCreate(entry.Category); a == nil || a.Type != arr.Sonarr {
		return
	}
	ahead := p.cfg().EpisodesAhead()
	if ahead == 0 || p.manager.usenet == nil {
		return
	}

	ctx, cancel := context.WithTimeout(p.baseCtx(), precacheWalkResolveTimeout)
	a, seriesId, seasonNumber, episodeNumber, ok := p.resolveSonarrEpisode(ctx, entry, filename)
	cancel()
	if !ok {
		p.logger.Debug().Str("entry", entry.Name).Msg("next-episode precache: no matching Sonarr episode resolved")
		return
	}

	p.startForwardWalk(walkIdentity{
		arr:           a,
		seriesName:    entry.Name,
		seriesId:      seriesId,
		seasonNumber:  seasonNumber,
		episodeNumber: episodeNumber,
	}, ahead)
}

// baseCtx is the service's lifetime context, so Stop ends a running walk.
func (p *Precache) baseCtx() context.Context {
	if p.ctx != nil {
		return p.ctx
	}
	return context.Background()
}

// startForwardWalk runs the walk for ident's season, or - when one is already
// running for that series and season - widens its window and returns. One
// walk per season keeps two bursts of the same season from racing each
// other for bandwidth.
func (p *Precache) startForwardWalk(ident walkIdentity, ahead int) {
	target := walkTarget(ident.episodeNumber, ahead)
	key := walkKey(ident)

	p.walksMu.Lock()
	if w, running := p.walks[key]; running {
		w.extend(target)
		p.walksMu.Unlock()
		p.logger.Debug().Str("series", ident.seriesName).Int("season", ident.seasonNumber).
			Int("fromEpisode", ident.episodeNumber).Int("target", target).
			Msg("next-episode precache: extended the running walk")
		return
	}
	w := &forwardWalk{target: target}
	p.walks[key] = w
	p.walksMu.Unlock()

	p.precacheForwardWalk(ident, key, w)
}

// walkContinues reports whether the walk should warm episode, removing the
// walk from p.walks when it should not. The check and the removal share one
// lock, so a trigger that extends the window can never land on a walk that
// has already decided to stop.
func (p *Precache) walkContinues(key string, w *forwardWalk, episode int) bool {
	p.walksMu.Lock()
	defer p.walksMu.Unlock()
	if w.covers(episode) {
		return true
	}
	delete(p.walks, key)
	return false
}

// precacheForwardWalk steps forward from ident through w's window, one
// episode at a time, burst-caching each already-grabbed episode and asking
// Sonarr to search for the first one that isn't. Pure Sonarr-coordinate walk:
// it never touches the decypharr entry it began at, so a caller whose entry
// has since been deleted still drives it.
func (p *Precache) precacheForwardWalk(ident walkIdentity, key string, w *forwardWalk) {
	ended := false
	defer func() {
		if !ended {
			p.walksMu.Lock()
			delete(p.walks, key)
			p.walksMu.Unlock()
		}
	}()

	a, seriesId, seasonNumber := ident.arr, ident.seriesId, ident.seasonNumber
	episodeNumber := ident.episodeNumber
	base := p.baseCtx()

	// Season warm-up summary: one INFO line when the forward walk ends
	// (whatever the reason - window reached, season boundary, Arr error, or
	// shutdown), so the log shows how far ahead the season actually got
	// warmed rather than just a scatter of per-episode lines.
	var burstsRun, searchesRequested, skipped int
	deferred := false
	walkStart := time.Now()
	defer func() {
		p.walksMu.Lock()
		target := w.target
		p.walksMu.Unlock()
		p.logger.Info().Str("series", ident.seriesName).Int("season", seasonNumber).
			Int("fromEpisode", ident.episodeNumber).
			Int("throughEpisode", episodeNumber).
			Int("target", target).
			Int("burstsRun", burstsRun).
			Int("searchesRequested", searchesRequested).
			Int("skipped", skipped).
			Bool("deferred", deferred).
			Dur("elapsed", time.Since(walkStart)).
			Msg("next-episode precache: forward walk complete")
	}()

	for {
		if base.Err() != nil {
			return
		}
		lookupCtx, cancel := context.WithTimeout(base, precacheWalkResolveTimeout)
		next, found, err := a.NextEpisode(lookupCtx, seriesId, seasonNumber, episodeNumber)
		cancel()
		if err != nil {
			p.logger.Debug().Err(err).Str("series", ident.seriesName).Msg("next-episode precache: NextEpisode lookup failed (Arr unreachable?)")
			return
		}
		if !found {
			p.logger.Info().Str("series", ident.seriesName).Int("season", seasonNumber).Int("afterEpisode", episodeNumber).
				Msg("next-episode precache: reached season boundary, no further episode to warm")
			return
		}
		if !p.walkContinues(key, w, next.EpisodeNumber) {
			ended = true
			return
		}
		episodeNumber = next.EpisodeNumber

		if !next.HasFile {
			// Not grabbed yet - ask Sonarr to fetch it. decypharr's normal
			// download pipeline (Sonarr -> emulated download client ->
			// import) brings it into the mount in due course; there's
			// nothing to burst-cache until then. Playing a later episode
			// walks forward again and reaches it once it has landed.
			searchCtx, cancel := context.WithTimeout(base, precacheWalkResolveTimeout)
			err := a.SearchEpisode(searchCtx, next.EpisodeId)
			cancel()
			if err != nil {
				p.logger.Debug().Err(err).Str("series", ident.seriesName).Int("season", next.SeasonNumber).Int("episode", next.EpisodeNumber).Msg("next-episode search failed")
			} else {
				searchesRequested++
				p.logger.Info().Str("series", ident.seriesName).Int("season", next.SeasonNumber).Int("episode", next.EpisodeNumber).Msg("next episode not yet grabbed; requested search")
			}
			return
		}

		epCtx, cancel := context.WithTimeout(base, precacheNextEpisodeTimeout)
		step := p.precacheEpisodeFile(epCtx, walkIdentity{
			arr:           a,
			seriesName:    ident.seriesName,
			seriesId:      seriesId,
			seasonNumber:  seasonNumber,
			episodeNumber: next.EpisodeNumber,
		}, next)
		cancel()
		switch step {
		case stepBurst:
			burstsRun++
		case stepSkipped:
			skipped++
		case stepDeferred:
			// Walking on would warm a later episode while this one stays
			// cold. Stop; the next episode played starts the walk again.
			deferred = true
			return
		}
	}
}

// resolveSonarrEpisode finds the Sonarr arr, series id, season, and episode
// number for a currently-playing entry, matching every Sonarr arr's media
// against entry the same way the repair sweep correlates Arr content to
// decypharr entries (symlink-target resolution - see
// collectArrFiles/readSymlinkTarget in repair_sweep.go). ok=false means no
// Sonarr arr claims this entry (e.g. it's a movie, or arrs are
// unreachable).
func (p *Precache) resolveSonarrEpisode(ctx context.Context, entry *storage.Entry, filename string) (a *arr.Arr, seriesId, seasonNumber, episodeNumber int, ok bool) {
	for _, cand := range p.manager.arr.GetAll() {
		if cand.Type != arr.Sonarr {
			continue
		}
		if ctx.Err() != nil {
			return nil, 0, 0, 0, false
		}
		media, err := cand.GetMedia(ctx, "")
		if err != nil {
			p.logger.Debug().Err(err).Str("arr", cand.Name).Msg("next-episode precache: Sonarr media lookup failed")
			continue
		}
		for _, content := range media {
			for entryPath, files := range collectArrFiles(content) {
				name := filepath.Clean(filepath.Base(entryPath))
				if name != entry.Name {
					continue
				}
				for _, f := range files {
					if f.EpisodeNumber <= 0 {
						continue
					}
					return cand, f.Id, f.SeasonNumber, f.EpisodeNumber, true
				}
			}
		}
	}
	return nil, 0, 0, 0, false
}

// precacheEpisodeFile resolves next's library path back to a decypharr entry
// (the same symlink-target trick resolveSonarrEpisode uses), burst-downloads
// it in full at high concurrency (subject to bandwidth headroom and
// PrecacheMaxBytes), then repairs it ahead of time if the burst surfaced any
// damage - cheap now, since intact slices come from the bytes just cached.
// Records the outcome for Commit D (notifications/GUI). Best-effort: any
// failure just means this episode isn't pre-cached, not a hard error.
// The step result tells the walk whether to go on: stepSkipped (not ours,
// paused, already done) steps past this episode, stepDeferred (budget or
// bandwidth) ends the walk so it never warms a later episode while this one
// stays cold - the next episode played restarts it. ref is next's Sonarr
// coordinate, kept so a re-grab of it can be re-warmed (see addRewarm).
func (p *Precache) precacheEpisodeFile(ctx context.Context, ref walkIdentity, next arr.NextEpisodeInfo) episodeStep {
	nextEntry, filename, ok := p.episodeEntry(next)
	if !ok {
		return stepSkipped
	}

	if p.keyPaused(nextEntry.InfoHash, filename) {
		p.logger.Debug().Str("entry", nextEntry.Name).Str("file", filename).
			Msg("next-episode precache skipped: paused")
		return stepSkipped
	}

	key := nextEntry.InfoHash + ":" + filename
	p.markInflight(key)
	defer p.unmarkInflight(key)

	p.mu.Lock()
	_, already := p.triggered[key]
	p.triggered[key] = time.Now()
	p.mu.Unlock()
	if already {
		p.logger.Debug().Str("key", key).Msg("next-episode precache: already triggered for this file")
		return stepSkipped
	}

	// Both deferrals below un-claim key: it was claimed above but no burst
	// ran, and leaving it claimed hid this episode from every later walk -
	// and from its own read-ahead when played - for precacheTriggeredTTL.
	if !p.reserveBudget(next.Size) {
		p.untrigger(key)
		p.logger.Debug().Str("entry", nextEntry.Name).Int64("size", next.Size).Msg("next-episode pre-cache deferred: PrecacheMaxBytes budget exhausted")
		return stepDeferred
	}
	if !p.manager.usenet.HasBandwidthHeadroom() {
		p.untrigger(key)
		p.logger.Debug().Str("entry", nextEntry.Name).Msg("next-episode pre-cache deferred: no bandwidth headroom outside reserve")
		p.releaseBudget(next.Size)
		return stepDeferred
	}

	// What does the DFS cache already hold for this file? Checked before the
	// burst so an operator can tell a genuine cold fetch apart from a re-warm
	// of an already-cached (or partially-evicted) file - and so a file the
	// durable cache already holds in full can skip the burst entirely.
	fullyCached := p.durableCacheComplete(p.cacheCoverageReader(), nextEntry.Name, filename)

	// No viewer is waiting on this read, so a dead article must surface as a
	// real fetch failure instead of being zero-filled - the fabricated bytes
	// could otherwise end up durably persisted into the DFS cache below as if
	// they were genuine data. The overlay still records the damage and queues
	// its repair exactly as a live read would - see ContextForBurstDownload.
	burstCtx := usenet.ContextForBurstDownload(ctx)

	// The whole point of the read-ahead is to get this file onto disk. If the
	// durable DFS cache already holds it end to end, there is nothing left to
	// do and both halves below are pure waste:
	//
	//   - ReadAhead goes through the usenet reader's SegmentCache, which is a
	//     per-reader scratch buffer under Usenet.DiskBufferPath that is
	//     RemoveAll'd when the reader closes. It is NOT the durable cache and
	//     it does NOT consult it. So on any reader opened after a restart (or
	//     after the reader idled out) every segment is StateEmpty, and
	//     SegmentFetcher.doFetch acquires an NNTP connection and re-downloads
	//     the article - the entire file, from usenet.
	//   - persistCleanRanges then reads all of it back and hands each segment
	//     to WriteCachedRange -> WriteAtNoOverwrite, which skips every byte
	//     because the durable cache already has it.
	//
	// Net effect before this gate: walking a watched season re-downloaded each
	// already-cached episode in full and discarded every byte. Measured on
	// a production install 2026-09-09 at ~4 min / ~4.2 GB per episode, chaining E10 -> E13
	// back to back. Skipping is also what the original code intended - its
	// comment assumed the burst would "fast-path a fully-cached file and pull
	// 0 bytes", which only holds while that scratch cache is still warm.
	if fullyCached {
		// Skip the burst and the persist walk only. Everything below - the
		// readiness record, the budget accounting and the forward cascade to
		// the episode after this one - still has to run, or a season of
		// already-cached episodes would stop walking forward.
		p.logger.Info().Str("entry", nextEntry.Name).Str("file", filename).
			Int64("size", next.Size).
			Msg("next-episode precache: durable cache already complete; nothing to fetch")
	} else {
		concurrency := p.cfg().ReadAheadConcurrency()
		p.logger.Info().Str("entry", nextEntry.Name).Str("file", filename).Int64("size", next.Size).Int("concurrency", concurrency).Msg("burst-downloading next episode ahead of playback")

		// Fetch and durably persist chunk by chunk, before waiting on repair -
		// see burstToDurable, and persistCleanRanges for why damaged segments
		// are deliberately excluded rather than persisted as padding. Fetching
		// the whole episode first and persisting after meant reading it back
		// through a 256 MB scratch cache, which re-downloaded all but its last
		// few hundred MB.
		res, burstErr := p.burstToDurable(burstCtx, nextEntry, filename, 0, next.Size, concurrency)
		if burstErr != nil {
			p.logger.Debug().Err(burstErr).Str("entry", nextEntry.Name).Str("file", filename).Msg("next-episode burst-download ended early")
		}
		if res.segmentsTotal > 0 {
			res.persist.log(p.logger, nextEntry.Name, filename, res.segmentsTotal, "durable persist complete")
		}
		p.logger.Debug().Str("entry", nextEntry.Name).Str("file", filename).
			Int64("fetchedBytes", res.fetched).Int64("skippedBytes", res.skipped).
			Msg("next-episode burst-download finished")
	}

	p.recordReadiness(ref, nextEntry, filename, next.Size)

	if p.cfg().PrecacheEvictAfterWatched {
		p.markPrecached(nextEntry.InfoHash, filename, next.Size)
	} else {
		p.releaseBudget(next.Size)
	}

	// No forward cascade from here any more: precacheForwardWalk steps to the
	// next episode itself, and playing this one extends the walk (see
	// tryMarkWalked). The cascade restarted the walk at full depth after
	// every burst, so any depth setting ran to the season's end.
	return stepBurst
}

// episodeStep is precacheEpisodeFile's outcome for the forward walk.
type episodeStep int

const (
	stepBurst    episodeStep = iota // a burst ran (or the cache already held it all)
	stepSkipped                     // nothing to do for this episode; walk on
	stepDeferred                    // budget or bandwidth said not now; end the walk
)

// recordReadiness checks for damage the burst surfaced and records the
// episode's readiness row. Damage is handed to repair and waited on beside
// the walk (awaitReadiness), which moves on to the next episode meanwhile -
// the wait used to hold the walk for up to 5 minutes per damaged episode.
func (p *Precache) recordReadiness(ref walkIdentity, entry *storage.Entry, filename string, fileSize int64) {
	row := EpisodeReadiness{EntryName: entry.Name, InfoHash: entry.InfoHash, Filename: filename, ReadyAt: time.Now()}
	n, ok := p.overlayPendingCount(entry, filename)
	if !ok || n <= 0 {
		row.Clean = true
		p.storeReadiness(row)
		p.notifyReadiness(entry, row)
		return
	}

	// Shown as still damaged until the wait settles it.
	row.SegmentsPending = n
	p.storeReadiness(row)

	key := entry.InfoHash + ":" + filename
	p.markInflight(key) // persistCleanRanges may still write into the cache
	go func() {
		defer p.unmarkInflight(key)
		p.awaitReadiness(ref, entry, filename, fileSize, n)
	}()
}

// awaitReadiness hands a pre-cached episode's damage to repair, waits for it
// (awaitPrecacheRepair), and settles the readiness row:
//   - repaired: the row says so and the patched segments are persisted.
//   - entry gone (a re-grab deleted it): the row is dropped, and the
//     replacement is re-warmed once Sonarr has imported it (addRewarm).
//   - still damaged: the row says how many segments; a PAR2 pass that
//     lands later still updates it (OnPar2Repaired).
//
// A re-grab that keeps the entry (one episode of a season pack) is re-warmed
// the same way.
func (p *Precache) awaitReadiness(ref walkIdentity, entry *storage.Entry, filename string, fileSize int64, n int) {
	// No zero-fill: persistCleanRanges reads the repaired segments back
	// through the reader, and a pad must not be persisted as data.
	ctx := usenet.ContextForBurstDownload(p.baseCtx())
	key := entry.InfoHash + ":" + filename

	pending := func() (int, bool) {
		if !p.entryExists(entry.InfoHash) {
			return 0, true
		}
		c, ok := p.overlayPendingCount(entry, filename)
		if !ok {
			return -1, false
		}
		return c, false
	}

	// Route damage through decideAutoRepairAction(RepairSourcePrecache, ...)
	// and the regrab guard, via HandlePrecacheDamage: PAR2 when usable (the
	// cache is warm), otherwise a re-grab, since nothing is playing yet and
	// the replacement has time to land before playback reaches this episode.
	// This used to go through the playback policy, which leaves degraded
	// damage without usable PAR2 padded - so a pre-cached episode without
	// PAR2 was never fixed ahead of the viewer. regrabGuard applies exactly
	// as for playback, so an episode that can't be fixed trips the guard
	// instead of looping.
	regrabbed := false
	handle := func() autoRepairOutcome {
		r := p.manager.Repair()
		if r == nil {
			return autoRepairOutcome{reason: "repair service unavailable"}
		}
		out, err := r.HandlePrecacheDamage(ctx, entry.Name, filename)
		evt := p.logger.Debug()
		if err != nil {
			evt = evt.Err(err)
		} else if out.acted {
			evt = p.logger.Info()
		}
		evt.Str("entry", entry.Name).Str("file", filename).Bool("acted", out.acted).
			Bool("regrab", out.regrab).Bool("retry", out.retry).Str("reason", out.reason).
			Msg("next-episode pre-cache: damage handed to repair")
		regrabbed = regrabbed || (out.acted && out.regrab)
		return out
	}

	res := awaitPrecacheRepair(ctx, precacheRepairWaitTimeout, precacheRepairPollInterval,
		precacheRepairRetryInterval, pending, handle)
	if regrabbed || res.gone {
		p.addRewarm(ref, entry.InfoHash, key)
	}

	row := EpisodeReadiness{EntryName: entry.Name, InfoHash: entry.InfoHash, Filename: filename, ReadyAt: time.Now()}
	switch {
	case res.gone:
		p.dropReadiness(key)
		p.logger.Info().Str("entry", entry.Name).Str("file", filename).
			Msg("next-episode pre-cache: episode was replaced; its replacement is re-warmed once imported")
		return
	case res.repaired:
		row.SegmentsRepaired = n
		defer p.persistRepaired(ctx, entry, filename, fileSize) // the row is settled first
	default:
		row.SegmentsPending = n
		if res.remaining > 0 {
			row.SegmentsPending = res.remaining
		}
	}
	p.storeReadiness(row)
	p.notifyReadiness(entry, row)
}

// storeReadiness records row under its "infoHash:filename" key.
func (p *Precache) storeReadiness(row EpisodeReadiness) {
	p.readinessMu.Lock()
	p.readiness[row.InfoHash+":"+row.Filename] = row
	p.readinessMu.Unlock()
}

// dropReadiness forgets the row under key.
func (p *Precache) dropReadiness(key string) {
	p.readinessMu.Lock()
	delete(p.readiness, key)
	p.readinessMu.Unlock()
}

// precacheRepairRetryInterval is how often awaitPrecacheRepair asks again
// after a retryable no-op (another handler owns the entry, or its cooldown
// is running - see autoRepairOutcome.retry).
const precacheRepairRetryInterval = 30 * time.Second

// precacheRepairResult is how awaitPrecacheRepair's wait ended.
type precacheRepairResult struct {
	repaired  bool // pending reached 0
	gone      bool // the entry no longer exists (a re-grab deleted it)
	remaining int  // last pending count seen, -1 if never read
}

// awaitPrecacheRepair hands damage to handle, then polls pending until it
// reaches 0, the entry is gone, wait runs out or ctx ends. While handle's
// last answer was a retryable no-op it is asked again every retryEvery: a
// season pack's episode found damaged while a sibling's re-grab or PAR2 pass
// holds the release used to be dropped until the nightly sweep (The Conjurors
// S04E03, 2026-09-07, about 5 h). pending reports gone=true once the entry
// has been deleted; before, the deleted overlay read as "0 pending" and the
// row claimed a repair.
func awaitPrecacheRepair(ctx context.Context, wait, poll, retryEvery time.Duration,
	pending func() (count int, gone bool), handle func() autoRepairOutcome) precacheRepairResult {
	res := precacheRepairResult{remaining: -1}
	out := handle()
	lastAsk := time.Now()
	deadline := lastAsk.Add(wait)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return res
		case <-time.After(poll):
		}
		count, gone := pending()
		if gone {
			res.gone = true
			return res
		}
		if res.remaining = count; count == 0 {
			res.repaired = true
			return res
		}
		if out.retry && time.Since(lastAsk) >= retryEvery {
			out = handle()
			lastAsk = time.Now()
		}
	}
	return res
}

// notifyReadiness fires the "next play is ready" notification for a
// completed next-episode pre-cache pass: "next episode cached, clean" or
// "next episode cached, N segments repaired ahead of time" (or, if the
// URGENT repair didn't land within precacheRepairWaitTimeout, a
// still-damaged variant so the operator isn't told it's clean when it
// isn't).
func (p *Precache) notifyReadiness(entry *storage.Entry, r EpisodeReadiness) {
	if p.manager.Notifications == nil {
		return
	}
	var msg string
	switch {
	case r.Clean:
		msg = fmt.Sprintf("Next episode cached, clean: %s / %s", entry.Name, r.Filename)
	case r.SegmentsRepaired > 0:
		msg = fmt.Sprintf("Next episode cached, %d segment(s) repaired ahead of time: %s / %s", r.SegmentsRepaired, entry.Name, r.Filename)
	default:
		msg = fmt.Sprintf("Next episode cached, %d segment(s) still damaged: %s / %s", r.SegmentsPending, entry.Name, r.Filename)
	}
	p.manager.Notifications.Notify(notifications.Event{
		Type:    config.EventPrecacheReady,
		Status:  "ready",
		Entry:   entry,
		Message: msg,
	})
}
