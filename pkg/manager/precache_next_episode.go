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
	// precacheNextEpisodeTimeout bounds one series' whole forward walk
	// (resolve + N episodes' worth of search/burst-download/repair-wait).
	precacheNextEpisodeTimeout = 45 * time.Minute

	// precacheRepairWaitTimeout bounds how long precacheEpisodeFile waits
	// for an URGENT repair it kicked off to finish before giving up and
	// recording the episode as still-damaged. Pre-caching happens well
	// before the episode is needed, so this can be generous without risking
	// a glitch - unlike the live read-ahead path in precache.go, nothing is
	// racing a playhead here.
	precacheRepairWaitTimeout  = 5 * time.Minute
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

// maybePrecacheNextEpisodes resolves the Sonarr series/episode context for a
// just-triggered (entry, filename) and, if found, walks forward up to
// config.Precache.NextEpisodes episodes: burst-downloading (and repairing)
// each one that's already grabbed, or triggering a targeted Sonarr search
// for one that isn't. No-op for movies (no Sonarr context resolves), when
// NextEpisodes is 0, or when arrs are unreachable.
func (p *Precache) maybePrecacheNextEpisodes(entry *storage.Entry, filename string) {
	// Next-episode precache only applies to series. Resolve the Arr this entry
	// came from and bail unless it's a Sonarr instance - a movie (Radarr) has no
	// next episode, and running the lookup for one wastes a call against every
	// Sonarr instance just to fail the filename match.
	if a := p.manager.arr.GetOrCreate(entry.Category); a == nil || a.Type != arr.Sonarr {
		return
	}
	n := p.cfg().NextEpisodes()
	if n <= 0 || p.manager.usenet == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), precacheNextEpisodeTimeout)
	defer cancel()

	a, seriesId, seasonNumber, episodeNumber, ok := p.resolveSonarrEpisode(ctx, entry, filename)
	if !ok {
		p.logger.Debug().Str("entry", entry.Name).Msg("next-episode precache: no matching Sonarr episode resolved")
		return
	}

	for range n {
		if ctx.Err() != nil {
			return
		}
		next, found, err := a.NextEpisode(ctx, seriesId, seasonNumber, episodeNumber)
		if err != nil {
			p.logger.Debug().Err(err).Str("series", entry.Name).Msg("next-episode precache: NextEpisode lookup failed (Arr unreachable?)")
			return
		}
		if !found {
			p.logger.Debug().Str("series", entry.Name).Msg("next-episode precache: no next episode (season boundary)")
			return
		}
		episodeNumber = next.EpisodeNumber

		if !next.HasFile {
			// Not grabbed yet - ask Sonarr to fetch it. decypharr's normal
			// download pipeline (Sonarr -> emulated download client ->
			// import) brings it into the mount in due course; there's
			// nothing to burst-cache until then, and no further episode can
			// be resolved without this one's season/episode context anyway.
			if err := a.SearchEpisode(ctx, next.EpisodeId); err != nil {
				p.logger.Debug().Err(err).Str("series", entry.Name).Int("season", next.SeasonNumber).Int("episode", next.EpisodeNumber).Msg("next-episode search failed")
			} else {
				p.logger.Info().Str("series", entry.Name).Int("season", next.SeasonNumber).Int("episode", next.EpisodeNumber).Msg("next episode not yet grabbed; requested search")
			}
			return
		}

		p.precacheEpisodeFile(ctx, next)
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
func (p *Precache) precacheEpisodeFile(ctx context.Context, next arr.NextEpisodeInfo) {
	target := readSymlinkTarget(next.Path)
	if target == "" {
		p.logger.Debug().Str("file", next.Path).Msg("next-episode precache: next episode is not a local symlink")
		return // not a decypharr-managed symlink (e.g. imported directly)
	}
	dir, filename := filepath.Split(target)
	entryName := filepath.Clean(filepath.Base(filepath.Clean(dir)))

	nextEntry, err := p.manager.GetEntryByName(entryName, filename)
	if err != nil || nextEntry == nil || nextEntry.Protocol != config.ProtocolNZB {
		return // not a decypharr entry, or not usenet-backed (overlay/repair is usenet-only)
	}

	if p.keyPaused(nextEntry.InfoHash, filename) {
		p.logger.Debug().Str("entry", nextEntry.Name).Str("file", filename).
			Msg("next-episode precache skipped: paused")
		return
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
		return
	}

	if !p.reserveBudget(next.Size) {
		p.logger.Debug().Str("entry", nextEntry.Name).Int64("size", next.Size).Msg("next-episode pre-cache skipped: PrecacheMaxBytes budget exhausted")
		return
	}
	if !p.manager.usenet.HasBandwidthHeadroom() {
		p.logger.Debug().Str("entry", nextEntry.Name).Msg("next-episode pre-cache deferred: no bandwidth headroom outside reserve")
		p.releaseBudget(next.Size)
		return
	}

	concurrency := p.cfg().ReadAheadConcurrency()
	p.logger.Info().Str("entry", nextEntry.Name).Str("file", filename).Int64("size", next.Size).Int("concurrency", concurrency).Msg("burst-downloading next episode ahead of playback")

	// No viewer is waiting on this read, so a dead article must surface as a
	// real fetch failure instead of being zero-filled - the fabricated bytes
	// could otherwise end up durably persisted into the DFS cache below as if
	// they were genuine data. The overlay still records the damage and queues
	// its repair exactly as a live read would - see ContextForBurstDownload.
	burstCtx := usenet.ContextForBurstDownload(ctx)

	burstErr := p.manager.usenet.ReadAhead(burstCtx, nextEntry.InfoHash, filename, 0, concurrency)
	if burstErr != nil {
		p.logger.Debug().Err(burstErr).Str("entry", nextEntry.Name).Str("file", filename).Msg("next-episode burst-download ended early")
	}

	// Durably persist whatever came back CLEAN into the DFS cache now, before
	// waiting on repair - see persistCleanRanges for why damaged segments are
	// deliberately excluded rather than persisted as padding.
	p.persistCleanRanges(burstCtx, nextEntry, filename, next.Size)

	p.recordReadiness(burstCtx, nextEntry, filename, next.Size)

	if p.cfg().PrecacheEvictAfterWatched {
		p.markPrecached(nextEntry.InfoHash, filename, next.Size)
	} else {
		p.releaseBudget(next.Size)
	}

	// Cascade forward. A completed burst is itself the trigger point for the
	// episode after this one, and checkSessionProgress can't provide it: this
	// burst just marked `key` in p.triggered, so when this file is eventually
	// played tryMarkTriggered short-circuits and readAhead ->
	// maybePrecacheNextEpisodes never runs for it. Only on a clean burst - a
	// failed or cancelled download has no reliable context to resolve the next
	// episode from. The forward walk stays bounded by tryMarkTriggered,
	// reserveBudget, and season boundaries. Detached so it doesn't run under
	// this pass's 45-minute deadline (ctx). Mirrors readAhead, where
	// maybePrecacheNextEpisodes is likewise the final step.
	if burstErr == nil && burstCtx.Err() == nil {
		go p.maybePrecacheNextEpisodes(nextEntry, filename)
	}
}

// recordReadiness checks for damage the burst-download surfaced, routes it
// through the live playback-failure policy if so, waits (bounded) for a
// PAR2 repair to land, and stores the outcome for Commit D.
func (p *Precache) recordReadiness(ctx context.Context, entry *storage.Entry, filename string, fileSize int64) {
	readiness := EpisodeReadiness{EntryName: entry.Name, InfoHash: entry.InfoHash, Filename: filename, ReadyAt: time.Now()}
	defer func() {
		p.readinessMu.Lock()
		p.readiness[entry.InfoHash+":"+filename] = readiness
		p.readinessMu.Unlock()
		p.notifyReadiness(entry, readiness)
	}()

	pendingCount := func() int {
		pending, err := p.manager.usenet.OverlayPendingRepair(entry.InfoHash)
		if err != nil {
			return -1
		}
		return len(pending[filename])
	}

	n := pendingCount()
	if n <= 0 {
		readiness.Clean = true
		readiness.ReadyAt = time.Now()
		return
	}

	// Route damage through the SAME decideAutoRepairAction(RepairSourcePlayback,
	// ...) policy (and regrab guard) the live playback-failure path uses -
	// HandlePlaybackFailure IS that path (see
	// pkg/mount/dfs/vfs/downloaders.go's call for a live read), called here
	// verbatim rather than re-implementing any part of its policy. Unlike
	// the currently-playing read-ahead path (precache.go's repairAhead),
	// this DOES re-grab on autoActionRegrab: nothing is playing yet, so
	// there's no live stream to disrupt, and re-grabbing now gives the
	// replacement time to land before playback actually reaches this
	// episode. Given this library is mostly pre-PAR2-retention records, the
	// common outcome here is re-grab, not PAR2 - expected and correct.
	// regrabGuard applies exactly as it does to a playback-triggered
	// re-grab, so a next episode that can't be fixed trips the guard and
	// goes terminal with its reason surfaced, instead of looping.
	if r := p.manager.Repair(); r != nil {
		if _, _, err := r.HandlePlaybackFailure(ctx, entry.Name, filename); err != nil {
			p.logger.Debug().Err(err).Str("entry", entry.Name).Str("file", filename).Msg("next-episode pre-cache: damage handling failed")
		}
	}

	deadline := time.Now().Add(precacheRepairWaitTimeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			readiness.SegmentsPending = n
			return
		case <-time.After(precacheRepairPollInterval):
		}
		remaining := pendingCount()
		if remaining == 0 {
			readiness.SegmentsRepaired = n
			readiness.ReadyAt = time.Now()
			// PENDING-REPAIR -> REPAIRED: persist the now-clean segments
			// durably (a no-op for anything a re-grab replaced under a
			// different InfoHash - there's nothing left here to persist).
			p.persistCleanRanges(ctx, entry, filename, fileSize)
			return
		}
		if remaining > 0 {
			n = remaining
		}
	}
	readiness.SegmentsPending = n
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
