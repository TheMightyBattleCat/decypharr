// Package manager: Precache implements read-ahead damage detection ahead of
// playback (movies and episodes alike). When a file being streamed crosses a
// configurable read-position threshold (config.Precache), Precache bursts an
// aggressive, high-concurrency read-ahead over the rest of the file - see
// pkg/usenet.Usenet.ReadAhead - distinct from the normal per-read streaming
// prefetch window. Any article confirmed missing during that burst is
// recorded by the overlay exactly as it would be during a live read; if it
// leaves damage pending, Precache enqueues an URGENT-lane PAR2 repair for it
// (see Par2Repair.EnqueueUrgent), budgeted by the estimated playback-time gap
// remaining before the playhead would reach it - so a completed repair
// serves patched bytes with no glitch, and existing padding covers the gap
// if it doesn't finish in time.
package manager

import (
	"context"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

const (
	// precacheReadAheadTimeout is the least time one read-ahead burst gets;
	// readAheadTimeout scales it with the bytes left, up to
	// precacheReadAheadTimeoutCap, so a stalled provider still can't wedge a
	// background goroutine forever.
	precacheReadAheadTimeout = 30 * time.Minute
	// precacheReadAheadFloorRate is the rate a burst is given time for. On
	// 2026-09-18 a REMUX burst under the nightly sweep ran at 4.2 MiB/s and
	// hit the flat 30 minutes with ~18 GB of its ~26 GB still to fetch. Since
	// bursts persist chunk by chunk, time past 30 minutes loses nothing.
	precacheReadAheadFloorRate  = 4 << 20
	precacheReadAheadTimeoutCap = 4 * time.Hour

	// precacheTriggeredTTL bounds how long a (entry,file) dedup key is
	// remembered, so the map backing it can't grow without bound across a
	// long-running process streaming many distinct files over time.
	precacheTriggeredTTL = 6 * time.Hour

	// precacheDefaultBitrateBytesPerSec is the fallback used to convert a
	// byte gap into an estimated playback-time gap when no real duration
	// metadata is available for the entry (~16 Mbps - a reasonable
	// mid-quality remux/transcode estimate). This only affects the relative
	// ordering of URGENT repair jobs, not whether they run.
	precacheDefaultBitrateBytesPerSec = 2 * 1024 * 1024
)

// Precache is the manager-level read-ahead precache service. One instance
// per Manager.
type Precache struct {
	manager *Manager
	logger  zerolog.Logger

	mu        sync.Mutex
	triggered map[string]time.Time // "infoHash:filename" -> when work on it was kicked off (read-ahead or next-episode burst)

	// walked records, per "infoHash:filename", when playback of that file
	// last started a forward walk (see tryMarkWalked). Separate from
	// triggered because a file the walk already burst-cached is marked
	// there, yet playing it must still move the walk's window forward.
	// Guarded by mu; pruned with triggered.
	walked map[string]time.Time

	// walks holds the forward walk running for each Sonarr series+season
	// (see walkKey), so a second trigger in the same season extends the
	// running walk's target instead of starting a parallel one. Guarded by
	// walksMu.
	walksMu sync.Mutex
	walks   map[string]*forwardWalk

	// paused/pausedKeys are runtime-only pause controls (SetPaused/
	// SetKeyPaused), guarded by mu alongside triggered. Neither is
	// persisted - they reset to false/empty on restart. The durable switch
	// remains config.Repair.PrecacheReadAheadEnabled(); this is purely an
	// operator's "hold off for now" toggle on top of it - see keyPaused.
	paused     bool
	pausedKeys map[string]struct{} // "infoHash:filename" -> paused

	// denyLogged records entry:file keys for which a read-ahead gate-denial has
	// already been logged, so a diagnostic deny (disabled / no budget / Plex gate)
	// is logged once per file rather than on every ranged read. Guarded by mu.
	// Never consulted by gating logic - purely to rate-limit the deny log.
	denyLogged map[string]struct{}

	// progressSkipLogged records, per Plex session path, the last reason
	// checkSessionProgress skipped it - so the 15s poll logs a standing skip
	// once instead of ~240 times per episode, while a changed reason (e.g.
	// "outside window" giving way to "already-triggered" as the playhead
	// crosses the threshold) still logs afresh. Cleared for a path once its
	// trigger fires. Guarded by mu; never consulted by the trigger path.
	progressSkipLogged map[string]string

	// precachedBytes is a running total of bytes this feature has
	// deliberately pulled ahead of need (Sonarr next-episode bursts only -
	// see PrecacheMaxBytes), checked/reserved before starting a new burst so
	// the total never exceeds config.Precache.MaxBytes. An approximation
	// (real disk accounting lives in the shared segment cache, which this
	// feature doesn't own exclusively), but a real, live-adjusted one: never
	// negative, incremented on reservation, decremented on release/eviction.
	precachedBytes atomic.Int64

	// precachedMu/precached track which (infoHash,filename) pairs currently
	// hold a next-episode budget reservation, keyed the same as triggered,
	// so a later watch of that same file can release it (see
	// PrecacheEvictAfterWatched / evictIfWatched).
	precachedMu sync.Mutex
	precached   map[string]int64

	readinessMu sync.Mutex
	readiness   map[string]EpisodeReadiness

	// inflightMu/inflight tracks which (infoHash,filename) pairs currently
	// have a burst actively writing into the DFS cache - either the
	// currently-playing read-ahead burst (readAhead) or a next-episode burst
	// (precacheEpisodeFile), keyed the same as triggered/precached. A
	// refcount rather than a bool since the same key can be entered by more
	// than one caller in rare overlapping-trigger races; the key is only
	// considered idle once every entrant has left. Consulted by future
	// cache-cleanup logic so it never deletes a cache dir a burst is still
	// writing into (see InflightHas) - not used for any dedup/gating
	// decision itself, that's triggered's job.
	inflightMu sync.Mutex
	inflight   map[string]int

	// cachePopulateOnce guards populateFromCache, the "Tier 1" restart fix
	// that seeds readiness rows from the DFS cache's on-disk state - run
	// lazily on the first Summary() call rather than from NewPrecache, so it
	// never races the mount not being ready yet at process startup.
	cachePopulateOnce sync.Once

	// rescanMu/rescanning single-flight Rescan(), the on-demand counterpart
	// to cachePopulateOnce for cache entries that appear after the initial
	// populate (a fresh import, or Plex reading a file during intro
	// detection or a library scan) - see Rescan's doc comment.
	rescanMu   sync.Mutex
	rescanning bool

	// plexChecker gates read-ahead bursts behind an active Plex playing
	// session for the file being read - see isPlexWatching's doc comment.
	// Always non-nil; a no-op (allows everything) when config.PlexConfig.URL
	// is unset.
	plexChecker *plexSessionChecker

	// ctx/cancel/wg govern progressTriggerLoop, the Plex playback-progress
	// poll loop - see Start/Stop.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewPrecache builds the precache service.
func NewPrecache(m *Manager) *Precache {
	return &Precache{
		manager:            m,
		logger:             logger.New("precache"),
		triggered:          make(map[string]time.Time),
		walked:             make(map[string]time.Time),
		walks:              make(map[string]*forwardWalk),
		pausedKeys:         make(map[string]struct{}),
		denyLogged:         make(map[string]struct{}),
		progressSkipLogged: make(map[string]string),
		precached:          make(map[string]int64),
		readiness:          make(map[string]EpisodeReadiness),
		inflight:           make(map[string]int),
		plexChecker:        newPlexSessionChecker(m),
	}
}

func (p *Precache) cfg() config.PrecacheConfig {
	return config.Get().Precache
}

// precacheProgressPollInterval is the progressTriggerLoop tick cadence -
// ~1.5x the default Plex SessionCacheTTL (10s, see PlexConfig.SessionTTL) so
// most ticks land on a live Plex fetch under default config, but decoupled
// from the user's TTL setting rather than tied to it. Playback progress
// isn't latency-sensitive, so this doesn't need to be tighter.
const precacheProgressPollInterval = 15 * time.Second

// Start launches progressTriggerLoop, cancellable via ctx or Stop.
func (p *Precache) Start(ctx context.Context) {
	p.ctx, p.cancel = context.WithCancel(ctx)
	p.wg.Add(1)
	go p.progressTriggerLoop()
}

// Stop cancels progressTriggerLoop and waits for it to exit.
func (p *Precache) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
}

// progressTriggerLoop periodically checks every active Plex "now playing"
// session's playback progress against PrecacheThresholdPercent - see
// checkSessionProgress's doc comment for why this exists alongside Observe.
// Exits when ctx (passed to Start) is cancelled.
func (p *Precache) progressTriggerLoop() {
	defer p.wg.Done()
	ticker := time.NewTicker(precacheProgressPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.checkSessionProgress()
		}
	}
}

// checkSessionProgress drives next-episode read-ahead from Plex playback
// PROGRESS rather than from cache-miss reads. This is deliberate: the
// next-episode burst is only reachable from Observe, and Observe only runs on
// a cache MISS (Manager.Stream is invoked solely for a missing byte range).
// A current episode that is already fully cached - including by the DFS
// mount's own unconditional read-ahead - produces no misses, so Observe never
// fires and the next episode never warms. Polling playback progress decouples
// the trigger from the miss, so "current episode already warm" still advances
// the next one.
//
// Every per-session skip below is logged once (via progressSkip, rate-limited
// per session path + reason) - a title that silently never cascades almost
// always shows up here as "unresolved" or "already-triggered".
func (p *Precache) checkSessionProgress() {
	cfg := config.Get()
	if !cfg.Plex.Enabled() {
		return
	}
	if !cfg.Repair.PrecacheReadAheadEnabled() {
		return
	}
	pc := p.cfg()
	if p.Paused() {
		return
	}
	threshold := int64(pc.ThresholdPercent())

	p.plexChecker.refresh(cfg.Plex)
	for path, prog := range p.plexChecker.sessionProgress() {
		if prog.duration <= 0 {
			p.progressSkip(path, "zero-duration").Msg("progress trigger skipped: zero duration")
			continue
		}
		pct := prog.viewOffset * 100 / prog.duration
		if pct < threshold || pct > 98 {
			p.progressSkip(path, "outside-window").
				Int64("pct", pct).Int64("threshold", threshold).
				Msg("progress trigger skipped: outside threshold-98 window")
			continue
		}
		entry, filename, ok := p.resolvedPathToEntry(path)
		if !ok {
			p.progressSkip(path, "unresolved").Msg("progress trigger skipped: path not resolved to an entry")
			continue
		}
		f, ok := entry.Files[filename]
		if !ok || f.Size <= 0 {
			p.progressSkip(path, "file-not-in-entry").
				Str("entry", entry.Name).Str("file", filename).
				Msg("progress trigger skipped: file not in entry")
			continue
		}
		key := entry.InfoHash + ":" + filename
		if !p.tryMarkTriggered(key) {
			// Already burst-cached (usually by the walk itself): no second
			// burst, but playing it still moves the walk's window forward.
			if p.tryMarkWalked(key) {
				go p.maybePrecacheNextEpisodes(entry, filename)
			}
			p.progressSkip(path, "already-triggered").
				Str("entry", entry.Name).Str("file", filename).
				Msg("progress trigger skipped: already triggered")
			continue
		}
		p.tryMarkWalked(key) // readAhead walks forward once its burst ends
		p.mu.Lock()
		delete(p.progressSkipLogged, path)
		p.mu.Unlock()
		// viewOffset/duration are milliseconds; readAhead wants a byte offset.
		from := f.Size * prog.viewOffset / prog.duration
		go p.readAhead(entry, filename, from, f.Size)
	}
}

// progressSkip returns a debug event for a checkSessionProgress per-session
// skip, or nil if this exact (session path, reason) pair was already logged -
// the 15s poll would otherwise repeat the same line ~240 times over one
// episode. A changed reason logs afresh (so "outside-window" giving way to
// "already-triggered" as the playhead advances stays visible), and a path's
// record is cleared once its trigger fires. Caller completes the event with
// .Msg(); a nil *zerolog.Event is a safe no-op. Behaviour-free: the trigger
// path never reads progressSkipLogged.
func (p *Precache) progressSkip(path, reason string) *zerolog.Event {
	p.mu.Lock()
	last, seen := p.progressSkipLogged[path]
	dup := seen && last == reason
	if !dup {
		p.progressSkipLogged[path] = reason
	}
	p.mu.Unlock()
	if dup {
		return nil
	}
	return p.logger.Debug().Str("sessionPath", path).Str("reason", reason)
}

// resolvedPathToEntry reverses GetTorrentMountPath - a flat
// <MountPath>/<EntryAllFolder>/<folder>/<nested/file> - back to the owning
// entry and the entry.Files key. filename is the full nested remainder,
// "/"-joined, matching the FUSE path resolver's derivation so the dedup key
// and entry.Files lookup align with Observe's. (Note: WebDAV serving passes a
// bare basename to Observe instead; that FUSE-vs-WebDAV split is pre-existing
// and only affects WebDAV-served nested files, harmlessly double-firing.)
func (p *Precache) resolvedPathToEntry(resolvedPath string) (*storage.Entry, string, bool) {
	base := filepath.Join(p.manager.config.Mount.MountPath, EntryAllFolder)
	rel, err := filepath.Rel(base, resolvedPath)
	if err != nil || strings.HasPrefix(rel, "..") {
		return nil, "", false
	}
	parts := strings.SplitN(rel, string(filepath.Separator), 2)
	if len(parts) != 2 {
		return nil, "", false
	}
	folder, filename := parts[0], parts[1]

	if config.Get().FolderNaming == config.WebdavUseHash {
		entry, err := p.manager.storage.Get(folder)
		if err != nil {
			return nil, "", false
		}
		return entry, filename, true
	}

	entries, err := p.manager.storage.List(func(e *storage.Entry) bool {
		return e.GetFolder() == folder
	})
	if err != nil || len(entries) == 0 {
		return nil, "", false
	}
	return entries[0], filename, true
}

// logGateDeny logs a read-ahead gate denial at most once per entry:file, so a
// standing denial (toggle off, no budget, or the Plex session gate not
// matching) is visible in the log without repeating on every ranged read.
// reason is a short stable tag (disabled / max_bytes / plex_gate).
// Behaviour-free: the denyLogged set is never read by the gating path.
func (p *Precache) logGateDeny(key, reason, entry, filename string) {
	p.mu.Lock()
	_, seen := p.denyLogged[key]
	if !seen {
		p.denyLogged[key] = struct{}{}
	}
	p.mu.Unlock()
	if seen {
		return
	}
	p.logger.Debug().
		Str("reason", reason).
		Str("entry", entry).
		Str("file", filename).
		Msg("read-ahead precache skipped")
}

// Observe is called on every usenet stream range request with the file's
// current read position and total size. The first time a given (entry,file)
// pair's position crosses PrecacheThresholdPercent, it kicks off a
// background read-ahead pass. Safe to call on a nil Precache (no-op) and
// from any goroutine.
func (p *Precache) Observe(entry *storage.Entry, filename string, start, size int64) {
	if p == nil || entry == nil || size <= 0 {
		return
	}
	p.evictIfWatched(entry, filename, start, size)

	key := entry.InfoHash + ":" + filename

	if !config.Get().Repair.PrecacheReadAheadEnabled() {
		p.logGateDeny(key, "disabled", entry.Name, filename)
		return
	}
	cfg := p.cfg()
	if !p.plexChecker.isPlexWatching(entry, filename) {
		// Plex gate configured and this file isn't part of an active
		// playing session (e.g. a library scan or thumbnail-generation
		// read) - don't let it trigger a read-ahead burst.
		p.logGateDeny(key, "plex_gate", entry.Name, filename)
		return
	}
	threshold := int64(cfg.ThresholdPercent())
	if start*100 < size*threshold {
		return
	}
	// Near the end of the file there's nothing meaningful left to pull ahead, so
	// don't start a read-ahead burst - this also stops a metadata/footer read
	// (which lands at ~99% of the file) from tripping read-ahead the way a real
	// playback position would.
	if start*100 > size*98 {
		return
	}

	if !p.tryMarkTriggered(key) {
		// See checkSessionProgress: a burst-cached file still extends the
		// walk once when it is played.
		if p.tryMarkWalked(key) {
			go p.maybePrecacheNextEpisodes(entry, filename)
		}
		return
	}
	p.tryMarkWalked(key) // readAhead walks forward once its burst ends

	go p.readAhead(entry, filename, start, size)
}

// tryMarkTriggered atomically checks-and-marks key ("infoHash:filename") in
// p.triggered, deduping Observe's byte-offset trigger against the Plex
// progress-poll trigger so the same file never starts two concurrent
// read-ahead bursts. Returns true if this call claimed key (caller should
// proceed); false if another caller already claimed it.
func (p *Precache) tryMarkTriggered(key string) bool {
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, done := p.triggered[key]; done {
		return false
	}
	p.triggered[key] = now
	p.pruneLocked(now)
	return true
}

// pruneLocked drops dedup entries older than precacheTriggeredTTL. Caller
// must hold p.mu.
func (p *Precache) pruneLocked(now time.Time) {
	for k, t := range p.triggered {
		if now.Sub(t) > precacheTriggeredTTL {
			delete(p.triggered, k)
		}
	}
	for k, t := range p.walked {
		if now.Sub(t) > precacheTriggeredTTL {
			delete(p.walked, k)
		}
	}
}

// tryMarkWalked is tryMarkTriggered's counterpart for the forward walk: true
// the first time playback of key asks for a walk, so a file whose burst
// already ran (triggered is set) still moves the walk forward once when it
// is played, and the 15 s progress poll or every cache miss doesn't start
// another.
func (p *Precache) tryMarkWalked(key string) bool {
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, done := p.walked[key]; done {
		return false
	}
	p.walked[key] = now
	p.pruneLocked(now)
	return true
}

// untrigger forgets key in triggered, for a burst that was claimed but never
// started (budget or bandwidth), so a later walk or its own playback can
// still burst it.
func (p *Precache) untrigger(key string) {
	p.mu.Lock()
	delete(p.triggered, key)
	p.mu.Unlock()
}

// SetPaused sets or clears the global runtime pause - see the paused field's
// doc comment. Halts new read-ahead and next-episode bursts from starting;
// anything already running finishes. Runtime-only, not persisted.
func (p *Precache) SetPaused(paused bool) {
	p.mu.Lock()
	p.paused = paused
	p.mu.Unlock()
}

// Paused reports the current global runtime pause state.
func (p *Precache) Paused() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.paused
}

// SetKeyPaused sets or clears the runtime pause for one (infoHash,filename)
// pair, keyed the same as triggered/precached/inflight. Runtime-only, not
// persisted.
func (p *Precache) SetKeyPaused(infoHash, filename string, paused bool) {
	key := infoHash + ":" + filename
	p.mu.Lock()
	if paused {
		p.pausedKeys[key] = struct{}{}
	} else {
		delete(p.pausedKeys, key)
	}
	p.mu.Unlock()
}

// keyPaused reports whether (infoHash,filename) should be held back from
// starting a new burst - true if the global pause is on, or that specific
// key was paused individually.
func (p *Precache) keyPaused(infoHash, filename string) bool {
	key := infoHash + ":" + filename
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.paused {
		return true
	}
	_, ok := p.pausedKeys[key]
	return ok
}

// readAhead runs the aggressive read-ahead burst for one file, then checks
// for damage it may have surfaced.
func (p *Precache) readAhead(entry *storage.Entry, filename string, from, size int64) {
	if p.manager.usenet == nil {
		p.logger.Debug().Str("entry", entry.Name).Str("file", filename).
			Msg("read-ahead precache skipped: usenet client not ready")
		return
	}
	if p.keyPaused(entry.InfoHash, filename) {
		p.logger.Debug().Str("entry", entry.Name).Str("file", filename).
			Msg("read-ahead precache skipped: paused")
		return
	}
	key := entry.InfoHash + ":" + filename
	p.markInflight(key)
	defer p.unmarkInflight(key)

	concurrency := p.cfg().ReadAheadConcurrency()

	ctx, cancel := context.WithTimeout(context.Background(), readAheadTimeout(size-from))
	defer cancel()

	// Same gate as the next-episode burst: the point of the read-ahead is to
	// get bytes onto disk, so if the durable DFS cache already holds this file
	// end to end there is nothing to fetch. Without this, the burst re-pulls
	// [from, EOF) over NNTP for a file that is already fully cached, because
	// it runs through the usenet reader's per-reader scratch SegmentCache
	// rather than the durable cache - see durableCacheComplete.
	//
	// repairAhead and the forward cascade below still run either way: the
	// first reads the overlay's pending-repair map (cheap, and a skipped burst
	// surfaces no new damage anyway) and the second is what walks the season.
	if p.durableCacheComplete(p.cacheCoverageReader(), entry.Name, filename) {
		p.logger.Info().
			Str("entry", entry.Name).
			Str("file", filename).
			Int64("from", from).
			Int64("size", size).
			Msg("read-ahead precache skipped: durable cache already complete")
	} else {
		p.logger.Info().
			Str("entry", entry.Name).
			Str("file", filename).
			Int64("from", from).
			Int64("size", size).
			Int("concurrency", concurrency).
			Msg("starting read-ahead precache")

		// Chunk by chunk into the durable cache - see burstToDurable. Before,
		// this fetched [from, EOF) into the reader's 256 MB scratch cache and
		// persisted nothing, so playback fetched the same bytes again when
		// it got there. This ctx keeps zero-fill on (unlike the next-episode
		// burst's): the reader is shared with this file's playback, and a
		// dead article left failed instead of padded would fail the viewer's
		// read. A padded segment is held for repair in the overlay, and
		// persisting skips those, so no fill bytes become durable.
		res, err := p.burstToDurable(ctx, entry, filename, from, size, concurrency)
		if err != nil {
			p.logger.Debug().Err(err).Str("entry", entry.Name).Str("file", filename).Msg("read-ahead precache ended early")
			p.logger.Warn().Str("entry", entry.Name).Str("file", filename).Int64("from", from).Err(err).
				Int64("fetchedBytes", res.fetched).Int64("skippedBytes", res.skipped).
				Msg("read-ahead incomplete")
		} else {
			p.logger.Info().Str("entry", entry.Name).Str("file", filename).Int64("from", from).
				Int64("fetchedBytes", res.fetched).Int64("skippedBytes", res.skipped).
				Msg("read-ahead complete")
		}
		if res.segmentsTotal > 0 {
			res.persist.log(p.logger, entry.Name, filename, res.segmentsTotal, "read-ahead: durable persist complete")
		}
	}

	p.repairAhead(entry, filename, from)
	p.maybePrecacheNextEpisodes(entry, filename)
}

// repairAhead checks whether the read-ahead pass left any damage pending for
// filename and, if so, enqueues an URGENT-lane PAR2 repair for it - budgeted
// by the estimated playback-time gap between the current playhead and the
// nearest damaged region still ahead of it. Damage entirely behind the
// playhead (already played past) is left for the normal BATCH lane, since
// repairing it can no longer prevent a glitch. Gated by the same
// decideAutoRepairAction(RepairSourcePlayback, ...) policy
// HandlePlaybackFailure consults - par2Usable, not the raw Par2Repair
// toggle - so a release with no usable PAR2 data never gets a doomed urgent
// pass queued for it; precache never re-grabs on autoActionRegrab itself,
// since nothing has actually failed yet, it just takes no PAR2 path.
func (p *Precache) repairAhead(entry *storage.Entry, filename string, from int64) {
	if p.manager.par2Repair == nil || p.manager.usenet == nil {
		return
	}

	pending, err := p.manager.usenet.OverlayPendingRepair(entry.InfoHash)
	if err != nil || len(pending) == 0 {
		return
	}
	segs, damaged := pending[filename]
	if !damaged || len(segs) == 0 {
		return
	}

	par2Usable, _ := p.manager.par2Repair.par2Usable(entry.InfoHash)
	verdict := p.manager.usenet.OverlayVerdict(entry.InfoHash, filename)
	if decideAutoRepairAction(RepairSourcePlayback, par2Usable, verdict) != autoActionQueuePar2 {
		return
	}

	nzb, err := p.manager.usenet.GetNZB(entry.InfoHash)
	if err != nil {
		return
	}
	file := nzb.GetFileByName(filename)
	if file == nil {
		return
	}

	nearest := int64(-1)
	for _, seg := range segs {
		if seg.Index < 0 || seg.Index >= len(file.Segments) {
			continue
		}
		off := file.Segments[seg.Index].StartOffset
		if off < from {
			continue // already played past - URGENT priority can't help this one
		}
		if nearest == -1 || off < nearest {
			nearest = off
		}
	}
	if nearest == -1 {
		return
	}

	proximity := estimatePlaybackGap(nearest - from)
	p.logger.Info().
		Str("entry", entry.Name).
		Str("file", filename).
		Dur("proximity", proximity).
		Msg("read-ahead found damage ahead of the playhead; requesting urgent repair")
	p.manager.par2Repair.EnqueueUrgent(entry.InfoHash, proximity)
}

// reserveBudget reserves size bytes against config.Precache.MaxBytes,
// returning false (reserving nothing) if doing so would exceed the cap. This
// is what makes PrecacheMaxBytes an actual bound rather than a suggestion -
// a next-episode burst never starts without a successful reservation.
func (p *Precache) reserveBudget(size int64) bool {
	if size <= 0 {
		return true
	}
	limit := p.cfg().MaxBytes()
	for {
		cur := p.precachedBytes.Load()
		if cur+size > limit {
			return false
		}
		if p.precachedBytes.CompareAndSwap(cur, cur+size) {
			return true
		}
	}
}

// releaseBudget returns size bytes to the PrecacheMaxBytes budget. Never
// drives the total negative (a defensive clamp - reserve/release calls are
// meant to be paired, but a double-release must not corrupt the budget for
// every other in-flight reservation).
func (p *Precache) releaseBudget(size int64) {
	if size <= 0 {
		return
	}
	for {
		cur := p.precachedBytes.Load()
		next := cur - size
		if next < 0 {
			next = 0
		}
		if p.precachedBytes.CompareAndSwap(cur, next) {
			return
		}
	}
}

// markPrecached records that (infoHash,filename) is holding a budget
// reservation of size bytes because PrecacheEvictAfterWatched is enabled, so
// evictIfWatched can find and release it once that file is actually watched.
func (p *Precache) markPrecached(infoHash, filename string, size int64) {
	key := infoHash + ":" + filename
	p.precachedMu.Lock()
	p.precached[key] = size
	p.precachedMu.Unlock()
}

// evictIfWatched reclaims a next-episode pre-cache's disk footprint once
// it's actually been watched (read position at/past 90% of the file),
// releasing its PrecacheMaxBytes reservation. No-op unless
// PrecacheEvictAfterWatched is enabled and this exact (entry,filename) was
// previously pre-cached by this feature (see markPrecached) - organically
// watched files that were never pre-cached are left entirely alone.
func (p *Precache) evictIfWatched(entry *storage.Entry, filename string, start, size int64) {
	if !p.cfg().PrecacheEvictAfterWatched || size <= 0 || start*100 < size*90 {
		return
	}
	key := entry.InfoHash + ":" + filename

	p.precachedMu.Lock()
	bytes, ok := p.precached[key]
	if ok {
		delete(p.precached, key)
	}
	p.precachedMu.Unlock()
	if !ok {
		return
	}

	if p.manager.usenet != nil && p.manager.usenet.EvictCache(entry.InfoHash, filename) {
		p.logger.Info().Str("entry", entry.Name).Str("file", filename).Msg("evicted pre-cached episode after it was watched")
	}
	p.releaseBudget(bytes)
}

// markInflight records that a burst has started writing into the DFS cache
// for (infoHash,filename), identified by the same "infoHash:filename" key
// triggered/precached use. Pair with a deferred unmarkInflight in the same
// burst-owning function (readAhead, precacheEpisodeFile) so the key is held
// for the entire span between the first write and the last, on every exit
// path.
func (p *Precache) markInflight(key string) {
	p.inflightMu.Lock()
	p.inflight[key]++
	p.inflightMu.Unlock()
}

// unmarkInflight reverses a prior markInflight call for key, dropping the
// entry once its refcount reaches zero so the map can't grow without bound.
func (p *Precache) unmarkInflight(key string) {
	p.inflightMu.Lock()
	if p.inflight[key] <= 1 {
		delete(p.inflight, key)
	} else {
		p.inflight[key]--
	}
	p.inflightMu.Unlock()
}

// InflightHas reports whether (infoHash,filename) currently has a burst
// actively writing into the DFS cache - the guard future cache-cleanup logic
// must consult before deleting a partially-cached entry's cache dir.
func (p *Precache) InflightHas(infoHash, filename string) bool {
	key := infoHash + ":" + filename
	p.inflightMu.Lock()
	defer p.inflightMu.Unlock()
	return p.inflight[key] > 0
}

// estimatePlaybackGap converts a byte gap into an estimated playback-time
// gap using a fixed bitrate assumption (no ffprobe/Arr duration metadata is
// threaded through this path). Only used to relatively order URGENT repair
// jobs by proximity - "roughly right" is sufficient.
func estimatePlaybackGap(gapBytes int64) time.Duration {
	if gapBytes <= 0 {
		return 0
	}
	return time.Duration(gapBytes) * time.Second / time.Duration(precacheDefaultBitrateBytesPerSec)
}

// PrecacheSummary is the GUI/API snapshot of this feature's live state - see
// Manager.PrecacheStatus.
type PrecacheSummary struct {
	ReadAheadEnabled     bool  `json:"read_ahead_enabled"`
	ThresholdPercent     int   `json:"threshold_percent"`
	ReadAheadConcurrency int   `json:"read_ahead_concurrency"`
	NextEpisodes         int   `json:"next_episodes"`
	WholeSeason          bool  `json:"whole_season"`
	EvictAfterWatched    bool  `json:"evict_after_watched"`
	PrecachedBytes       int64 `json:"precached_bytes"`
	MaxBytes             int64 `json:"max_bytes"`
	// Paused is the global runtime pause toggle - see Precache.SetPaused.
	// Runtime-only, not persisted.
	Paused bool `json:"paused"`
	// Readiness lists the most recent next-episode pre-cache outcomes
	// (newest first), capped at precacheReadinessDisplayLimit.
	Readiness []EpisodeReadiness `json:"readiness"`
}

// precacheReadinessDisplayLimit bounds how many EpisodeReadiness records
// Summary returns, so a long-running process with many pre-cached episodes
// doesn't grow an unbounded response.
const precacheReadinessDisplayLimit = 500

// Summary returns a snapshot of the precache feature's live config and
// state, for the overlay/repair GUI and API. Safe to call on a nil Precache.
func (p *Precache) Summary() PrecacheSummary {
	if p == nil {
		return PrecacheSummary{}
	}
	p.cachePopulateOnce.Do(p.populateFromCache)
	cfg := p.cfg()

	// Snapshot pause state under mu and release it before taking readinessMu
	// below, so the two locks are never held nested.
	p.mu.Lock()
	globalPaused := p.paused
	pausedSnap := make(map[string]struct{}, len(p.pausedKeys))
	for k := range p.pausedKeys {
		pausedSnap[k] = struct{}{}
	}
	p.mu.Unlock()

	p.readinessMu.Lock()
	keys := make([]string, 0, len(p.readiness))
	readiness := make([]EpisodeReadiness, 0, len(p.readiness))
	for k, r := range p.readiness {
		keys = append(keys, k)
		readiness = append(readiness, r)
	}
	p.readinessMu.Unlock()

	// Refresh each row's cache-coverage figure from the live DFS cache
	// outside the lock (CacheCoverage may hit disk) - see
	// refreshCacheCoverage's doc comment for why a miss never clears an
	// already-known figure.
	reader := p.cacheCoverageReader()
	for i := range readiness {
		p.refreshCacheCoverage(reader, &readiness[i])
		_, keyPaused := pausedSnap[readiness[i].InfoHash+":"+readiness[i].Filename]
		readiness[i].Paused = globalPaused || keyPaused
	}
	if reader != nil {
		p.readinessMu.Lock()
		for i, k := range keys {
			p.readiness[k] = readiness[i]
		}
		p.readinessMu.Unlock()
	}

	sort.Slice(readiness, func(i, j int) bool { return readiness[i].ReadyAt.After(readiness[j].ReadyAt) })
	if len(readiness) > precacheReadinessDisplayLimit {
		readiness = readiness[:precacheReadinessDisplayLimit]
	}

	return PrecacheSummary{
		ReadAheadEnabled:     config.Get().Repair.PrecacheReadAheadEnabled(),
		ThresholdPercent:     cfg.ThresholdPercent(),
		ReadAheadConcurrency: cfg.ReadAheadConcurrency(),
		NextEpisodes:         cfg.NextEpisodes(),
		WholeSeason:          cfg.WholeSeason(),
		EvictAfterWatched:    cfg.PrecacheEvictAfterWatched,
		PrecachedBytes:       p.precachedBytes.Load(),
		MaxBytes:             cfg.MaxBytes(),
		Paused:               globalPaused,
		Readiness:            readiness,
	}
}

// Rescan re-runs the on-disk cache scan so entries cached after startup - a
// fresh import, or Plex reading a file during intro detection or a library
// scan - become visible in the readiness table without waiting for a
// restart. populateFromCache only inserts rows it doesn't already have,
// under readinessMu, so re-running it can't disturb a row the live tracker
// is mid-write on. Single-flighted so two overlapping refreshes don't launch
// two disk walks.
func (p *Precache) Rescan() {
	p.rescanMu.Lock()
	if p.rescanning {
		p.rescanMu.Unlock()
		return
	}
	p.rescanning = true
	p.rescanMu.Unlock()
	defer func() {
		p.rescanMu.Lock()
		p.rescanning = false
		p.rescanMu.Unlock()
	}()
	p.populateFromCache()
}

// PurgeFailure describes a cache directory PurgeIncomplete tried and failed
// to remove.
type PurgeFailure struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

// PurgeIncomplete finds every readiness row whose DFS cache coverage is
// still short of 1.0 (see EpisodeReadiness.CacheCoverage), groups them by
// the (infoHash, entryName) cache directory they actually live in, and, when
// execute is true, removes each directory once via
// Repair.RemoveEntryCacheDir - the same path the stale-NZB sweep already
// uses, so it keeps that path's safeguards (won't touch a directory another
// healthy entry still shares, won't leave the configured cache root). A
// directory with any file a burst is actively writing into (see
// InflightHas) is always skipped, dry-run or not, so this is safe to call
// while precache/read-ahead is running.
//
// execute=false previews what would be deleted without touching disk -
// freedBytes is then the candidates' last-known CachedBytes rather than what
// RemoveEntryCacheDir would actually free.
func (p *Precache) PurgeIncomplete(execute bool) (deleted, skippedInflight []string, failed []PurgeFailure, freedBytes int64, err error) {
	if p == nil {
		return nil, nil, nil, 0, nil
	}
	type group struct {
		infoHash    string
		entryName   string
		keys        []string
		cachedBytes int64
		inflightAny bool
	}

	p.readinessMu.Lock()
	groups := make(map[string]*group)
	order := make([]string, 0)
	for key, r := range p.readiness {
		if r.CacheCoverage >= 1.0 {
			continue
		}
		infoHash, filename, ok := strings.Cut(key, ":")
		if !ok {
			continue
		}
		gKey := infoHash + ":" + r.EntryName
		g, exists := groups[gKey]
		if !exists {
			g = &group{infoHash: infoHash, entryName: r.EntryName}
			groups[gKey] = g
			order = append(order, gKey)
		}
		g.keys = append(g.keys, key)
		g.cachedBytes += r.CachedBytes
		if p.InflightHas(infoHash, filename) {
			g.inflightAny = true
		}
	}
	p.readinessMu.Unlock()

	for _, gKey := range order {
		g := groups[gKey]
		if g.inflightAny {
			skippedInflight = append(skippedInflight, g.entryName)
			continue
		}
		if !execute {
			deleted = append(deleted, g.entryName)
			freedBytes += g.cachedBytes
			continue
		}
		if p.manager.repair == nil {
			failed = append(failed, PurgeFailure{Name: g.entryName, Reason: "repair service unavailable"})
			continue
		}
		freed, ok := p.manager.repair.RemoveEntryCacheDir(g.entryName, g.infoHash)
		if !ok {
			failed = append(failed, PurgeFailure{Name: g.entryName, Reason: "could not be removed (shared with another entry, in use, or already gone)"})
			continue
		}
		deleted = append(deleted, g.entryName)
		freedBytes += freed

		p.readinessMu.Lock()
		for _, key := range g.keys {
			delete(p.readiness, key)
		}
		p.readinessMu.Unlock()
	}

	return deleted, skippedInflight, failed, freedBytes, nil
}

// readAheadTimeout is how long a read-ahead burst over remaining bytes gets:
// long enough to fetch them at precacheReadAheadFloorRate, never less than
// precacheReadAheadTimeout nor more than precacheReadAheadTimeoutCap.
func readAheadTimeout(remaining int64) time.Duration {
	d := time.Duration(float64(max(remaining, 0)) / precacheReadAheadFloorRate * float64(time.Second))
	return min(max(d, precacheReadAheadTimeout), precacheReadAheadTimeoutCap)
}
