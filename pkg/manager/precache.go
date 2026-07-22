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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

const (
	// precacheReadAheadTimeout bounds one read-ahead burst so a stalled
	// provider can't wedge a background goroutine forever.
	precacheReadAheadTimeout = 30 * time.Minute

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
}

// NewPrecache builds the precache service.
func NewPrecache(m *Manager) *Precache {
	return &Precache{
		manager:   m,
		logger:    logger.New("precache"),
		triggered: make(map[string]time.Time),
		precached: make(map[string]int64),
		readiness: make(map[string]EpisodeReadiness),
	}
}

func (p *Precache) cfg() config.PrecacheConfig {
	return config.Get().Precache
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

	if !config.Get().Repair.PrecacheReadAheadEnabled() {
		return
	}
	threshold := int64(p.cfg().ThresholdPercent())
	if start*100 < size*threshold {
		return
	}

	key := entry.InfoHash + ":" + filename
	now := time.Now()

	p.mu.Lock()
	if _, done := p.triggered[key]; done {
		p.mu.Unlock()
		return
	}
	p.triggered[key] = now
	p.pruneLocked(now)
	p.mu.Unlock()

	go p.readAhead(entry, filename, start, size)
}

// pruneLocked drops dedup entries older than precacheTriggeredTTL. Caller
// must hold p.mu.
func (p *Precache) pruneLocked(now time.Time) {
	for k, t := range p.triggered {
		if now.Sub(t) > precacheTriggeredTTL {
			delete(p.triggered, k)
		}
	}
}

// readAhead runs the aggressive read-ahead burst for one file, then checks
// for damage it may have surfaced.
func (p *Precache) readAhead(entry *storage.Entry, filename string, from, size int64) {
	if p.manager.usenet == nil {
		return
	}
	concurrency := p.cfg().ReadAheadConcurrency()

	ctx, cancel := context.WithTimeout(context.Background(), precacheReadAheadTimeout)
	defer cancel()

	p.logger.Info().
		Str("entry", entry.Name).
		Str("file", filename).
		Int64("from", from).
		Int64("size", size).
		Int("concurrency", concurrency).
		Msg("starting read-ahead precache")

	if err := p.manager.usenet.ReadAhead(ctx, entry.InfoHash, filename, from, concurrency); err != nil {
		p.logger.Debug().Err(err).Str("entry", entry.Name).Str("file", filename).Msg("read-ahead precache ended early")
	}

	p.repairAhead(entry, filename, from)
	p.maybePrecacheNextEpisodes(entry, filename)
}

// repairAhead checks whether the read-ahead pass left any damage pending for
// filename and, if so, enqueues an URGENT-lane PAR2 repair for it - budgeted
// by the estimated playback-time gap between the current playhead and the
// nearest damaged region still ahead of it. Damage entirely behind the
// playhead (already played past) is left for the normal BATCH lane, since
// repairing it can no longer prevent a glitch.
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
	EvictAfterWatched    bool  `json:"evict_after_watched"`
	PrecachedBytes       int64 `json:"precached_bytes"`
	MaxBytes             int64 `json:"max_bytes"`
	// Readiness lists the most recent next-episode pre-cache outcomes
	// (newest first), capped at precacheReadinessDisplayLimit.
	Readiness []EpisodeReadiness `json:"readiness"`
}

// precacheReadinessDisplayLimit bounds how many EpisodeReadiness records
// Summary returns, so a long-running process with many pre-cached episodes
// doesn't grow an unbounded response.
const precacheReadinessDisplayLimit = 25

// Summary returns a snapshot of the precache feature's live config and
// state, for the overlay/repair GUI and API. Safe to call on a nil Precache.
func (p *Precache) Summary() PrecacheSummary {
	if p == nil {
		return PrecacheSummary{}
	}
	cfg := p.cfg()

	p.readinessMu.Lock()
	readiness := make([]EpisodeReadiness, 0, len(p.readiness))
	for _, r := range p.readiness {
		readiness = append(readiness, r)
	}
	p.readinessMu.Unlock()
	sort.Slice(readiness, func(i, j int) bool { return readiness[i].ReadyAt.After(readiness[j].ReadyAt) })
	if len(readiness) > precacheReadinessDisplayLimit {
		readiness = readiness[:precacheReadinessDisplayLimit]
	}

	return PrecacheSummary{
		ReadAheadEnabled:     config.Get().Repair.PrecacheReadAheadEnabled(),
		ThresholdPercent:     cfg.ThresholdPercent(),
		ReadAheadConcurrency: cfg.ReadAheadConcurrency(),
		NextEpisodes:         cfg.NextEpisodes(),
		EvictAfterWatched:    cfg.PrecacheEvictAfterWatched,
		PrecachedBytes:       p.precachedBytes.Load(),
		MaxBytes:             cfg.MaxBytes(),
		Readiness:            readiness,
	}
}
