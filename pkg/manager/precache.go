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
	"sync"
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
	triggered map[string]time.Time // "infoHash:filename" -> when read-ahead was kicked off
}

// NewPrecache builds the precache service.
func NewPrecache(m *Manager) *Precache {
	return &Precache{
		manager:   m,
		logger:    logger.New("precache"),
		triggered: make(map[string]time.Time),
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
	if !p.cfg().ReadAheadEnabled() {
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
