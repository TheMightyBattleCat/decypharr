package reader

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// Read-ahead bursts yield to stalled playback on other files.
//
// A read-ahead burst (FetchRange: next-episode precache, read-ahead of a file
// nobody is watching yet) runs a dozen fetches wide on its own reader. Each
// reader has its own connection semaphore, so nothing stops a burst from
// competing with playback of a different file for the same providers. On
// 2026-09-16 a 34 Mbit/s Remux buffered for two hours on ~2.5 MiB/s while three
// ~4 GB next-episode bursts for another series ran back to back against the
// same slow provider at ~2-3 MiB/s.
//
// A playback read - one serving a client stream, marked by ContextForPlayback -
// that waits playbackStallAfter or longer for its segments marks its reader
// stalled. Through the DFS mount the wait that counts is the client's own read
// waiting on the mount's downloaders (NotePlaybackWait); the downloaders' reads
// run ahead of the player and don't (ContextForBufferedPlayback). While a reader other than its own stalled within readAheadYieldFor,
// a burst starts no new segment (fetches already running finish), and carries
// on once playback has gone that long without a stall. A reader that is itself
// being played never yields: its read-ahead fills the cache that playback
// reads next.
//
// Paused time counts against the burst's own deadline, so a burst paused for a
// long buffering session ends incomplete; the next trigger fills the gaps.
// config.Precache.PrecacheYieldToPlayback turns the pause off.
const (
	// playbackStallAfter: how long a playback read must wait for its segments
	// to count as stalled - about the point a player starts to buffer.
	playbackStallAfter = 2 * time.Second
	// readAheadYieldFor: how long a stall keeps other files' bursts paused, and
	// how recent a playback read must be for a reader to count as being played.
	readAheadYieldFor = 30 * time.Second
	// readAheadYieldPoll: how often a paused burst looks again.
	readAheadYieldPoll = 500 * time.Millisecond
)

// stallBoard records each reader's latest playback stall.
type stallBoard struct {
	yieldFor time.Duration
	poll     time.Duration

	lastAny atomic.Int64 // unix nanoseconds of the latest stall on any reader
	mu      sync.Mutex
	last    map[*StreamingReader]int64
}

func newStallBoard(yieldFor, poll time.Duration) *stallBoard {
	return &stallBoard{yieldFor: yieldFor, poll: poll, last: map[*StreamingReader]int64{}}
}

// playbackStalls is shared by every reader in the process: a burst on one file
// yields to playback of any other.
var playbackStalls = newStallBoard(readAheadYieldFor, readAheadYieldPoll)

func (b *stallBoard) note(sr *StreamingReader, now time.Time) {
	ns := now.UnixNano()
	b.mu.Lock()
	b.last[sr] = ns
	b.mu.Unlock()
	for {
		old := b.lastAny.Load()
		if old >= ns || b.lastAny.CompareAndSwap(old, ns) {
			return
		}
	}
}

func (b *stallBoard) forget(sr *StreamingReader) {
	b.mu.Lock()
	delete(b.last, sr)
	b.mu.Unlock()
}

// stalledOther reports whether a reader other than self stalled within yieldFor
// of now, dropping stalls older than that.
func (b *stallBoard) stalledOther(self *StreamingReader, now time.Time) bool {
	since := now.Add(-b.yieldFor).UnixNano()
	if b.lastAny.Load() <= since {
		return false
	}
	found := false
	b.mu.Lock()
	for r, ns := range b.last {
		switch {
		case ns <= since:
			delete(b.last, r)
		case r != self:
			found = true
		}
	}
	b.mu.Unlock()
	return found
}

// notePlaybackRead records a playback read that waited d for its segments. A
// read filling a buffer in front of the client (ContextForBufferedPlayback)
// counts only as the file being played: on 2026-09-18 every "stall" logged
// during playback was a DFS downloader read 2-4.6 s long, up to 512 MB ahead
// of a player that never waited, and each paused other files' bursts.
func (sr *StreamingReader) notePlaybackRead(ctx context.Context, d time.Duration, now time.Time) {
	sr.lastPlaybackRead.Store(now.UnixNano())
	if !bufferedPlayback(ctx) && d >= playbackStallAfter && sr.stalls != nil {
		sr.stalls.note(sr, now)
	}
}

// NotePlaybackWait records that a client read of this reader's file waited d
// for its data in a buffer in front of the reader - the DFS mount's read
// waiting on its downloaders. That is the player waiting, so it marks a stall
// once d reaches playbackStallAfter.
func (sr *StreamingReader) NotePlaybackWait(d time.Duration, now time.Time) {
	sr.lastPlaybackRead.Store(now.UnixNano())
	if d >= playbackStallAfter && sr.stalls != nil {
		sr.stalls.note(sr, now)
		sr.logger.Debug().Dur("wait", d).Msg("playback stall: a client read waited on the mount for the network")
	}
}

// PlaybackStallAfter is how long a client read must wait to count as a
// playback stall, for callers that log a wait they could not record.
const PlaybackStallAfter = playbackStallAfter

// beingPlayed reports whether this reader served a playback read within the
// yield window.
func (sr *StreamingReader) beingPlayed(now time.Time) bool {
	last := sr.lastPlaybackRead.Load()
	return last != 0 && now.UnixNano()-last < int64(sr.stalls.yieldFor)
}

// yieldToPlayback blocks a read-ahead burst while playback of another file is
// stalling, returning early only when ctx or the reader ends.
func (sr *StreamingReader) yieldToPlayback(ctx context.Context, nextSeg int) error {
	b := sr.stalls
	if b == nil {
		return nil
	}
	start := time.Now()
	if sr.beingPlayed(start) || !b.stalledOther(sr, start) || !yieldEnabled() {
		return nil
	}
	sr.logger.Debug().Int("next_seg", nextSeg).
		Msg("read-ahead paused: playback of another file is waiting on the network")
	tick := time.NewTicker(b.poll)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sr.ctx.Done():
			return sr.ctx.Err()
		case <-tick.C:
		}
		now := time.Now()
		if sr.beingPlayed(now) || !b.stalledOther(sr, now) || !yieldEnabled() {
			sr.logger.Debug().Int("next_seg", nextSeg).Dur("paused", now.Sub(start)).
				Msg("read-ahead resumed: no playback stalls for the yield window")
			return nil
		}
	}
}

// verifyStallWorkers is how many of a yielding verification read's prefetch
// workers keep fetching while playback of another file is stalling; the rest
// wait until it has gone readAheadYieldFor without a stall. On 2026-09-18 the
// nightly sweep's checks held all 150 connections of the two fast providers
// and the production install's 92 MiB/s link while a REMUX stalled. Four workers is about the
// synchronous verification read's pace (~24 MiB/s), which keeps a narrowed
// check well inside ffprobe's time limits, where pausing it outright could
// time a healthy file out into a false verdict.
const verifyStallWorkers = 4

// verificationThrottled reports whether a yielding verification read's
// prefetch should run narrowed now.
func (sr *StreamingReader) verificationThrottled(ctx context.Context, now time.Time) bool {
	return yieldingVerification(ctx) && sr.stalls != nil && sr.stalls.stalledOther(sr, now) && yieldEnabled()
}

// waitVerifyThrottle holds FetchRangeWindowed's worker (0-based) before it
// claims a segment while the read is throttled, returning false only when ctx
// or the reader ends. Workers below verifyStallWorkers, and every worker of a
// read not marked ContextForYieldingVerification, never wait. A held worker
// also stops waiting once exhausted reports nothing left to claim, so the
// call returns when the narrowed workers finish rather than when playback
// stops stalling. since is shared by the call's workers and logs the
// narrowing and its end once each.
func (sr *StreamingReader) waitVerifyThrottle(ctx context.Context, worker, concurrency int, since *atomic.Int64, exhausted func() bool) bool {
	if worker < verifyStallWorkers || !yieldingVerification(ctx) {
		return true
	}
	poll := readAheadYieldPoll
	if sr.stalls != nil {
		poll = sr.stalls.poll
	}
	for {
		now := time.Now()
		if exhausted() || !sr.verificationThrottled(ctx, now) {
			if at := since.Swap(0); at != 0 {
				sr.logger.Debug().Dur("throttled", now.Sub(time.Unix(0, at))).
					Msg("verification prefetch back to full width: no playback stalls for the yield window")
			}
			return true
		}
		if since.CompareAndSwap(0, now.UnixNano()) {
			sr.logger.Debug().Int("workers", verifyStallWorkers).Int("of", concurrency).
				Msg("verification prefetch narrowed: playback of another file is waiting on the network")
		}
		select {
		case <-ctx.Done():
			return false
		case <-sr.ctx.Done():
			return false
		case <-time.After(poll):
		}
	}
}

// yieldEnabled reports whether read-ahead bursts pause for stalled playback.
func yieldEnabled() bool {
	return config.Get().Precache.YieldToPlayback()
}
