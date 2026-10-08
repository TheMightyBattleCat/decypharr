package manager

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// Bursts run one at a time, and a read-ahead nobody is watching any more is
// stopped.
//
// On 2026-10-08 three bursts ran at once on a production install whose cache
// sits on two spinning disks. Each burst writes what it downloads to the
// reader's scratch cache and again to the durable cache, and together they
// kept the disks busy enough that a viewer starting another film waited 3.3 s
// for its first uncached read - with the news server answering in under
// 100 ms throughout. One of the three was the read-ahead for a film whose
// viewer had stopped watching minutes earlier; it ran on for the rest of its
// 27 GB.
//
// A burst now takes a turn (burstGate) for each durableBurstChunk it fetches
// and persists, so only one writes at a time. Turns go to read-aheads for
// files being played before next-episode bursts, in arrival order within
// each, which shares the turn between several viewers' read-aheads and lets a
// new one in within a chunk of a long next-episode burst. A burst asks again
// only once its chunk is done, so one read-ahead alternates chunks with a
// waiting next-episode burst, and next-episode bursts wait outright only
// while two or more read-aheads are queued. Time spent waiting for a turn
// does not count against a burst's time limit (burstOpts.budget).
//
// A burst that holds the turn keeps it while paused for another file's
// stalling playback (reader.yieldToPlayback), so the stalled file's own
// read-ahead waits behind it. That is left as it is: the mount's downloaders
// serve that playback either way, and starting another burst is the last
// thing a disk that is already making playback wait needs.

// burstClass orders the bursts waiting for a turn.
type burstClass int

const (
	// burstPlaying is the read-ahead for a file being played.
	burstPlaying burstClass = iota
	// burstAhead is a burst for a file nobody is playing yet: a next episode
	// or a re-grabbed episode's replacement.
	burstAhead
	burstClasses
)

// burstGate lets one burst chunk run at a time.
type burstGate struct {
	mu      sync.Mutex
	busy    bool
	waiting [burstClasses][]chan struct{} // oldest first; closed when handed the turn
}

// acquire waits for the turn and returns the func that gives it up, which
// must be called exactly once. It fails only when ctx ends first.
func (g *burstGate) acquire(ctx context.Context, class burstClass) (release func(), err error) {
	g.mu.Lock()
	if !g.busy {
		g.busy = true
		g.mu.Unlock()
		return g.release, nil
	}
	turn := make(chan struct{})
	g.waiting[class] = append(g.waiting[class], turn)
	g.mu.Unlock()

	select {
	case <-turn:
		return g.release, nil
	case <-ctx.Done():
	}
	g.mu.Lock()
	for i, w := range g.waiting[class] {
		if w == turn {
			g.waiting[class] = append(g.waiting[class][:i], g.waiting[class][i+1:]...)
			g.mu.Unlock()
			return nil, ctx.Err()
		}
	}
	g.mu.Unlock()
	// Handed the turn just as ctx ended: pass it on.
	g.release()
	return nil, ctx.Err()
}

// release hands the turn to the longest-waiting burst of the first class
// that has one, or frees it.
func (g *burstGate) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for c := range g.waiting {
		if len(g.waiting[c]) > 0 {
			turn := g.waiting[c][0]
			g.waiting[c] = g.waiting[c][1:]
			close(turn)
			return
		}
	}
	g.busy = false
}

// turn is acquire for one class, in the shape burstOpts wants.
func (g *burstGate) turn(class burstClass) func(context.Context) (func(), error) {
	return func(ctx context.Context) (func(), error) { return g.acquire(ctx, class) }
}

// errViewerLeft ends a read-ahead whose file is no longer being played.
var errViewerLeft = errors.New("nobody is watching this file any more")

// viewerLeftGraceMinimum is the least time a file must be missing from
// Plex's sessions before its read-ahead is stopped. A player that restarts
// its stream (a quality change, a subtitle switch) drops out of the list for
// a moment.
const viewerLeftGraceMinimum = time.Minute

// viewerLeftGrace is how long a file must be missing from Plex's sessions:
// two refreshes of the session list, and never under a minute.
func viewerLeftGrace(cfg config.PlexConfig) time.Duration {
	return max(2*cfg.SessionTTL(), viewerLeftGraceMinimum)
}

// plexWatch is what Plex's session list says about one file.
type plexWatch int

const (
	// plexWatchUnknown: the Plex gate is off, or Plex has been unreachable
	// past its grace window. Nothing can be concluded.
	plexWatchUnknown plexWatch = iota
	// plexWatchPlaying: the file is in a session. A paused or buffering
	// session counts.
	plexWatchPlaying
	// plexWatchAbsent: Plex answered and no session has the file.
	plexWatchAbsent
)

// viewerWatch decides when a read-ahead's viewer has left: the file has to
// be seen absent, and still absent a grace period later, with nothing but
// absences in between. Used by one burst's goroutine only.
type viewerWatch struct {
	grace       time.Duration
	absentSince time.Time
}

// left takes one look at Plex's answer and reports whether the viewer has
// been gone for the grace period. Anything but a plain absence starts the
// count again, so a Plex outage never stops a burst.
func (v *viewerWatch) left(state plexWatch, now time.Time) bool {
	if state != plexWatchAbsent {
		v.absentSince = time.Time{}
		return false
	}
	if v.absentSince.IsZero() {
		v.absentSince = now
		return false
	}
	return now.Sub(v.absentSince) >= v.grace
}
