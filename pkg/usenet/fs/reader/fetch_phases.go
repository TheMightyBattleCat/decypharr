package reader

import (
	"context"
	"io"
	"sync/atomic"
	"time"
)

// Where a slow segment fetch spent its time. A playback stall is logged as
// the client "waiting for the network", but the article download is only one
// part of a fetch: it also waits for one of the reader's connection slots,
// then for a connection, and once the article is in memory it is written to
// the reader's scratch cache while still holding that connection. The news
// server's own timings (nntp fetch_timing) stop before that write, so a disk
// too busy to take it looked exactly like a slow server.
const (
	// slowFetchLogThreshold is how long one segment's fetch must take to be
	// logged: the same bar as a playback stall and the blocked-read line.
	slowFetchLogThreshold = 2 * time.Second
	// slowFetchLogEvery spaces the lines out per reader. A burst on a
	// struggling disk can have every one of its segments over the threshold,
	// several a second; the line in between carries how many were skipped.
	slowFetchLogEvery = 5 * time.Second
)

// fetchPhases is one doFetch call's time, split by what it waited on.
type fetchPhases struct {
	slotWait time.Duration // waiting for one of this reader's connection slots
	exec     time.Duration // the whole ExecuteWithFailover call
	onConn   time.Duration // holding a connection, over every provider tried
	store    time.Duration // of onConn: writing the article into the scratch cache
}

// total is the fetch's time from asking for a slot to the article stored.
func (p fetchPhases) total() time.Duration { return p.slotWait + p.exec }

// connect is the time spent getting a connection: checkout, dialling, and
// moving between providers on failover.
func (p fetchPhases) connect() time.Duration { return max(p.exec-p.onConn, 0) }

// server is the time spent asking the news server for the article and
// receiving it.
func (p fetchPhases) server() time.Duration { return max(p.onConn-p.store, 0) }

// timedWriter adds up the time spent inside the wrapped writer.
type timedWriter struct {
	w     io.Writer
	spent time.Duration
}

func (t *timedWriter) Write(p []byte) (int, error) {
	start := time.Now()
	n, err := t.w.Write(p)
	t.spent += time.Since(start)
	return n, err
}

// slowFetchLog decides which slow fetches get a line.
type slowFetchLog struct {
	lastNS  atomic.Int64 // unix nanoseconds of the last line
	skipped atomic.Int64 // slow fetches since then that got none
}

// allow reports whether a slow fetch at now should be logged, and how many
// were skipped since the last one that was.
func (l *slowFetchLog) allow(now time.Time, every time.Duration) (skipped int64, ok bool) {
	ns := now.UnixNano()
	last := l.lastNS.Load()
	if last != 0 && ns-last < int64(every) || !l.lastNS.CompareAndSwap(last, ns) {
		l.skipped.Add(1)
		return 0, false
	}
	return l.skipped.Swap(0), true
}

// readKind names the read a context belongs to, for the log lines that say
// what was kept waiting.
func readKind(ctx context.Context) string {
	switch {
	case paddingDisabled(ctx):
		return "verification read"
	case isPlaybackRead(ctx) && bufferedPlayback(ctx):
		return "mount read-ahead"
	case isPlaybackRead(ctx):
		return "playback read"
	default:
		return "background read"
	}
}

// noteSlowFetch logs a segment fetch that took slowFetchLogThreshold or
// longer, with where the time went. ctx is the fetch's own, so the read
// named is the one that started the download - a prefetch worker's is a
// background read even when playback then waits on it.
func (sf *SegmentFetcher) noteSlowFetch(ctx context.Context, segIdx int, p fetchPhases, err error) {
	total := p.total()
	if total < slowFetchLogThreshold {
		return
	}
	skipped, ok := sf.slowLog.allow(time.Now(), slowFetchLogEvery)
	if !ok {
		return
	}
	ev := sf.logger.Debug().
		Str("component", "fetcher").
		Int("segment", segIdx).
		Str("read", readKind(ctx)).
		Dur("total", total).
		Dur("slot_wait", p.slotWait).
		Dur("connect", p.connect()).
		Dur("server", p.server()).
		Dur("store", p.store).
		Int64("slow_since_last", skipped)
	if err != nil {
		ev = ev.Err(err)
	}
	ev.Msg("slow segment fetch")
}
