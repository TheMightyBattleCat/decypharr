package nntp

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/internal/config"
)

// shortReadTimeout bounds each reply read for tests where a server stays
// silent, so a missing reply fails in 2 s instead of StreamBodyTimeout.
func shortReadTimeout(t *testing.T) {
	t.Helper()
	saved := timeouts
	timeouts.StreamBodyTimeout = 2 * time.Second
	t.Cleanup(func() { timeouts = saved })
}

func pipelineTestClient(t *testing.T, srv *fakeNNTP) (*Client, config.UsenetProvider) {
	t.Helper()
	p := config.UsenetProvider{Host: "127.0.0.1", Port: srv.port(), Priority: 1, MaxConnections: 2}
	return newStatTestClient(t, []config.UsenetProvider{p}, 100), p
}

func isConnErr(err error) bool {
	var nntpErr *Error
	return errors.As(err, &nntpErr) && nntpErr.Type == ErrorTypeConnection
}

// idleConns is the provider's pooled connection count; slots is how many are
// still checked out.
func idleConns(c *Client, host string) (idle, slots int) {
	pp := c.pools[host]
	pp.mu.Lock()
	defer pp.mu.Unlock()
	return len(pp.conns), len(pp.slots)
}

// Every STAT in a window goes out before the first reply is read.
func TestStatBatchSendsWindowInOneWrite(t *testing.T) {
	srv := startFakeNNTP(t, 0, "gone@test")
	c, p := pipelineTestClient(t, srv)

	msgIDs := append(ids("present", statPipelineDepth-1), "gone@test")
	res, err := c.batchStatOnProvider(context.Background(), p, msgIDs)
	if err != nil {
		t.Fatalf("batchStatOnProvider: %v", err)
	}
	for i, r := range res[:len(res)-1] {
		if !r.Available || r.MessageID != msgIDs[i] {
			t.Fatalf("result %d = %+v, want %s available", i, r, msgIDs[i])
		}
	}
	if last := res[len(res)-1]; last.Available || !IsArticleNotFoundError(last.Error) {
		t.Fatalf("missing article = %+v, want article-not-found", last)
	}
	if srv.maxQueued.Load() == 0 {
		t.Fatal("the server never had a second command queued behind a STAT: nothing was pipelined")
	}
	if idle, slots := idleConns(c, p.Host); idle != 1 || slots != 0 {
		t.Fatalf("pool after a clean window: idle %d, checked out %d, want 1 and 0", idle, slots)
	}
}

// More IDs than one window: every result comes back in order, across windows.
func TestStatBatchSpansWindows(t *testing.T) {
	srv := startFakeNNTP(t, 0, "present-20@test")
	c, p := pipelineTestClient(t, srv)

	msgIDs := ids("present", 2*statPipelineDepth+5)
	res, err := c.batchStatOnProvider(context.Background(), p, msgIDs)
	if err != nil {
		t.Fatalf("batchStatOnProvider: %v", err)
	}
	if len(res) != len(msgIDs) {
		t.Fatalf("%d results for %d IDs", len(res), len(msgIDs))
	}
	for i, r := range res {
		want := i != 20
		if r.MessageID != msgIDs[i] || r.Available != want {
			t.Fatalf("result %d = %+v, want %s available=%v", i, r, msgIDs[i], want)
		}
	}
}

// A server that drops one reply shifts the rest onto the wrong IDs. Read in
// order with nothing to check against, the present article would take the
// next one's 430. The DATE check catches it: nothing in the window may come
// back as not-found, and the connection must not be reused.
func TestStatBatchDroppedReplyIsNotA430(t *testing.T) {
	shortReadTimeout(t)
	srv := startFakeNNTP(t, 0, "gone@test")
	srv.noReply = map[string]bool{"lost@test": true}
	c, p := pipelineTestClient(t, srv)

	res, err := c.batchStatOnProvider(context.Background(), p, []string{"lost@test", "gone@test", "c@test"})
	for _, r := range res {
		if r.Available || IsArticleNotFoundError(r.Error) || !isConnErr(r.Error) {
			t.Fatalf("result %+v: a desynced window must be a connection error, never a verdict", r)
		}
	}
	if !errors.Is(err, errStatDesync) {
		t.Fatalf("err = %v, want a desync", err)
	}
	if got := c.pools[p.Host].stat.desyncs.Load(); got != 1 {
		t.Fatalf("desyncs = %d, want 1", got)
	}
	if got := time.Duration(c.pools[p.Host].stat.nsPerStat.Load()); got != statErrorPenalty {
		t.Fatalf("recorded latency %v, want the error penalty", got)
	}
	if idle, slots := idleConns(c, p.Host); idle != 0 || slots != 0 {
		t.Fatalf("pool after a desync: idle %d, checked out %d, want the connection closed", idle, slots)
	}
}

// The last reply dropped: the DATE's 111 lands in a STAT slot.
func TestStatBatchDroppedLastReply(t *testing.T) {
	shortReadTimeout(t)
	srv := startFakeNNTP(t, 0)
	srv.noReply = map[string]bool{"b@test": true}
	c, p := pipelineTestClient(t, srv)

	start := time.Now()
	_, err := c.batchStatOnProvider(context.Background(), p, []string{"a@test", "b@test"})
	if !errors.Is(err, errStatDesync) {
		t.Fatalf("err = %v, want a desync", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("took %v: a misplaced 111 should fail at once, not wait for a read timeout", took)
	}
}

// A stray extra line: the DATE slot gets a STAT reply. The window fails and
// the connection is closed, so the stray reply can never be read as the
// answer to the next command.
func TestStatBatchExtraLineClosesConnection(t *testing.T) {
	srv := startFakeNNTP(t, 0, "gone@test")
	srv.extraLine = map[string]bool{"a@test": true}
	c, p := pipelineTestClient(t, srv)

	res, err := c.batchStatOnProvider(context.Background(), p, []string{"a@test", "gone@test"})
	if !errors.Is(err, errStatDesync) {
		t.Fatalf("err = %v, want a desync", err)
	}
	for _, r := range res {
		if r.Available || IsArticleNotFoundError(r.Error) {
			t.Fatalf("result %+v: a desynced window must be a connection error", r)
		}
	}
	if idle, slots := idleConns(c, p.Host); idle != 0 || slots != 0 {
		t.Fatalf("pool after a stray line: idle %d, checked out %d, want the connection closed", idle, slots)
	}

	// A fresh connection still works.
	srv.extraLine = nil
	res, err = c.batchStatOnProvider(context.Background(), p, []string{"b@test", "gone@test"})
	if err != nil || !res[0].Available || !IsArticleNotFoundError(res[1].Error) {
		t.Fatalf("after the desync: %+v, %v", res, err)
	}
}

// A reply that is neither found nor not-found (here 502) is not a verdict
// the window can carry: the whole window fails and the connection closes.
func TestStatBatchUnexpectedReplyFailsWindow(t *testing.T) {
	srv := startFakeNNTP(t, 0, "gone@test")
	srv.denied = map[string]bool{"b@test": true}
	c, p := pipelineTestClient(t, srv)

	res, err := c.batchStatOnProvider(context.Background(), p, []string{"a@test", "gone@test", "b@test"})
	if !errors.Is(err, errStatDesync) {
		t.Fatalf("err = %v, want the window failed", err)
	}
	for _, r := range res {
		if r.Available || IsArticleNotFoundError(r.Error) {
			t.Fatalf("result %+v, want a connection error", r)
		}
	}
	if idle, slots := idleConns(c, p.Host); idle != 0 || slots != 0 {
		t.Fatalf("pool: idle %d, checked out %d, want the connection closed", idle, slots)
	}
}

// Cancelling the caller unblocks a window waiting on a slow server, and
// nothing in it becomes a verdict.
func TestStatBatchCancelMidWindow(t *testing.T) {
	srv := startFakeNNTP(t, 200*time.Millisecond)
	c, p := pipelineTestClient(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	res, err := c.batchStatOnProvider(ctx, p, ids("slow", statPipelineDepth))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the context's error", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("took %v after a 100 ms deadline", took)
	}
	for _, r := range res {
		if r.Available || IsArticleNotFoundError(r.Error) {
			t.Fatalf("result %+v after cancel, want no verdict", r)
		}
	}
	if idle, slots := idleConns(c, p.Host); idle != 0 || slots != 0 {
		t.Fatalf("pool after cancel: idle %d, checked out %d, want the connection closed", idle, slots)
	}
	// A cancelled window is the caller's doing: counted, never penalised.
	st := &c.pools[p.Host].stat
	if got := st.cancelled.Load(); got != 1 {
		t.Fatalf("cancelled = %d, want 1", got)
	}
	if got := st.penalties(); got != 0 || st.nsPerStat.Load() != 0 {
		t.Fatalf("a cancelled window recorded %d penalties and latency %v, want none", got, time.Duration(st.nsPerStat.Load()))
	}
}

// Every failure that costs a provider the routing penalty is counted under
// its cause, so the stats say why a provider keeps dropping out.
func TestStatFailureCounters(t *testing.T) {
	t.Run("refused dial", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := ln.Addr().(*net.TCPAddr).Port
		_ = ln.Close()
		p := config.UsenetProvider{Host: "127.0.0.1", Port: port, Priority: 1, MaxConnections: 2}
		c := newStatTestClient(t, []config.UsenetProvider{p}, 100)
		if _, err := c.batchStatOnProvider(context.Background(), p, []string{"a@test"}); err == nil {
			t.Fatal("expected a connection error")
		}
		st := &c.pools[p.Host].stat
		if st.errCheckout.Load() != 1 || st.penalties() != 1 {
			t.Fatalf("checkout = %d of %d penalties, want 1 of 1", st.errCheckout.Load(), st.penalties())
		}
	})

	// The server hangs up partway through a window. The read sees EOF or a
	// reset depending on timing; both are the provider's close.
	t.Run("dropped mid-window", func(t *testing.T) {
		srv := startFakeNNTP(t, 0)
		srv.dropAfter = 2
		c, p := pipelineTestClient(t, srv)
		res, err := c.batchStatOnProvider(context.Background(), p, []string{"a@test", "b@test", "c@test", "d@test"})
		if err == nil {
			t.Fatal("expected a mid-window connection error")
		}
		for _, r := range res {
			if r.Available || IsArticleNotFoundError(r.Error) {
				t.Fatalf("result %+v from a dropped window, want no verdict", r)
			}
		}
		st := &c.pools[p.Host].stat
		if st.errEOF.Load() != 1 || st.penalties() != 1 {
			t.Fatalf("eof = %d of %d penalties (err %v), want 1 of 1", st.errEOF.Load(), st.penalties(), err)
		}
	})

	t.Run("read timeout", func(t *testing.T) {
		shortReadTimeout(t)
		srv := startFakeNNTP(t, 0)
		srv.noReply = map[string]bool{"silent@test": true, "b@test": true}
		srv.noDate = true
		c, p := pipelineTestClient(t, srv)
		_, err := c.batchStatOnProvider(context.Background(), p, []string{"silent@test", "b@test"})
		if err == nil || errors.Is(err, errStatDesync) {
			t.Fatalf("err = %v, want a read timeout", err)
		}
		st := &c.pools[p.Host].stat
		if st.errTimeout.Load() != 1 || st.penalties() != 1 {
			t.Fatalf("timeout = %d of %d penalties (err %v), want 1 of 1", st.errTimeout.Load(), st.penalties(), err)
		}
	})

	t.Run("stats", func(t *testing.T) {
		srv := startFakeNNTP(t, 0)
		srv.dropAfter = 1
		c, p := pipelineTestClient(t, srv)
		c.speedTestResults = xsync.NewMap[string, SpeedTestResult]()
		_, _ = c.batchStatOnProvider(context.Background(), p, []string{"a@test", "b@test"})
		info := c.Stats()["providers"].([]map[string]any)[0]
		if info["stat_errors"] != int64(1) || info["stat_err_eof"] != int64(1) || info["stat_cancelled"] != int64(0) {
			t.Fatalf("stats = errors %v, eof %v, cancelled %v; want 1, 1, 0", info["stat_errors"], info["stat_err_eof"], info["stat_cancelled"])
		}
	})
}
