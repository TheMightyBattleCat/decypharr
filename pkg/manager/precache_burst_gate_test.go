package manager

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// waiters blocks until n bursts are queued at the gate.
func waiters(t *testing.T, g *burstGate, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		g.mu.Lock()
		got := 0
		for c := range g.waiting {
			got += len(g.waiting[c])
		}
		g.mu.Unlock()
		if got == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d bursts waiting, want %d", got, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// The turn goes to read-aheads for files being played before next-episode
// bursts, whatever order they asked in, and in arrival order within each.
func TestBurstGateServesPlayingBeforeAheadInArrivalOrder(t *testing.T) {
	var g burstGate
	release, err := g.acquire(context.Background(), burstAhead)
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var order []string
	var wg sync.WaitGroup
	queue := func(name string, class burstClass, queued int) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rel, err := g.acquire(context.Background(), class)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			rel()
		}()
		waiters(t, &g, queued)
	}
	queue("ahead-1", burstAhead, 1)
	queue("playing-1", burstPlaying, 2)
	queue("ahead-2", burstAhead, 3)
	queue("playing-2", burstPlaying, 4)

	release()
	wg.Wait()
	want := []string{"playing-1", "playing-2", "ahead-1", "ahead-2"}
	for i := range want {
		if i >= len(order) || order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
	if g.busy {
		t.Fatal("gate still busy with nothing running or waiting")
	}
}

// A burst cancelled while it queues leaves the queue, and the turn it never
// took still reaches the next burst.
func TestBurstGateCancelledWaiterGivesNothingUp(t *testing.T) {
	var g burstGate
	release, _ := g.acquire(context.Background(), burstPlaying)

	ctx, cancel := context.WithCancel(context.Background())
	failed := make(chan error, 1)
	go func() {
		_, err := g.acquire(ctx, burstPlaying)
		failed <- err
	}()
	waiters(t, &g, 1)
	cancel()
	if err := <-failed; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled acquire = %v, want context.Canceled", err)
	}
	waiters(t, &g, 0)

	got := make(chan struct{})
	go func() {
		rel, err := g.acquire(context.Background(), burstAhead)
		if err == nil {
			close(got)
			rel()
		}
	}()
	waiters(t, &g, 1)
	release()
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("the next burst never got the turn")
	}
}

// overlapSource is a scratchSource that notices two chunk fetches running at
// once. Each fetch takes long enough for another burst to start if nothing
// held it back.
type overlapSource struct {
	*scratchSource
	mu      sync.Mutex // scratchSource is not safe for two bursts at once
	running *atomic.Int32
	overlap *atomic.Bool
	delay   time.Duration
}

func (s *overlapSource) ReadAheadRange(ctx context.Context, id, name string, off, length int64, c int) error {
	if s.running.Add(1) > 1 {
		s.overlap.Store(true)
	}
	defer s.running.Add(-1)
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.scratchSource.ReadAheadRange(ctx, id, name, off, length, c)
}

func (s *overlapSource) ReadCachedAt(ctx context.Context, id, name string, p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.scratchSource.ReadCachedAt(ctx, id, name, p, off)
}

// lockedStore is a durableStore two bursts can write at once.
type lockedStore struct {
	mu sync.Mutex
	d  *durableStore
}

func (l *lockedStore) WriteCachedRange(e, f string, size int64, p []byte, off int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.d.WriteCachedRange(e, f, size, p, off)
}

func (l *lockedStore) HasCachedRange(e, f string, off, length int64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.d.HasCachedRange(e, f, off, length)
}

// Two bursts sharing a gate never fetch at the same time, and both finish
// with their files whole. Without the gate they overlap.
func TestBurstChunksTakeTurns(t *testing.T) {
	const size = 10 * burstSegSize
	run := func(gated bool) (overlapped bool) {
		var g burstGate
		var running atomic.Int32
		var overlap atomic.Bool
		var wg sync.WaitGroup
		for _, name := range []string{"a.mkv", "b.mkv"} {
			src := &overlapSource{scratchSource: newScratchSource(name, 10, 4), running: &running, overlap: &overlap, delay: 5 * time.Millisecond}
			store := &lockedStore{d: newDurableStore()}
			var opts burstOpts
			if gated {
				opts.turn = g.turn(burstPlaying)
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := burstChunks(context.Background(), src, store, store, "Entry", "hash", name,
					0, size, 4, burstChunk, zerolog.Nop(), opts); err != nil {
					t.Error(err)
				}
				if h := store.d.holes(size); len(h) != 0 {
					t.Errorf("%s: durable holes %v, want none", name, h)
				}
			}()
		}
		wg.Wait()
		return overlap.Load()
	}
	if !run(false) {
		t.Fatal("ungated bursts did not overlap: the test proves nothing")
	}
	if run(true) {
		t.Fatal("two bursts fetched at the same time through one gate")
	}
}

// Time spent waiting for a turn is not taken from the burst's time limit:
// a burst that queued for longer than its whole budget still runs.
func TestBurstChunksBudgetExcludesTimeQueued(t *testing.T) {
	const filename = "episode.mkv"
	const size = 10 * burstSegSize
	const budget = 200 * time.Millisecond
	src := newScratchSource(filename, 10, 4)
	store := newDurableStore()

	var g burstGate
	release, _ := g.acquire(context.Background(), burstAhead)
	go func() {
		time.Sleep(2 * budget)
		release()
	}()

	res, err := burstChunks(context.Background(), src, store, store, "Entry", "hash", filename,
		0, size, 4, burstChunk, zerolog.Nop(), burstOpts{turn: g.turn(burstPlaying), budget: budget})
	if err != nil {
		t.Fatalf("burst that only queued past its budget failed: %v", err)
	}
	if res.queued < budget {
		t.Fatalf("queued = %v, want at least the budget %v", res.queued, budget)
	}
	if h := store.holes(size); len(h) != 0 {
		t.Fatalf("durable holes %v, want none", h)
	}
}

// slowSource takes delay over each chunk fetch.
type slowSource struct {
	*scratchSource
	delay time.Duration
}

func (s *slowSource) ReadAheadRange(ctx context.Context, id, name string, off, length int64, c int) error {
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.scratchSource.ReadAheadRange(ctx, id, name, off, length, c)
}

// The budget still ends a burst that spends it fetching.
func TestBurstChunksBudgetEndsASlowBurst(t *testing.T) {
	const filename = "episode.mkv"
	const size = 10 * burstSegSize
	src := &slowSource{scratchSource: newScratchSource(filename, 10, 4), delay: 30 * time.Millisecond}
	store := newDurableStore()

	_, err := burstChunks(context.Background(), src, store, store, "Entry", "hash", filename,
		0, size, 4, burstChunk, zerolog.Nop(), burstOpts{budget: 45 * time.Millisecond})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if len(store.holes(size)) == 0 {
		t.Fatal("a burst over its budget fetched the whole file")
	}
}

// keep ending the burst stops it before the next chunk is fetched, hands
// back the turn, and leaves what was already persisted.
func TestBurstChunksStopWhenKeepFails(t *testing.T) {
	const filename = "film.mkv"
	const size = 10 * burstSegSize
	src := newScratchSource(filename, 10, 4)
	store := newDurableStore()
	var g burstGate

	chunks := 0
	res, err := burstChunks(context.Background(), src, store, store, "Entry", "hash", filename,
		0, size, 4, burstChunk, zerolog.Nop(), burstOpts{
			turn: g.turn(burstPlaying),
			keep: func(holding bool) error {
				if !holding {
					chunks++
				}
				if chunks > 2 {
					return errViewerLeft
				}
				return nil
			},
		})
	if !errors.Is(err, errViewerLeft) {
		t.Fatalf("err = %v, want errViewerLeft", err)
	}
	if want := int64(2 * burstChunk); res.fetched != want {
		t.Fatalf("fetched = %d, want the two chunks before the viewer left (%d)", res.fetched, want)
	}
	if g.busy {
		t.Fatal("the stopped burst kept the turn")
	}
	if !store.HasCachedRange("Entry", filename, 0, 2*burstChunk) {
		t.Fatal("the chunks fetched before the viewer left were not kept")
	}
}

// A read-ahead is stopped only once its file has been missing from Plex's
// sessions for the whole grace period. A session seen in between - a paused
// one counts as playing - or Plex not answering starts the count again, so
// a brief drop-out or a Plex outage never stops one.
func TestViewerWatchNeedsAFullGraceOfAbsence(t *testing.T) {
	t0 := time.Unix(1_000_000, 0)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }

	steps := []struct {
		name  string
		state plexWatch
		at    time.Time
		want  bool
	}{
		{"playing", plexWatchPlaying, at(0), false},
		{"first absence", plexWatchAbsent, at(10), false},
		{"absent, inside the grace", plexWatchAbsent, at(40), false},
		{"back (or paused) in the list", plexWatchPlaying, at(50), false},
		{"absent again: the count restarts", plexWatchAbsent, at(60), false},
		{"absent, 59 s on", plexWatchAbsent, at(119), false},
		{"Plex unreachable", plexWatchUnknown, at(125), false},
		{"absent after the outage: the count restarts", plexWatchAbsent, at(130), false},
		{"Plex unreachable for an hour", plexWatchUnknown, at(3700), false},
		{"absent", plexWatchAbsent, at(3710), false},
		{"absent a full grace later", plexWatchAbsent, at(3770), true},
	}
	v := &viewerWatch{grace: time.Minute}
	for _, s := range steps {
		if got := v.left(s.state, s.at); got != s.want {
			t.Fatalf("%s: left = %v, want %v", s.name, got, s.want)
		}
	}
}

func TestViewerLeftGraceIsTwoSessionRefreshesAndAtLeastAMinute(t *testing.T) {
	if got := viewerLeftGrace(config.PlexConfig{}); got != time.Minute {
		t.Fatalf("default grace = %v, want 1m", got)
	}
	if got := viewerLeftGrace(config.PlexConfig{SessionCacheTTL: 45 * time.Second}); got != 90*time.Second {
		t.Fatalf("grace with a 45 s session cache = %v, want 1m30s", got)
	}
}

// A viewer who leaves while their read-ahead is queued behind another burst
// costs nothing more: the absence is counted while it waits, and it stops on
// its first turn without fetching.
func TestBurstChunksStopOnFirstTurnWhenViewerLeftWhileQueued(t *testing.T) {
	const filename = "film.mkv"
	const size = 10 * burstSegSize
	src := newScratchSource(filename, 10, 4)
	store := newDurableStore()

	var g burstGate
	release, _ := g.acquire(context.Background(), burstPlaying)
	go func() {
		waiters(t, &g, 1)
		time.Sleep(60 * time.Millisecond)
		release()
	}()

	watch := &viewerWatch{grace: 50 * time.Millisecond}
	res, err := burstChunks(context.Background(), src, store, store, "Entry", "hash", filename,
		0, size, 4, burstChunk, zerolog.Nop(), burstOpts{
			turn: g.turn(burstPlaying),
			keep: func(bool) error {
				if watch.left(plexWatchAbsent, time.Now()) {
					return errViewerLeft
				}
				return nil
			},
		})
	if !errors.Is(err, errViewerLeft) {
		t.Fatalf("err = %v, want errViewerLeft", err)
	}
	if res.fetched != 0 || src.totalDownloads() != 0 {
		t.Fatalf("fetched %d bytes in %d downloads for a viewer who had left, want none", res.fetched, src.totalDownloads())
	}
	if g.busy {
		t.Fatal("the stopped burst kept the turn")
	}
}

// watchState tells "no session has it" from "can't tell": the Plex gate
// off, Plex unreachable past its grace window, or a session list too old to
// trust is never an absence. A paused session counts as watching though it
// does not open the gate.
func TestPlexWatchStateKeepsUnknownApartFromAbsent(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	cfg := config.Get()
	prevURL, prevMount := cfg.Plex.URL, cfg.Mount.MountPath
	t.Cleanup(func() { cfg.Plex.URL, cfg.Mount.MountPath = prevURL, prevMount })
	cfg.Mount.MountPath = "/mnt"

	m := &Manager{config: cfg}
	c := newPlexSessionChecker(m)
	entry := &storage.Entry{InfoHash: "h", Name: "Film.2016"}
	path := func(name string) string { return filepath.Join(m.GetTorrentMountPath(entry), name) }

	cfg.Plex.URL = ""
	if got := c.watchState(entry, "film.mkv", true); got != plexWatchUnknown {
		t.Fatalf("gate off: state = %v, want unknown", got)
	}

	// A fresh session list, as refresh stores one: no fetch during the test.
	cfg.Plex.URL = "http://plex.invalid"
	files := []plexSessionFile{{path: path("film.mkv")}, {path: path("paused.mkv"), idle: true}}
	c.fetched, c.lastSuccess = time.Now(), time.Now()
	c.resolved = resolvePlexSessionPaths(files)
	c.listed = map[string]struct{}{}
	for _, f := range files {
		c.listed[resolvePlexSessionPath(f.path)] = struct{}{}
	}

	if got := c.watchState(entry, "film.mkv", true); got != plexWatchPlaying {
		t.Fatalf("playing: state = %v, want playing", got)
	}
	if got := c.watchState(entry, "paused.mkv", true); got != plexWatchPlaying {
		t.Fatalf("paused: state = %v, want playing (a paused viewer keeps the read-ahead)", got)
	}
	if c.isPlexWatching(entry, "paused.mkv") {
		t.Fatal("a paused session opened the precache gate")
	}
	if got := c.watchState(entry, "other.mkv", true); got != plexWatchAbsent {
		t.Fatalf("in no session: state = %v, want absent", got)
	}

	// Without a refresh, a list older than plexWatchStaleAfter refreshes
	// says nothing.
	c.lastSuccess = time.Now().Add(-(plexWatchStaleAfter*cfg.Plex.SessionTTL() + time.Second))
	if got := c.watchState(entry, "other.mkv", false); got != plexWatchUnknown {
		t.Fatalf("stale list, no refresh: state = %v, want unknown", got)
	}
	c.lastSuccess = time.Now()
	if got := c.watchState(entry, "other.mkv", false); got != plexWatchAbsent {
		t.Fatalf("fresh list, no refresh: state = %v, want absent", got)
	}

	c.degraded = true
	if got := c.watchState(entry, "other.mkv", true); got != plexWatchUnknown {
		t.Fatalf("Plex unreachable: state = %v, want unknown", got)
	}
}

// Without the Plex gate nothing says whether anyone is watching, so the
// read-ahead's check never stops it.
func TestStillWatchedNeverStopsWithoutThePlexGate(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	cfg := config.Get()
	prev := cfg.Plex.URL
	t.Cleanup(func() { cfg.Plex.URL = prev })
	cfg.Plex.URL = ""

	p := NewPrecache(&Manager{config: cfg})
	keep := p.stillWatched(&storage.Entry{InfoHash: "h", Name: "Film.2016"}, "film.mkv")
	for _, holding := range []bool{false, true, false, true} {
		if err := keep(holding); err != nil {
			t.Fatalf("keep = %v, want nil with the Plex gate off", err)
		}
	}
}
