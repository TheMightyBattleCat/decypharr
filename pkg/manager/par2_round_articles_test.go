package manager

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

// PI-3: a pass asks Usenet for each posted-file article once per round. The
// fixture's slices (16 KiB) are smaller than its articles (100 KiB), so up to
// seven slices overlap one article and eight workers read them at once.
// Before, the fetcher kept only its last article and shared nothing in
// flight: the harness counted 44 BODY commands for 20 distinct articles.
func TestPar2RoundFetchesEachArticleOnce(t *testing.T) {
	servers, fx := newP2eEnv(t, 1)
	srv := servers[0]
	h := newP2eHarness(t, srv, fx, "p2e-round-once")
	f1 := fx.posted[0]
	h.recordDead(p2eSeg{f1, 1}, p2eSeg{f1, 3})
	for _, s := range h.dead {
		srv.set(s.file.ids[s.idx], p2eNotFound)
	}

	o := h.run(nil)
	t.Logf("outcome: %s err=%q tally=%s", o.LogMsg, o.Err, o.Tally)
	if o.LogMsg != "par2 repair completed" || o.Err != "" {
		t.Fatalf("pass ended %q err=%q, want completed", o.LogMsg, o.Err)
	}
	if !containsField(o.Tally, "rounds=1") {
		t.Fatalf("tally %q: the test needs a one-round pass", o.Tally)
	}

	var bad []string
	for _, f := range fx.posted {
		for i, id := range f.ids {
			if f == f1 && (i == 1 || i == 3) {
				continue // recorded dead: rebuilt, never read
			}
			if n := srv.bodyCount(id) - matchFetches(fx, f, i); n != 1 {
				bad = append(bad, fmt.Sprintf("%s#%d=%d", short(f.name), i, n))
			}
		}
	}
	if len(bad) > 0 {
		t.Fatalf("intact articles not fetched exactly once in the round: %v (server BODY total %d)", bad, o.Server.Body)
	}
}

// PI-3: an intact article that STATs alive but whose BODY is gone is asked
// for once in the round, not once per slice that overlaps it. Before, each of
// its slices asked again (S5b: seven 430s; S9a: 28 fetches of a truncating
// article).
func TestPar2RoundAsksAFailedArticleOnce(t *testing.T) {
	servers, fx := newP2eEnv(t, 1)
	srv := servers[0]
	h := newP2eHarness(t, srv, fx, "p2e-round-fail-once")
	f1, f2 := fx.posted[0], fx.posted[1]
	h.recordDead(p2eSeg{f1, 1})
	srv.set(f1.ids[1], p2eNotFound)
	// STAT 223, BODY 430: the pass's STAT sweep sees it alive, the stream
	// finds it gone. Its slices plus f1#1's fit the 17 recovery slices, so
	// a second round repairs everything.
	srv.set(f2.ids[2], p2eStatLie)

	o := h.run(nil)
	t.Logf("outcome: %s err=%q tally=%s", o.LogMsg, o.Err, o.Tally)
	if o.LogMsg != "par2 repair completed" || o.Err != "" {
		t.Fatalf("pass ended %q err=%q, want completed", o.LogMsg, o.Err)
	}
	if !containsField(o.Tally, "rounds=2") {
		t.Fatalf("tally %q: the test needs a two-round pass (found in round 1, rebuilt in round 2)", o.Tally)
	}
	// Round 1 asks once and keeps the 430 for the rest of the round; every
	// slice overlapping it is folded into the damaged set together, so
	// round 2 does not read it at all.
	if n := srv.bodyCount(f2.ids[2]); n != 1 {
		t.Fatalf("BODY for the gone article = %d, want 1 (one ask in round 1)", n)
	}
	// The intact articles: once in each of the two rounds.
	var bad []string
	for _, f := range fx.posted {
		for i, id := range f.ids {
			if (f == f1 && i == 1) || (f == f2 && i == 2) {
				continue
			}
			if n := srv.bodyCount(id) - matchFetches(fx, f, i); n != 2 {
				bad = append(bad, fmt.Sprintf("%s#%d=%d", short(f.name), i, n))
			}
		}
	}
	if len(bad) > 0 {
		t.Fatalf("intact articles not fetched once per round (want 2 each): %v", bad)
	}
}

// matchFetches is how many times the pass fetches article i of f before
// any round: part1 and part2 share a length, so matching them to their PAR2
// FileDescs reads the first 16 KiB of each (computeMD5_16k).
func matchFetches(fx *p2eFixture, f *p2eFile, i int) int64 {
	if i == 0 && (f == fx.posted[0] || f == fx.posted[1]) {
		return 1
	}
	return 0
}

func containsField(tally, field string) bool {
	return slices.Contains(strings.Fields(tally), field)
}

// roundTestFetcher builds a fetcher of n 100-byte articles over fetch, with
// a round attached the way runRepair attaches one.
func roundTestFetcher(t *testing.T, n int, maxConc int, fetch postedFetchFunc) (*postedFileFetcher, *par2RoundArticles) {
	t.Helper()
	ref := storage.PostedFileRef{Name: "f.mkv", Size: int64(100 * n)}
	for i := 0; i < n; i++ {
		ref.Segments = append(ref.Segments, storage.Par2SegmentRef{MessageID: fmt.Sprintf("a%d", i), Bytes: 100, Real: true})
	}
	f := newPostedFileFetcher(context.Background(), fetch, ref, nil, 0, zerolog.Nop())
	r := attachPar2RoundArticles(&par2.Index{SliceSize: 16}, map[[16]byte]*postedFileFetcher{{1}: f}, maxConc)
	t.Cleanup(r.detach)
	return f, r
}

// Concurrent askers of one article share its fetch; an error is kept for
// the round and handed to later askers without a second fetch.
func TestPar2RoundArticlesShareFlightAndKeepErrors(t *testing.T) {
	var calls sync.Map // id -> *atomic.Int64
	release := make(chan struct{})
	gone := error(&nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430, Message: "no such article"})
	fetch := func(ctx context.Context, id string, _ usenet.ArticleCheck) ([]byte, error) {
		v, _ := calls.LoadOrStore(id, new(atomic.Int64))
		v.(*atomic.Int64).Add(1)
		<-release
		if id == "a1" {
			return nil, gone
		}
		return make([]byte, 100), nil
	}
	count := func(id string) int64 {
		if v, ok := calls.Load(id); ok {
			return v.(*atomic.Int64).Load()
		}
		return 0
	}
	f, _ := roundTestFetcher(t, 4, 8, fetch)

	var wg sync.WaitGroup
	errs := make([]error, 16)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = f.segmentData(context.Background(), i%2) // a0 and a1
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	for i, err := range errs {
		if i%2 == 0 && err != nil {
			t.Fatalf("asker %d of a0: %v", i, err)
		}
		if i%2 == 1 && !errors.Is(err, gone) {
			t.Fatalf("asker %d of a1: err %v, want the article's 430", i, err)
		}
	}
	// Later asks in the same round: kept bytes for a0 (also the last-fetched
	// one), the kept error for a1.
	if _, err := f.segmentData(context.Background(), 1); !errors.Is(err, gone) {
		t.Fatalf("later ask of a1: %v, want the kept 430", err)
	}
	if count("a0") != 1 || count("a1") != 1 {
		t.Fatalf("fetches: a0=%d a1=%d, want 1 each", count("a0"), count("a1"))
	}
}

// An asker whose own context ends stops waiting at once. A fetch cut off by
// its asker's context is not kept: the next asker fetches again. A fetch that
// timed out on its own (the asker's context still live) is kept.
func TestPar2RoundArticlesContextRules(t *testing.T) {
	var calls atomic.Int64
	block := make(chan struct{})
	fetch := func(ctx context.Context, id string, _ usenet.ArticleCheck) ([]byte, error) {
		calls.Add(1)
		switch id {
		case "a0": // blocks until its context ends
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-block:
				return make([]byte, 100), nil
			}
		case "a2": // the per-article deadline, not the asker's
			return nil, context.DeadlineExceeded
		}
		return make([]byte, 100), nil
	}
	f, _ := roundTestFetcher(t, 4, 8, fetch)

	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	ownerDone := make(chan error, 1)
	go func() {
		_, err := f.segmentData(ownerCtx, 0)
		ownerDone <- err
	}()
	waitCalls(t, &calls, 1)
	// A waiter whose own context ends returns its own error promptly.
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancelWait()
	start := time.Now()
	if _, err := f.segmentData(waitCtx, 0); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter: %v, want its own deadline", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("waiter returned after %s, want at its own deadline", d)
	}
	// A live waiter joins, then the owner's context ends: the waiter fetches
	// again on its own context instead of taking the owner's cancel.
	liveDone := make(chan error, 1)
	go func() {
		_, err := f.segmentData(context.Background(), 0)
		liveDone <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancelOwner()
	if err := <-ownerDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("owner: %v, want canceled", err)
	}
	// The live waiter fetches again on its own context; a waiter that
	// took the owner's cancel instead has already returned it.
	select {
	case err := <-liveDone:
		t.Fatalf("live waiter returned %v without fetching again: it took the owner's cancel", err)
	case <-waitCallsCh(&calls, 2):
	case <-time.After(5 * time.Second):
		t.Fatal("live waiter did not fetch again within 5 s")
	}
	close(block)
	if err := <-liveDone; err != nil {
		t.Fatalf("live waiter after the owner's cancel: %v, want its own successful fetch", err)
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("a0 fetched %d times, want 2 (owner cut off, live waiter again)", n)
	}

	// A per-article timeout with the asker's context live is kept.
	before := calls.Load()
	for range 3 {
		if _, err := f.segmentData(context.Background(), 2); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("a2: %v, want its timeout", err)
		}
	}
	if n := calls.Load() - before; n != 1 {
		t.Fatalf("a2 fetched %d times in the round, want 1", n)
	}
}

// waitCalls waits up to 5 s for calls to reach n.
func waitCalls(t *testing.T, calls *atomic.Int64, n int64) {
	t.Helper()
	select {
	case <-waitCallsCh(calls, n):
	case <-time.After(5 * time.Second):
		t.Fatalf("fetch calls = %d after 5 s, want %d", calls.Load(), n)
	}
}

// waitCallsCh is closed once calls reaches n.
func waitCallsCh(calls *atomic.Int64, n int64) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		deadline := time.Now().Add(5 * time.Second)
		for calls.Load() < n && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if calls.Load() >= n {
			close(ch)
		}
	}()
	return ch
}

// A round's kept bytes are dropped lowest read position first once over
// the bound; errors stay. After detach the fetcher asks Usenet directly
// again, so a retry round re-asks a failed article.
func TestPar2RoundArticlesBoundAndDetach(t *testing.T) {
	calls := map[string]int{}
	var mu sync.Mutex
	fetch := func(ctx context.Context, id string, _ usenet.ArticleCheck) ([]byte, error) {
		mu.Lock()
		calls[id]++
		mu.Unlock()
		if id == "a0" {
			return nil, &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430, Message: "no such article"}
		}
		return make([]byte, 100), nil
	}
	const n = 40
	f, r := roundTestFetcher(t, n, 4, fetch) // window 8: at most 18 articles and 8*16 bytes, so the minimum of 8 is kept
	for i := 0; i < n; i++ {
		_, _ = f.segmentData(context.Background(), i)
	}
	r.mu.Lock()
	kept, entries := r.kept, len(r.entries)
	var lowest int64 = -1
	for k, e := range r.entries {
		if e.held && (lowest < 0 || int64(k.idx) < lowest) {
			lowest = int64(k.idx)
		}
	}
	_, errKept := r.entries[par2RoundArticleKey{f: f, idx: 0}]
	r.mu.Unlock()
	if kept != par2RoundArticlesMinKeep || entries != kept+1 || !errKept {
		t.Fatalf("kept=%d entries=%d errKept=%v, want %d kept + the a0 error", kept, entries, errKept, par2RoundArticlesMinKeep)
	}
	if lowest != n-par2RoundArticlesMinKeep {
		t.Fatalf("lowest kept article = %d, want %d (the highest ones kept)", lowest, n-par2RoundArticlesMinKeep)
	}

	// One connection: the count bound (2*window+2 = 6) is below the
	// minimum, and the minimum wins.
	g, rg := roundTestFetcher(t, n, 1, fetch)
	for i := 1; i < n; i++ {
		_, _ = g.segmentData(context.Background(), i)
	}
	rg.mu.Lock()
	keptOne := rg.kept
	rg.mu.Unlock()
	if keptOne != par2RoundArticlesMinKeep {
		t.Fatalf("one connection: kept=%d, want %d", keptOne, par2RoundArticlesMinKeep)
	}

	r.detach()
	if f.round.Load() != nil {
		t.Fatal("fetcher still attached after detach")
	}
	_, _ = f.segmentData(context.Background(), 0)
	mu.Lock()
	defer mu.Unlock()
	if calls["a0"] != 2 {
		t.Fatalf("a0 fetched %d times, want 2 (once in the round, once after it)", calls["a0"])
	}
}
