package manager

import (
	"context"
	"sync"

	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

// par2RoundArticles makes each posted-file article a repair round needs one
// Usenet fetch for the whole round, however many slices overlap it and
// however many workers ask for it at once. It is shared by every fetcher of
// the job and lives for one round only (attachPar2RoundArticles ..
// detach): a retry round asks the network again.
//
// Before it, postedFileFetcher kept only the last article it fetched and
// shared nothing in flight. With slices smaller than articles, several
// workers fetched the same article at once (44 BODY commands for 20 distinct
// articles in the PAR2 harness), and an article that failed was asked for
// once per slice it overlaps, each ask with its own retries and timeout (28
// fetches of one broken article; seven parallel 60 s stalls).
//
// What it keeps:
//   - In-flight fetches, keyed by (fetcher, segment index): a second asker
//     waits for the first instead of fetching too. It waits on its own
//     context.
//   - Every error, for the rest of the round. An error whose fetch was cut
//     off by the asker's own context ending is not kept: it says nothing
//     about the article, so the entry is dropped and the next asker fetches.
//   - Fetched article bytes, bounded. The oldest article in the round's read
//     order is dropped first. In-flight slices always sit in a short run of
//     the read order (the slice source's read-ahead window), so an article a
//     slice still needs is never the lowest one. The bound is at most
//     maxCount articles, and no more bytes than the read-ahead window itself
//     holds in slices, but always keeps at least par2RoundArticlesMinKeep.
//
// Keyed by fetcher and segment, not by message ID: identityCheck is per
// segment slot, so bytes checked for one file's slot are never handed to
// another's.
type par2RoundArticles struct {
	mu       sync.Mutex
	entries  map[par2RoundArticleKey]*par2RoundArticle
	bytes    int64 // sum of len(data) over the successful entries kept
	kept     int   // successful entries kept
	maxCount int
	maxBytes int64
	fetchers []*postedFileFetcher
}

// par2RoundArticlesMinKeep is the fewest fetched articles a round keeps
// whatever its byte bound: enough for the articles a window of small slices
// spans, plus a file boundary or a damaged gap inside it.
const par2RoundArticlesMinKeep = 8

type par2RoundArticleKey struct {
	f   *postedFileFetcher
	idx int
}

type par2RoundArticle struct {
	pos  int64         // byte position in the round's read order
	done chan struct{} // closed when data/err/retry are set
	data []byte
	err  error
	// retry: the fetch was cut off by its asker's context ending; the
	// entry has been dropped and a waiter fetches again on its own context.
	retry bool
	// held: a successful fetch whose bytes count toward the round's bound.
	// Guarded by par2RoundArticles.mu.
	held bool
}

// attachPar2RoundArticles starts a round: every fetcher of the job routes its
// Usenet fetches through one shared par2RoundArticles until detach. Call it
// before the round's workers start and detach after they have all ended
// (concurrentSliceSource.Wait), so no worker reads or writes the round's
// entries after it is over. maxConc is the round's worker count, and with
// idx.SliceSize it sets the byte bound.
func attachPar2RoundArticles(idx *par2.Index, fetchers map[[16]byte]*postedFileFetcher, maxConc int) *par2RoundArticles {
	window := 2 * max(1, maxConc) // concurrentSliceSource's read-ahead window, in slices
	r := &par2RoundArticles{
		entries:  make(map[par2RoundArticleKey]*par2RoundArticle),
		maxCount: max(2*window+2, par2RoundArticlesMinKeep),
		maxBytes: int64(window) * idx.SliceSize,
	}
	for fileID, f := range fetchers {
		if f == nil {
			continue
		}
		var base int64
		if b, err := idx.SliceBase(fileID); err == nil {
			base = b * idx.SliceSize
		}
		f.roundBase = base
		f.round.Store(r)
		r.fetchers = append(r.fetchers, f)
	}
	return r
}

// detach ends the round: the fetchers go back to fetching directly, and the
// round's articles and errors are released.
func (r *par2RoundArticles) detach() {
	for _, f := range r.fetchers {
		f.round.CompareAndSwap(r, nil)
	}
	r.mu.Lock()
	r.entries = nil
	r.bytes, r.kept = 0, 0
	r.mu.Unlock()
}

// segmentData returns article idx of f for the round: kept bytes or error,
// a share of a fetch already in flight, or a new fetch.
func (r *par2RoundArticles) segmentData(ctx context.Context, f *postedFileFetcher, idx int) ([]byte, error) {
	key := par2RoundArticleKey{f: f, idx: idx}
	for {
		r.mu.Lock()
		if r.entries == nil { // detached under us: fetch directly
			r.mu.Unlock()
			return f.fetchSegment(ctx, idx)
		}
		e, ok := r.entries[key]
		if !ok {
			e = &par2RoundArticle{pos: f.roundBase + f.base[idx], done: make(chan struct{})}
			r.entries[key] = e
			r.mu.Unlock()
			return r.fill(ctx, f, idx, key, e)
		}
		r.mu.Unlock()
		select {
		case <-e.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if !e.retry {
			return e.data, e.err
		}
		// The fetch ended with its asker's context; ask again on ours.
	}
}

// fill fetches article idx into entry e, which the caller has just added.
func (r *par2RoundArticles) fill(ctx context.Context, f *postedFileFetcher, idx int, key par2RoundArticleKey, e *par2RoundArticle) ([]byte, error) {
	data, err := f.fetchSegment(ctx, idx)
	r.mu.Lock()
	defer r.mu.Unlock()
	defer close(e.done)
	if err != nil && ctx.Err() != nil {
		e.retry = true
		if r.entries != nil && r.entries[key] == e {
			delete(r.entries, key)
		}
		return nil, err
	}
	e.data, e.err = data, err
	if err == nil && r.entries != nil && r.entries[key] == e {
		e.held = true
		r.kept++
		r.bytes += int64(len(data))
		r.evict()
	}
	return data, err
}

// evict drops kept article bytes, lowest read position first, while the
// round is over its bound. Errors and fetches in flight are never dropped.
// Called with r.mu held.
func (r *par2RoundArticles) evict() {
	for r.kept > r.maxCount || (r.bytes > r.maxBytes && r.kept > par2RoundArticlesMinKeep) {
		var lowKey par2RoundArticleKey
		var low *par2RoundArticle
		for k, e := range r.entries {
			if !e.held {
				continue // in flight, or an error
			}
			if low == nil || e.pos < low.pos {
				lowKey, low = k, e
			}
		}
		if low == nil {
			return
		}
		delete(r.entries, lowKey)
		r.kept--
		r.bytes -= int64(len(low.data))
	}
}
