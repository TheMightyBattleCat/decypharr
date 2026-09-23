package reader

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// forceMissingMessageIDs is a one-time (per process), test-only set of
// message IDs the fetch layer treats as permanently missing (as if every
// provider had returned 430), without ever attempting a real NNTP fetch.
// Populated once at startup from DECYPHARR_FORCE_MISSING_SEGMENTS
// (comma-separated message IDs); empty/unset in production, so this is a
// no-op there. Lets E2E tests exercise padding/PAR2 repair deterministically
// instead of depending on an actually-broken article somewhere upstream.
var forceMissingMessageIDs = sync.OnceValue(func() map[string]struct{} {
	raw := os.Getenv("DECYPHARR_FORCE_MISSING_SEGMENTS")
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	set := make(map[string]struct{})
	for _, id := range strings.Split(raw, ",") {
		id = strings.TrimSpace(id)
		if id != "" {
			set[id] = struct{}{}
		}
	}
	return set
})

func isForcedMissing(messageID string) bool {
	set := forceMissingMessageIDs()
	if len(set) == 0 {
		return false
	}
	_, ok := set[messageID]
	return ok
}

// maxDownloadTimeoutStreak is how many consecutive DownloadTimeout expiries on
// the SAME segment are tolerated before it is treated as unfetchable and handed
// to the confirmed-missing path (pad + queue a repair), exactly as a 430 is.
//
// Why this exists: an article that times out instead of returning 430 was
// previously retried forever. doFetch's cancel/deadline branch calls
// ReleaseFetching, which resets the slot to Empty WITHOUT MarkFailed, so the
// segment was never classified, never padded and never queued for repair - the
// next read simply started over. Observed live on Grant S04E10 segment 139
// (~100MB): reads spanning it blocked until DFS's 90s no-progress watchdog
// killed them, and because a stall is transient by design the downloader's
// error budget was wiped every 2 minutes. The file looped for 55 minutes and
// could never reach repair, while every neighbouring offset read in under 1.5s.
//
// How fast this escalates, for the DFS playback path: within one DFS stream
// attempt, fetchWithRetry's first try hits the 60s DownloadTimeout while the
// caller is still alive (so it counts), and its second try is cut short by the
// 90s no-progress watchdog (so it does not). That is exactly ONE increment per
// DFS stream attempt, and DFS makes 3 of those per `download error` cycle - so
// a wedged segment escalates inside the FIRST cycle, roughly 4.5 minutes in,
// instead of looping for the better part of an hour.
//
// 3 is deliberately conservative: a merely slow segment gets three full
// DownloadTimeout windows to land, and any success clears the streak.
const maxDownloadTimeoutStreak = 3

// noteDownloadTimeout records one DownloadTimeout expiry for segIdx and returns
// the resulting consecutive count.
func (sf *SegmentFetcher) noteDownloadTimeout(segIdx int) int {
	sf.timeoutStreakMu.Lock()
	defer sf.timeoutStreakMu.Unlock()
	sf.timeoutStreak[segIdx]++
	return sf.timeoutStreak[segIdx]
}

// clearDownloadTimeout forgets any timeout streak for segIdx. Called on every
// successful download so a segment that is merely slow (or briefly unreachable)
// never accumulates its way to a permanent verdict.
func (sf *SegmentFetcher) clearDownloadTimeout(segIdx int) {
	sf.timeoutStreakMu.Lock()
	defer sf.timeoutStreakMu.Unlock()
	if len(sf.timeoutStreak) == 0 {
		return
	}
	delete(sf.timeoutStreak, segIdx)
}

// SegmentFetcher handles downloading segments from NNTP with deduplication and retry.
//
// Key features:
//   - Request deduplication: Only one goroutine fetches a segment at a time
//   - Semaphore for connection limiting
//   - Background prefetch queue for read-ahead
//   - Streams directly to disk via cache's StreamWriter
type SegmentFetcher struct {
	client *nntp.Client
	cache  *SegmentCache
	config Config
	logger zerolog.Logger
	stats  *ReaderStats

	// Concurrency control
	semaphore chan struct{} // Limits concurrent downloads

	// Request deduplication
	inFlight   map[int]*fetchPromise
	inFlightMu sync.Mutex

	// Consecutive whole-download-timeout failures per segment. A segment that
	// keeps exhausting DownloadTimeout without ever returning a 430 is
	// unfetchable in practice but classified transient, so it would otherwise
	// be retried forever - see noteDownloadTimeout.
	timeoutStreak   map[int]int
	timeoutStreakMu sync.Mutex

	// Committed articles whose yEnc part number matched their segment number,
	// counted up to partNumberTrust - see articleMismatch.
	partNumbersMatched atomic.Int32

	// Background prefetch
	prefetchCh     chan int
	prefetchQueued []atomic.Uint64 // one deduplication bit per segment
	prefetchWg     sync.WaitGroup

	// patched has one bit per segment the overlay holds a PAR2 patch for:
	// seeded when the fetcher is built, set by MarkPatched when a repair
	// lands on this live reader. doFetch serves those from the patch
	// without asking any provider - see servePatch.
	patched []atomic.Uint64

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
}

// fetchPromise allows multiple goroutines to wait for the same segment download.
type fetchPromise struct {
	done chan struct{}
	err  error
}

// NewSegmentFetcher creates a new segment fetcher.
func NewSegmentFetcher(
	ctx context.Context,
	client *nntp.Client,
	cache *SegmentCache,
	config Config,
	stats *ReaderStats,
	logger zerolog.Logger,
) *SegmentFetcher {
	ctx, cancel := context.WithCancel(ctx)

	maxConns := config.MaxConnections
	if maxConns < 1 {
		maxConns = 8
	}

	// An ffprobe verification read's background prefetch (FetchRangeWindowed)
	// runs at VerificationConnections wide. Give it its OWN slots on top of
	// maxConns rather than sharing: the prefetch keeps its workers busy
	// continuously, so a shared semaphore would starve the foreground
	// EnsureSegmentsConcurrent (which needs slots for the exact segments the
	// read is blocked on right now) behind the prefetch's far-ahead fetches -
	// observed live as multi-second foreground stalls. numPrefetchWorkers and
	// EnsureSegmentsConcurrent still bound themselves by maxConns.
	semCap := maxConns
	if config.VerificationConnections > 0 {
		semCap = maxConns + config.VerificationConnections
	}

	sf := &SegmentFetcher{
		client:     client,
		cache:      cache,
		config:     config,
		logger:     logger.With().Str("component", "fetcher").Logger(),
		stats:      stats,
		semaphore:     make(chan struct{}, semCap),
		inFlight:      make(map[int]*fetchPromise),
		timeoutStreak: make(map[int]int),
		prefetchCh:    make(chan int, 256), // Buffer for prefetch hints
		// A packed atomic bitmap keeps duplicate suppression cheap even for
		// very large NZBs: 100k segments consume about 12 KiB, versus roughly
		// 400 KiB for one atomic.Bool per segment.
		prefetchQueued: make([]atomic.Uint64, (cache.SegmentCount()+63)/64),
		patched:        make([]atomic.Uint64, (cache.SegmentCount()+63)/64),
		ctx:            ctx,
		cancel:         cancel,
	}
	for _, idx := range config.Overlay.PatchedSegments(config.OverlayFile) {
		sf.MarkPatched(idx)
	}

	// Start fewer prefetch workers than foreground connection slots. Seeky
	// callers such as ffprobe can jump to the tail while head read-ahead is
	// still running; reserving at least one slot prevents background prefetch
	// from starving the blocking read that the caller is waiting on.
	numPrefetchWorkers := maxConns - 1
	if numPrefetchWorkers > 0 {
		for i := range numPrefetchWorkers {
			sf.prefetchWg.Add(1)
			go sf.prefetchWorker(i)
		}
	}

	return sf
}

// Fetch downloads a segment synchronously, with deduplication.
// Multiple goroutines calling Fetch for the same segment will share the download.
func (sf *SegmentFetcher) Fetch(ctx context.Context, segIdx int) error {
	// Fast path: already cached, or wait out an in-progress eviction so we
	// don't dedup/fetch against a segment whose disk range is mid-punch.
	for {
		state := sf.cache.GetState(segIdx)
		if state == StateEvicting {
			if err := sf.cache.WaitForEvictionRelease(ctx, segIdx); err != nil {
				return err
			}
			continue // slot is Empty now; re-evaluate
		}
		switch state {
		case StateOnDisk:
			return nil
		case StateFailed:
			return sf.cache.GetError(segIdx)
		}
		break
	}

	// Check if someone else is already fetching
	sf.inFlightMu.Lock()
	if promise, ok := sf.inFlight[segIdx]; ok {
		sf.inFlightMu.Unlock()
		// Wait for existing fetch
		select {
		case <-promise.done:
			return promise.err
		case <-ctx.Done():
			return ctx.Err()
		case <-sf.ctx.Done():
			return sf.ctx.Err()
		}
	}

	// We're the first - create promise
	promise := &fetchPromise{done: make(chan struct{})}
	sf.inFlight[segIdx] = promise
	sf.inFlightMu.Unlock()

	// Actually fetch
	err := sf.doFetch(ctx, segIdx)
	promise.err = err
	close(promise.done)

	// Cleanup
	sf.inFlightMu.Lock()
	delete(sf.inFlight, segIdx)
	sf.inFlightMu.Unlock()

	return err
}

// doFetch performs the actual NNTP download.
func (sf *SegmentFetcher) doFetch(ctx context.Context, segIdx int) error {
	seg := sf.cache.GetSegment(segIdx)
	if seg == nil {
		return ErrSegmentNotFound
	}

	// Try to mark as fetching (atomic transition Empty -> Fetching)
	if !sf.cache.MarkFetching(segIdx) {
		// Someone else is fetching or it's already cached
		state := sf.cache.GetState(segIdx)
		switch state {
		case StateOnDisk:
			return nil
		case StateFailed:
			return sf.cache.GetError(segIdx)
		case StateFetching:
			// Wait for the other fetcher
			return sf.cache.WaitForSegment(ctx, segIdx)
		case StateEvicting:
			// An evictor grabbed the slot between Fetch's check and here.
			// Wait for the punch to finish, then retry the fetch into the
			// released range.
			if err := sf.cache.WaitForEvictionRelease(ctx, segIdx); err != nil {
				return err
			}
			return sf.doFetch(ctx, segIdx)
		}
	}

	if sf.servePatch(segIdx) {
		sf.stats.Downloads.Add(1)
		return nil
	}

	// Acquire connection slot
	select {
	case sf.semaphore <- struct{}{}:
		defer func() { <-sf.semaphore }()
	case <-ctx.Done():
		sf.cache.ReleaseFetching(segIdx)
		return ctx.Err()
	case <-sf.ctx.Done():
		sf.cache.ReleaseFetching(segIdx)
		return sf.ctx.Err()
	}

	messageID := seg.MessageID
	timeout := sf.config.DownloadTimeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}

	downloadCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Test hook (DECYPHARR_FORCE_MISSING_SEGMENTS): treat this article as
	// permanently missing without ever touching the network, so E2E tests can
	// exercise padding/PAR2 repair deterministically. Synthesizes exactly the
	// error a real all-providers 430 would produce, so it goes through the
	// identical handling below.
	var err error
	if isForcedMissing(messageID) {
		err = &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Message: "forced missing (DECYPHARR_FORCE_MISSING_SEGMENTS)"}
	} else {
		// ExecuteWithFailover already retries per provider and across providers —
		// a single call is sufficient.  An outer retry loop would multiply the
		// total attempts by retries×providers, leading to very long failure times.
		err = sf.client.ExecuteWithFailover(downloadCtx, func(conn *nntp.Connection) error {
			stopCancel := context.AfterFunc(downloadCtx, func() {
				_ = conn.Close()
			})
			defer stopCancel()

			// Get the segment writer for the disk cache.
			writer := sf.cache.StreamWriter(segIdx)
			if writer == nil {
				return ErrCacheClosed
			}

			// Stream the decoded body into the chosen tier.
			n, meta, err := conn.StreamBodyMeta(messageID, writer)
			if err != nil {
				writer.Discard()
				if ctxErr := downloadCtx.Err(); ctxErr != nil {
					return ctxErr
				}
				return err
			}
			if ctxErr := downloadCtx.Err(); ctxErr != nil {
				writer.Discard()
				return ctxErr
			}

			// Treat zero-byte articles as missing — the article exists on the
			// server but its body is empty/corrupted after yEnc decoding.
			if n == 0 {
				writer.Discard()
				return &nntp.Error{
					Type:    nntp.ErrorTypeArticleNotFound,
					Message: "article produced no data after decoding",
				}
			}

			// A different upload's article under the same Message-ID decodes
			// cleanly. Refuse it before it is committed, as a 430 from this
			// provider, so failover asks the next one; if none has the right
			// part it is confirmed missing like any other dead article.
			if reason := articleMismatch(meta, seg, sf.partNumbersMatched.Load() >= partNumberTrust); reason != "" {
				writer.Discard()
				sf.logger.Debug().
					Str("component", "fetcher").
					Int("segment", segIdx).
					Str("reason", reason).
					Msg("Provider returned a different upload's article; trying the next provider")
				return &nntp.Error{
					Type:    nntp.ErrorTypeArticleNotFound,
					Message: "article belongs to a different upload: " + reason,
				}
			}

			// Commit (updates cache state to StateOnDisk).
			writer.Finalize()
			if meta != nil && meta.Part > 0 && meta.Part == int64(seg.Number) && sf.partNumbersMatched.Load() < partNumberTrust {
				sf.partNumbersMatched.Add(1)
			}

			return nil
		})
	}

	if err != nil {
		sf.stats.DownloadErrors.Add(1)

		// A fetch that ran out of time says something about the article; a
		// caller that walked away (DFS's 90s no-progress watchdog, a closed
		// player) says nothing, so only the former is counted - hence the
		// caller/fetcher guards below. Once the same segment has burned
		// maxDownloadTimeoutStreak consecutive windows it is unfetchable in
		// practice, even though no provider ever said 430, so convert it into
		// the confirmed-missing error and let the identical handling below pad
		// it and queue a repair.
		//
		// Without this, the branch underneath calls ReleaseFetching, which
		// resets the slot to Empty WITHOUT MarkFailed: the segment is never
		// classified, never padded, never queued for repair, and the next read
		// starts over. That is what let one article wedge a whole file for
		// 55 minutes (Grant S04E10) while other offsets read in under 1.5s.
		callerDone := ctx.Err() != nil
		fetcherDone := sf.ctx.Err() != nil
		isCtxDeadline := errors.Is(err, context.DeadlineExceeded)
		isNNTPTimeout := nntp.IsTimeoutError(err)

		// "The fetch ran out of time" arrives in TWO shapes and the first cut
		// only matched one of them:
		//   - context.DeadlineExceeded, when DownloadTimeout expires on an
		//     article that keeps trickling just under the NNTP idle deadline;
		//   - *nntp.Error{ErrorTypeTimeout}, when the connection goes fully
		//     idle and StreamBodyTimeout fires first.
		// Both mean the same thing about the article, so both must count.
		if (isCtxDeadline || isNNTPTimeout) && !callerDone && !fetcherDone {
			if streak := sf.noteDownloadTimeout(segIdx); streak >= maxDownloadTimeoutStreak {
				shape := "nntp_idle_timeout"
				if isCtxDeadline {
					shape = "download_timeout"
				}
				sf.logger.Warn().
					Int("segment", segIdx).
					Int("timeouts", streak).
					Dur("download_timeout", timeout).
					Str("shape", shape).
					Msg("segment repeatedly timed out; treating as unfetchable")
				err = &nntp.Error{
					Type: nntp.ErrorTypeArticleNotFound,
					Message: fmt.Sprintf(
						"article unfetchable: fetch timed out on %d consecutive attempts (%s)", streak, shape),
				}
				// The verdict is made and the slot ends up OnDisk (padded) or
				// Failed either way, so stop tracking this segment rather than
				// retaining an entry per timed-out segment for the life of the
				// fetcher. A later re-fetch starts counting from scratch.
				sf.clearDownloadTimeout(segIdx)
			}
		}

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			sf.cache.ReleaseFetching(segIdx)
			return err
		}

		// The article is confirmed gone across every provider (or the test
		// hook says so). Before giving up, check whether the overlay already
		// has real bytes for it (PAR2 already repaired this segment in a
		// prior pass) or whether the padding policy allows serving zeros for
		// it instead of failing the read. Both paths write the segment's
		// bytes straight into the cache via the exact same Put the rest of
		// the fetch path uses, so nothing downstream (reader, downloaders.go
		// circuit breaker, escalation) needs to know padding/patching ever
		// happened — the segment is just OnDisk.
		//
		// Skipped entirely when ctx is marked no-pad (an internal-token
		// ffprobe verification read, see ContextWithoutPadding): that read
		// must observe the real failure so a broken import/sweep candidate
		// can never look healthy by way of the padding that makes it
		// playable.
		if sf.config.Overlay != nil && nntp.IsArticleNotFoundError(err) && !paddingDisabled(ctx) {
			// An entry under a sweep probe is treated exactly like a
			// verification read: no padding, so the real 430 propagates up,
			// ffprobe sees the corruption and the sweep re-grabs. Scoped to
			// this one entry - every other entry keeps full padding here.
			if sf.config.Overlay.IsSweepActive() {
				// Mechanism A: latch the entry's sweep deadSeen flag so this
				// run's probeFile can short-circuit to a broken verdict
				// without waiting on (or being fooled by) ffprobe. This branch
				// is the playback-read path - a viewer hitting the same dead
				// article while the sweep probes the entry.
				sf.config.Overlay.MarkSweepDead()
				sf.logger.Debug().
					Str("component", "fetcher").
					Str("entry", sf.config.Overlay.NzbID()).
					Str("file", sf.config.OverlayFile).
					Int("segment", segIdx).
					Msg("segment not padded: entry under sweep probe")
			} else if sf.handleConfirmedMissing(ctx, segIdx, messageID, nntp.DescribeOutcomes(nntp.FailoverOutcomes(err))) {
				// The slot now holds patch/pad bytes and is OnDisk, so the
				// streak that may have brought us here is spent.
				sf.clearDownloadTimeout(segIdx)
				sf.stats.Downloads.Add(1)
				return nil
			}
		}

		// The above branch was skipped because this is a verification read
		// observing a genuine confirmed-dead segment - the case that was
		// previously invisible in logs (see the "verification read" comment
		// above). DEBUG, not INFO: the sweep's own ffprobe-failure log
		// already captures the downstream effect at INFO.
		if paddingDisabled(ctx) && nntp.IsArticleNotFoundError(err) {
			sf.logger.Debug().Str("component", "fetcher").Int("segment", segIdx).Msg("segment dead during verification read")
			// Mechanism B: trip the dead-segment signal the ffprobe checker
			// attached to this read's context (via the WebDAV handler), so it
			// can override an otherwise-healthy ffprobe verdict to broken.
			if sig := DeadSignalFromContext(ctx); sig != nil {
				sig.Trip()
			}
			// Mechanism A belt-and-suspenders: if this verification read is
			// itself the sweep probe, latch the entry's deadSeen flag too -
			// covers the case where ffprobe's own read hit the dead range.
			sf.config.Overlay.MarkSweepDead()
		}

		sf.cache.MarkFailed(segIdx, err)
		return err
	}

	// A real download landed: the segment is not the wedged kind, so forget any
	// timeout streak it built up. Only CONSECUTIVE timeouts escalate.
	sf.clearDownloadTimeout(segIdx)
	sf.stats.Downloads.Add(1)
	return nil
}

// providers is the per-provider outcome summary for the fetch that failed,
// recorded so a pad can be explained afterwards.
//
// handleConfirmedMissing consults the overlay for a segment whose article
// fetch has permanently failed across every provider. Returns true if it
// wrote replacement bytes into the cache (patch or pad), in which case the
// caller treats the fetch as a success; false means the overlay declined
// (FAIL verdict, or an I/O error saving overlay state), or ctx is a
// burst-download read that must not have zero-fill bytes fabricated for it
// (see ContextForBurstDownload) - either way, the original
// article-not-found error should propagate exactly as it did before this
// feature existed.
func (sf *SegmentFetcher) handleConfirmedMissing(ctx context.Context, segIdx int, messageID, providers string) bool {
	overlayHandle := sf.config.Overlay
	file := sf.config.OverlayFile

	if patch, ok := overlayHandle.PatchBytes(file, segIdx); ok {
		if providers != "" {
			sf.logger.Debug().Str("component", "fetcher").Int("segment", segIdx).Str("providers", providers).
				Msg("serving an overlay patch for a segment the providers could not")
		}
		if err := sf.cache.Put(segIdx, patch); err != nil {
			sf.logger.Warn().Err(err).Int("segment", segIdx).Msg("failed to write overlay patch into cache")
			return false
		}
		return true
	}

	logicalLen := sf.cache.SegmentDataSize(segIdx)
	if logicalLen <= 0 {
		return false
	}

	decision, _ := overlayHandle.Decide(file, segIdx, messageID, logicalLen, sf.cache.TotalSize(), sf.cache.SegmentCount())
	if decision != overlay.DecisionPad {
		return false
	}

	// The segment is recorded dead and a repair gets queued regardless of
	// whether this particular read is allowed to fabricate replacement
	// bytes - a burst download still needs its own damage-detection to see
	// this (see Precache.recordReadiness), it just must not receive
	// zero-fill bytes that could be mistaken for genuine cached data.
	overlayHandle.EnqueueRepair()

	if burstNoFill(ctx) {
		return false
	}

	if overlayHandle.ShouldLogPad(file, segIdx) {
		// providers is what each provider actually answered for this
		// article (see nntp.FailoverOutcomes). Without it a pad only says
		// "missing everywhere", which on 2026-09-22 turned out to be wrong
		// for nine Deep in Orbit articles three providers still held, with
		// nothing in the log to show which had been asked.
		sf.logger.Info().
			Str("entry", overlayHandle.NzbID()).
			Str("file", file).
			Int("segment", segIdx).
			Str("providers", providers).
			Msg("segment padded")
	}

	if err := sf.cache.Put(segIdx, make([]byte, logicalLen)); err != nil {
		sf.logger.Warn().Err(err).Int("segment", segIdx).Msg("failed to write zero-fill padding into cache")
		return false
	}
	return true
}

// MarkPatched records that the overlay now holds a PAR2 patch for segIdx.
func (sf *SegmentFetcher) MarkPatched(segIdx int) {
	if segIdx < 0 || segIdx >= sf.cache.SegmentCount() {
		return
	}
	sf.patched[segIdx>>6].Or(uint64(1) << uint(segIdx&63))
}

func (sf *SegmentFetcher) isPatched(segIdx int) bool {
	if segIdx < 0 || segIdx >= sf.cache.SegmentCount() {
		return false
	}
	return sf.patched[segIdx>>6].Load()&(uint64(1)<<uint(segIdx&63)) != 0
}

// servePatch puts segIdx's PAR2 patch in the cache, for a segment marked
// patched, and reports whether it did. A patch is the segment's recovered
// bytes, so it needs no provider: before this, every read of a patched
// segment first asked each provider in turn for the dead article (ten on
// a production install) and only served the patch once all had said 430 - seconds at
// the playhead, including the first read after a repair landed. Anything
// that goes wrong here falls back to the normal fetch, which still ends in
// the patch via handleConfirmedMissing.
func (sf *SegmentFetcher) servePatch(segIdx int) bool {
	if !sf.isPatched(segIdx) {
		return false
	}
	patch, ok := sf.config.Overlay.PatchBytes(sf.config.OverlayFile, segIdx)
	if !ok {
		return false
	}
	if err := sf.cache.Put(segIdx, patch); err != nil {
		sf.logger.Warn().Err(err).Int("segment", segIdx).Msg("failed to write overlay patch into cache")
		return false
	}
	sf.clearDownloadTimeout(segIdx)
	return true
}

func (sf *SegmentFetcher) markPrefetchQueued(segIdx int) bool {
	if segIdx < 0 || segIdx >= sf.cache.SegmentCount() {
		return false
	}
	word := &sf.prefetchQueued[segIdx>>6]
	mask := uint64(1) << uint(segIdx&63)
	for {
		old := word.Load()
		if old&mask != 0 {
			return false
		}
		if word.CompareAndSwap(old, old|mask) {
			return true
		}
	}
}

func (sf *SegmentFetcher) clearPrefetchQueued(segIdx int) {
	if segIdx < 0 || segIdx >= sf.cache.SegmentCount() {
		return
	}
	word := &sf.prefetchQueued[segIdx>>6]
	mask := uint64(1) << uint(segIdx&63)
	word.And(^mask)
}

// QueuePrefetch adds a segment to the background prefetch queue (non-blocking).
func (sf *SegmentFetcher) QueuePrefetch(segIdx int) {
	// Check if already cached
	state := sf.cache.GetState(segIdx)
	if state == StateOnDisk || state == StateFetching {
		return
	}
	// State remains Empty while a hint is waiting in prefetchCh. Track that
	// interval separately so frequent small ReadAt calls cannot enqueue the
	// same read-ahead window hundreds of times and crowd useful hints out.
	if !sf.markPrefetchQueued(segIdx) {
		return
	}

	select {
	case sf.prefetchCh <- segIdx:
		// Queued successfully
	default:
		sf.clearPrefetchQueued(segIdx)
		// Queue full, drop the hint
		sf.stats.PrefetchMisses.Add(1)
	}
}

// QueuePrefetchRange queues multiple segments for prefetch.
func (sf *SegmentFetcher) QueuePrefetchRange(startSeg, endSeg int) {
	for i := startSeg; i <= endSeg; i++ {
		sf.QueuePrefetch(i)
	}
}

// prefetchWorker processes segments from the prefetch queue.
func (sf *SegmentFetcher) prefetchWorker(id int) {
	defer sf.prefetchWg.Done()

	for {
		select {
		case <-sf.ctx.Done():
			return
		case segIdx, ok := <-sf.prefetchCh:
			if !ok {
				return
			}
			sf.prefetchOne(segIdx)
			sf.clearPrefetchQueued(segIdx)
		}
	}
}

// prefetchOne uses the deduplicated, failover-aware single-segment fetch path.
func (sf *SegmentFetcher) prefetchOne(segIdx int) {
	state := sf.cache.GetState(segIdx)
	if state == StateOnDisk {
		sf.stats.PrefetchHits.Add(1)
		return
	}

	fetchCtx, cancel := context.WithTimeout(sf.ctx, sf.config.DownloadTimeout)
	err := sf.Fetch(fetchCtx, segIdx)
	cancel()

	if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		sf.logger.Debug().Err(err).Int("segment", segIdx).Msg("prefetch failed")
	}
}

// EnsureSegments fetches all segments in the range, returning when all are
// available. Segments are fetched in order; in steady-state playback the
// background prefetch workers have already downloaded them, so this loop
// usually just confirms cache presence. fetchWithRetry keeps a single
// transient segment failure from tearing down the whole stream.
func (sf *SegmentFetcher) EnsureSegments(ctx context.Context, startSeg, endSeg int) error {
	for i := startSeg; i <= endSeg; i++ {
		state := sf.cache.GetState(i)
		if state != StateOnDisk {
			if err := sf.fetchWithRetry(ctx, i); err != nil {
				return err
			}
		}
	}
	return nil
}

// EnsureSegmentsConcurrent fetches every missing segment in
// [startSeg, endSeg] using a worker pool capped at MaxConnections,
// returning once all are on disk or the first error is seen. It exists for
// verification reads (padding disabled): readAtPlain queues no prefetch on
// that path, so the serial EnsureSegments would pull one ~750KB article at
// a time with the other connection slots idle.
//
// The per-fetcher semaphore in doFetch still bounds real NNTP connections,
// so the worker count only limits how many fetchWithRetry calls are in
// flight. Workers never cancel each other: a sibling failure must not
// surface as a synthetic context.Canceled that masks a genuine dead-article
// verdict (cf. the padding/PAR2 ctx-cancel-misclassification fixes). Each
// worker honours the caller's ctx; the first non-nil error - a real fetch
// error or the caller's ctx error - is returned, and the rest of the
// window is still probed.
func (sf *SegmentFetcher) EnsureSegmentsConcurrent(ctx context.Context, startSeg, endSeg int) error {
	var needed []int
	for i := startSeg; i <= endSeg; i++ {
		if sf.cache.GetState(i) != StateOnDisk {
			needed = append(needed, i)
		}
	}
	if len(needed) == 0 {
		return nil
	}

	workers := sf.config.MaxConnections
	if workers < 1 {
		workers = 8
	}
	if workers > len(needed) {
		workers = len(needed)
	}

	ch := make(chan int, len(needed))
	for _, idx := range needed {
		ch <- idx
	}
	close(ch)

	var (
		wg       sync.WaitGroup
		errMu    sync.Mutex
		firstErr error
	)
	record := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range ch {
				if err := ctx.Err(); err != nil {
					record(err)
					return
				}
				record(sf.fetchWithRetry(ctx, idx))
			}
		}()
	}
	wg.Wait()

	return firstErr
}

// fetchWithRetry fetches a single segment, retrying transient failures so a
// momentary provider hiccup or stall does not tear down the whole stream.
// Permanent failures (article-not-found) and cancellations return immediately.
func (sf *SegmentFetcher) fetchWithRetry(ctx context.Context, segIdx int) error {
	maxAttempts := sf.config.MaxRetries
	if maxAttempts < 1 {
		maxAttempts = 3
	}

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			// Clear the failed state so the segment can be re-fetched, then
			// back off briefly before retrying. ResetFailed is a CAS: if a
			// concurrent reader fetched the segment meanwhile it stays OnDisk.
			sf.cache.ResetFailed(segIdx)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-sf.ctx.Done():
				return sf.ctx.Err()
			case <-time.After(sf.retryBackoff(attempt)):
			}
		}

		err := sf.Fetch(ctx, segIdx)
		if err == nil {
			return nil
		}
		lastErr = err

		// Don't retry permanent errors or cancellations.
		if nntp.IsArticleNotFoundError(err) || ctx.Err() != nil || sf.ctx.Err() != nil {
			return err
		}
	}
	return lastErr
}

// retryBackoff returns the delay before the given (1-indexed) retry attempt.
func (sf *SegmentFetcher) retryBackoff(attempt int) time.Duration {
	base := sf.config.RetryDelay
	if base <= 0 {
		base = time.Second
	}
	d := base << (attempt - 1)
	if maxDelay := 5 * time.Second; d > maxDelay {
		d = maxDelay
	}
	return d
}

// Close stops all workers and waits for them to finish.
//
// prefetchCh is deliberately never closed: QueuePrefetch can race Close (a
// ReadAtContext already past the reader's closed check), and a send on a
// closed channel panics even inside a select. Workers exit via sf.ctx instead,
// and the channel is garbage-collected with the fetcher.
func (sf *SegmentFetcher) Close() {
	sf.cancel()
	sf.prefetchWg.Wait()
}

// Error types
var (
	ErrSegmentNotFound = &segmentError{msg: "segment not found"}
	ErrCacheClosed     = &segmentError{msg: "cache closed"}
)

type segmentError struct {
	msg string
}

func (e *segmentError) Error() string {
	return e.msg
}
