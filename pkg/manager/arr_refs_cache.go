package manager

// arrRefsCache caches the result of Repair.BuildArrReferencedSet, the
// exported form of buildArrReferencedSet used only by the overlay file list
// API (pkg/server/api_overlay.go). That endpoint uses the reference set only
// to tell a current overlay record apart from an orphaned one for display
// purposes, and overlayFileIsOrphan(nil, ...) already treats a nil set as
// "skip the filter" - so a cold or failed lookup is not an error case for
// this path, it's the same degraded-but-correct behaviour as no filter at
// all.
//
// get NEVER blocks the overlay path on the Arr fan-out. It always returns
// whatever is currently cached - nil if nothing has ever succeeded - and,
// when the cache is stale or cold, kicks a background refresh (deduplicated
// through singleflight) without waiting for it. If that refresh succeeds, the
// next call to get sees the populated set; if it fails, the cache keeps
// whatever it had (nil, if it never had anything) and simply tries again
// after a short interval rather than retrying every single request.
//
// The sweep (repair_sweep.go), supersession cleanup, and stale-NZB cleanup
// all call the UNEXPORTED buildArrReferencedSet directly instead, never
// through this cache: those decide whether to delete or re-grab something
// right now, so a stale or missing reference set could make them act on data
// that's already been superseded. They must always see a fresh, uncached
// fan-out across every configured Arr.

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	arrRefsCacheTTL       = 60 * time.Second
	arrRefsRefreshTimeout = 5 * time.Second

	// arrRefsFailedRetryInterval is the minimum time between background
	// refresh attempts when the cache is cold or the last attempt failed.
	// Without this gate, every overlay request arriving while the Arrs are
	// down would spawn (a deduplicated, but still repeatedly-scheduled) new
	// refresh the instant the in-flight one finishes. Reusing
	// arrRefsRefreshTimeout as this interval keeps behaviour simple: at most
	// one build attempt in flight or freshly failed per timeout window.
	arrRefsFailedRetryInterval = arrRefsRefreshTimeout
)

// arrRefsCache holds the most recently built Arr reference set for the
// overlay path. A stale or cold value is served immediately - never blocking
// the caller - while a refresh runs in the background, so a slow or
// unreachable Arr never makes an overlay request wait on a full fan-out.
type arrRefsCache struct {
	mu          sync.Mutex
	value       map[string]map[string]string
	cachedAt    time.Time
	hasValue    bool
	lastAttempt time.Time
	sf          singleflight.Group
}

func newArrRefsCache() *arrRefsCache {
	return &arrRefsCache{}
}

// get returns the current cached Arr reference set immediately, without ever
// blocking on build. When the cache is stale (older than arrRefsCacheTTL) or
// has never successfully populated, and no refresh attempt is already
// recent, it spawns a background refresh and returns the current value
// (possibly nil) right away; a later call picks up the refreshed value once
// it lands.
//
// build always runs under a bounded, detached context
// (arrRefsRefreshTimeout), independent of any request context, so a slow Arr
// can't hold a refresh open indefinitely and a cancelled request can't cut a
// shared refresh short for everyone else. Concurrent refreshes collapse into
// a single in-flight fan-out via singleflight.
//
// On a build error the prior value (nil if there wasn't one yet) is kept -
// a failed refresh never overwrites good cached state - and the attempt is
// recorded so repeated cold requests don't each spawn their own refresh.
//
// c == nil only happens in bare-Repair{} test literals, never in production
// code paths, and has no state to cache against; that path keeps the old
// behaviour of one bounded, blocking build per call.
func (c *arrRefsCache) get(build func(ctx context.Context) (map[string]map[string]string, error)) map[string]map[string]string {
	if c == nil {
		ctx, cancel := context.WithTimeout(context.Background(), arrRefsRefreshTimeout)
		defer cancel()
		v, err := build(ctx)
		if err != nil {
			return nil
		}
		return v
	}

	c.mu.Lock()
	value := c.value
	hasValue := c.hasValue
	cachedAt := c.cachedAt
	lastAttempt := c.lastAttempt

	fresh := hasValue && time.Since(cachedAt) < arrRefsCacheTTL
	shouldRefresh := !fresh && time.Since(lastAttempt) >= arrRefsFailedRetryInterval
	if shouldRefresh {
		c.lastAttempt = time.Now()
	}
	c.mu.Unlock()

	if fresh {
		return value
	}

	if shouldRefresh {
		go func() {
			_, _, _ = c.sf.Do("arr-refs", func() (any, error) {
				ctx, cancel := context.WithTimeout(context.Background(), arrRefsRefreshTimeout)
				defer cancel()
				v, err := build(ctx)
				if err != nil {
					return nil, err
				}
				c.mu.Lock()
				c.value = v
				c.cachedAt = time.Now()
				c.hasValue = true
				c.mu.Unlock()
				return v, nil
			})
		}()
	}

	return value
}
