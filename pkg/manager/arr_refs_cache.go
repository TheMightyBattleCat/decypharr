package manager

// arrRefsCache caches the result of Repair.BuildArrReferencedSet, the
// exported form of buildArrReferencedSet used only by the overlay file list
// API (pkg/server/api_overlay.go). That endpoint rebuilds the Arr reference
// set on every page load just to tell a current overlay record apart from an
// orphaned one for display purposes, so a result that's a few seconds old is
// still correct - it doesn't need to observe an Arr change the instant it
// happens.
//
// The sweep (repair_sweep.go), supersession cleanup, and stale-NZB cleanup
// all call the UNEXPORTED buildArrReferencedSet directly instead, never
// through this cache: those decide whether to delete or re-grab something
// right now, so a stale reference set could make them act on data that's
// already been superseded. They must always see a fresh, uncached fan-out
// across every configured Arr.

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	arrRefsCacheTTL       = 60 * time.Second
	arrRefsRefreshTimeout = 5 * time.Second
)

// arrRefsCache holds the most recently built Arr reference set for the
// overlay path. A stale value is served immediately while a refresh runs in
// the background, so a slow or unreachable Arr never makes an overlay
// request wait on a full fan-out.
type arrRefsCache struct {
	mu       sync.Mutex
	value    map[string]map[string]string
	cachedAt time.Time
	hasValue bool
	sf       singleflight.Group
}

func newArrRefsCache() *arrRefsCache {
	return &arrRefsCache{}
}

// get returns a recent Arr reference set, calling build to produce one when
// the cache is cold or stale. build always runs under a bounded, detached
// context (arrRefsRefreshTimeout), independent of any request context, so a
// slow Arr can't hold a refresh open past that timeout and a cancelled
// request can't cut a shared refresh short for everyone else. Concurrent
// refreshes collapse into a single in-flight fan-out via singleflight.
//
// On a build error the prior value (nil if there wasn't one yet) is kept and
// returned - a failed refresh never overwrites good cached state.
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
	if c.hasValue && time.Since(c.cachedAt) < arrRefsCacheTTL {
		v := c.value
		c.mu.Unlock()
		return v
	}
	prior := c.value
	hadValue := c.hasValue
	c.mu.Unlock()

	refresh := func() (any, error) {
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
	}

	if hadValue {
		// Serve the prior value now; refresh in a deduplicated goroutine so
		// no caller pays for the fan-out.
		go func() {
			_, _, _ = c.sf.Do("arr-refs", refresh)
		}()
		return prior
	}

	res, _, _ := c.sf.Do("arr-refs", refresh)
	v, _ := res.(map[string]map[string]string)
	return v
}
