package usenet

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
)

// deadPostingTTL bounds how long a confirmed-damaged NZB's content hash
// stays in deadPostingCache. Short-lived deliberately: this exists to
// suppress an Arr hammering the identical re-grab of a genuinely dead
// posting every 60s (the observed FLUX/ETHEL/Kitsune/RAWR cycling), not to
// permanently blacklist content that might recover (a provider completion
// lag, a propagation gap) - an hour is long enough to break the hammering
// loop without outliving a transient cause.
const deadPostingTTL = time.Hour

// deadPostingCache is a short-lived negative cache keyed on NZB content
// identity (see hashNZBContent), not nzbID - a re-grab of the same dead
// release gets a fresh nzbID every time, but hashes identically, since the
// NZB's posted articles (the actual thing that's dead) don't change. A hit
// means "this exact NZB content was already confirmed unavailable within
// the last hour" - the caller should reject it immediately without
// re-parsing or re-probing the same dead articles again.
type deadPostingCache struct {
	entries *xsync.Map[string, time.Time]
}

func newDeadPostingCache() *deadPostingCache {
	return &deadPostingCache{entries: xsync.NewMap[string, time.Time]()}
}

// hashNZBContent derives deadPostingCache's key from raw NZB bytes: the
// identity of "what was posted", independent of nzbID (fresh per grab),
// filename, or category.
func hashNZBContent(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

// Mark records hash as a confirmed-unavailable posting, starting a fresh
// deadPostingTTL window.
func (c *deadPostingCache) Mark(hash string) {
	if c == nil || hash == "" {
		return
	}
	c.entries.Store(hash, time.Now())
}

// Check reports whether hash was marked dead within deadPostingTTL. An
// expired entry is treated as a miss and pruned.
func (c *deadPostingCache) Check(hash string) bool {
	if c == nil || hash == "" {
		return false
	}
	recordedAt, ok := c.entries.Load(hash)
	if !ok {
		return false
	}
	if time.Since(recordedAt) > deadPostingTTL {
		c.entries.Delete(hash)
		return false
	}
	return true
}
