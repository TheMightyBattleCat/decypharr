package usenet

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"sort"
	"strings"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"golang.org/x/net/html/charset"
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

// postingKeyFromNZB derives a deadPostingCache key from the posting the NZB
// describes rather than its bytes: the first article's Message-ID of every
// <file>, sorted and hashed. Another indexer's NZB for the same upload -
// different bytes, same articles - shares it, so a posting proven dead is
// refused however it is re-listed (Under Reef S11E06 came back 4 s after
// its blocklist through another indexer's copy). "" when the XML does not
// decode or lists no articles; the caller falls back to hashNZBContent.
func postingKeyFromNZB(content []byte) string {
	var doc struct {
		Files []struct {
			Segments []struct {
				Number int    `xml:"number,attr"`
				ID     string `xml:",chardata"`
			} `xml:"segments>segment"`
		} `xml:"file"`
	}
	dec := xml.NewDecoder(bytes.NewReader(content))
	dec.CharsetReader = charset.NewReaderLabel
	dec.Strict = false
	if err := dec.Decode(&doc); err != nil {
		return ""
	}
	var ids []string
	for _, f := range doc.Files {
		first, best := "", 0
		for _, s := range f.Segments {
			id := strings.TrimSpace(s.ID)
			if id != "" && (first == "" || s.Number < best) {
				first, best = id, s.Number
			}
		}
		if first != "" {
			ids = append(ids, first)
		}
	}
	if len(ids) == 0 {
		return ""
	}
	sort.Strings(ids)
	sum := sha256.Sum256([]byte(strings.Join(ids, "\n")))
	return "posting:" + hex.EncodeToString(sum[:])
}

// Mark records hash as a confirmed-unavailable posting, starting a fresh
// deadPostingTTL window.
func (c *deadPostingCache) Mark(hash string) {
	if c == nil || hash == "" {
		return
	}
	// Check prunes only the hash it is asked about, so a posting never
	// seen again stayed for the life of the process. Sweep expired entries
	// here; the map holds at most the postings marked within the TTL.
	now := time.Now()
	c.entries.Range(func(k string, at time.Time) bool {
		if now.Sub(at) > deadPostingTTL {
			c.entries.Delete(k)
		}
		return true
	})
	c.entries.Store(hash, now)
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
