package usenet

import (
	"testing"
	"time"
)

// TestDeadPostingCacheMarkAndCheck proves the basic negative-cache contract:
// a hash that was never marked is a miss, and one that was marked is a hit
// until it expires.
func TestDeadPostingCacheMarkAndCheck(t *testing.T) {
	c := newDeadPostingCache()
	const hash = "abc123"

	if c.Check(hash) {
		t.Fatal("Check() = true before Mark(), want false")
	}
	c.Mark(hash)
	if !c.Check(hash) {
		t.Fatal("Check() = false right after Mark(), want true")
	}
}

// TestDeadPostingCacheExpires proves the TTL property: a marked hash stops
// being a hit once deadPostingTTL has elapsed, so a genuinely-recovered
// posting (a provider completion lag, not permanent death) isn't
// blackholed forever.
func TestDeadPostingCacheExpires(t *testing.T) {
	c := newDeadPostingCache()
	const hash = "expiring-hash"
	c.entries.Store(hash, time.Now().Add(-(deadPostingTTL + time.Minute)))

	if c.Check(hash) {
		t.Fatal("Check() = true for an entry past deadPostingTTL, want false")
	}
	// The expired entry must also be pruned, not just skipped.
	if _, ok := c.entries.Load(hash); ok {
		t.Error("expired entry was not pruned from the map")
	}
}

// TestDeadPostingCacheNilSafe proves a nil *deadPostingCache (the zero value
// of a struct field that failed to initialize) never panics - Mark/Check
// degrade to no-op/miss instead.
func TestDeadPostingCacheNilSafe(t *testing.T) {
	var c *deadPostingCache
	c.Mark("x") // must not panic
	if c.Check("x") {
		t.Fatal("Check() on a nil cache = true, want false")
	}
}

// TestHashNZBContentStableAndDistinct proves hashNZBContent is the right key
// for a negative cache keyed on posting identity, not nzbID: identical
// content must hash identically (so a re-grab of the same dead release is
// recognized), and different content must hash differently.
func TestHashNZBContentStableAndDistinct(t *testing.T) {
	a := []byte("<nzb><file>same release</file></nzb>")
	b := []byte("<nzb><file>same release</file></nzb>")
	c := []byte("<nzb><file>different release</file></nzb>")

	if hashNZBContent(a) != hashNZBContent(b) {
		t.Error("identical content hashed differently")
	}
	if hashNZBContent(a) == hashNZBContent(c) {
		t.Error("different content hashed identically")
	}
}
