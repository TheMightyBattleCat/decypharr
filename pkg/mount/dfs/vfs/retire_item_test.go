package vfs

import (
	"os"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// newIdentityTestItem is newWritableTestItem bound to a grab (InfoHash).
func newIdentityTestItem(t *testing.T, c *Cache, infoHash string) *CacheItem {
	t.Helper()
	item := newWritableTestItem(t, c, "Show.S01E01", "e01.mkv", 1024)
	item.entry = &storage.Entry{InfoHash: infoHash}
	item.entryName = "Show.S01E01"
	return item
}

// Once a same-name re-grab has replaced the grab an item was built for, the
// item must leave service: PeekItem (the pre-cache/PAR2 cache view) stops
// returning it, it leaves the map, and - with no handle open - it is closed.
func TestPeekItemRetiresReplacedGrab(t *testing.T) {
	c := newWritableTestCache(t)
	item := newIdentityTestItem(t, c, "old")
	c.identity = func(string, string) (string, int64, bool) { return "old", 1024, true }
	if got, ok := c.PeekItem("Show.S01E01", "e01.mkv"); !ok || got != item {
		t.Fatal("setup: current item not returned")
	}

	c.identity = func(string, string) (string, int64, bool) { return "new", 1024, true }
	item.identityAt.Store(0) // past the recheck interval
	if _, ok := c.PeekItem("Show.S01E01", "e01.mkv"); ok {
		t.Fatal("PeekItem returned an item for the replaced grab")
	}
	if !item.retired.Load() {
		t.Fatal("replaced item not retired")
	}
	if _, ok := c.items.Load(item.key); ok {
		t.Fatal("retired item still in the map")
	}
	deadline := time.Now().Add(time.Second)
	for !item.isClaimed() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !item.isClaimed() {
		t.Fatal("idle retired item not claimed for close")
	}
}

// A retired item shares its sidecar path with its replacement, so it must
// not write metadata any more.
func TestRetiredItemWritesNoMetadata(t *testing.T) {
	c := newWritableTestCache(t)
	item := newIdentityTestItem(t, c, "old")
	item.retired.Store(true)
	item.flushMetadata(true)
	if _, err := os.Stat(item.metaPath); !os.IsNotExist(err) {
		t.Fatalf("retired item wrote its sidecar: stat err=%v", err)
	}
}

// An item still open when retired is closed by its last Release.
func TestRetiredItemClosesOnLastRelease(t *testing.T) {
	c := newWritableTestCache(t)
	item := newIdentityTestItem(t, c, "old")
	if !item.Open() {
		t.Fatal("setup: Open failed")
	}
	c.retireItem(item)
	if item.isClaimed() {
		t.Fatal("retired item closed while a handle still held it")
	}
	item.Release()
	if !item.isClaimed() {
		t.Fatal("retired item not closed on its last release")
	}
}

// A handle opened on an entry that has since been retired must release that
// entry, not the replacement now mapped under the same name.
func TestReleaseFileReleasesTheHandlesOwnEntry(t *testing.T) {
	c := newWritableTestCache(t)
	m := &Manager{cache: c, files: xsync.NewMap[string, *fileEntry]()}
	key := buildFileKey("Show.S01E01", "e01.mkv")

	old := &fileEntry{}
	old.refCount.Store(1)
	replacement := &fileEntry{}
	replacement.refCount.Store(1)
	m.retireEntry(key, old)
	m.files.Store(key, replacement)

	info := manager.NewFileInfoForTest("Show.S01E01", "e01.mkv", 1024)
	m.ReleaseFile(info, &StreamingFile{entry: old})

	if got := replacement.refCount.Load(); got != 1 {
		t.Fatalf("replacement refCount = %d after releasing the old handle, want 1", got)
	}
	if cur, ok := m.files.Load(key); !ok || cur != replacement {
		t.Fatal("releasing the old handle unmapped the replacement")
	}
}
