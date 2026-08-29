package vfs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/buffer"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	fuseconfig "github.com/sirrobot01/decypharr/pkg/mount/dfs/config"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs/ranges"
)

// newWritableTestItem builds a real, disk-backed CacheItem (a genuine
// buffer.Buffer over a temp-dir data file, exactly like Cache.newItem
// constructs) and registers it directly in c.items - bypassing
// Cache.GetItem's create-if-absent path (which needs a resolvable
// storage.Entry via c.manager.GetEntryByName, requiring a full
// *manager.Manager this package cannot construct without an import cycle).
// This exercises GetItem's fast path (already-loaded item) exactly as
// WriteCachedRange takes it once precache has resolved the entry - the
// real call site always has one by construction (see
// Precache.persistCleanRanges).
func newWritableTestItem(t *testing.T, c *Cache, entryName, filename string, fileSize int64) *CacheItem {
	t.Helper()
	entryDir := filepath.Join(c.config.CacheDir, entryName)
	if err := os.MkdirAll(entryDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	dataPath := filepath.Join(entryDir, filename)
	metaPath := dataPath + ".json"

	buf, err := c.pool.NewBuffer(buffer.Config{
		MemorySize: 1 << 20,
		DiskPath:   dataPath,
		TotalSize:  fileSize,
	})
	if err != nil {
		t.Fatalf("NewBuffer: %v", err)
	}

	key := buildCacheKey(entryName, filename)
	log := logger.NewRateLimitedLogger(logger.WithLogger(zerolog.Nop()))
	item := &CacheItem{
		cache:    c,
		key:      key,
		filename: filename,
		buf:      buf,
		metaPath: metaPath,
		info:     ItemInfo{Size: fileSize},
		logger:   log.Rate(key),
	}
	c.items.Store(item.key, item)
	c.itemCount.Add(1)
	return item
}

func newWritableTestCache(t *testing.T) *Cache {
	t.Helper()
	// logger.Default() (reached transitively via logger.NewRateLimitedLogger
	// and buffer.NewPool) reads config.Get(), which os.Exit(1)s if no config
	// path was ever set for this process - point it at a scratch dir so the
	// very first config.Get() in the test binary succeeds.
	config.SetConfigPath(t.TempDir())
	cacheDir := t.TempDir()
	pool := buffer.NewPool(buffer.PoolConfig{Name: "test"})
	t.Cleanup(func() { _ = pool.Close() })
	return &Cache{
		config: &fuseconfig.FuseConfig{CacheDir: cacheDir},
		items:  xsync.NewMap[string, *CacheItem](),
		pool:   pool,
		logger: zerolog.Nop(),
	}
}

// TestWriteCachedRangeProducesDurableEntryReadableByPeek proves bytes
// written through the Manager.WriteCachedRange seam land on disk (data file
// + .json metadata sidecar) and are readable back via PeekCachedRange -
// Step 1's contract: a pure disk write, discoverable exactly like anything
// the live cache-writer path (Downloaders -> WriteAtNoOverwrite) produces.
func TestWriteCachedRangeProducesDurableEntryReadableByPeek(t *testing.T) {
	c := newWritableTestCache(t)
	m := &Manager{cache: c}

	const entryName, filename = "Some.Release", "episode.mkv"
	const fileSize = 1024
	item := newWritableTestItem(t, c, entryName, filename, fileSize)

	payload := []byte("clean-bytes-from-precache")
	if err := m.WriteCachedRange(entryName, filename, fileSize, payload, 100); err != nil {
		t.Fatalf("WriteCachedRange: %v", err)
	}
	// Force a synchronous metadata flush instead of waiting on the periodic
	// ticker, so the .json sidecar assertion below is deterministic.
	item.flushMetadata(true)

	dataPath := filepath.Join(c.config.CacheDir, entryName, filename)
	if _, err := os.Stat(dataPath); err != nil {
		t.Fatalf("expected durable data file at %s: %v", dataPath, err)
	}
	metaPath := dataPath + ".json"
	if _, err := os.Stat(metaPath); err != nil {
		t.Fatalf("expected durable .json sidecar at %s: %v", metaPath, err)
	}

	got := make([]byte, len(payload))
	if !m.PeekCachedRange(entryName, filename, got, 100) {
		t.Fatalf("PeekCachedRange: expected the written range to read back as present")
	}
	if string(got) != string(payload) {
		t.Fatalf("PeekCachedRange returned %q, want %q", got, payload)
	}

	// The range tracker (persisted metadata view) must reflect exactly what
	// was written - not more, not less.
	if !item.HasRange(ranges.Range{Pos: 100, Size: int64(len(payload))}) {
		t.Fatalf("range tracker does not reflect the written range")
	}
	if item.HasRange(ranges.Range{Pos: 0, Size: 50}) {
		t.Fatalf("range tracker reports a range that was never written as present")
	}
}

// TestWriteCachedRangeSkipsAlreadyPresentBytes proves a second write over a
// range that's already durable doesn't clobber it (WriteAtNoOverwrite's
// contract, exercised through the seam).
func TestWriteCachedRangeSkipsAlreadyPresentBytes(t *testing.T) {
	c := newWritableTestCache(t)
	m := &Manager{cache: c}

	const entryName, filename = "Some.Release", "episode.mkv"
	const fileSize = 1024
	newWritableTestItem(t, c, entryName, filename, fileSize)

	first := []byte("aaaaaaaaaa")
	if err := m.WriteCachedRange(entryName, filename, fileSize, first, 0); err != nil {
		t.Fatalf("WriteCachedRange (first): %v", err)
	}
	second := []byte("bbbbbbbbbb")
	if err := m.WriteCachedRange(entryName, filename, fileSize, second, 0); err != nil {
		t.Fatalf("WriteCachedRange (second): %v", err)
	}

	got := make([]byte, len(first))
	if !m.PeekCachedRange(entryName, filename, got, 0) {
		t.Fatalf("PeekCachedRange: expected range to read back as present")
	}
	if string(got) != string(first) {
		t.Fatalf("second write clobbered existing bytes: got %q, want %q (original)", got, first)
	}
}

// TestForgetCachedRangeOpenItem proves ForgetCachedRange drops the range
// from a live in-memory item so a subsequent read treats it as missing -
// and that the range can then be rewritten with different bytes (the stale
// zero-fill a PAR2 repair needs to replace).
func TestForgetCachedRangeOpenItem(t *testing.T) {
	c := newWritableTestCache(t)
	m := &Manager{cache: c}

	const entryName, filename = "Some.Release", "episode.mkv"
	const fileSize = 1024
	item := newWritableTestItem(t, c, entryName, filename, fileSize)

	stale := []byte("\x00\x00\x00\x00\x00\x00\x00\x00")
	if err := m.WriteCachedRange(entryName, filename, fileSize, stale, 200); err != nil {
		t.Fatalf("WriteCachedRange: %v", err)
	}
	if !item.HasRange(ranges.Range{Pos: 200, Size: int64(len(stale))}) {
		t.Fatalf("precondition: range should be present after write")
	}

	m.ForgetCachedRange(entryName, filename, 200, int64(len(stale)))

	if item.HasRange(ranges.Range{Pos: 200, Size: int64(len(stale))}) {
		t.Fatalf("ForgetCachedRange left the range present in the tracker")
	}
	if miss := item.FindMissing(ranges.Range{Pos: 200, Size: int64(len(stale))}); miss.IsEmpty() {
		t.Fatalf("FindMissing should report the forgotten range as missing")
	}
	if m.PeekCachedRange(entryName, filename, make([]byte, len(stale)), 200) {
		t.Fatalf("PeekCachedRange should miss on a forgotten range")
	}

	// The range is now writable again with corrected bytes.
	fixed := []byte("REALDATA!")[:len(stale)]
	if err := m.WriteCachedRange(entryName, filename, fileSize, fixed, 200); err != nil {
		t.Fatalf("WriteCachedRange (rewrite): %v", err)
	}
	got := make([]byte, len(fixed))
	if !m.PeekCachedRange(entryName, filename, got, 200) {
		t.Fatalf("PeekCachedRange: rewritten range should read back present")
	}
	if string(got) != string(fixed) {
		t.Fatalf("rewrite after forget returned %q, want %q", got, fixed)
	}
}

// TestForgetCachedRangeClosedItem proves ForgetCachedRange rewrites the
// on-disk .json sidecar directly when no item is open (the common case: a
// PAR2 repair runs long after the item was closed on itemIdleTimeout), so a
// later reopen does not re-seed the stale range.
func TestForgetCachedRangeClosedItem(t *testing.T) {
	c := newWritableTestCache(t)
	m := &Manager{cache: c}

	const entryName, filename = "Some.Release", "episode.mkv"
	const fileSize = 1024
	item := newWritableTestItem(t, c, entryName, filename, fileSize)

	stale := make([]byte, 16)
	if err := m.WriteCachedRange(entryName, filename, fileSize, stale, 300); err != nil {
		t.Fatalf("WriteCachedRange: %v", err)
	}
	item.flushMetadata(true)

	// Simulate the janitor closing the idle item.
	c.items.Delete(buildCacheKey(entryName, filename))

	m.ForgetCachedRange(entryName, filename, 300, int64(len(stale)))

	metaPath := filepath.Join(c.config.CacheDir, entryName, filename+".json")
	f, err := os.Open(metaPath)
	if err != nil {
		t.Fatalf("open sidecar: %v", err)
	}
	defer f.Close()
	var info ItemInfo
	if err := json.NewDecoder(f).Decode(&info); err != nil {
		t.Fatalf("decode sidecar: %v", err)
	}
	if info.Rs.Present(ranges.Range{Pos: 300, Size: int64(len(stale))}) {
		t.Fatalf("on-disk sidecar still claims the forgotten range as present: %+v", info.Rs)
	}
}
