package vfs

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/config"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs/ranges"
)

// Manager manages VFS lifecycle
type Manager struct {
	manager *manager.Manager
	cache   *Cache
	logger  zerolog.Logger

	files *xsync.Map[string, *fileEntry]

	ctx    context.Context
	cancel context.CancelFunc

	totalFiles  atomic.Int32
	activeFiles atomic.Int32
}

// fileEntry tracks file metadata
type fileEntry struct {
	item     *CacheItem
	refCount atomic.Int32
	// deleted is set to true by ReleaseFile before the entry is removed from the
	// map. GetFile checks this after incrementing refCount so it can detect and
	// undo a concurrent deletion without holding a coarse lock.
	deleted atomic.Bool
}

// NewManager creates a new VFS manager
func NewManager(ctx context.Context, mgr *manager.Manager, config *config.FuseConfig) (*Manager, error) {
	ctx, cancel := context.WithCancel(ctx)

	cache, err := NewCache(ctx, mgr, config)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create cache: %w", err)
	}

	m := &Manager{
		manager: mgr,
		cache:   cache,
		logger:  logger.New("vfs"),
		files:   xsync.NewMap[string, *fileEntry](),
		ctx:     ctx,
		cancel:  cancel,
	}

	return m, nil
}

func (m *Manager) GetManager() *manager.Manager {
	return m.manager
}

// GetFile returns a streaming file handle
func (m *Manager) GetFile(info *manager.FileInfo) (*StreamingFile, error) {
	key := buildFileKey(info.Parent(), info.Name())

	// NewStreamingFile returns nil when the entry's cache item was claimed for
	// teardown by the cache janitor between handle releases — the fileEntry is
	// then stale and must be retired so a fresh item can be created. The loop
	// is bounded: each retry either succeeds or removes the stale entry it
	// observed, and the janitor's claim/delete pair is near-instantaneous.
	for range 8 {
		// Fast path: existing file.
		// Increment refCount first, then verify the entry wasn't concurrently
		// deleted by ReleaseFile between our Load and the Add. If it was, undo
		// the increment and fall through to create a fresh entry.
		if entry, ok := m.files.Load(key); ok {
			entry.refCount.Add(1)
			// An open is when a player reopens a name a re-grab has
			// replaced, so check the item's grab unthrottled: the old
			// handle may still hold it, and handing it out again streamed
			// the deleted grab.
			current := !entry.item.retired.Load() && m.cache.itemCurrent(entry.item, true)
			if !current {
				m.cache.retireItem(entry.item)
			}
			if current && !entry.deleted.Load() {
				if sf := NewStreamingFile(entry.item); sf != nil {
					sf.entry = entry
					return sf, nil
				}
			}
			entry.refCount.Add(-1)
			m.retireEntry(key, entry)
		}

		// Get or create cache item
		item, err := m.cache.GetItem(info.Parent(), info.Name(), info.Size())
		if err != nil {
			return nil, fmt.Errorf("failed to get cache item: %w", err)
		}

		entry := &fileEntry{item: item}
		entry.refCount.Store(1)

		// Store or return existing
		actual, loaded := m.files.LoadOrStore(key, entry)
		if loaded {
			// Another goroutine created it first
			actual.refCount.Add(1)
			if !actual.deleted.Load() {
				if sf := NewStreamingFile(actual.item); sf != nil {
					sf.entry = actual
					return sf, nil
				}
			}
			actual.refCount.Add(-1)
			m.retireEntry(key, actual)
			continue
		}

		sf := NewStreamingFile(item)
		if sf == nil {
			// Our freshly-fetched item was claimed before we could open it
			// (possible during a forced purge). Retire and retry.
			m.retireEntry(key, entry)
			continue
		}
		sf.entry = entry
		m.totalFiles.Add(1)
		m.activeFiles.Add(1)
		return sf, nil
	}
	return nil, fmt.Errorf("file %s: cache item kept being torn down; giving up", key)
}

// retireEntry marks a stale fileEntry deleted and removes it from the map —
// but only if it is still the mapped entry, so a fresh replacement stored by
// a concurrent GetFile is never clobbered.
func (m *Manager) retireEntry(key string, entry *fileEntry) {
	entry.deleted.Store(true)
	m.files.Compute(key, func(old *fileEntry, loaded bool) (*fileEntry, xsync.ComputeOp) {
		if loaded && old == entry {
			return nil, xsync.DeleteOp
		}
		return old, xsync.CancelOp
	})
}

// ReleaseFile decrements the reference count sf's handle holds. With sf (a
// handle from GetFile) that is the entry it was opened on; by name, a handle
// on a retired entry released the replacement's reference instead. A nil
// sf falls back to the entry the name maps to.
func (m *Manager) ReleaseFile(info *manager.FileInfo, sf *StreamingFile) {
	key := buildFileKey(info.Parent(), info.Name())

	if sf != nil && sf.entry != nil {
		entry := sf.entry
		if entry.refCount.Add(-1) <= 0 {
			entry.deleted.Store(true)
			m.files.Compute(key, func(old *fileEntry, loaded bool) (*fileEntry, xsync.ComputeOp) {
				if loaded && old == entry {
					return nil, xsync.DeleteOp
				}
				return old, xsync.CancelOp
			})
			m.activeFiles.Add(-1)
		}
		return
	}

	if entry, ok := m.files.Load(key); ok {
		if entry.refCount.Add(-1) <= 0 {
			// Mark deleted before removing from the map so that any concurrent
			// GetFile that already loaded this entry can detect the deletion and
			// undo its refCount increment rather than using a stale entry.
			entry.deleted.Store(true)
			m.files.Delete(key)
			m.activeFiles.Add(-1)
			// Downloaders are stopped in CacheItem.Release() when opens reaches 0.
		}
	}
}

// FlushCaches writes every open cache item's in-memory bytes to disk. See
// Cache.FlushAll.
func (m *Manager) FlushCaches() {
	if m.cache != nil {
		m.cache.FlushAll()
	}
}

// Close shuts down the manager
func (m *Manager) Close() error {
	m.cancel()

	// Close all files
	m.files.Range(func(key string, entry *fileEntry) bool {
		if entry.item != nil {
			entry.item.Close()
		}
		return true
	})
	m.files.Clear()

	// Close cache
	if m.cache != nil {
		m.cache.Close()
	}

	return nil
}

// GetStats returns manager statistics
func (m *Manager) GetStats() map[string]any {
	stats := map[string]any{
		"type":         "dfs",
		"ready":        true,
		"enabled":      true,
		"total_files":  m.totalFiles.Load(),
		"active_files": m.activeFiles.Load(),
	}

	// Add cache stats
	if m.cache != nil {
		for k, v := range m.cache.GetStats() {
			stats["cache_"+k] = v
		}
	}

	return stats
}

func (m *Manager) CleanupCache() map[string]any {
	if m.cache == nil {
		return map[string]any{
			"cleanup_status": "unsupported",
			"cleanup_result": "cache is not initialized",
		}
	}
	return m.cache.RunCleanup()
}

func (m *Manager) PurgeCache() map[string]any {
	if m.cache == nil {
		return map[string]any{
			"purge_status": "unsupported",
			"purge_result": "cache is not initialized",
		}
	}
	return m.cache.PurgeCache()
}

// EvictCachedFile removes filename's cached bytes under entryName unless it
// is open - see Cache.EvictFile.
func (m *Manager) EvictCachedFile(entryName, filename string) (int64, bool) {
	if m.cache == nil {
		return 0, false
	}
	return m.cache.EvictFile(entryName, filename)
}

// EntryCacheInUse reports whether any file under entryName is open in the
// cache - see Cache.EntryInUse.
func (m *Manager) EntryCacheInUse(entryName string) bool {
	return m.cache != nil && m.cache.EntryInUse(entryName)
}

// PeekCachedRange reads [off, off+len(p)) of filename's cache item under
// entryName directly from local disk, if already fully cached - see
// Cache.PeekItem / CacheItem.ReadCachedRange. Never creates a cache item or
// triggers a download; false means there is nothing to read from here.
func (m *Manager) PeekCachedRange(entryName, filename string, p []byte, off int64) bool {
	if m.cache == nil {
		return false
	}
	item, ok := m.cache.PeekItem(entryName, filename)
	if !ok {
		return false
	}
	return item.ReadCachedRange(p, off)
}

// HasCachedRange reports whether [off, off+length) of filename's cache item
// under entryName is already cached, without reading or fetching anything:
// the live in-memory item if one is open, else its on-disk metadata sidecar.
func (m *Manager) HasCachedRange(entryName, filename string, off, length int64) bool {
	if m.cache == nil || length <= 0 {
		return false
	}
	if item, ok := m.cache.PeekItem(entryName, filename); ok {
		return item.HasRange(ranges.Range{Pos: off, Size: length})
	}
	return m.cache.DiskHasRange(entryName, filename, off, length)
}

// WriteCachedRange durably writes p at [off, off+len(p)) into filename's
// cache item under entryName - the write-side mirror of PeekCachedRange.
// Creates the cache item (data file + .json metadata sidecar), sized by
// fileSize, if it doesn't already exist. Bytes already present at that
// range are left untouched - see CacheItem.WriteAtNoOverwrite. This is a
// pure disk write: no NNTP fetch, no padding, no Stream, no Downloaders -
// the caller is responsible for only ever handing it bytes it already
// knows are correct.
func (m *Manager) WriteCachedRange(entryName, filename string, fileSize int64, p []byte, off int64) error {
	if m.cache == nil {
		return fmt.Errorf("cache not initialized")
	}
	item, err := m.cache.GetItem(entryName, filename, fileSize)
	if err != nil {
		return fmt.Errorf("get cache item: %w", err)
	}
	_, _, err = item.WriteAtNoOverwrite(p, off)
	return err
}

// ForgetCachedRange drops [off, off+length) from filename's cache item
// under entryName so a later read treats it as missing and re-downloads it
// - see CacheItem.ForgetRange. Routes to the live in-memory item if one is
// open, otherwise rewrites the on-disk metadata sidecar directly (items are
// closed after itemIdleTimeout, so a PAR2 repair completing later usually
// finds nothing open). No-op when there is no VFS cache or the file was
// never cached. Never creates a cache item.
func (m *Manager) ForgetCachedRange(entryName, filename string, off, length int64) {
	if m.cache == nil || length <= 0 {
		return
	}
	if item, ok := m.cache.PeekItem(entryName, filename); ok {
		item.ForgetRange(off, length)
		return
	}
	m.cache.forgetDiskRange(entryName, filename, off, length)
}

// CacheCoverage returns filename's cache coverage under entryName: cached
// bytes against the file's total declared size, plus the item's last write
// time (modTime). Tries the live in-memory item first (Cache.PeekItem /
// CacheItem.Coverage) - the freshest source, current mid-download state
// included - and falls back to the on-disk metadata sidecar
// (Cache.DiskCoverage) for a file cached in a prior run that hasn't been
// reopened yet this process. ok=false means neither source has anything.
func (m *Manager) CacheCoverage(entryName, filename string) (cached, total int64, modTime time.Time, ok bool) {
	if m.cache == nil {
		return 0, 0, time.Time{}, false
	}
	if item, found := m.cache.PeekItem(entryName, filename); found {
		if c, t, mt, ok := item.Coverage(); ok {
			return c, t, mt, true
		}
	}
	return m.cache.DiskCoverage(entryName, filename)
}

func buildFileKey(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "/" + name
}
