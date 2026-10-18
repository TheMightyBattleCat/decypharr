package backup

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// CacheEntry is what the file cache remembers about one file.
type CacheEntry struct {
	Size  int64  `json:"size"`
	MTime int64  `json:"mtime"` // UnixNano
	SHA   string `json:"sha256"`
}

// FileCache maps an absolute path to the size, mtime and hash it had when it
// was last stored, so unchanged files are not read again. It is safe for
// concurrent use.
type FileCache struct {
	mu      sync.Mutex
	entries map[string]CacheEntry
}

type fileCacheDoc struct {
	Version int                   `json:"version"`
	Files   map[string]CacheEntry `json:"files"`
}

// NewFileCache returns an empty cache.
func NewFileCache() *FileCache { return &FileCache{entries: map[string]CacheEntry{}} }

// Len is the number of entries.
func (c *FileCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// Lookup returns the recorded hash when size and mtime both match.
func (c *FileCache) Lookup(path string, size, mtimeNano int64) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[path]
	if !ok || e.Size != size || e.MTime != mtimeNano {
		return "", false
	}
	return e.SHA, true
}

// Put records a file.
func (c *FileCache) Put(path string, size, mtimeNano int64, sha string) {
	c.mu.Lock()
	c.entries[path] = CacheEntry{Size: size, MTime: mtimeNano, SHA: sha}
	c.mu.Unlock()
}

// forget drops the entry for path.
func (c *FileCache) forget(path string) {
	c.mu.Lock()
	delete(c.entries, path)
	c.mu.Unlock()
}

// retainUnder drops every entry below root that is not in seen.
func (c *FileCache) retainUnder(root string, seen map[string]struct{}) {
	prefix := strings.TrimRight(root, string(filepath.Separator)) + string(filepath.Separator)
	c.mu.Lock()
	defer c.mu.Unlock()
	for p := range c.entries {
		if strings.HasPrefix(p, prefix) {
			if _, ok := seen[p]; !ok {
				delete(c.entries, p)
			}
		}
	}
}

func (r *FileRepo) fileCachePath() string {
	return filepath.Join(r.dir, "index", "filecache.json.zst")
}

// LoadFileCache reads the repository's file cache. A missing or unreadable
// cache is not an error: it only costs a full re-hash, so an empty cache is
// returned.
func (r *FileRepo) LoadFileCache() *FileCache {
	c := NewFileCache()
	var doc fileCacheDoc
	if err := readZstdJSON(r.fileCachePath(), &doc); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			_ = os.Remove(r.fileCachePath())
		}
		return c
	}
	if doc.Files != nil {
		c.entries = doc.Files
	}
	return c
}

// SaveFileCache writes the cache atomically.
func (r *FileRepo) SaveFileCache(c *FileCache) error {
	c.mu.Lock()
	doc := fileCacheDoc{Version: 1, Files: make(map[string]CacheEntry, len(c.entries))}
	for k, v := range c.entries {
		doc.Files[k] = v
	}
	c.mu.Unlock()
	return r.writeZstdJSON(r.fileCachePath(), doc)
}
