package manager

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
)

// arrLibraryMapFileName is where decypharr remembers, alongside config.json,
// where each downloaded file was linked into the library - keyed by both the
// symlink path decypharr wrote and the mount path it points at - so a later
// Sonarr/Radarr delete or upgrade webhook, which only reports the post-import
// renamed library file name, can still be traced back to the entry and file
// it came from.
const arrLibraryMapFileName = "arr_library_paths.json"

// arrLibraryMapEntry is what a recorded path resolves back to.
type arrLibraryMapEntry struct {
	EntryName string `json:"entry_name"`
	FileName  string `json:"file_name"`
}

// arrLibraryMap is a small persisted path -> entry/file index. It is
// populated when decypharr creates a symlink for a file, and consulted by the
// Arr webhook cleanup path when the notification's own downloadId/filename
// match fails. Methods are nil-safe (a nil *arrLibraryMap behaves as an
// always-empty, no-op map) so tests that build a Manager by hand without one
// don't need to care about it.
type arrLibraryMap struct {
	mu     sync.RWMutex
	path   string
	byPath map[string]arrLibraryMapEntry
	logger zerolog.Logger
}

func newArrLibraryMap(logger zerolog.Logger) *arrLibraryMap {
	a := &arrLibraryMap{
		path:   filepath.Join(config.GetMainPath(), arrLibraryMapFileName),
		byPath: make(map[string]arrLibraryMapEntry),
		logger: logger,
	}
	a.load()
	return a
}

func (a *arrLibraryMap) load() {
	data, err := os.ReadFile(a.path)
	if err != nil {
		return
	}
	var byPath map[string]arrLibraryMapEntry
	if err := json.Unmarshal(data, &byPath); err != nil {
		a.logger.Warn().Err(err).Msg("Arr webhook: failed to parse persisted library path map, starting empty")
		return
	}
	a.mu.Lock()
	a.byPath = byPath
	a.mu.Unlock()
}

// saveLocked writes the current map out atomically (tmp file + rename), the
// same style used for the usenet bandwidth ledger. Caller must hold a.mu.
func (a *arrLibraryMap) saveLocked() {
	data, err := json.MarshalIndent(a.byPath, "", "  ")
	if err != nil {
		return
	}
	tmp := a.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		a.logger.Warn().Err(err).Msg("Arr webhook: failed to persist library path map")
		return
	}
	_ = os.Rename(tmp, a.path)
}

// record stores every non-empty path in paths as pointing at (entryName,
// fileName) - called once per file at symlink-creation time with both the
// symlink path decypharr wrote and the mount path it targets.
func (a *arrLibraryMap) record(entryName, fileName string, paths ...string) {
	if a == nil || entryName == "" || fileName == "" {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	changed := false
	for _, p := range paths {
		if p == "" {
			continue
		}
		a.byPath[filepath.Clean(p)] = arrLibraryMapEntry{EntryName: entryName, FileName: fileName}
		changed = true
	}
	if changed {
		a.saveLocked()
	}
}

// lookup returns the entry/file recorded for an exact path.
func (a *arrLibraryMap) lookup(p string) (arrLibraryMapEntry, bool) {
	if a == nil || p == "" {
		return arrLibraryMapEntry{}, false
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	e, ok := a.byPath[filepath.Clean(p)]
	return e, ok
}

// lookupBasename returns every distinct entry/file recorded under a path
// whose basename matches name - the caller decides what to do with more than
// one distinct result (ambiguous vs. a single unanimous candidate).
func (a *arrLibraryMap) lookupBasename(name string) []arrLibraryMapEntry {
	if a == nil || name == "" {
		return nil
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	seen := make(map[arrLibraryMapEntry]struct{})
	var matches []arrLibraryMapEntry
	for p, e := range a.byPath {
		if filepath.Base(p) != name {
			continue
		}
		if _, ok := seen[e]; ok {
			continue
		}
		seen[e] = struct{}{}
		matches = append(matches, e)
	}
	return matches
}

// removeEntry drops every recorded path belonging to entryName - called when
// the entry itself is deleted, so the map can't grow forever.
func (a *arrLibraryMap) removeEntry(entryName string) {
	if a == nil || entryName == "" {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	changed := false
	for p, e := range a.byPath {
		if e.EntryName == entryName {
			delete(a.byPath, p)
			changed = true
		}
	}
	if changed {
		a.saveLocked()
	}
}
