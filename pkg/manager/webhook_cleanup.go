package manager

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// arrWebhookFreshWindow guards against tearing down the wrong entry on an
// upgrade: the replacement download is already in decypharr by the time the
// old file's delete webhook arrives (seconds later), so any candidate that's
// still downloading or was only just added is never a valid match - it's the
// new download, not the one being superseded.
const arrWebhookFreshWindow = 5 * time.Minute

// ArrWebhookEvent is the subset of a Sonarr/Radarr webhook payload the
// cleanup orchestration needs. Decoding the Arr's actual notification shape
// (which differs slightly between Sonarr and Radarr) happens in
// pkg/server/webhook.go; this stays a plain, Arr-agnostic value.
type ArrWebhookEvent struct {
	// DownloadID is the downloadId field from the payload, if present - the
	// same value decypharr reported as this entry's hash/nzo_id via the
	// qBittorrent- and SABnzbd-emulation APIs (both report entry.InfoHash).
	DownloadID string
	// DeleteReason is "upgrade", "manual", etc. Logged only; an upgrade's
	// file-delete is handled by the exact same teardown as any other delete.
	DeleteReason string
	// FileName is the basename of the deleted/upgraded file (from the
	// payload's episodeFile/movieFile relativePath or path), used as a
	// fallback match when DownloadID is absent or doesn't resolve.
	FileName string
	// LibraryPath is the full path Arr reported for the file (episodeFile/
	// movieFile's path, falling back to relativePath) - this is the
	// post-import, renamed library location, checked against the paths
	// recorded when decypharr symlinked the file in.
	LibraryPath string
	// SceneName is the payload's episodeFile/movieFile sceneName - the
	// original release name, which survives Arr's import rename and so still
	// matches the entry/file names decypharr stored the download under.
	SceneName string
}

// HandleArrWebhookCleanup tears down the usenet download a Sonarr/Radarr
// file-delete or file-upgrade webhook refers to, immediately instead of
// waiting for the next scheduled sweep to notice. This is purely an
// optional, event-driven fast path: the scheduled superseded/stale cleanup
// remains fully responsible for teardown regardless of whether any webhook
// is configured, and both must be safe to run in either order - calling
// this for an entry the sweep already removed is a normal, quiet outcome
// ("no-match" / "already-gone"), never an error surfaced to the caller.
//
// Usenet only: an entry that resolves to a torrent/debrid download is left
// completely untouched ("torrent-skipped"), checked before any deletion
// begins - never mid-teardown.
//
// action is one of: "no-match", "already-gone", "torrent-skipped",
// "file-removed", "entry-deleted", "cache-kept-twin". matchedBy is
// "downloadId", "filename", "symlink_path", "symlink_basename",
// "scene_name", or "" (when action is "no-match").
//
// Known gaps this does not fix (pre-existing, shared with every other
// deletion path in decypharr, not introduced or worsened here):
//   - storage's per-Name merged entry-item record is updated with an
//     unlocked Get-then-Put - no lock spans the read and the write, so two
//     deletes of same-named entries racing (this webhook firing at the same
//     moment as a concurrent scheduled sweep, say) can lose an update.
//   - no code path checks whether the file being torn down is actively
//     being streamed before removing it.
func (m *Manager) HandleArrWebhookCleanup(ev ArrWebhookEvent) (action string, entryName string, matchedBy string) {
	entry, fileName, matchedBy := m.resolveArrWebhookEntry(ev)
	if entry == nil {
		return "no-match", "", ""
	}
	entryName = entry.Name

	// Usenet-only guard, applied before anything is touched, regardless of
	// which tier resolved the entry - a torrent/debrid download is always
	// left completely alone.
	if !entry.IsNZB() {
		return "torrent-skipped", entryName, matchedBy
	}

	infoHash := entry.InfoHash

	if fileName != "" {
		if err := m.RemoveTorrentFile(entryName, fileName); err != nil {
			// RemoveTorrentFile errors when the entry or file is already
			// gone - a concurrent sweep, or a duplicate webhook delivery.
			// Idempotent, not a failure.
			return "already-gone", entryName, matchedBy
		}
	} else {
		// No file identified in the payload - nothing to selectively
		// remove, so the whole download is what's being cleaned up.
		if err := m.DeleteEntry(infoHash, true); err != nil {
			return "already-gone", entryName, matchedBy
		}
	}

	// RemoveTorrentFile only fully deletes the entry once every file in it
	// is gone (a season pack with other live episodes survives with just
	// the one file marked deleted) - check which happened before touching
	// the stored NZB or cache dir, both keyed to the whole download rather
	// than the one file.
	if exists, _ := m.storage.Exists(infoHash); exists {
		return "file-removed", entryName, matchedBy
	}

	if m.finishUsenetTeardown(infoHash, entryName) {
		return "entry-deleted", entryName, matchedBy
	}
	return "cache-kept-twin", entryName, matchedBy
}

// arrWebhookMatch is the outcome of one matching tier: no candidate found at
// all, exactly one (usable), or two-or-more - which must never be guessed
// between and always ends the search rather than falling through to a
// weaker tier.
type arrWebhookMatch int

const (
	arrWebhookNoCandidate arrWebhookMatch = iota
	arrWebhookMatched
	arrWebhookAmbiguous
)

// resolveArrWebhookEntry finds the entry (and, for tiers 2-4, the entry's own
// internal file name for the file the event refers to - the payload's own
// FileName is the renamed library name, which won't exist in the entry's
// Files map) a webhook event refers to, trying each tier in turn and
// stopping at the first hit:
//  1. downloadId, then the event's filename against every usenet entry's
//     files (both unchanged from before the library-path/scene-name tiers
//     below existed).
//  2. the reported library path against the paths recorded when decypharr
//     symlinked the file in.
//  3. that same reported path's basename against those same recorded paths.
//  4. the reported sceneName (the original release name, which survives
//     Arr's import rename) against entry and file names.
//
// Tiers 2-4 never resolve to an entry that is still downloading or was only
// just added - on an upgrade, the replacement is already in decypharr by the
// time the old file's delete webhook arrives, and it must never be mistaken
// for the entry being superseded.
func (m *Manager) resolveArrWebhookEntry(ev ArrWebhookEvent) (entry *storage.Entry, fileName string, matchedBy string) {
	if ev.DownloadID != "" {
		if entry, err := m.GetEntry(ev.DownloadID); err == nil && entry != nil {
			return entry, ev.FileName, "downloadId"
		}
	}
	if ev.FileName != "" {
		if entry, err := m.findUsenetEntryByFileName(ev.FileName); err == nil && entry != nil {
			return entry, ev.FileName, "filename"
		}
	}

	if ev.LibraryPath != "" {
		if entry, fileName, result := m.resolveByRecordedPath(ev.LibraryPath, false); result == arrWebhookMatched {
			return entry, fileName, "symlink_path"
		} else if result == arrWebhookAmbiguous {
			return nil, "", ""
		}

		if entry, fileName, result := m.resolveByRecordedPath(ev.LibraryPath, true); result == arrWebhookMatched {
			return entry, fileName, "symlink_basename"
		} else if result == arrWebhookAmbiguous {
			return nil, "", ""
		}
	}

	if ev.SceneName != "" {
		if entry, fileName, result := m.resolveBySceneName(ev.SceneName); result == arrWebhookMatched {
			return entry, fileName, "scene_name"
		}
	}

	return nil, "", ""
}

// isFreshOrDownloading reports whether entry is too new/active to be a valid
// webhook-cleanup target (see arrWebhookFreshWindow), logging which of the
// two guards tripped.
func (m *Manager) isFreshOrDownloading(entry *storage.Entry) bool {
	if entry.IsDownloading {
		m.logger.Debug().Str("entry", entry.Name).Str("infohash", entry.InfoHash).
			Msg("Arr webhook: skipping candidate that is still downloading")
		return true
	}
	if !entry.CreatedAt.IsZero() && time.Since(entry.CreatedAt) < arrWebhookFreshWindow {
		m.logger.Debug().Str("entry", entry.Name).Str("infohash", entry.InfoHash).
			Dur("age", time.Since(entry.CreatedAt)).
			Msg("Arr webhook: skipping candidate added too recently to be the file being deleted")
		return true
	}
	return false
}

// resolveByRecordedPath looks libraryPath up in the symlink-creation-time
// path map, either exactly or (byBasename) by basename, and resolves the
// recorded entry/file back to a full *storage.Entry plus the entry's own
// name for that file.
func (m *Manager) resolveByRecordedPath(libraryPath string, byBasename bool) (*storage.Entry, string, arrWebhookMatch) {
	var candidates []arrLibraryMapEntry
	if byBasename {
		candidates = m.arrLibraryMap.lookupBasename(filepath.Base(libraryPath))
	} else if rec, ok := m.arrLibraryMap.lookup(libraryPath); ok {
		candidates = []arrLibraryMapEntry{rec}
	}

	var match *storage.Entry
	var matchFile string
	for _, c := range candidates {
		entry, err := m.GetEntryByName(c.EntryName, c.FileName)
		if err != nil || entry == nil || m.isFreshOrDownloading(entry) {
			continue
		}
		if match != nil && match.InfoHash != entry.InfoHash {
			return nil, "", arrWebhookAmbiguous
		}
		match = entry
		matchFile = c.FileName
	}
	if match == nil {
		return nil, "", arrWebhookNoCandidate
	}
	return match, matchFile, arrWebhookMatched
}

// resolveBySceneName searches every entry (any protocol - the usenet-only
// guard in HandleArrWebhookCleanup is what actually protects torrent/debrid
// downloads, uniformly across every tier) for one whose name, or one of whose
// file names, equals sceneName case-insensitively, with or without an
// extension - the original release name Arr reports survives its own import
// rename, so this still matches entries added before the symlink-path
// mapping existed. Returns the entry's own name for the matched file (never
// the reported sceneName itself, which won't be a key in the entry's Files
// map).
func (m *Manager) resolveBySceneName(sceneName string) (*storage.Entry, string, arrWebhookMatch) {
	normExt := strings.ToLower(sceneName)
	normNoExt := strings.ToLower(utils.RemoveExtension(sceneName))

	var hashes []string
	_ = m.storage.ForEachMeta(func(meta *storage.EntryMetaInfo) error {
		hashes = append(hashes, meta.InfoHash)
		return nil
	})

	var match *storage.Entry
	var matchFile string
	for _, hash := range hashes {
		entry, err := m.storage.Get(hash)
		if err != nil || entry == nil {
			continue
		}
		fileName, ok := sceneNameMatchedFile(normExt, normNoExt, entry)
		if !ok || m.isFreshOrDownloading(entry) {
			continue
		}
		if match != nil && match.InfoHash != entry.InfoHash {
			return nil, "", arrWebhookAmbiguous
		}
		match = entry
		matchFile = fileName
	}
	if match == nil {
		return nil, "", arrWebhookNoCandidate
	}
	return match, matchFile, arrWebhookMatched
}

// sceneNameMatchedFile checks a scene name (already lowercased, both with and
// without its extension) against an entry's file names first - the common
// case, since a stored file's own name is usually the release name Arr
// preserves as sceneName. Only when no specific file matches does it fall
// back to the entry's own name, and only when that entry has exactly one
// active file left, so a season pack with several live episodes is never
// resolved to an arbitrary one of them.
func sceneNameMatchedFile(normExt, normNoExt string, entry *storage.Entry) (string, bool) {
	for fileName := range entry.Files {
		if namesMatch(normExt, normNoExt, fileName) {
			return fileName, true
		}
	}
	if namesMatch(normExt, normNoExt, entry.Name) {
		if active := entry.GetActiveFiles(); len(active) == 1 {
			return active[0].Name, true
		}
	}
	return "", false
}

func namesMatch(normExt, normNoExt, candidate string) bool {
	candidateExt := strings.ToLower(candidate)
	candidateNoExt := strings.ToLower(utils.RemoveExtension(candidate))
	return candidateExt == normExt || candidateExt == normNoExt ||
		candidateNoExt == normExt || candidateNoExt == normNoExt
}

// findUsenetEntryByFileName searches every NZB-protocol entry for one whose
// Files map contains fileName, returning an error (no match, not a nil
// entry) when zero or more than one entry matches - an ambiguous match must
// never guess which download to tear down.
func (m *Manager) findUsenetEntryByFileName(fileName string) (*storage.Entry, error) {
	if fileName == "" {
		return nil, fmt.Errorf("no filename to match")
	}

	var hashes []string
	_ = m.storage.ForEachMeta(func(meta *storage.EntryMetaInfo) error {
		if meta.Protocol == string(config.ProtocolNZB) {
			hashes = append(hashes, meta.InfoHash)
		}
		return nil
	})

	var match *storage.Entry
	for _, hash := range hashes {
		entry, err := m.storage.Get(hash)
		if err != nil || entry == nil {
			continue
		}
		if _, ok := entry.Files[fileName]; ok {
			if match != nil {
				return nil, fmt.Errorf("ambiguous match: multiple usenet entries contain file %q", fileName)
			}
			match = entry
		}
	}
	if match == nil {
		return nil, fmt.Errorf("no usenet entry contains file %q", fileName)
	}
	return match, nil
}

// finishUsenetTeardown completes cleanup for a usenet entry that's now fully
// gone from the main entries store: removes the stored NZB source/metadata
// and any leftover broken-list record, then removes the DFS cache directory
// if it's safe to. Returns whether the cache directory was actually removed
// (false covers both "nothing to remove" and "left alone for a twin" - see
// removeWebhookCacheDir).
func (m *Manager) finishUsenetTeardown(infoHash, entryName string) bool {
	if m.usenet != nil {
		if err := m.usenet.Delete(infoHash); err != nil {
			m.logger.Warn().Err(err).Str("entry", entryName).Str("infohash", infoHash).
				Msg("Arr webhook: failed to delete stored NZB (may already be gone)")
		}
	}

	if err := m.storage.DeleteEntryHealth(entryName); err != nil {
		m.logger.Debug().Err(err).Str("entry", entryName).
			Msg("Arr webhook: no broken/health record to clear (or already gone)")
	}

	return m.removeWebhookCacheDir(entryName)
}

// removeWebhookCacheDir removes the DFS cache directory for entryName, but
// only when it's safe to: DFS mode must be active with a configured cache
// dir, the resolved path must stay inside it, and - since the cache
// directory is keyed by folder Name rather than InfoHash - no other entry
// may still occupy that Name in the merged EntryItem view (a same-name
// duplicate/twin still being served). Reimplemented locally here (rather
// than shared with the equivalent stale-NZB cleanup logic elsewhere) so this
// branch has no dependency on that feature and merges independently.
func (m *Manager) removeWebhookCacheDir(entryName string) bool {
	if entryName == "" {
		return false
	}

	if item, err := m.storage.GetEntryItem(entryName); err == nil && item != nil {
		for _, f := range item.Files {
			if f != nil && !f.Deleted {
				m.logger.Debug().Str("entry", entryName).
					Msg("Arr webhook: cache dir still claimed by another entry sharing this name; leaving it")
				return false
			}
		}
	}

	cfg := config.Get()
	if cfg.Mount.Type != config.MountTypeDFS || cfg.Mount.DFS.CacheDir == "" {
		return false
	}

	base := filepath.Clean(cfg.Mount.DFS.CacheDir)
	resolved := filepath.Clean(filepath.Join(base, entryName))
	if resolved != base && !strings.HasPrefix(resolved, base+string(filepath.Separator)) {
		m.logger.Warn().Str("entry", entryName).Str("resolved", resolved).
			Msg("Arr webhook: refusing a cache path outside the configured cache dir")
		return false
	}

	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return false
	}
	if err := os.RemoveAll(resolved); err != nil {
		m.logger.Warn().Err(err).Str("path", resolved).Msg("Arr webhook: failed to remove cache dir")
		return false
	}
	return true
}
