package manager

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

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
// "downloadId", "filename", or "" (when action is "no-match").
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
	entry, matchedBy := m.resolveArrWebhookEntry(ev)
	if entry == nil {
		return "no-match", "", ""
	}
	entryName = entry.Name

	// Usenet-only guard, applied before anything is touched. A torrent/debrid
	// entry resolved by downloadId or (impossible, since the filename search
	// below only looks at NZB entries, but checked anyway for safety) by
	// filename is left completely alone.
	if !entry.IsNZB() {
		return "torrent-skipped", entryName, matchedBy
	}

	infoHash := entry.InfoHash

	if ev.FileName != "" {
		if err := m.RemoveTorrentFile(entryName, ev.FileName); err != nil {
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

// resolveArrWebhookEntry finds the entry a webhook event refers to. downloadId
// is tried first (direct, unambiguous); a payload without one, or one that no
// longer resolves (already cleaned up, or stale), falls back to matching the
// event's filename against every usenet entry's files - deliberately scoped
// to NZB entries only, so a torrent's file can never be matched by name.
func (m *Manager) resolveArrWebhookEntry(ev ArrWebhookEvent) (*storage.Entry, string) {
	if ev.DownloadID != "" {
		if entry, err := m.GetEntry(ev.DownloadID); err == nil && entry != nil {
			return entry, "downloadId"
		}
	}
	if ev.FileName != "" {
		if entry, err := m.findUsenetEntryByFileName(ev.FileName); err == nil && entry != nil {
			return entry, "filename"
		}
	}
	return nil, ""
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
