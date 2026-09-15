package manager

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// Reap verdict statuses.
const (
	reapReapable = "reapable" // every guard passed
	reapWaiting  = "waiting"  // not yet safe, may become safe (retry later)
	reapSkipped  = "skipped"  // will not reap (terminal for this item)
)

// Reap reason codes, stable for the UI.
const (
	reasonNoStale         = "no_unavailable_version"
	reasonNotMarkedYet    = "plex_not_marked_unavailable_yet"
	reasonTargetNotInItem = "old_file_not_in_item"
	reasonAllUnavailable  = "all_versions_unavailable"
	reasonLiveUnreadable  = "no_live_version_on_disk"
	reasonStaleOnDisk     = "old_file_still_on_disk"
	reasonNoArrOwner      = "no_arr_tracks_title"
	reasonArrReferences   = "arr_still_references_old_file"
	reasonArrNotLive      = "arr_current_file_not_live_in_plex"
	reasonSamePath        = "reimported_at_same_path"
	reasonPlaying         = "playing_now"
	reasonLookupFailed    = "lookup_failed"
)

// reapVerdict is the outcome of evaluating one Plex item.
type reapVerdict struct {
	Status string
	Reason string
	Detail string
	// Stale are the versions that would be removed (only when reapable).
	Stale []plexMedia
}

// reapArrLookup reports whether any Arr tracks the item (owned) and the set
// of file paths the owning Arrs currently hold for it.
type reapArrLookup func() (owned bool, refs map[string]bool, err error)

// reapEnv is what evaluateReap needs from the outside world, injectable for
// tests.
type reapEnv struct {
	lstat   func(string) (os.FileInfo, error)
	stat    func(string) (os.FileInfo, error)
	arr     reapArrLookup
	playing func() (bool, error)
}

func defaultReapEnv(arr reapArrLookup, playing func() (bool, error)) reapEnv {
	return reapEnv{lstat: os.Lstat, stat: os.Stat, arr: arr, playing: playing}
}

func mediaFiles(m plexMedia) []string {
	files := make([]string, 0, len(m.Part))
	for _, p := range m.Part {
		if p.File != "" {
			files = append(files, filepath.Clean(p.File))
		}
	}
	return files
}

func mediaHasAny(m plexMedia, paths map[string]bool) bool {
	for _, f := range mediaFiles(m) {
		if paths[f] {
			return true
		}
	}
	return false
}

// evaluateReap decides whether an item's unavailable versions may be
// removed. targets, when non-empty, restricts the decision to versions whose
// files are among those paths (the old files named by an upgrade, rename or
// re-grab); empty means every unavailable version of the item (backlog).
//
// Every guard must pass, cheapest first, before any Arr or session lookup:
//  1. the version is marked unavailable by Plex (deletedAt) - so a
//     deliberately kept second version (4K beside 1080p) is never touched;
//  2. the item keeps at least one live version, whose files exist on disk
//     (Stat follows the library symlink, so a mount outage fails here);
//  3. every file of the version is gone from disk (Lstat, so a dangling
//     symlink still counts as present) - Plex's delete would otherwise
//     remove the file itself;
//  4. an Arr tracks the title, none of its current files is the old file,
//     and one of its current files is a live version in Plex;
//  5. nobody is playing the item.
func evaluateReap(item *plexItem, targets []string, env reapEnv) reapVerdict {
	targetSet := make(map[string]bool, len(targets))
	for _, t := range targets {
		targetSet[filepath.Clean(t)] = true
	}

	var stale, live []plexMedia
	for _, m := range item.Media {
		if m.DeletedAt > 0 {
			stale = append(stale, m)
		} else {
			live = append(live, m)
		}
	}

	if len(targetSet) > 0 {
		var matched []plexMedia
		for _, m := range stale {
			if mediaHasAny(m, targetSet) {
				matched = append(matched, m)
			}
		}
		if len(matched) == 0 {
			targetLive := false
			for _, m := range live {
				if mediaHasAny(m, targetSet) {
					targetLive = true
					break
				}
			}
			if !targetLive {
				return reapVerdict{Status: reapSkipped, Reason: reasonTargetNotInItem}
			}
			// Plex still shows the old file as live: either it hasn't
			// rescanned yet, or the replacement landed at the same path.
			if env.arr != nil {
				if owned, refs, err := env.arr(); err == nil && owned {
					for t := range targetSet {
						if refs[t] {
							return reapVerdict{Status: reapSkipped, Reason: reasonSamePath, Detail: t}
						}
					}
				}
			}
			return reapVerdict{Status: reapWaiting, Reason: reasonNotMarkedYet}
		}
		stale = matched
	}

	if len(stale) == 0 {
		return reapVerdict{Status: reapSkipped, Reason: reasonNoStale}
	}
	if len(live) == 0 {
		return reapVerdict{Status: reapSkipped, Reason: reasonAllUnavailable}
	}

	liveOnDisk := false
	for _, m := range live {
		files := mediaFiles(m)
		if len(files) == 0 {
			continue
		}
		ok := true
		for _, f := range files {
			if _, err := env.stat(f); err != nil {
				ok = false
				break
			}
		}
		if ok {
			liveOnDisk = true
			break
		}
	}
	if !liveOnDisk {
		return reapVerdict{Status: reapWaiting, Reason: reasonLiveUnreadable}
	}

	for _, m := range stale {
		files := mediaFiles(m)
		if len(files) == 0 {
			return reapVerdict{Status: reapSkipped, Reason: reasonStaleOnDisk, Detail: fmt.Sprintf("version %d has no file path", m.ID)}
		}
		for _, f := range files {
			if _, err := env.lstat(f); err == nil || !errors.Is(err, fs.ErrNotExist) {
				detail := f
				if err != nil {
					detail = f + ": " + err.Error()
				}
				return reapVerdict{Status: reapSkipped, Reason: reasonStaleOnDisk, Detail: detail}
			}
		}
	}

	if env.arr == nil {
		return reapVerdict{Status: reapWaiting, Reason: reasonLookupFailed, Detail: "no Arr lookup"}
	}
	owned, refs, err := env.arr()
	if err != nil {
		return reapVerdict{Status: reapWaiting, Reason: reasonLookupFailed, Detail: err.Error()}
	}
	if !owned {
		return reapVerdict{Status: reapSkipped, Reason: reasonNoArrOwner}
	}
	for _, m := range stale {
		for _, f := range mediaFiles(m) {
			if refs[f] {
				return reapVerdict{Status: reapSkipped, Reason: reasonArrReferences, Detail: f}
			}
		}
	}
	arrLive := false
	for _, m := range live {
		if mediaHasAny(m, refs) {
			arrLive = true
			break
		}
	}
	if !arrLive {
		return reapVerdict{Status: reapWaiting, Reason: reasonArrNotLive}
	}

	if env.playing != nil {
		playing, err := env.playing()
		if err != nil {
			return reapVerdict{Status: reapWaiting, Reason: reasonLookupFailed, Detail: err.Error()}
		}
		if playing {
			return reapVerdict{Status: reapWaiting, Reason: reasonPlaying}
		}
	}

	return reapVerdict{Status: reapReapable, Stale: stale}
}
