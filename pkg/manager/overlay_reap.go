package manager

import "fmt"

// Reap mode strings reported by ReapOverlay - see OverlayReapResult.
const (
	// ReapModeRefusedLive means OverlayReapVerdict.WouldReap was false: the
	// record is still the live owner of its (entry, file) slot, so nothing
	// is touched regardless of the execute flag. The load-bearing guard.
	ReapModeRefusedLive = "refused_live"
	// ReapModeDryRun means the verdict allowed reaping but execute was
	// false - nothing is touched, this only reports what would happen.
	ReapModeDryRun = "dry_run"
	// ReapModeReapedEntry means the whole overlay entry was deleted because
	// its backing Entry record no longer exists at all.
	ReapModeReapedEntry = "reaped_entry"
	// ReapModeReapedFile means just this file's overlay record was deleted
	// from a still-existing entry whose slot moved on to a different owner.
	ReapModeReapedFile = "reaped_file"
)

// OverlayReapResult reports what ReapOverlay did (or would do) for a single
// (nzbID, file) request.
type OverlayReapResult struct {
	Verdict OverlayReapVerdict
	Mode    string
	Removed bool
}

// ReapOverlay resolves nzbID/file against OverlayReapVerdict and, only if
// the verdict says it's safe to reap AND execute is true, deletes the stale
// overlay record. A verdict of WouldReap=false is refused even when
// execute=true - this guard is the only thing standing between this route
// and deleting a live, still-owned overlay record, so it is checked before
// the execute flag is even consulted. Deliberately Arr-independent, like
// OverlayReapVerdict itself: never consults BuildArrReferencedSet or any
// gc-orphans machinery.
func (m *Manager) ReapOverlay(nzbID, file string, execute bool) (OverlayReapResult, error) {
	v := m.OverlayReapVerdict(nzbID, file)
	result := OverlayReapResult{Verdict: v}

	if !v.WouldReap {
		result.Mode = ReapModeRefusedLive
		return result, nil
	}
	if !execute {
		result.Mode = ReapModeDryRun
		return result, nil
	}

	u := m.Usenet()
	if u == nil {
		return result, fmt.Errorf("usenet client not available")
	}

	if v.EntryGone {
		result.Mode = ReapModeReapedEntry
		// OverlayDeleteEntry reports no removed signal of its own (it's an
		// unconditional RemoveAll), so check what's there first.
		manifest, _ := u.OverlayManifest(nzbID)
		result.Removed = manifest != nil
		if err := u.OverlayDeleteEntry(nzbID); err != nil {
			return result, err
		}
		return result, nil
	}

	result.Mode = ReapModeReapedFile
	removed, err := u.OverlayDeleteFile(nzbID, file)
	result.Removed = removed
	if err != nil {
		return result, err
	}
	return result, nil
}
