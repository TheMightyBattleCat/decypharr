package manager

// OverlayReapVerdict is a read-only report of whether an overlay record for
// (nzbID, file) is safe for a future manual "reap this one orphan" action to
// delete. It answers a narrower question than the Arr-referenced GC pass
// (overlayOrphanScan/BuildArrReferencedSet): not "is this file still wanted
// by an *arr", but "does the live entry index still agree that nzbID is the
// current owner of this (entry-name, file) slot" - i.e. has a re-grab moved
// the slot's InfoHash on without the old overlay record being cleaned up.
// Deliberately Arr-independent: it never consults BuildArrReferencedSet or
// any gc-orphans machinery, so it stays meaningful even when the Arr lookup
// is unavailable or disabled.
//
// Performs no delete. Every ambiguous case (entry gone, name no longer
// resolves, file no longer present) resolves to WouldReap=true because a
// vanished entry/name/file can no longer be live-owned by anything -
// unlike fileSuperseded's contract (see supersession_test.go), where an
// empty/absent signal must resolve to "not superseded" because it could
// mean either "still referenced" or "gone", and only the latter is safe to
// act on.
type OverlayReapVerdict struct {
	NzbID            string
	Name             string
	ResolvedInfoHash string
	EntryGone        bool
	WouldReap        bool
}

// OverlayReapVerdict resolves nzbID/file against the live entry index and
// reports whether it looks safe to reap: the entry backing nzbID must still
// exist, its name must still resolve to an EntryItem, that item must still
// have the file, and the file's current InfoHash must differ from nzbID
// (proving something else now owns the slot). Read-only - callers must not
// treat WouldReap=true as anything more than "safe to reap", it performs no
// deletion itself.
func (m *Manager) OverlayReapVerdict(nzbID, file string) OverlayReapVerdict {
	v := OverlayReapVerdict{NzbID: nzbID}

	entry, err := m.GetEntry(nzbID)
	if err != nil || entry == nil {
		v.EntryGone = true
		v.WouldReap = true
		return v
	}
	v.Name = entry.Name

	li, err := m.GetEntryItem(entry.Name)
	if err != nil || li == nil {
		v.WouldReap = true
		return v
	}

	f, err := li.GetFile(file)
	if err != nil || f == nil {
		v.WouldReap = true
		return v
	}

	v.ResolvedInfoHash = f.InfoHash
	v.WouldReap = f.InfoHash != nzbID
	return v
}
