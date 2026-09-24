package manager

// markHealthDirty flags nzbID's entry for a re-probe on the next sweep. The
// overlay or PAR2 just learned something about it - a file went past the pad
// caps, or PAR2 gave up - that the health record the last probe wrote can't
// reflect, and without this it waited out the full recheck interval.
func (m *Manager) markHealthDirty(nzbID, reason string) {
	if m == nil || m.storage == nil || nzbID == "" {
		return
	}
	entry, err := m.GetEntry(nzbID)
	if err != nil || entry == nil || entry.Name == "" {
		return
	}
	m.storage.MarkEntryDirty(entry.Name, entry.Protocol, reason)
}

// EntryDamage is the damage the overlay holds right now for an entry's files,
// which a stored health record (written by the last probe) can't know about.
type EntryDamage struct {
	// PendingDeadSegments counts dead or padded segments not yet patched.
	PendingDeadSegments int
	// Par2Terminal is set when PAR2 has given up on a release that still has
	// pending damage.
	Par2Terminal bool
}

// EntryDamage reads entryName's live overlay damage: the files of the merged
// entry, grouped by the nzbID each is stored under.
func (m *Manager) EntryDamage(entryName string) EntryDamage {
	var out EntryDamage
	if m == nil || m.storage == nil || m.usenet == nil || entryName == "" {
		return out
	}
	item, err := m.storage.GetEntryItem(entryName)
	if err != nil || item == nil {
		return out
	}
	filesByNZB := make(map[string][]string)
	for name, f := range item.Files {
		if f == nil || f.Deleted || f.InfoHash == "" {
			continue
		}
		filesByNZB[f.InfoHash] = append(filesByNZB[f.InfoHash], name)
	}
	for nzbID, names := range filesByNZB {
		pending, err := m.usenet.OverlayPendingRepair(nzbID)
		if err != nil || len(pending) == 0 {
			continue
		}
		n := 0
		for _, name := range names {
			n += len(pending[name])
		}
		if n == 0 {
			continue
		}
		out.PendingDeadSegments += n
		if st, err := m.storage.GetPar2RepairState(nzbID); err == nil && st != nil && st.Terminal {
			out.Par2Terminal = true
		}
	}
	return out
}
