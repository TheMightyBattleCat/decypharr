package overlay

import "sort"

// Padding policy caps. These are intentionally hardcoded, not configurable:
// the feature's safety property (never let padding turn "a few glitches"
// into "unwatchable garbage") depends on a strict, unsurprising ceiling.
const (
	// maxPadRunSegments is the longest run of consecutive confirmed-dead
	// segments (by segment index) still eligible for padding. A longer
	// contiguous run means a large enough hole that zero-filling it would
	// produce a long freeze/glitch rather than a brief one - fail instead.
	maxPadRunSegments = 4

	// maxPadTotalSegments is the most dead (non-patched) segments a single
	// file may accumulate before it's failed outright, regardless of how
	// they're distributed.
	maxPadTotalSegments = 64

	// maxPadByteRatio is the maximum fraction of a file's total size that may
	// be padded (zero-filled) before it's failed outright.
	maxPadByteRatio = 0.02
)

// recomputeVerdictLocked derives fe.Verdict from its current DeadSegments set
// (excluding patched ones, which no longer count as damage) without
// consulting file size - used after WritePatch, where nothing new is being
// added and the only possible transitions are unchanged or an improvement
// back toward clean/degraded.
func recomputeVerdictLocked(fe *FileEntry) {
	// A prior FAIL verdict is sticky: patching individual segments after the
	// fact doesn't retroactively make the file's playback history "degraded"
	// or "clean" instead of "failed" - the legacy repair path may already be
	// in flight for it. Checked before the hasDead recompute below, since a
	// FAIL verdict can be reached with as few as one dead segment (a
	// non-video-container file, or one run/ratio-cap violation) and that
	// segment being the one just patched must not un-fail the file.
	if fe.Verdict == VerdictFailed {
		return
	}

	hasDead := false
	for _, d := range fe.DeadSegments {
		if d.Status != StatusPatched {
			hasDead = true
			break
		}
	}
	if hasDead {
		fe.Verdict = VerdictDegraded
	} else {
		fe.Verdict = VerdictClean
	}
}

// Decide records segIndex as dead (if not already known) and returns whether
// it should be padded under the current policy, plus file's resulting
// verdict. Only ever called after a segment's article fetch has permanently
// failed across every provider.
func (s *Store) Decide(nzbID, file string, segIndex int, msgID string, segBytes, fileSize int64) (Decision, Verdict) {
	mu := s.lockFor(nzbID)
	mu.Lock()
	defer mu.Unlock()

	m, err := s.loadManifestLocked(nzbID)
	if err != nil {
		// Fail safe: if policy state can't be consulted/persisted, never
		// fabricate padding - fall through to the existing repair flow.
		return DecisionFail, VerdictFailed
	}
	fe := fileEntryLocked(m, file)

	// A file already failed never pads again - the legacy repair path owns
	// it now.
	if fe.Verdict == VerdictFailed {
		return DecisionFail, VerdictFailed
	}

	existing := -1
	for i := range fe.DeadSegments {
		if fe.DeadSegments[i].Index == segIndex {
			existing = i
			break
		}
	}
	if existing == -1 {
		fe.DeadSegments = append(fe.DeadSegments, DeadSegment{
			Index: segIndex, MessageID: msgID, Bytes: segBytes, Status: StatusDead,
		})
		sortDeadSegments(fe)
	} else if fe.DeadSegments[existing].Status == StatusPadded {
		// Already decided (and padded) in an earlier session that hit this
		// same dead article again - honor that decision rather than
		// re-running the caps math against an unchanged set.
		return DecisionPad, fe.Verdict
	}

	if !IsVideoContainer(file) {
		fe.Verdict = VerdictFailed
		_ = s.saveManifestLocked(nzbID, m)
		return DecisionFail, VerdictFailed
	}

	if !withinPadCaps(fe, fileSize) {
		fe.Verdict = VerdictFailed
		_ = s.saveManifestLocked(nzbID, m)
		return DecisionFail, VerdictFailed
	}

	for i := range fe.DeadSegments {
		if fe.DeadSegments[i].Index == segIndex {
			fe.DeadSegments[i].Status = StatusPadded
			break
		}
	}
	fe.Verdict = VerdictDegraded
	_ = s.saveManifestLocked(nzbID, m)
	return DecisionPad, VerdictDegraded
}

// withinPadCaps evaluates every cap against fe's current (non-patched) dead
// segments. Patched segments are excluded throughout: PAR2 repair having
// already recovered a segment's true bytes means it no longer counts as
// damage against the file's remaining pad budget.
func withinPadCaps(fe *FileEntry, fileSize int64) bool {
	var (
		total    int
		padBytes int64
		indices  = make([]int, 0, len(fe.DeadSegments))
	)
	for _, d := range fe.DeadSegments {
		if d.Status == StatusPatched {
			continue
		}
		total++
		padBytes += d.Bytes
		indices = append(indices, d.Index)
	}

	if total > maxPadTotalSegments {
		return false
	}
	if fileSize > 0 && float64(padBytes) > float64(fileSize)*maxPadByteRatio {
		return false
	}

	sort.Ints(indices)
	run, best := 1, 1
	for i := 1; i < len(indices); i++ {
		if indices[i] == indices[i-1]+1 {
			run++
		} else {
			run = 1
		}
		if run > best {
			best = run
		}
	}
	if len(indices) > 0 && best > maxPadRunSegments {
		return false
	}
	return true
}
