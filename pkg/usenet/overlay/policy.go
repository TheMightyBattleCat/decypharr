package overlay

import "sort"

// Policy holds the padding caps that bound how much damage a file may
// accumulate before it's failed outright instead of padded. The safety
// property (never let padding turn "a few glitches" into "unwatchable
// garbage") depends on these staying strict - see internal/config's
// RepairConfig.PadMaxRunSegments/PadMaxTotalSegments/PadMaxByteRatio for the
// configurable, clamped source of these values. The Store holds one Policy,
// set (and refreshed on config changes) by its owner - see Store.SetPolicy -
// so Decide, on the reader hot path, never re-derives or re-clamps it itself.
type Policy struct {
	// MaxRunSegments is the longest run of consecutive confirmed-dead
	// segments (by segment index) still eligible for padding. A longer
	// contiguous run means a large enough hole that zero-filling it would
	// produce a long freeze/glitch rather than a brief one - fail instead.
	MaxRunSegments int

	// MaxTotalSegments is the most dead (non-patched) segments a single
	// file may accumulate before it's failed outright, regardless of how
	// they're distributed.
	MaxTotalSegments int

	// MaxByteRatio is the maximum fraction of a file's total size that may
	// be padded (zero-filled) before it's failed outright.
	MaxByteRatio float64
}

// Built-in padding caps, used whenever a Store has no explicit Policy set
// (e.g. tests that construct a Store directly) - identical to this
// feature's original, pre-configurable values.
const (
	defaultRunSegments   = 4
	defaultTotalSegments = 64
	defaultByteRatio     = 0.02
)

// headerRegionDivisor defines a file's leading "header region" as the first
// 1/headerRegionDivisor (1%) of its segments. A confirmed-dead segment
// landing in that region fails the file outright, regardless of the other
// caps, since the container's leading metadata/seek data lives there -
// matching the availability sampler's own header-region notion.
const headerRegionDivisor = 100

// DefaultPolicy returns the built-in padding caps.
func DefaultPolicy() Policy {
	return Policy{
		MaxRunSegments:   defaultRunSegments,
		MaxTotalSegments: defaultTotalSegments,
		MaxByteRatio:     defaultByteRatio,
	}
}

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
func (s *Store) Decide(nzbID, file string, segIndex int, msgID string, segBytes, fileSize int64, totalSegments int) (Decision, Verdict) {
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
	}

	if !IsVideoContainer(file) {
		fe.Verdict = VerdictFailed
		_ = s.saveManifestLocked(nzbID, m)
		s.notifyFailed(nzbID, file)
		return DecisionFail, VerdictFailed
	}

	// Read the caps once - already clamped by whoever set the policy (see
	// Store.SetPolicy) - and reuse them for every check below rather than
	// re-deriving or re-clamping anything on this hot path.
	policy := s.Policy()
	if _, ok := WithinPadCaps(fe, fileSize, policy, totalSegments); !ok {
		fe.Verdict = VerdictFailed
		_ = s.saveManifestLocked(nzbID, m)
		s.notifyFailed(nzbID, file)
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

// CapEvaluation carries the raw damage figures WithinPadCaps derived while
// checking fe against a Policy, so a caller that needs to explain WHY a
// verdict landed where it did (e.g. the read-only damage screen) doesn't
// have to re-derive them itself.
type CapEvaluation struct {
	TotalDead    int     // non-patched dead+padded segment count
	PadBytes     int64   // total bytes of those segments
	LongestRun   int     // longest run of consecutive dead segment indices
	ByteRatio    float64 // PadBytes / fileSize (0 if fileSize <= 0)
	HeaderDamage bool    // a non-patched dead segment sits in the header region
}

// WithinPadCaps evaluates every cap against fe's current (non-patched) dead
// segments and returns both the evaluation and whether every cap passed.
// Patched segments are excluded throughout: PAR2 repair having already
// recovered a segment's true bytes means it no longer counts as damage
// against the file's remaining pad budget.
//
// This is the one place the padding caps are checked - Decide calls it for
// the real, persisting decision; the read-only damage screen (see
// usenet.OverlayScreenFile) calls it against a hypothetical, not-yet-recorded
// dead-set to project what Decide would return, without persisting anything.
// Keeping both behind this single function means the two can never drift.
func WithinPadCaps(fe *FileEntry, fileSize int64, policy Policy, totalSegments int) (CapEvaluation, bool) {
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
	longestRun := best
	if len(indices) == 0 {
		longestRun = 0
	}

	var byteRatio float64
	if fileSize > 0 {
		byteRatio = float64(padBytes) / float64(fileSize)
	}

	headerDamage := totalSegments > 0 && len(indices) > 0 && indices[0] < max(1, totalSegments/headerRegionDivisor)

	eval := CapEvaluation{TotalDead: total, PadBytes: padBytes, LongestRun: longestRun, ByteRatio: byteRatio, HeaderDamage: headerDamage}

	ok := true
	if total > policy.MaxTotalSegments {
		ok = false
	}
	if fileSize > 0 && float64(padBytes) > float64(fileSize)*policy.MaxByteRatio {
		ok = false
	}
	if len(indices) > 0 && best > policy.MaxRunSegments {
		ok = false
	}
	if headerDamage {
		ok = false
	}
	return eval, ok
}
