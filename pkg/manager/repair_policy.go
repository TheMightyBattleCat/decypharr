package manager

import "github.com/sirrobot01/decypharr/pkg/usenet/overlay"

// autoRepairAction is what the coordinated auto-repair policy says an
// automatic caller (playback-failure escalation, the repair sweep, or the
// PAR2 worker's own queueing) should do for one file's current damage.
type autoRepairAction int

const (
	// autoActionNone means nothing automatic to do: either the file isn't
	// damaged (VerdictClean/unknown), or the damage is within the padding
	// caps and PAR2 is disabled - padding alone already covers it and an
	// automatic re-grab would be premature (the whole point of padding is to
	// keep playing a file with tolerable damage without treating it as
	// broken).
	autoActionNone autoRepairAction = iota
	// autoActionQueuePar2 means hand the file to the PAR2 worker instead of
	// re-grabbing: either the damage is within caps (background repair,
	// playback stays smooth in the meantime) or beyond them but PAR2 is
	// enabled and gets first refusal - only a PAR2-classified terminal
	// failure (see classifyPar2Failure) ever escalates beyond this, and even
	// then to a manual-only "unrepairable" mark, never an automatic re-grab.
	autoActionQueuePar2
	// autoActionRegrab means delete + blocklist + re-search via the Arr
	// (the legacy path) - the ONLY quadrant that reaches this is PAR2
	// disabled AND the file beyond the padding caps, i.e. today's
	// pre-PAR2-repair behavior for a verdict padding itself refuses to
	// cover.
	autoActionRegrab
)

// decideAutoRepairAction implements the coordinated auto-repair policy's
// four-quadrant truth table over (par2Enabled, verdict):
//
//	par2 enabled  + degraded (within padding caps): queue PAR2 (background). No re-grab.
//	par2 enabled  + failed   (beyond caps):          queue PAR2 (urgent if actively playing).
//	                                                  A PAR2-terminal outcome marks the file
//	                                                  unrepairable for MANUAL "Delete & re-search" -
//	                                                  it never falls back to an automatic re-grab.
//	par2 disabled + degraded: nothing automatic (padding alone covers it).
//	par2 disabled + failed:   auto re-grab (today's legacy behavior).
//
// VerdictClean (or any other value) never triggers automatic action here -
// there is no damage for the policy to act on.
//
// This function is pure and deliberately knows nothing about the handler
// registry, storage, or which caller invoked it - every call site is
// responsible for consulting repairHandlerRegistry around the action this
// returns, so an entry already being handled is never double-queued or
// double-re-grabbed.
func decideAutoRepairAction(par2Enabled bool, verdict overlay.Verdict) autoRepairAction {
	switch verdict {
	case overlay.VerdictDegraded:
		if par2Enabled {
			return autoActionQueuePar2
		}
		return autoActionNone
	case overlay.VerdictFailed:
		if par2Enabled {
			return autoActionQueuePar2
		}
		return autoActionRegrab
	default:
		return autoActionNone
	}
}
