package manager

import "github.com/sirrobot01/decypharr/pkg/usenet/overlay"

// autoRepairAction is what the coordinated auto-repair policy says an
// automatic caller (playback-failure escalation, the import gate, the
// repair sweep, or the PAR2 worker's own queueing) should do for one file's
// current damage.
type autoRepairAction int

const (
	// autoActionNone means nothing automatic to do: the file isn't damaged
	// (VerdictClean/unknown), or - source=playback only - the damage is
	// within the padding caps and PAR2 is disabled, so padding alone already
	// covers it and an automatic re-grab would be premature (the whole point
	// of padding is to keep playing a file with tolerable damage without
	// treating it as broken). Never returned for source=import or
	// source=sweep on a real verdict - see decideAutoRepairAction.
	autoActionNone autoRepairAction = iota
	// autoActionQueuePar2 means hand the file to the PAR2 worker instead of
	// re-grabbing: either the damage is within caps (background repair,
	// playback stays smooth in the meantime) or beyond them but PAR2 is
	// enabled and gets first refusal - only a PAR2-classified terminal
	// failure (see classifyPar2Failure) ever escalates beyond this, and even
	// then to a manual-only "unrepairable" mark, never an automatic re-grab.
	autoActionQueuePar2
	// autoActionRegrab means delete + blocklist + re-search via the Arr
	// (the legacy path).
	autoActionRegrab
)

// RepairSource identifies where a confirmed-dead segment was detected -
// decideAutoRepairAction's outcome depends on it. Playback padding is a
// live-viewer survival mechanism: someone is watching right now, so leaving
// tolerable damage padded and fixing it in the background is the right
// call. Import and sweep detection have no such excuse - nobody is
// mid-playback, so a file that's merely "padded" there is a broken file
// wearing a healthy verdict; every confirmed-dead segment from these two
// sources must be actively resolved, never left as a no-op.
type RepairSource string

const (
	// RepairSourcePlayback is a segment confirmed missing while a real
	// client streamed the file (SegmentFetcher.doFetch's normal, padded
	// read path - see pkg/usenet/fs/reader) and padding declined to cover
	// it, surfacing as a hard playback failure (see HandlePlaybackFailure).
	RepairSourcePlayback RepairSource = "playback"

	// RepairSourceImport is a segment confirmed missing by the import-time
	// availability gate, before the download is ever reported complete to
	// the Arr (see Downloader.importAvailabilityGate).
	RepairSourceImport RepairSource = "import"

	// RepairSourceSweep is a segment confirmed missing by the repair
	// sweep's own probe (see Repair.probeNZBFile / routeAutoRepair).
	RepairSourceSweep RepairSource = "sweep"
)

// decideAutoRepairAction implements the coordinated auto-repair policy over
// (source, par2Enabled, verdict).
//
// source=playback keeps the original four-quadrant table exactly as before
// this source distinction existed:
//
//	par2 enabled  + degraded (within padding caps): queue PAR2 (background). No re-grab.
//	par2 enabled  + failed   (beyond caps):          queue PAR2 (urgent if actively playing).
//	                                                  A PAR2-terminal outcome marks the file
//	                                                  unrepairable for MANUAL "Delete & re-search" -
//	                                                  it never falls back to an automatic re-grab.
//	par2 disabled + degraded: nothing automatic (padding alone covers it, a live viewer keeps watching).
//	par2 disabled + failed:   auto re-grab (today's legacy behavior).
//
// source=import and source=sweep collapse that "par2 disabled + degraded ->
// nothing automatic" cell into a re-grab instead: detection there is never
// pad-and-forget, so ANY confirmed-dead segment (degraded or failed verdict
// alike) resolves to PAR2 repair when enabled, otherwise a full re-grab -
// never a no-op:
//
//	par2 enabled  + degraded: queue PAR2. No re-grab.
//	par2 enabled  + failed:   queue PAR2. No re-grab.
//	par2 disabled + degraded: auto re-grab (would have been "none" for playback).
//	par2 disabled + failed:   auto re-grab.
//
// VerdictClean (or any other value) never triggers automatic action for any
// source - there is no damage for the policy to act on.
//
// This function is pure and deliberately knows nothing about the handler
// registry, storage, or which caller invoked it - every call site is
// responsible for consulting repairHandlerRegistry around the action this
// returns, so an entry already being handled is never double-queued or
// double-re-grabbed.
func decideAutoRepairAction(source RepairSource, par2Enabled bool, verdict overlay.Verdict) autoRepairAction {
	switch verdict {
	case overlay.VerdictDegraded:
		if par2Enabled {
			return autoActionQueuePar2
		}
		if source == RepairSourcePlayback {
			return autoActionNone
		}
		return autoActionRegrab
	case overlay.VerdictFailed:
		if par2Enabled {
			return autoActionQueuePar2
		}
		return autoActionRegrab
	default:
		return autoActionNone
	}
}
