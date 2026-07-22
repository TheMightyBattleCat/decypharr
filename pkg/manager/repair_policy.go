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
	// re-grabbing. PAR2 is a playback-only mechanism: it is only ever
	// returned for source=playback, where a live viewer is watching and PAR2
	// can repair from the DFS cache the viewer is already warming. Import
	// and sweep detection happen against a cold cache (nothing has played),
	// so PAR2 there would fetch the entire release from Usenet - never worth
	// it - and always resolve to autoActionRegrab instead, regardless of
	// whether PAR2 repair is enabled.
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
// PAR2 is a playback-only mechanism, so only source=playback ever consults
// par2Enabled. It keeps the original four-quadrant table exactly as before
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
// source=import and source=sweep ignore par2Enabled entirely and always
// re-grab on any damage (degraded or failed verdict alike) - never a no-op,
// never PAR2:
//
//	degraded: auto re-grab (would have been "none" for playback).
//	failed:   auto re-grab (would have been "queue PAR2" for playback when par2 is enabled).
//
// This isn't just "detection there is never pad-and-forget" - it's that PAR2
// only pays off once the DFS cache is warm from playback. At import (and at
// sweep time, since nothing is playing then either) the cache is cold, so a
// PAR2 pass would have to fetch the entire release from Usenet: the one case
// where PAR2's cache-source optimization gives zero benefit and costs the
// most (import latency). A re-grab is strictly better there, so import and
// sweep detection route straight to it regardless of the PAR2 toggle. The
// sweep is also the backstop for anything playback PAR2 left terminal: a
// file PAR2 couldn't fix during playback is left padded until the next
// sweep re-grabs it.
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
		if source != RepairSourcePlayback {
			return autoActionRegrab
		}
		if par2Enabled {
			return autoActionQueuePar2
		}
		return autoActionNone
	case overlay.VerdictFailed:
		if source != RepairSourcePlayback {
			return autoActionRegrab
		}
		if par2Enabled {
			return autoActionQueuePar2
		}
		return autoActionRegrab
	default:
		return autoActionNone
	}
}
