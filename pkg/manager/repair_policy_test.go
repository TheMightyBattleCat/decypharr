package manager

import (
	"testing"

	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// TestDecideAutoRepairActionFourQuadrantTruthTable pins down the exact
// coordination policy for source=playback: auto-re-grab fires ONLY when
// PAR2 repair is disabled AND the file's verdict is failed (beyond the
// padding caps). Every other combination either queues PAR2 or does nothing
// automatic - see decideAutoRepairAction's doc comment for the full
// rationale. This is the table every other source is defined as a variation
// of (see TestDecideAutoRepairActionImportAndSweepNeverPad below).
func TestDecideAutoRepairActionFourQuadrantTruthTable(t *testing.T) {
	cases := []struct {
		name       string
		par2Usable bool
		verdict    overlay.Verdict
		want       autoRepairAction
	}{
		{
			name:       "par2 enabled + within caps (degraded): pad + queue par2 background, no re-grab",
			par2Usable: true,
			verdict:    overlay.VerdictDegraded,
			want:       autoActionQueuePar2,
		},
		{
			name:       "par2 enabled + failed: queue par2 (urgent if playing), never an automatic re-grab",
			par2Usable: true,
			verdict:    overlay.VerdictFailed,
			want:       autoActionQueuePar2,
		},
		{
			name:       "par2 disabled + within caps (degraded): pad only, no re-grab",
			par2Usable: false,
			verdict:    overlay.VerdictDegraded,
			want:       autoActionNone,
		},
		{
			name:       "par2 disabled + failed: auto re-grab (legacy behavior)",
			par2Usable: false,
			verdict:    overlay.VerdictFailed,
			want:       autoActionRegrab,
		},
		{
			// par2Usable=false covers both "toggle off" and "toggle on but
			// not usable for this file" (e.g. a FAILED file whose record
			// predates PAR2 retention, with no backfillable source NZB left
			// on disk - see Par2Repair.par2Usable). Either way this row
			// fires: no new truth-table row, same regrab outcome.
			name:       "par2 usable=false (toggle on, but no par2 data for this file) + failed: auto re-grab, not left terminal",
			par2Usable: false,
			verdict:    overlay.VerdictFailed,
			want:       autoActionRegrab,
		},
		{
			name:       "clean verdict never triggers action, par2 enabled",
			par2Usable: true,
			verdict:    overlay.VerdictClean,
			want:       autoActionNone,
		},
		{
			name:       "clean verdict never triggers action, par2 disabled",
			par2Usable: false,
			verdict:    overlay.VerdictClean,
			want:       autoActionNone,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := decideAutoRepairAction(RepairSourcePlayback, c.par2Usable, c.verdict)
			if got != c.want {
				t.Errorf("decideAutoRepairAction(playback, par2Usable=%v, verdict=%v) = %v, want %v",
					c.par2Usable, c.verdict, got, c.want)
			}
		})
	}
}

// TestDecideAutoRepairActionPrecacheTriggeredUrgentJob proves the precache
// read-ahead and next-episode features' urgent-repair trigger
// (Precache.repairAhead / Precache.recordReadiness) reuses source=playback's
// par2Usable gate exactly like HandlePlaybackFailure does: par2Usable=true
// queues PAR2, par2Usable=false takes no PAR2 path at all - precache never
// acts on autoActionRegrab itself (nothing has actually failed yet), so
// "not queuePar2" is the only outcome that matters for these callers.
func TestDecideAutoRepairActionPrecacheTriggeredUrgentJob(t *testing.T) {
	cases := []struct {
		name       string
		par2Usable bool
		verdict    overlay.Verdict
		wantQueue  bool
	}{
		{"usable + degraded: queues PAR2", true, overlay.VerdictDegraded, true},
		{"usable + failed: queues PAR2", true, overlay.VerdictFailed, true},
		{"not usable + degraded: no PAR2 path", false, overlay.VerdictDegraded, false},
		{"not usable + failed: no PAR2 path (precache never regrabs)", false, overlay.VerdictFailed, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			action := decideAutoRepairAction(RepairSourcePlayback, c.par2Usable, c.verdict)
			gotQueue := action == autoActionQueuePar2
			if gotQueue != c.wantQueue {
				t.Errorf("decideAutoRepairAction(playback, par2Usable=%v, verdict=%v) = %v; queuePar2=%v, want %v",
					c.par2Usable, c.verdict, action, gotQueue, c.wantQueue)
			}
		})
	}
}

// TestDecideAutoRepairActionOnlyOneQuadrantRegrabsForPlayback is a direct
// assertion of source=playback's single most important safety property:
// across the entire (par2Usable, verdict) space this test enumerates,
// auto-re-grab is chosen in exactly one combination - padding is always
// given the chance to cover tolerable, within-caps damage while a live
// viewer is watching.
func TestDecideAutoRepairActionOnlyOneQuadrantRegrabsForPlayback(t *testing.T) {
	verdicts := []overlay.Verdict{overlay.VerdictClean, overlay.VerdictDegraded, overlay.VerdictFailed}
	regrabCount := 0
	for _, par2Usable := range []bool{true, false} {
		for _, v := range verdicts {
			if decideAutoRepairAction(RepairSourcePlayback, par2Usable, v) == autoActionRegrab {
				regrabCount++
				if par2Usable {
					t.Errorf("autoActionRegrab chosen with par2Usable=true, verdict=%v - re-grab must never fire while PAR2 repair is enabled", v)
				}
				if v != overlay.VerdictFailed {
					t.Errorf("autoActionRegrab chosen for verdict=%v - re-grab must only fire for a failed verdict", v)
				}
			}
		}
	}
	if regrabCount != 1 {
		t.Errorf("autoActionRegrab chosen %d times across the (par2Usable, verdict) space, want exactly 1 (disabled + failed)", regrabCount)
	}
}

// TestDecideAutoRepairActionImportAndSweepNeverPad is the extended
// truth-table test: source=import and source=sweep must NEVER resolve to
// autoActionNone for a real (degraded/failed) verdict - detection there is
// never pad-and-forget. PAR2 is a playback-only mechanism (see
// decideAutoRepairAction's doc comment: at import/sweep time the DFS cache
// is cold, so PAR2 would fetch the entire release from Usenet instead of
// the cache-warm slices it relies on to be cheap), so import/sweep also
// never resolve to autoActionQueuePar2 - par2Usable is ignored entirely for
// these sources. The assertion holds across the whole (par2Usable, verdict)
// space: every combination collapses to the single re-grab action.
func TestDecideAutoRepairActionImportAndSweepNeverPad(t *testing.T) {
	sources := []RepairSource{RepairSourceImport, RepairSourceSweep}
	verdicts := []overlay.Verdict{overlay.VerdictDegraded, overlay.VerdictFailed}

	for _, source := range sources {
		for _, par2Usable := range []bool{true, false} {
			for _, verdict := range verdicts {
				got := decideAutoRepairAction(source, par2Usable, verdict)
				if got != autoActionRegrab {
					t.Errorf("decideAutoRepairAction(%s, par2Usable=%v, verdict=%v) = %v, want autoActionRegrab - import/sweep never pad and never queue PAR2 (playback-only)",
						source, par2Usable, verdict, got)
				}
			}
		}
	}
}

// TestDecideAutoRepairActionSourceTruthTable is the full extended truth
// table: (source, par2Usable, verdict) -> action, across all three
// sources. PAR2 is a playback-only mechanism: only source=playback ever
// resolves to autoActionQueuePar2. This splits what was previously a single
// "any source | par2 enabled | degraded/failed -> queue PAR2" row: for
// source=playback that still holds (par2Usable fully determines the
// outcome alongside verdict), but for source=import and source=sweep every
// combination - INCLUDING par2Usable=true - now resolves to a re-grab,
// since PAR2 would run against a cold DFS cache at import/sweep time and
// gets no benefit from it. Proves the three cases the divergence hinges on:
// import + par2-on + degraded -> regrab, sweep + par2-on + failed -> regrab,
// and playback + par2-on + degraded -> queuePar2 (the one source that still
// consults par2Usable at all).
func TestDecideAutoRepairActionSourceTruthTable(t *testing.T) {
	cases := []struct {
		name       string
		source     RepairSource
		par2Usable bool
		verdict    overlay.Verdict
		want       autoRepairAction
	}{
		// --- source=playback: unchanged four-quadrant behavior; the only
		// source that ever consults par2Usable ---
		{"playback + degraded + par2 enabled -> queue par2", RepairSourcePlayback, true, overlay.VerdictDegraded, autoActionQueuePar2},
		{"playback + degraded + par2 disabled -> pad (none)", RepairSourcePlayback, false, overlay.VerdictDegraded, autoActionNone},
		{"playback + failed + par2 enabled -> queue par2", RepairSourcePlayback, true, overlay.VerdictFailed, autoActionQueuePar2},
		{"playback + failed + par2 disabled -> regrab", RepairSourcePlayback, false, overlay.VerdictFailed, autoActionRegrab},
		{"playback + clean -> none", RepairSourcePlayback, true, overlay.VerdictClean, autoActionNone},

		// --- source=import: never pad-and-forget, never PAR2 (par2Usable ignored) ---
		{"import + degraded + par2 enabled -> REGRAB (would be queue-par2 for playback)", RepairSourceImport, true, overlay.VerdictDegraded, autoActionRegrab},
		{"import + degraded + par2 disabled -> REGRAB (would be pad for playback)", RepairSourceImport, false, overlay.VerdictDegraded, autoActionRegrab},
		{"import + failed + par2 enabled -> REGRAB (would be queue-par2 for playback)", RepairSourceImport, true, overlay.VerdictFailed, autoActionRegrab},
		{"import + failed + par2 disabled -> regrab", RepairSourceImport, false, overlay.VerdictFailed, autoActionRegrab},
		{"import + clean -> none", RepairSourceImport, true, overlay.VerdictClean, autoActionNone},

		// --- source=sweep: never pad-and-forget, never PAR2 (par2Usable ignored) ---
		{"sweep + degraded + par2 enabled -> REGRAB (would be queue-par2 for playback)", RepairSourceSweep, true, overlay.VerdictDegraded, autoActionRegrab},
		{"sweep + degraded + par2 disabled -> REGRAB (would be pad for playback)", RepairSourceSweep, false, overlay.VerdictDegraded, autoActionRegrab},
		{"sweep + failed + par2 enabled -> REGRAB (would be queue-par2 for playback)", RepairSourceSweep, true, overlay.VerdictFailed, autoActionRegrab},
		{"sweep + failed + par2 disabled -> regrab", RepairSourceSweep, false, overlay.VerdictFailed, autoActionRegrab},
		{"sweep + clean -> none", RepairSourceSweep, true, overlay.VerdictClean, autoActionNone},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := decideAutoRepairAction(c.source, c.par2Usable, c.verdict)
			if got != c.want {
				t.Errorf("decideAutoRepairAction(%s, par2Usable=%v, verdict=%v) = %v, want %v",
					c.source, c.par2Usable, c.verdict, got, c.want)
			}
		})
	}
}
