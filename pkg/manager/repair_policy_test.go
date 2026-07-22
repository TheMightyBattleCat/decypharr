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
		name        string
		par2Enabled bool
		verdict     overlay.Verdict
		want        autoRepairAction
	}{
		{
			name:        "par2 enabled + within caps (degraded): pad + queue par2 background, no re-grab",
			par2Enabled: true,
			verdict:     overlay.VerdictDegraded,
			want:        autoActionQueuePar2,
		},
		{
			name:        "par2 enabled + failed: queue par2 (urgent if playing), never an automatic re-grab",
			par2Enabled: true,
			verdict:     overlay.VerdictFailed,
			want:        autoActionQueuePar2,
		},
		{
			name:        "par2 disabled + within caps (degraded): pad only, no re-grab",
			par2Enabled: false,
			verdict:     overlay.VerdictDegraded,
			want:        autoActionNone,
		},
		{
			name:        "par2 disabled + failed: auto re-grab (legacy behavior)",
			par2Enabled: false,
			verdict:     overlay.VerdictFailed,
			want:        autoActionRegrab,
		},
		{
			name:        "clean verdict never triggers action, par2 enabled",
			par2Enabled: true,
			verdict:     overlay.VerdictClean,
			want:        autoActionNone,
		},
		{
			name:        "clean verdict never triggers action, par2 disabled",
			par2Enabled: false,
			verdict:     overlay.VerdictClean,
			want:        autoActionNone,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := decideAutoRepairAction(RepairSourcePlayback, c.par2Enabled, c.verdict)
			if got != c.want {
				t.Errorf("decideAutoRepairAction(playback, par2Enabled=%v, verdict=%v) = %v, want %v",
					c.par2Enabled, c.verdict, got, c.want)
			}
		})
	}
}

// TestDecideAutoRepairActionOnlyOneQuadrantRegrabsForPlayback is a direct
// assertion of source=playback's single most important safety property:
// across the entire (par2Enabled, verdict) space this test enumerates,
// auto-re-grab is chosen in exactly one combination - padding is always
// given the chance to cover tolerable, within-caps damage while a live
// viewer is watching.
func TestDecideAutoRepairActionOnlyOneQuadrantRegrabsForPlayback(t *testing.T) {
	verdicts := []overlay.Verdict{overlay.VerdictClean, overlay.VerdictDegraded, overlay.VerdictFailed}
	regrabCount := 0
	for _, par2Enabled := range []bool{true, false} {
		for _, v := range verdicts {
			if decideAutoRepairAction(RepairSourcePlayback, par2Enabled, v) == autoActionRegrab {
				regrabCount++
				if par2Enabled {
					t.Errorf("autoActionRegrab chosen with par2Enabled=true, verdict=%v - re-grab must never fire while PAR2 repair is enabled", v)
				}
				if v != overlay.VerdictFailed {
					t.Errorf("autoActionRegrab chosen for verdict=%v - re-grab must only fire for a failed verdict", v)
				}
			}
		}
	}
	if regrabCount != 1 {
		t.Errorf("autoActionRegrab chosen %d times across the (par2Enabled, verdict) space, want exactly 1 (disabled + failed)", regrabCount)
	}
}

// TestDecideAutoRepairActionImportAndSweepNeverPad is the extended
// truth-table test: source=import and source=sweep must NEVER resolve to
// autoActionNone for a real (degraded/failed) verdict - detection there is
// never pad-and-forget. This directly proves the divergence from
// source=playback for the one cell where they differ: par2 disabled +
// degraded. Playback pads (a live viewer is watching now); import/sweep
// re-grab instead (nobody is watching a padded, undetected glitch resolve
// itself outside a playback session).
func TestDecideAutoRepairActionImportAndSweepNeverPad(t *testing.T) {
	sources := []RepairSource{RepairSourceImport, RepairSourceSweep}
	verdicts := []overlay.Verdict{overlay.VerdictDegraded, overlay.VerdictFailed}

	for _, source := range sources {
		for _, par2Enabled := range []bool{true, false} {
			for _, verdict := range verdicts {
				got := decideAutoRepairAction(source, par2Enabled, verdict)
				if got == autoActionNone {
					t.Errorf("decideAutoRepairAction(%s, par2Enabled=%v, verdict=%v) = none, want pad/none is never valid for source=%s",
						source, par2Enabled, verdict, source)
				}
			}
		}
	}
}

// TestDecideAutoRepairActionSourceTruthTable is the full extended truth
// table: (source, par2Enabled, verdict) -> action, across all three
// sources. It proves the specific divergence the task requires: import/sweep
// + degraded + par2-disabled resolves to a re-grab, whereas playback +
// degraded + par2-disabled pads (autoActionNone - padding is applied by the
// reader itself, not this function; "none" here means "no automatic repair
// action", which is exactly what leaves the segment padded).
func TestDecideAutoRepairActionSourceTruthTable(t *testing.T) {
	cases := []struct {
		name        string
		source      RepairSource
		par2Enabled bool
		verdict     overlay.Verdict
		want        autoRepairAction
	}{
		// --- source=playback: unchanged four-quadrant behavior ---
		{"playback + degraded + par2 enabled -> queue par2", RepairSourcePlayback, true, overlay.VerdictDegraded, autoActionQueuePar2},
		{"playback + degraded + par2 disabled -> pad (none)", RepairSourcePlayback, false, overlay.VerdictDegraded, autoActionNone},
		{"playback + failed + par2 enabled -> queue par2", RepairSourcePlayback, true, overlay.VerdictFailed, autoActionQueuePar2},
		{"playback + failed + par2 disabled -> regrab", RepairSourcePlayback, false, overlay.VerdictFailed, autoActionRegrab},
		{"playback + clean -> none", RepairSourcePlayback, true, overlay.VerdictClean, autoActionNone},

		// --- source=import: never pad-and-forget ---
		{"import + degraded + par2 enabled -> queue par2", RepairSourceImport, true, overlay.VerdictDegraded, autoActionQueuePar2},
		{"import + degraded + par2 disabled -> REGRAB (would be pad for playback)", RepairSourceImport, false, overlay.VerdictDegraded, autoActionRegrab},
		{"import + failed + par2 enabled -> queue par2", RepairSourceImport, true, overlay.VerdictFailed, autoActionQueuePar2},
		{"import + failed + par2 disabled -> regrab", RepairSourceImport, false, overlay.VerdictFailed, autoActionRegrab},
		{"import + clean -> none", RepairSourceImport, true, overlay.VerdictClean, autoActionNone},

		// --- source=sweep: never pad-and-forget ---
		{"sweep + degraded + par2 enabled -> queue par2", RepairSourceSweep, true, overlay.VerdictDegraded, autoActionQueuePar2},
		{"sweep + degraded + par2 disabled -> REGRAB (would be pad for playback)", RepairSourceSweep, false, overlay.VerdictDegraded, autoActionRegrab},
		{"sweep + failed + par2 enabled -> queue par2", RepairSourceSweep, true, overlay.VerdictFailed, autoActionQueuePar2},
		{"sweep + failed + par2 disabled -> regrab", RepairSourceSweep, false, overlay.VerdictFailed, autoActionRegrab},
		{"sweep + clean -> none", RepairSourceSweep, true, overlay.VerdictClean, autoActionNone},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := decideAutoRepairAction(c.source, c.par2Enabled, c.verdict)
			if got != c.want {
				t.Errorf("decideAutoRepairAction(%s, par2Enabled=%v, verdict=%v) = %v, want %v",
					c.source, c.par2Enabled, c.verdict, got, c.want)
			}
		})
	}
}
