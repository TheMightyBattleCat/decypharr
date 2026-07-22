package manager

import (
	"testing"

	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// TestDecideAutoRepairActionFourQuadrantTruthTable pins down the exact
// coordination policy: auto-re-grab fires ONLY when PAR2 repair is disabled
// AND the file's verdict is failed (beyond the padding caps). Every other
// combination either queues PAR2 or does nothing automatic - see
// decideAutoRepairAction's doc comment for the full rationale.
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
			got := decideAutoRepairAction(c.par2Enabled, c.verdict)
			if got != c.want {
				t.Errorf("decideAutoRepairAction(par2Enabled=%v, verdict=%v) = %v, want %v",
					c.par2Enabled, c.verdict, got, c.want)
			}
		})
	}
}

// TestDecideAutoRepairActionOnlyOneQuadrantRegrabs is a direct assertion of
// the policy's single most important safety property: across the entire
// (par2Enabled, verdict) space this test enumerates, auto-re-grab is chosen
// in exactly one combination.
func TestDecideAutoRepairActionOnlyOneQuadrantRegrabs(t *testing.T) {
	verdicts := []overlay.Verdict{overlay.VerdictClean, overlay.VerdictDegraded, overlay.VerdictFailed}
	regrabCount := 0
	for _, par2Enabled := range []bool{true, false} {
		for _, v := range verdicts {
			if decideAutoRepairAction(par2Enabled, v) == autoActionRegrab {
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
