package manager

import (
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
)

// A follow-up pass is an automatic pass: it obeys Par2RepairMode as
// AutoEnqueue does. In manual mode a "repair now" that defers a set, or that
// received requests while it ran, does not chain a second pass; under the
// threshold mode only damage at Par2RepairMinSegments is queued. A refused
// follow-up leaves no par2_queued claim behind.
func TestFollowUpsObeyPar2RepairMode(t *testing.T) {
	const minSegments = 5
	modes := []struct {
		name string
		mode config.Par2RepairMode
		dead int
		want bool
	}{
		{"manual", config.Par2RepairModeManual, 50, false},
		{"auto all", config.Par2RepairModeAutoAll, 1, true},
		{"threshold below min", config.Par2RepairModeAutoThreshold, minSegments - 1, false},
		{"threshold at min", config.Par2RepairModeAutoThreshold, minSegments, true},
	}
	followUps := []struct {
		name string
		run  func(p *Par2Repair, dead int)
	}{
		{"deferred sets", func(p *Par2Repair, dead int) {
			p.followUpDeferredSets("x", "Some Release", dead, 0)
		}},
		{"merged requests", func(p *Par2Repair, dead int) {
			p.pendingDamage = func(string) (int, error) { return dead, nil }
			p.followUpMerged("x", "Some Release", 2, 0)
		}},
	}
	for _, fu := range followUps {
		for _, tc := range modes {
			t.Run(fu.name+"/"+tc.name, func(t *testing.T) {
				p, repair := newTestPar2Repair(t)
				cfg := config.Get()
				prevMode, prevMin := cfg.Repair.Par2RepairMode, cfg.Repair.Par2RepairMinSegments
				t.Cleanup(func() {
					cfg.Repair.Par2RepairMode, cfg.Repair.Par2RepairMinSegments = prevMode, prevMin
				})
				cfg.Repair.Par2RepairMode = tc.mode
				cfg.Repair.Par2RepairMinSegments = minSegments

				fu.run(p, tc.dead)

				if got := urgentWaiting(p, "x"); got != tc.want {
					t.Fatalf("follow-up queued = %v, want %v", got, tc.want)
				}
				if _, _, claimed := repair.handlers.State("x"); claimed != tc.want {
					t.Fatalf("handler claim held = %v, want %v", claimed, tc.want)
				}
			})
		}
	}
}

// With PAR2 repair switched off, a pass that was already running may still
// finish and ask for a follow-up. Neither follow-up queues anything or leaves
// a claim behind: switching repair off stops further automatic passes.
func TestFollowUpsRefusedWhenPar2RepairOff(t *testing.T) {
	followUps := map[string]func(p *Par2Repair){
		"deferred sets": func(p *Par2Repair) {
			p.followUpDeferredSets("x", "Some Release", 50, 0)
		},
		"merged requests": func(p *Par2Repair) {
			p.pendingDamage = func(string) (int, error) { return 50, nil }
			p.followUpMerged("x", "Some Release", 2, 0)
		},
	}
	for name, run := range followUps {
		t.Run(name, func(t *testing.T) {
			p, repair := newTestPar2Repair(t)
			cfg := config.Get()
			prevMode, prevOn := cfg.Repair.Par2RepairMode, cfg.Repair.Par2Repair
			t.Cleanup(func() {
				cfg.Repair.Par2RepairMode, cfg.Repair.Par2Repair = prevMode, prevOn
			})
			off := false
			cfg.Repair.Par2RepairMode = config.Par2RepairModeAutoAll
			cfg.Repair.Par2Repair = &off

			run(p)

			if urgentWaiting(p, "x") {
				t.Fatal("follow-up queued while PAR2 repair is off")
			}
			if _, _, claimed := repair.handlers.State("x"); claimed {
				t.Fatal("handler claim held while PAR2 repair is off")
			}
		})
	}
}
