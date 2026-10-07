package manager

import (
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
)

func TestColdSweepEligible(t *testing.T) {
	on, off := true, false
	base := config.RepairConfig{
		Par2RepairOnSweep:     true,
		Par2Repair:            &on,
		Par2RepairMode:        config.Par2RepairModeAutoAll,
		Par2RepairMinSegments: 8,
	}
	with := func(f func(*config.RepairConfig)) config.RepairConfig {
		c := base
		f(&c)
		return c
	}
	cases := []struct {
		name          string
		cfg           config.RepairConfig
		reason        string
		file, release int
		want          bool
	}{
		{"eligible", base, "usenet_segment_missing", 2, 2, true},
		{"setting off (the default)", with(func(c *config.RepairConfig) { c.Par2RepairOnSweep = false }), "usenet_segment_missing", 2, 2, false},
		{"par2 repair off", with(func(c *config.RepairConfig) { c.Par2Repair = &off }), "usenet_segment_missing", 2, 2, false},
		{"manual mode", with(func(c *config.RepairConfig) { c.Par2RepairMode = config.Par2RepairModeManual }), "usenet_segment_missing", 2, 2, false},
		{"threshold not reached", with(func(c *config.RepairConfig) { c.Par2RepairMode = config.Par2RepairModeAutoThreshold }), "usenet_segment_missing", 3, 7, false},
		{"threshold reached across the release", with(func(c *config.RepairConfig) { c.Par2RepairMode = config.Par2RepairModeAutoThreshold }), "usenet_segment_missing", 3, 8, true},
		// A decode or assembly failure has no dead segments for PAR2 to
		// rebuild; a pass would "complete" and hide a broken file.
		{"not a dead-segment failure", base, "ffprobe_decode_error", 2, 2, false},
		{"nothing pending for this file", base, "usenet_segment_missing", 0, 5, false},
	}
	for _, tc := range cases {
		if got := coldSweepEligible(tc.cfg, tc.reason, tc.file, tc.release); got != tc.want {
			t.Errorf("%s: coldSweepEligible = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestColdSweepWanted(t *testing.T) {
	on, off := true, false
	base := config.RepairConfig{Par2RepairOnSweep: true, Par2Repair: &on, Par2RepairMode: config.Par2RepairModeAutoAll}
	if !coldSweepWanted(base) {
		t.Errorf("sweep PAR2 on: coldSweepWanted = false, want true")
	}
	// Threshold mode still wants the count: the threshold is judged later,
	// against what the probe recorded.
	threshold := base
	threshold.Par2RepairMode = config.Par2RepairModeAutoThreshold
	if !coldSweepWanted(threshold) {
		t.Errorf("threshold mode: coldSweepWanted = false, want true")
	}
	for name, f := range map[string]func(*config.RepairConfig){
		"setting off":     func(c *config.RepairConfig) { c.Par2RepairOnSweep = false },
		"par2 repair off": func(c *config.RepairConfig) { c.Par2Repair = &off },
		"manual mode":     func(c *config.RepairConfig) { c.Par2RepairMode = config.Par2RepairModeManual },
	} {
		c := base
		f(&c)
		if coldSweepWanted(c) {
			t.Errorf("%s: coldSweepWanted = true, want false", name)
		}
	}
}

func TestBeyondPar2Recovery(t *testing.T) {
	cases := []struct {
		name            string
		dead, par2Bytes int64
		want            bool
	}{
		{"more than half a 2.5 GB file dead, 250 MB of PAR2", 1_400_000_000, 250_000_000, true},
		{"a few dead articles", 12_000_000, 250_000_000, false},
		{"as much dead as there is PAR2 data", 250_000_000, 250_000_000, false},
		// Neither unknown may skip a repair: the probe did not count, or the
		// PAR2 files' sizes are not on record.
		{"damage not counted", 0, 250_000_000, false},
		{"PAR2 size unknown", 1_400_000_000, 0, false},
	}
	for _, tc := range cases {
		if got := beyondPar2Recovery(tc.dead, tc.par2Bytes); got != tc.want {
			t.Errorf("%s: beyondPar2Recovery = %v, want %v", tc.name, got, tc.want)
		}
	}
}
