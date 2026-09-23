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
