package parser

import "testing"

// TestGetRARVolumeOrder_Schemes locks getRARVolumeOrder's contract across all
// naming schemes it classifies, so the .NNN addition can't silently perturb
// the existing .rar/.rNN/.partNN.rar orderings that RARParser.Process's
// plain-path sort and the 7z path's sortRARFilesByVolumeOrder both depend on.
func TestGetRARVolumeOrder_Schemes(t *testing.T) {
	cases := []struct {
		name     string
		filename string
		want     int
	}{
		{"old-style base .rar", "movie.rar", 0},
		{"old-style continuation .r00", "movie.r00", 1},
		{"old-style continuation .r14", "movie.r14", 15},
		{"new-style .part01.rar", "movie.part01.rar", 1},
		{"new-style .part02.rar", "movie.part02.rar", 2},
		{"numeric-suffix .001", "movie.rar.001", 1},
		{"numeric-suffix .002", "movie.rar.002", 2},
		{"unknown extension", "movie.mkv", 999999},
		{"unknown numeric-looking but non-digit suffix", "movie.rXX", 999999},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := getRARVolumeOrder(tc.filename)
			if got != tc.want {
				t.Errorf("getRARVolumeOrder(%q) = %d, want %d", tc.filename, got, tc.want)
			}
		})
	}
}
