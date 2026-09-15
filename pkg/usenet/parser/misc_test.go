package parser

import (
	"testing"

	"github.com/Tensai75/nzbparser"
)

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

// TestDropOddSchemeVolumes_StrayPlainRarInPartSet replays Far from the Hurried
// Crowd KRaLiMaRKo: a 1-article "name.rar" posted beside the .partNNN.rar
// volumes sorted ahead of part001 and became the archive's first volume.
func TestDropOddSchemeVolumes_StrayPlainRarInPartSet(t *testing.T) {
	const base = "Far.from.the.Hurried.Crowd.2015.1080p.Blu-ray.Remux.AVC.DTS-HD.MA.5.1.-.KRaLiMaRKo"
	files := []nzbparser.NzbFile{
		postedVolume(2, base+".rar", 1, 10000),
		postedVolume(12, base+".part001.rar", 261, article),
		postedVolume(13, base+".part002.rar", 261, article),
		postedVolume(14, base+".part003.rar", 40, 5000),
	}
	kept, dropped := dropOddSchemeVolumes(files)
	if len(dropped) != 1 || dropped[0] != base+".rar" {
		t.Fatalf("dropped %v, want the stray %s.rar", dropped, base)
	}
	if len(kept) != 3 || kept[0].Filename != base+".part001.rar" {
		t.Fatalf("kept %d files starting %q, want the three .partNNN.rar volumes", len(kept), kept[0].Filename)
	}
}

func TestDropOddSchemeVolumes_KeepsOneSchemeObfuscatedNamesAndTies(t *testing.T) {
	cases := []struct {
		name  string
		files []string
	}{
		{"old style .rar and .rNN are one scheme", []string{"a.rar", "a.r00", "a.r01"}},
		{"new style only", []string{"a.part1.rar", "a.part2.rar"}},
		{"obfuscated names carry no scheme", []string{"a.part1.rar", "a.part2.rar", "0f3c9e1a"}},
		{"a tie has no majority", []string{"a.rar", "a.part1.rar"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var files []nzbparser.NzbFile
			for i, name := range tc.files {
				files = append(files, postedVolume(i+1, name, 2, 100))
			}
			if kept, dropped := dropOddSchemeVolumes(files); len(dropped) != 0 || len(kept) != len(files) {
				t.Fatalf("dropped %v, want nothing dropped", dropped)
			}
		})
	}
}
