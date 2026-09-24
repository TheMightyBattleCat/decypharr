package manager

import (
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// Obfuscated PAR2 names carry no .volN+M marker, so every file is census-ed
// as an index file and the name-only coverage check saw 0 recovery slices -
// par2Usable then sent playback damage straight to a re-grab.
func TestEstimateUnnamedRecovery(t *testing.T) {
	obfuscated := []storage.Par2FileRef{
		{Name: "a8f3e2b1c4d5.par2", Size: 40_000},
		{Name: "0f9e8d7c6b5a.par2", Size: 4_040_000},  // 1 slice
		{Name: "1a2b3c4d5e6f.par2", Size: 8_040_000},  // 2 slices
		{Name: "9f8e7d6c5b4a.par2", Size: 16_040_000}, // 4 slices
	}
	if got := estimateUnnamedRecovery(obfuscated); got != 7 {
		t.Errorf("obfuscated set: %d, want 7", got)
	}

	// A season pack's per-episode index files are similar sizes: none is a
	// recovery volume.
	indexes := []storage.Par2FileRef{
		{Name: "Show.S01E01.par2", Size: 40_000},
		{Name: "Show.S01E02.par2", Size: 43_500},
		{Name: "Show.S01E03.par2", Size: 41_200},
	}
	if got := estimateUnnamedRecovery(indexes); got != 0 {
		t.Errorf("index files only: %d, want 0", got)
	}

	if estimateUnnamedRecovery(nil) != 0 || estimateUnnamedRecovery([]storage.Par2FileRef{{Size: 0}}) != 0 {
		t.Error("empty input")
	}
}
