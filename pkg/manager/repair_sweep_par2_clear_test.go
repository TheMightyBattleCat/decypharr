package manager

import (
	"errors"
	"sync"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// A completed sweep PAR2 pass clears only the files it could have repaired:
// missing segments with nothing left pending in the overlay. In a season pack
// stored as one NZB, episode 3 (missing articles, repaired) must not take
// episode 7 (broken for another reason) off the broken list with it, and the entry must
// not be saved healthy while episode 7 is still broken.
func TestApplySweepPar2ResultClearsOnlyRepairedMissingSegments(t *testing.T) {
	const hash = "nzb-pack"
	cases := []struct {
		name         string
		pending      map[string][]overlay.DeadSegment
		pendingErr   error
		wantBroken   []string
		wantCredited bool
	}{
		{
			name:         "other fault in the same release stays broken",
			pending:      map[string][]overlay.DeadSegment{},
			wantBroken:   []string{"S01E07.mkv"},
			wantCredited: true,
		},
		{
			name:         "missing-segment file still pending stays broken",
			pending:      map[string][]overlay.DeadSegment{"S01E03.mkv": {{}}},
			wantBroken:   []string{"S01E03.mkv", "S01E07.mkv"},
			wantCredited: false,
		},
		{
			name:         "overlay unreadable: nothing is cleared",
			pendingErr:   errors.New("overlay down"),
			wantBroken:   []string{"S01E03.mkv", "S01E07.mkv"},
			wantCredited: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			repair := newTestRepairForFix(t)
			h := &storage.EntryHealth{
				EntryName: "Pack",
				Status:    storage.HealthBroken,
				FileCount: 10,
				BrokenFiles: []storage.BrokenFile{
					{FileName: "S01E03.mkv", Reason: "usenet_segment_missing", InfoHash: hash},
					{FileName: "S01E07.mkv", Reason: "usenet_probe_error", InfoHash: hash},
				},
			}
			h.BrokenCount = len(h.BrokenFiles)
			pendingAfter := func(nzbID string) (map[string][]overlay.DeadSegment, error) {
				if nzbID != hash {
					t.Fatalf("pending read for %q, want %q", nzbID, hash)
				}
				return c.pending, c.pendingErr
			}
			var mu sync.Mutex
			run := &storage.RepairRun{}
			credited, resolved := repair.applySweepPar2Result(run, &mu, "Pack", h, map[string]struct{}{hash: {}}, pendingAfter)

			if resolved {
				t.Fatal("resolved = true, want false: a file is still broken")
			}
			if credited != c.wantCredited {
				t.Fatalf("credited = %v, want %v", credited, c.wantCredited)
			}
			var got []string
			for _, bf := range h.BrokenFiles {
				got = append(got, bf.FileName)
			}
			if len(got) != len(c.wantBroken) {
				t.Fatalf("broken files %v, want %v", got, c.wantBroken)
			}
			for i := range got {
				if got[i] != c.wantBroken[i] {
					t.Fatalf("broken files %v, want %v", got, c.wantBroken)
				}
			}
			if h.BrokenCount != len(c.wantBroken) {
				t.Fatalf("BrokenCount = %d, want %d", h.BrokenCount, len(c.wantBroken))
			}
			if h.Status != storage.HealthBroken {
				t.Fatalf("status = %v, want broken", h.Status)
			}
			if saved, err := repair.manager.storage.GetEntryHealth("Pack"); err == nil && saved != nil && saved.Status == storage.HealthHealthy {
				t.Fatal("entry saved healthy while a file is still broken")
			}
			wantRepaired := 0
			if c.wantCredited {
				wantRepaired = 1
			}
			if run.Stats.Repaired != wantRepaired {
				t.Fatalf("Stats.Repaired = %d, want %d", run.Stats.Repaired, wantRepaired)
			}
		})
	}
}

// When every broken file was missing segments and the pass left nothing
// pending, the entry is resolved and saved healthy, as before.
func TestApplySweepPar2ResultResolvesWhenAllRepaired(t *testing.T) {
	const hash = "nzb-pack"
	repair := newTestRepairForFix(t)
	h := &storage.EntryHealth{
		EntryName: "Pack",
		Status:    storage.HealthBroken,
		FileCount: 10,
		BrokenFiles: []storage.BrokenFile{
			{FileName: "S01E03.mkv", Reason: "usenet_segment_missing", InfoHash: hash},
			{FileName: "S01E04.mkv", Reason: "usenet_segment_missing", InfoHash: hash},
		},
	}
	h.BrokenCount = len(h.BrokenFiles)
	pendingAfter := func(string) (map[string][]overlay.DeadSegment, error) {
		return map[string][]overlay.DeadSegment{"S01E03.mkv": nil}, nil
	}
	var mu sync.Mutex
	credited, resolved := repair.applySweepPar2Result(&storage.RepairRun{}, &mu, "Pack", h, map[string]struct{}{hash: {}}, pendingAfter)
	if !credited || !resolved {
		t.Fatalf("credited=%v resolved=%v, want both true", credited, resolved)
	}
	if len(h.BrokenFiles) != 0 || h.Status != storage.HealthHealthy {
		t.Fatalf("broken=%d status=%v, want 0 and healthy", len(h.BrokenFiles), h.Status)
	}
}
