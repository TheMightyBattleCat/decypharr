package manager

import (
	"context"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

const (
	fixTestEntry = "Clarity 2023 BluRay 1080p DTS-HD MA 5 1 AVC REMUX-FraMeSToR"
	fixTestFile  = fixTestEntry + ".mkv"
	fixTestHash  = "1328cb84-b83d-4536-8673-7ec2e3960f77"
)

// fixTestCandidate is what collectArrMediaCandidates returns for an entry the
// Arr still references, currently served by the upload servedHash.
func fixTestCandidate(servedHash string) *candidate {
	return &candidate{
		name: fixTestEntry,
		item: &storage.EntryItem{
			Name:  fixTestEntry,
			Files: map[string]*storage.File{fixTestFile: {Name: fixTestFile, InfoHash: servedHash}},
		},
		arrName: "radarr",
		arrKind: storage.ArrKindRadarr,
		contentMap: map[string]arr.ContentFile{
			fixTestFile: {Id: 3884, FileId: 17884, TargetPath: fixTestFile, Path: "/movies/Clarity (2023)/Clarity.mkv", Size: 22625602772},
		},
	}
}

// fixTestBareHealth is a broken entry recorded without Arr identifiers, the
// way a 430 found by a verification read stores it.
func fixTestBareHealth(brokenHash string) *storage.EntryHealth {
	return &storage.EntryHealth{
		EntryName: fixTestEntry,
		Status:    storage.HealthBroken,
		BrokenFiles: []storage.BrokenFile{{
			EntryName: fixTestEntry,
			FileName:  fixTestFile,
			InfoHash:  brokenHash,
			Reason:    "dead_segment_detected: NNTP 430 during verification read",
		}},
		BrokenCount: 1,
	}
}

func TestFillBrokenArrContextFillsTheSameUpload(t *testing.T) {
	h := fixTestBareHealth(fixTestHash)
	if n := fillBrokenArrContext(h, fixTestCandidate(fixTestHash)); n != 1 {
		t.Fatalf("filled %d files, want 1", n)
	}
	bf := h.BrokenFiles[0]
	if !hasArrFile(bf) || bf.ArrName != "radarr" || bf.ArrKind != storage.ArrKindRadarr ||
		bf.MediaID != 3884 || bf.ArrFileID != 17884 || bf.SourcePath != "/movies/Clarity (2023)/Clarity.mkv" || bf.TargetPath != fixTestFile {
		t.Fatalf("broken file not filled from the Arr: %+v", bf)
	}
}

func TestFillBrokenArrContextLeavesFilesItCannotPin(t *testing.T) {
	cases := []struct {
		name   string
		health *storage.EntryHealth
		cand   *candidate
	}{
		{"replacement under the same name", fixTestBareHealth(fixTestHash), fixTestCandidate("another-upload")},
		{"broken file without an InfoHash", fixTestBareHealth(""), fixTestCandidate(fixTestHash)},
		{"entry the Arr does not reference", fixTestBareHealth(fixTestHash), nil},
		{"file the Arr does not list", fixTestBareHealth(fixTestHash), func() *candidate {
			c := fixTestCandidate(fixTestHash)
			c.contentMap = map[string]arr.ContentFile{"other.mkv": {Id: 1, FileId: 2}}
			return c
		}()},
		{"Arr file without a file id", fixTestBareHealth(fixTestHash), func() *candidate {
			c := fixTestCandidate(fixTestHash)
			c.contentMap[fixTestFile] = arr.ContentFile{Id: 3884}
			return c
		}()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if n := fillBrokenArrContext(tc.health, tc.cand); n != 0 {
				t.Fatalf("filled %d files, want 0", n)
			}
			if hasArrFile(tc.health.BrokenFiles[0]) {
				t.Fatalf("broken file gained an Arr file: %+v", tc.health.BrokenFiles[0])
			}
		})
	}
}

func TestFillBrokenArrContextKeepsExistingArrFile(t *testing.T) {
	h := fixTestBareHealth(fixTestHash)
	h.BrokenFiles[0].ArrName = "bh-radarr"
	h.BrokenFiles[0].ArrFileID = 111
	if n := fillBrokenArrContext(h, fixTestCandidate(fixTestHash)); n != 0 {
		t.Fatalf("filled %d files, want 0", n)
	}
	if bf := h.BrokenFiles[0]; bf.ArrName != "bh-radarr" || bf.ArrFileID != 111 {
		t.Fatalf("existing Arr file overwritten: %+v", bf)
	}
}

// newTestRepairForFix is newTestRepairForClaims plus the empty Arr registry
// FixBroken's background pass reads.
func newTestRepairForFix(t *testing.T) *Repair {
	t.Helper()
	repair := newTestRepairForClaims(t)
	repair.manager.arr = arr.NewStorage()
	repair.parentCtx = context.Background()
	return repair
}

// TestFixBrokenStartsARunForAnEntryWithoutArrFile covers the dead Fix button:
// an entry marked broken without Arr identifiers used to be rejected with
// "no fixable broken entries" and never produced a run.
func TestFixBrokenStartsARunForAnEntryWithoutArrFile(t *testing.T) {
	repair := newTestRepairForFix(t)
	if err := repair.manager.storage.SaveEntryHealth(fixTestBareHealth(fixTestHash)); err != nil {
		t.Fatalf("SaveEntryHealth: %v", err)
	}

	run, err := repair.FixBroken(context.Background(), []string{fixTestEntry})
	if err != nil {
		t.Fatalf("FixBroken: %v", err)
	}
	repair.runWG.Wait()

	got, err := repair.manager.storage.GetRepairRun(run.ID)
	if err != nil || got == nil {
		t.Fatalf("GetRepairRun: %v", err)
	}
	if got.Status != storage.RepairRunCompleted {
		t.Fatalf("run status %q, want %q", got.Status, storage.RepairRunCompleted)
	}
	if got.Stats.Candidates != 1 || got.Stats.Repaired != 0 {
		t.Fatalf("run stats %+v, want 1 candidate and 0 repaired", got.Stats)
	}
	// With no Arr to own the file, the run must say so rather than finish
	// silently, and the broken record must stay for the user to act on.
	if !strings.Contains(got.Error, fixTestEntry) || !strings.Contains(got.Error, "no Arr owns") {
		t.Fatalf("run error %q does not name the entry no Arr owns", got.Error)
	}
	h, err := repair.manager.storage.GetEntryHealth(fixTestEntry)
	if err != nil || h == nil || h.Status != storage.HealthBroken {
		t.Fatalf("broken record changed: %+v, %v", h, err)
	}
}

func TestFixBrokenWithNothingBrokenIsRejected(t *testing.T) {
	repair := newTestRepairForFix(t)
	if _, err := repair.FixBroken(context.Background(), nil); err == nil {
		t.Fatalf("FixBroken with no broken entries returned no error")
	}
	if _, err := repair.FixBroken(context.Background(), []string{"__no_such_entry__"}); err == nil {
		t.Fatalf("FixBroken for an unknown entry returned no error")
	}
}

func TestUnownedSummary(t *testing.T) {
	if got := unownedSummary(nil); got != "" {
		t.Fatalf("unownedSummary(nil) = %q, want empty", got)
	}
	if got := unownedSummary([]string{"A"}); !strings.HasPrefix(got, "A: ") {
		t.Fatalf("unownedSummary(one) = %q", got)
	}
	if got := unownedSummary([]string{"A", "B"}); !strings.HasPrefix(got, "2 entries") || !strings.Contains(got, "first: A") {
		t.Fatalf("unownedSummary(two) = %q", got)
	}
}
