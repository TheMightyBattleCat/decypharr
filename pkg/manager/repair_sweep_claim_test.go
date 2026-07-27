package manager

import (
	"sync"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// newTestRepairForClaims builds a minimal, storage-backed Repair for
// exercising finalizeBrokenEntry's registry interaction directly, without the
// heavy manager.New() init path. Mirrors newTestPar2Repair's rationale
// (par2_repair_registry_test.go).
func newTestRepairForClaims(t *testing.T) *Repair {
	t.Helper()
	config.SetConfigPath(t.TempDir())

	strg, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("storage.NewStorage: %v", err)
	}
	t.Cleanup(func() { _ = strg.Close() })

	m := &Manager{storage: strg}
	repair := &Repair{
		manager:     m,
		logger:      logger.New("test-repair"),
		handlers:    newRepairHandlerRegistry(defaultRepairHandlerTTL),
		regrabGuard: newRegrabGuard(),
	}
	m.repair = repair
	return repair
}

// TestFinalizeBrokenEntryReleasesRegrabClaimWithoutAutoRepair proves the fix
// for the third divergence found auditing the playback-failure path against
// the sweep-probe path: routeAutoRepair claims the handlerRegrab slot for a
// Usenet segment-missing failure UNCONDITIONALLY (regardless of autoRepair),
// but probeAndHealCandidates used to only release it inside an
// `if autoRepair` block. A pure health-check sweep (autoRepair=false, an
// intentional, documented mode - see executeSweep) therefore claimed the
// slot and never released it, stranding it for the registry's full TTL and
// silently blocking HandlePlaybackFailure's re-grab and PAR2's enqueue for
// that same file - the same stranded-claim shape cae264b fixed for
// AutoEnqueue, just reached via the sweep instead of the padding path.
func TestFinalizeBrokenEntryReleasesRegrabClaimWithoutAutoRepair(t *testing.T) {
	repair := newTestRepairForClaims(t)

	nzbID := "nzb-segment-missing"
	// Simulate what routeAutoRepair does while probing, independent of
	// autoRepair - see repair_sweep.go's routeAutoRepair.
	if !repair.handlers.TryAcquire(nzbID, handlerRegrab) {
		t.Fatalf("TryAcquire: expected to claim a free slot")
	}

	h := &storage.EntryHealth{
		EntryName: "My.Show.S01E01",
		Status:    storage.HealthBroken,
		BrokenFiles: []storage.BrokenFile{
			{EntryName: "My.Show.S01E01", FileName: "My.Show.S01E01.mkv", InfoHash: nzbID},
		},
	}
	run := &storage.RepairRun{ID: "test-run"}
	var statsMu sync.Mutex

	repair.finalizeBrokenEntry(nil, run, &statsMu, h.EntryName, h, false)

	if _, _, exists := repair.handlers.State(nzbID); exists {
		t.Fatalf("finalizeBrokenEntry(autoRepair=false) left the handlerRegrab claim in place - " +
			"this permanently blocks HandlePlaybackFailure's re-grab and PAR2's enqueue for this file " +
			"until the registry's TTL reaps it")
	}
}

// TestFinalizeBrokenEntryReleasesRegrabClaimWithAutoRepair is the control
// case: with autoRepair on, the claim must still be released (unchanged
// behavior from before the fix).
func TestFinalizeBrokenEntryReleasesRegrabClaimWithAutoRepair(t *testing.T) {
	repair := newTestRepairForClaims(t)

	nzbID := "nzb-segment-missing"
	if !repair.handlers.TryAcquire(nzbID, handlerRegrab) {
		t.Fatalf("TryAcquire: expected to claim a free slot")
	}

	// No ArrName/ArrFileID set, so healBrokenEntry's byArr grouping is empty
	// and it no-ops before ever needing a real Arr - only the registry
	// release is under test here.
	h := &storage.EntryHealth{
		EntryName: "My.Show.S01E01",
		Status:    storage.HealthBroken,
		BrokenFiles: []storage.BrokenFile{
			{EntryName: "My.Show.S01E01", FileName: "My.Show.S01E01.mkv", InfoHash: nzbID},
		},
	}
	run := &storage.RepairRun{ID: "test-run"}
	var statsMu sync.Mutex

	repair.finalizeBrokenEntry(nil, run, &statsMu, h.EntryName, h, true)

	if _, _, exists := repair.handlers.State(nzbID); exists {
		t.Fatalf("finalizeBrokenEntry(autoRepair=true) left the handlerRegrab claim in place, want released")
	}
}
