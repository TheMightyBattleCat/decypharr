package manager

import (
	"sync"
	"testing"
)

func TestPar2JobProgressStateSnapshotReflectsUpdates(t *testing.T) {
	s := newPar2JobProgressState("nzb-1", "entry-1")

	snap := s.Snapshot()
	if snap.Phase != Par2PhaseQueued {
		t.Fatalf("initial phase = %q, want %q", snap.Phase, Par2PhaseQueued)
	}
	if snap.NzbID != "nzb-1" || snap.EntryName != "entry-1" {
		t.Fatalf("Snapshot() = %+v, want nzb-1/entry-1", snap)
	}

	s.SetPhase(Par2PhaseStreamingIntact)
	s.SetEntryName("entry-2")
	s.SetLastError("boom")
	s.SetRecoveryVolsFetched(3)
	s.SetRecoverySlices(10, 20)
	s.SetIntactTotal(100)
	s.AddIntactRead(1)
	s.AddIntactRead(2)
	s.AddCacheBytes(512)
	s.AddUsenetBytes(1024)

	snap = s.Snapshot()
	if snap.Phase != Par2PhaseStreamingIntact {
		t.Errorf("Phase = %q, want %q", snap.Phase, Par2PhaseStreamingIntact)
	}
	if snap.EntryName != "entry-2" {
		t.Errorf("EntryName = %q, want entry-2", snap.EntryName)
	}
	if snap.LastError != "boom" {
		t.Errorf("LastError = %q, want boom", snap.LastError)
	}
	if snap.RecoveryVolsFetched != 3 {
		t.Errorf("RecoveryVolsFetched = %d, want 3", snap.RecoveryVolsFetched)
	}
	if snap.RecoverySlicesFetched != 10 || snap.RecoverySlicesNeeded != 20 {
		t.Errorf("RecoverySlicesFetched/Needed = %d/%d, want 10/20", snap.RecoverySlicesFetched, snap.RecoverySlicesNeeded)
	}
	if snap.IntactSlicesTotal != 100 {
		t.Errorf("IntactSlicesTotal = %d, want 100", snap.IntactSlicesTotal)
	}
	if snap.IntactSlicesRead != 3 {
		t.Errorf("IntactSlicesRead = %d, want 3", snap.IntactSlicesRead)
	}
	if snap.CacheBytes != 512 {
		t.Errorf("CacheBytes = %d, want 512", snap.CacheBytes)
	}
	if snap.UsenetBytes != 1024 {
		t.Errorf("UsenetBytes = %d, want 1024", snap.UsenetBytes)
	}
	if snap.StartedAt.IsZero() {
		t.Errorf("StartedAt is zero, want set")
	}
	if snap.UpdatedAt.IsZero() {
		t.Errorf("UpdatedAt is zero, want set")
	}
}

func TestPar2JobProgressStateNilReceiverIsSafe(t *testing.T) {
	var s *par2JobProgressState

	// None of these should panic on a nil receiver - callers (e.g.
	// cacheSlicedSource.progress in tests that don't wire a tracked job)
	// deliberately leave this nil.
	s.SetPhase(Par2PhaseFailed)
	s.SetEntryName("x")
	s.SetLastError("x")
	s.SetRecoveryVolsFetched(1)
	s.SetRecoverySlices(1, 2)
	s.SetIntactTotal(1)
	s.AddIntactRead(1)
	s.AddCacheBytes(1)
	s.AddUsenetBytes(1)

	if snap := s.Snapshot(); snap != (Par2JobProgress{}) {
		t.Errorf("Snapshot() on nil receiver = %+v, want zero value", snap)
	}
}

func TestPar2ProgressTrackerStartAndGet(t *testing.T) {
	tracker := newPar2ProgressTracker()

	if _, ok := tracker.Get("missing"); ok {
		t.Fatalf("Get(missing) ok = true, want false before any Start")
	}

	s := tracker.Start("nzb-1", "entry-1")
	s.SetPhase(Par2PhaseSolving)

	got, ok := tracker.Get("nzb-1")
	if !ok {
		t.Fatalf("Get(nzb-1) ok = false, want true after Start")
	}
	if got.Snapshot().Phase != Par2PhaseSolving {
		t.Errorf("Phase = %q, want %q", got.Snapshot().Phase, Par2PhaseSolving)
	}

	// Starting again for the same nzbID replaces the previous state.
	tracker.Start("nzb-1", "entry-1")
	got, _ = tracker.Get("nzb-1")
	if got.Snapshot().Phase != Par2PhaseQueued {
		t.Errorf("Phase after re-Start = %q, want %q (fresh state)", got.Snapshot().Phase, Par2PhaseQueued)
	}
}

func TestPar2JobProgressStateConcurrentUpdatesDoNotRace(t *testing.T) {
	s := newPar2JobProgressState("nzb-1", "entry-1")
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.AddIntactRead(1)
			s.AddCacheBytes(1)
			s.AddUsenetBytes(1)
			s.SetPhase(Par2PhaseStreamingIntact)
			_ = s.Snapshot()
		}()
	}
	wg.Wait()

	snap := s.Snapshot()
	if snap.IntactSlicesRead != 50 {
		t.Errorf("IntactSlicesRead = %d, want 50", snap.IntactSlicesRead)
	}
}
