package manager

import (
	"context"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// finishRecheck stamps the record with the run that ended and wakes waiters;
// until then WaitRecheck times out.
func TestFinishRecheckStampsRecordAndWakesWaiters(t *testing.T) {
	repair := newTestRepairForFix(t)
	if err := repair.manager.storage.SaveEntryHealth(&storage.EntryHealth{
		EntryName: "Show", Status: storage.HealthHealthy, ActiveRunID: "recheck-Show-1",
	}); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	repair.rechecks.Store("recheck-Show-1", done)

	if repair.WaitRecheck(context.Background(), "recheck-Show-1", 20*time.Millisecond) {
		t.Fatal("WaitRecheck reported a running recheck as ended")
	}
	woke := make(chan bool, 1)
	go func() { woke <- repair.WaitRecheck(context.Background(), "recheck-Show-1", 5*time.Second) }()

	repair.finishRecheck("Show", "recheck-Show-1", done)

	if !<-woke {
		t.Fatal("WaitRecheck did not see the recheck end")
	}
	h := health(t, repair, "Show")
	if h.LastRecheckRunID != "recheck-Show-1" || h.LastRecheckFinishedAt.IsZero() || h.ActiveRunID != "" {
		t.Fatalf("record not stamped: run=%q finished=%v active=%q", h.LastRecheckRunID, h.LastRecheckFinishedAt, h.ActiveRunID)
	}
}

// A recheck that removed the entry's record (superseded and cleaned up) must
// not write a fresh record back; the caller sees the record gone.
func TestFinishRecheckDoesNotRecreateARemovedRecord(t *testing.T) {
	repair := newTestRepairForFix(t)
	done := make(chan struct{})
	repair.rechecks.Store("recheck-Gone-1", done)
	repair.finishRecheck("Gone", "recheck-Gone-1", done)
	if h, err := repair.manager.storage.GetEntryHealth("Gone"); err == nil && h != nil {
		t.Fatalf("finishRecheck created a record: %+v", h)
	}
	if !repair.WaitRecheck(context.Background(), "recheck-Gone-1", time.Second) {
		t.Fatal("WaitRecheck did not see the recheck end")
	}
}

func TestWaitRecheckUnknownRun(t *testing.T) {
	repair := newTestRepairForFix(t)
	if repair.WaitRecheck(context.Background(), "recheck-nope-1", 10*time.Millisecond) {
		t.Fatal("unknown run reported as ended")
	}
}

// Two rechecks of one entry get different run IDs, and the record names the
// one that ended.
func TestRecheckEntryRunIDsAreUnique(t *testing.T) {
	repair := newTestRepairForFix(t)
	e := torrentEntry("h1", "Show", "")
	if err := repair.manager.storage.AddOrUpdate(e); err != nil {
		t.Fatal(err)
	}
	first, err := repair.RecheckEntry(context.Background(), "Show", false)
	if err != nil {
		t.Fatalf("RecheckEntry: %v", err)
	}
	if !repair.WaitRecheck(context.Background(), first.ActiveRunID, 30*time.Second) {
		t.Fatal("first recheck did not end")
	}
	second, err := repair.RecheckEntry(context.Background(), "Show", false)
	if err != nil {
		t.Fatalf("RecheckEntry: %v", err)
	}
	if !repair.WaitRecheck(context.Background(), second.ActiveRunID, 30*time.Second) {
		t.Fatal("second recheck did not end")
	}
	if first.ActiveRunID == second.ActiveRunID {
		t.Fatalf("both rechecks got run ID %q", first.ActiveRunID)
	}
	if h := health(t, repair, "Show"); h.LastRecheckRunID != second.ActiveRunID {
		t.Fatalf("record names run %q, want %q", h.LastRecheckRunID, second.ActiveRunID)
	}
}
