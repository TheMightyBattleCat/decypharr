package manager

import (
	"context"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// A sweep stopped mid-entry (StopSchedule, StopRun) must not record the files
// it never probed as healthy, nor stamp the entry decode-verified.
func TestProbeCutShortRecordsNothing(t *testing.T) {
	results := []fileResult{
		{name: "E01", healthy: true},
		{name: "E02", healthy: true},
		{name: "E03", reason: "context_cancelled"},
		{name: "E04", reason: "context_cancelled"},
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	if !probeCutShort(cancelled, results) {
		t.Fatal("a probe stopped before E03/E04 was not detected")
	}
	if probeCutShort(context.Background(), results) {
		t.Fatal("live ctx reported as cut short")
	}
	if probeCutShort(cancelled, results[:2]) {
		t.Fatal("every file probed, then the ctx ended: not cut short")
	}

	verified := time.Now().Add(-48 * time.Hour)
	checked := time.Now().Add(-24 * time.Hour)
	h := &storage.EntryHealth{
		EntryName:        "Show.S01",
		Status:           storage.HealthRepairing,
		PreviousStatus:   storage.HealthHealthy,
		ActiveRunID:      "run-1",
		LastCheckedAt:    checked,
		DecodeVerifiedAt: verified,
	}
	markProbeInterrupted(h, storage.HealthHealthy)
	if h.Status != storage.HealthHealthy || h.ActiveRunID != "" {
		t.Fatalf("status=%v run=%q, want the previous status and no active run", h.Status, h.ActiveRunID)
	}
	if !h.Dirty || !h.IsDue(time.Now(), 7*24*time.Hour) {
		t.Fatal("an interrupted entry must be due for the next sweep")
	}
	if !h.LastCheckedAt.Equal(checked) || !h.DecodeVerifiedAt.Equal(verified) {
		t.Fatal("an interrupted probe changed LastCheckedAt or the decode stamp")
	}
}
