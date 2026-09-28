package manager

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// newApplyConfigRepair builds a started Repair with a sweep in flight: run
// "run-1" is persisted as running and cancelRun counts its calls.
func newApplyConfigRepair(t *testing.T) (*Repair, *storage.Storage, *atomic.Int32) {
	t.Helper()
	m, strg := newTestManagerForReapVerdict(t)
	sched, err := gocron.NewScheduler()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sched.Shutdown() })
	m.scheduler = sched
	r := &Repair{manager: m, scheduler: sched, logger: zerolog.Nop(), parentCtx: context.Background()}
	m.repair = r

	live := config.Get()
	saved := live.Repair
	t.Cleanup(func() { config.Get().Repair = saved })
	live.Repair.Enabled = true
	live.Repair.Schedule = "04:00"
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	run := &storage.RepairRun{ID: "run-1", Status: storage.RepairRunRunning, StartedAt: time.Now()}
	if err := strg.SaveRepairRun(run); err != nil {
		t.Fatal(err)
	}
	var cancels atomic.Int32
	r.mu.Lock()
	r.activeRunID = run.ID
	r.cancelRun = func() { cancels.Add(1) }
	r.mu.Unlock()
	return r, strg, &cancels
}

// Saving settings while a sweep runs must reschedule, not kill the sweep: on
// a production install every Settings save (editing an Arr, say) cancelled the running
// sweep and recorded it as "interrupted by restart".
func TestApplyConfigKeepsRunningSweep(t *testing.T) {
	r, strg, cancels := newApplyConfigRepair(t)

	config.Get().Repair.Schedule = "05:00"
	if err := r.ApplyConfig(); err != nil {
		t.Fatalf("ApplyConfig: %v", err)
	}

	if n := cancels.Load(); n != 0 {
		t.Fatalf("settings save cancelled the running sweep (%d cancel calls)", n)
	}
	got, err := strg.GetRepairRun("run-1")
	if err != nil || got == nil || got.Status != storage.RepairRunRunning {
		t.Fatalf("run after ApplyConfig = %+v, %v; want still running", got, err)
	}
	if n := len(r.scheduler.Jobs()); n != 1 {
		t.Fatalf("scheduled jobs = %d, want 1 (the rescheduled sweep)", n)
	}
}

// Turning Repair off still stops the scheduled sweep that is running.
func TestApplyConfigDisableStopsRunningSweep(t *testing.T) {
	r, strg, cancels := newApplyConfigRepair(t)

	config.Get().Repair.Enabled = false
	if err := r.ApplyConfig(); err != nil {
		t.Fatalf("ApplyConfig: %v", err)
	}

	if n := cancels.Load(); n != 1 {
		t.Fatalf("cancel calls = %d, want 1", n)
	}
	got, err := strg.GetRepairRun("run-1")
	if err != nil || got == nil || got.Status != storage.RepairRunCancelled {
		t.Fatalf("run after disabling = %+v, %v; want cancelled", got, err)
	}
	if n := len(r.scheduler.Jobs()); n != 0 {
		t.Fatalf("scheduled jobs = %d, want 0", n)
	}
}

// A settings save no longer restarts a running sweep, so the sweep must
// re-check Auto-repair itself: unticking it mid-sweep stops further deletes
// and re-searches. A "Run now" that asked for auto-repair keeps it.
func TestSweepRechecksAutoRepairBeforeHealing(t *testing.T) {
	m, _ := newTestManagerForReapVerdict(t)
	r := &Repair{manager: m, logger: zerolog.Nop(), parentCtx: context.Background()}
	live := config.Get()
	saved := live.Repair
	t.Cleanup(func() { config.Get().Repair = saved })

	fromSettings := context.WithValue(context.Background(), autoRepairFollowsConfigKey{}, true)
	live.Repair.AutoRepair = true
	if !r.autoRepairStillOn(fromSettings, true) {
		t.Fatal("auto-repair on in the settings: the sweep should heal")
	}
	live.Repair.AutoRepair = false
	if r.autoRepairStillOn(fromSettings, true) {
		t.Fatal("auto-repair turned off mid-sweep: the sweep kept healing")
	}
	// A per-run override is honoured as given.
	if !r.autoRepairStillOn(context.Background(), true) {
		t.Fatal("a Run now override for auto-repair was dropped")
	}
	if r.autoRepairStillOn(context.Background(), false) || r.autoRepairStillOn(nil, false) {
		t.Fatal("a sweep that started without auto-repair must never heal")
	}
}
