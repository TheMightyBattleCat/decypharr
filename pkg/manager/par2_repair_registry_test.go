package manager

import (
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// newTestPar2Repair builds a minimal, storage-backed Par2Repair + Repair
// pair for exercising the enqueue/registry wiring directly, without the
// heavy manager.New() init path (debrid clients, arr connections, a real
// usenet/NNTP client) that path requires. Sufficient for anything that
// doesn't reach into runJob's usenet-dependent body - the registry-gating
// logic in enqueue() itself has no usenet dependency.
func newTestPar2Repair(t *testing.T) (*Par2Repair, *Repair) {
	t.Helper()
	// logger.New (used below, and transitively by storage/other manager
	// pieces) reads config.Get(), which os.Exit(1)s if no config path was
	// ever set for this process - point it at a scratch dir so the very
	// first config.Get() in the test binary succeeds instead of aborting it.
	config.SetConfigPath(t.TempDir())

	strg, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("storage.NewStorage: %v", err)
	}
	t.Cleanup(func() { _ = strg.Close() })

	m := &Manager{storage: strg}
	repair := &Repair{
		manager:  m,
		logger:   logger.New("test-repair"),
		handlers: newRepairHandlerRegistry(defaultRepairHandlerTTL),
	}
	m.repair = repair

	p := &Par2Repair{
		manager:    m,
		repair:     repair,
		logger:     logger.New("test-par2-repair"),
		queued:     make(map[string]struct{}),
		deferred:   make(map[string]struct{}),
		queue:      make(chan string, par2QueueDepth),
		progress:   newPar2ProgressTracker(),
		urgentSet:  make(map[string]*urgentJob),
		urgentWake: make(chan struct{}, 1),
		running:    make(map[string]*runningJob),
	}
	m.par2Repair = p
	return p, repair
}

// TestPar2RepairEnqueueClaimsRegistryAndDedupsQueue proves Enqueue's
// registry claim is what makes a second call for the same nzbID a no-op:
// the underlying channel receives exactly one entry, not two, even though
// Enqueue was called twice.
func TestPar2RepairEnqueueClaimsRegistryAndDedupsQueue(t *testing.T) {
	p, repair := newTestPar2Repair(t)

	p.Enqueue("nzb1")
	kind, terminal, exists := repair.handlers.State("nzb1")
	if !exists || terminal || kind != handlerPar2Queued {
		t.Fatalf("after Enqueue: kind=%v terminal=%v exists=%v, want par2_queued/false/true", kind, terminal, exists)
	}

	// A second Enqueue for the same entry must not add a second item to the
	// channel - the registry claim (and the pre-existing p.queued dedup) must
	// both block it.
	p.Enqueue("nzb1")
	if len(p.queue) != 1 {
		t.Fatalf("p.queue has %d items after two Enqueue calls for the same entry, want 1 (no double-queueing)", len(p.queue))
	}
}

// TestPar2RepairEnqueueUrgentUsesThePriorityLane proves EnqueueUrgent lands
// its job on the URGENT lane's priority heap, not the normal queue, and that
// a subsequent normal Enqueue for the same entry is suppressed by
// handledByUrgent rather than double-queueing across lanes.
func TestPar2RepairEnqueueUrgentUsesThePriorityLane(t *testing.T) {
	p, _ := newTestPar2Repair(t)

	p.EnqueueUrgent("nzb1", 0)
	if !p.handledByUrgent("nzb1") {
		t.Fatalf("EnqueueUrgent must land nzb1 on the URGENT lane (handledByUrgent should report true)")
	}
	if len(p.queue) != 0 {
		t.Fatalf("queue has %d items, want 0 - EnqueueUrgent must use the urgent lane", len(p.queue))
	}

	// A normal Enqueue for the same entry, while the urgent claim is still
	// held, must not also queue it on the normal lane.
	p.Enqueue("nzb1")
	if len(p.queue) != 0 {
		t.Fatalf("queue has %d items after Enqueue raced an existing urgent claim, want 0 (no double-queueing across lanes)", len(p.queue))
	}
}

// TestPar2RepairAutoEnqueueNoOpsWhenPar2RepairDisabled proves AutoEnqueue
// never claims the handler registry while config.Repair.Par2Repair is
// disabled, regardless of Par2RepairMode. Claiming a par2_queued slot the
// worker's readyToRun() will then always refuse to run leaves a stale claim
// sitting forever, which permanently blocks HandlePlaybackFailure's
// TryAcquire(handlerRegrab) for that file - confirmed live: a padded
// segment claimed the slot, the worker correctly never ran it, and every
// later playback failure on that file silently no-opped with "entry
// already being handled by another repair mechanism" instead of ever
// re-grabbing it.
func TestPar2RepairAutoEnqueueNoOpsWhenPar2RepairDisabled(t *testing.T) {
	p, repair := newTestPar2Repair(t)

	cfg := config.Get()
	disabled := false
	cfg.Repair.Par2Repair = &disabled
	cfg.Repair.Par2RepairMode = config.Par2RepairModeAutoAll

	p.AutoEnqueue("nzb1", 5)

	if len(p.queue) != 0 {
		t.Fatalf("AutoEnqueue queued %d items while Par2Repair is disabled, want 0", len(p.queue))
	}
	if _, _, exists := repair.handlers.State("nzb1"); exists {
		t.Fatalf("AutoEnqueue claimed the handler registry while Par2Repair is disabled - this permanently blocks HandlePlaybackFailure's regrab path")
	}
}

// TestPar2RepairAutoEnqueueQueuesWhenPar2RepairEnabled is the control case
// for the fix above: proves AutoEnqueue still queues normally once
// Par2Repair is enabled.
func TestPar2RepairAutoEnqueueQueuesWhenPar2RepairEnabled(t *testing.T) {
	p, repair := newTestPar2Repair(t)

	cfg := config.Get()
	enabled := true
	cfg.Repair.Par2Repair = &enabled
	cfg.Repair.Par2RepairMode = config.Par2RepairModeAutoAll

	p.AutoEnqueue("nzb1", 5)

	if len(p.queue) != 1 {
		t.Fatalf("AutoEnqueue queued %d items while Par2Repair is enabled, want 1", len(p.queue))
	}
	if kind, _, exists := repair.handlers.State("nzb1"); !exists || kind != handlerPar2Queued {
		t.Fatalf("AutoEnqueue must claim the registry when Par2Repair is enabled: kind=%v exists=%v", kind, exists)
	}
}

// TestPar2RepairEnqueueSkippedWhenPersistedTerminal proves Enqueue never
// queues (and never claims the registry) for an entry storage already
// records as terminal - par2ShouldAutoEnqueue's existing durable gate, which
// enqueue() must still respect on top of the in-memory registry.
func TestPar2RepairEnqueueSkippedWhenPersistedTerminal(t *testing.T) {
	p, repair := newTestPar2Repair(t)

	if err := p.manager.storage.SavePar2RepairState(&storage.Par2RepairState{
		NzbID: "nzb1", Terminal: true, TerminalReason: "no PAR2 data available",
	}); err != nil {
		t.Fatalf("SavePar2RepairState: %v", err)
	}

	p.Enqueue("nzb1")
	if len(p.queue) != 0 {
		t.Fatalf("Enqueue must not queue an entry storage already marked terminal")
	}
	if _, _, exists := repair.handlers.State("nzb1"); exists {
		t.Fatalf("Enqueue must not claim the registry for an entry it declined to queue")
	}
}

// TestPar2RepairRunNowOverridesRegistryClaimAndClearsTerminal proves the
// manual "repair now" path force-claims the registry (Set, not TryAcquire)
// even over an existing terminal mark - the manual-override guarantee.
func TestPar2RepairRunNowOverridesRegistryClaimAndClearsTerminal(t *testing.T) {
	p, repair := newTestPar2Repair(t)
	repair.handlers.MarkTerminal("nzb1")

	// RunNow requires config.Repair.Par2RepairEnabled() (defaults true when
	// unset - see config.RepairConfig.Par2RepairEnabled) and spawns runJob in
	// its own goroutine; runJob will fail fast ("usenet client not
	// configured" - p.manager.usenet is nil here) and release the claim right
	// back via its own deferred registry handling. To observe the claim
	// Set() takes in RunNow itself (before that goroutine runs), race the
	// registry state against the p.queued bookkeeping RunNow sets
	// synchronously before returning.
	if err := p.RunNow("nzb1"); err != nil {
		t.Fatalf("RunNow: %v", err)
	}
	if repair.handlers.IsTerminal("nzb1") {
		t.Fatalf("RunNow must clear a terminal mark as part of its manual override")
	}

	p.wg.Wait() // let the spawned runJob goroutine finish and release its claim
	if _, _, exists := repair.handlers.State("nzb1"); exists {
		t.Fatalf("after RunNow's job finishes (usenet unconfigured -> immediate non-terminal exit), the registry claim should be released, not left dangling")
	}
}
