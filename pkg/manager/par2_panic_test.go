package manager

import (
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

// A panic inside a pass is recovered: the process survives, the pass is
// recorded as a transient failure, and the run and handler claims are
// released. Before, every PAR2 worker ran runJob on a bare goroutine with no
// recover, so one bad release (e.g. a duplicate posting indexing past a
// slice) took the whole service down - and, with its pairing cached, did so
// again on every retry.
func TestRunJobRecoversPanic(t *testing.T) {
	p, repair := newTestPar2Repair(t)
	const nzbID = "nzb-panic"
	if err := p.manager.storage.AddOrUpdate(&storage.Entry{InfoHash: nzbID, Name: "Panic.Release"}); err != nil {
		t.Fatalf("store entry: %v", err)
	}
	// A zero Usenet has no stores behind it: the first thing the pass asks of
	// it dereferences nil.
	p.manager.usenet = &usenet.Usenet{}
	repair.handlers.Set(nzbID, handlerPar2Running)

	panicked := func() (r any) {
		defer func() { r = recover() }()
		p.runJob(nzbID, laneUrgent)
		return nil
	}()
	if panicked != nil {
		t.Fatalf("runJob let a panic escape: %v", panicked)
	}

	st, err := p.manager.storage.GetPar2RepairState(nzbID)
	if err != nil || st == nil {
		t.Fatalf("no repair state recorded after the panic: %v", err)
	}
	if st.Terminal || st.NextRetryAt.IsZero() {
		t.Fatalf("state = %+v, want a transient back-off", st)
	}
	p.runningMu.Lock()
	_, stillRunning := p.running[nzbID]
	p.runningMu.Unlock()
	if stillRunning {
		t.Fatal("run claim not released after the panic")
	}
}
