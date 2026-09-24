package manager

import (
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

func TestEntryDamageAndTerminalMarksHealthDirty(t *testing.T) {
	p, repair := newTestPar2Repair(t)
	m := repair.manager
	store, err := overlay.NewStore(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("overlay.NewStore: %v", err)
	}
	m.usenet = usenet.NewWithOverlayForTest(store)

	const nzbID, name, file = "nzb-h", "Show.S01E01", "Show.S01E01.mkv"
	if err := m.storage.AddOrUpdate(&storage.Entry{
		InfoHash: nzbID, Name: name, Protocol: config.ProtocolNZB,
		Files: map[string]*storage.File{file: {Name: file, InfoHash: nzbID, Size: 1 << 20, AddedOn: time.Now()}},
	}); err != nil {
		t.Fatalf("AddOrUpdate: %v", err)
	}
	if err := m.storage.SaveEntryHealth(&storage.EntryHealth{EntryName: name, Status: storage.HealthHealthy}); err != nil {
		t.Fatalf("SaveEntryHealth: %v", err)
	}

	if d := m.EntryDamage(name); d.PendingDeadSegments != 0 || d.Par2Terminal {
		t.Fatalf("clean entry: EntryDamage = %+v", d)
	}
	if err := store.RecordDead(nzbID, file, 4, "<a@test>", 1024); err != nil {
		t.Fatalf("RecordDead: %v", err)
	}
	if d := m.EntryDamage(name); d.PendingDeadSegments != 1 || d.Par2Terminal {
		t.Fatalf("after a dead segment: EntryDamage = %+v, want 1 pending, not terminal", d)
	}

	// A terminal PAR2 verdict marks the record dirty, so the next sweep
	// re-probes it rather than waiting out the recheck interval.
	class := p.recordPar2Outcome(nzbID, errors.New("no PAR2 data available for this release"), 0)
	if !class.terminal {
		t.Fatalf("setup: the error did not classify terminal")
	}
	h, err := m.storage.GetEntryHealth(name)
	if err != nil || h == nil || !h.Dirty || h.DirtyReason != "par2_terminal" {
		t.Fatalf("health after terminal = %+v, %v; want dirty with reason par2_terminal", h, err)
	}
	if d := m.EntryDamage(name); !d.Par2Terminal {
		t.Fatalf("EntryDamage = %+v, want Par2Terminal", d)
	}
}
