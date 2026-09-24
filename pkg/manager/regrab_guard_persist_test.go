package manager

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

func newPersistedGuard(t *testing.T, st *storage.Storage, now time.Time) *regrabGuard {
	t.Helper()
	g := newRegrabGuard()
	g.nowFn = func() time.Time { return now }
	g.attach(st, zerolog.Nop())
	return g
}

// A tripped guard survives a restart (it used to reset on every one - the production install
// restarts several times a day) and expires after regrabGuardTerminalTTL.
func TestRegrabGuardTerminalSurvivesRestart(t *testing.T) {
	st, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	start := time.Date(2026, 9, 18, 23, 10, 0, 0, time.UTC)
	const id = "arr:radarr:42:0"

	g := newPersistedGuard(t, st, start)
	for _, rel := range []string{"Release.A", "Release.B"} {
		if ok, _, _ := g.checkAndRecord(id, rel); !ok {
			t.Fatalf("attempt for %s refused", rel)
		}
	}
	if ok, _, first := g.checkAndRecord(id, "Release.C"); ok || !first {
		t.Fatalf("third attempt: allowed=%v firstTrip=%v, want a trip", ok, first)
	}

	restarted := newPersistedGuard(t, st, start.Add(time.Hour))
	if ok, reason, _ := restarted.checkAndRecord(id, "Release.D"); ok || reason != regrabGuardTerminalReason {
		t.Fatalf("after restart: allowed=%v reason=%q, want still terminal", ok, reason)
	}

	later := newPersistedGuard(t, st, start.Add(regrabGuardTerminalTTL+time.Hour))
	if ok, _, _ := later.checkAndRecord(id, "Release.E"); !ok {
		t.Fatal("terminal mark did not expire after regrabGuardTerminalTTL")
	}
}

// clear (a manual action) removes the persisted record too.
func TestRegrabGuardClearRemovesPersisted(t *testing.T) {
	st, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	now := time.Now()
	g := newPersistedGuard(t, st, now)
	g.recordKeepRelease("id", "Rel")
	g.clear("id")
	if newPersistedGuard(t, st, now).keepReleaseRepeated("id", "Rel") {
		t.Fatal("cleared record came back after a restart")
	}
}

func TestKeepReleaseRepeatedAcrossRestart(t *testing.T) {
	st, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	night1 := time.Date(2026, 9, 18, 23, 11, 47, 0, time.UTC)
	const id, rel = "arr:radarr:7:0", "8-Bit-Midwinter.2021.REPACK.1080p.WEB.H264-PECULATE"

	g := newPersistedGuard(t, st, night1)
	if g.keepReleaseRepeated(id, rel) {
		t.Fatal("first keep-release re-grab refused")
	}
	g.recordKeepRelease(id, rel)

	// The next night's sweep, just over 24 h later, after a restart.
	night2 := newPersistedGuard(t, st, night1.Add(24*time.Hour+3*time.Second))
	if !night2.keepReleaseRepeated(id, rel) {
		t.Fatal("second keep-release re-grab of the same release allowed")
	}
	if night2.keepReleaseRepeated(id, "Another.Release") {
		t.Fatal("a different release was refused")
	}
	if newPersistedGuard(t, st, night1.Add(keepReleaseRetryWindow+time.Hour)).keepReleaseRepeated(id, rel) {
		t.Fatal("keep-release record did not expire")
	}
}

// The sweep path: night 1 re-grabbed the release keeping it; after a restart,
// night 2 finds the same fault and must mark the entry for a manual replace
// instead of re-grabbing the same release again.
func TestSweepDoesNotRepeatKeepReleaseRegrab(t *testing.T) {
	m, _ := newTestManagerForReap(t)
	const name = "8-Bit-Midwinter.2021.REPACK.1080p.WEB.H264-PECULATE"
	bf := storage.BrokenFile{FileName: "8bit.mkv", Reason: reasonSplicedVolumes, ArrName: "radarr", MediaID: 7}
	identity := regrabIdentityKey(bf.ArrName, bf.MediaID, bf.EpisodeID, name)

	night1 := NewRepair(m)
	night1.regrabGuard.recordKeepRelease(identity, name)

	night2 := NewRepair(m) // a restart: the guard reloads from storage
	h := &storage.EntryHealth{EntryName: name, Status: storage.HealthBroken, FileCount: 1, BrokenFiles: []storage.BrokenFile{bf}}
	run := &storage.RepairRun{}
	night2.healBrokenEntryGuarded(context.Background(), run, &sync.Mutex{}, name, h, false)

	got, err := m.storage.GetEntryHealth(name)
	if err != nil {
		t.Fatalf("health not saved: %v", err)
	}
	if got.FailureReason != keepReleaseRepeatReason {
		t.Fatalf("FailureReason = %q, want %q", got.FailureReason, keepReleaseRepeatReason)
	}
}
