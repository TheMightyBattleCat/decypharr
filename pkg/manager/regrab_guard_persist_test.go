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
	g.recordKeepRelease("id", "posting:p", "nzb-1")
	g.clear("id")
	if newPersistedGuard(t, st, now).keepReleaseRepeated("id", "posting:p", "nzb-2") {
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
	const id, posting = "arr:radarr:7:0", "posting:p"

	g := newPersistedGuard(t, st, night1)
	if g.keepReleaseRepeated(id, posting, "nzb-1") {
		t.Fatal("first keep-release re-grab refused")
	}
	g.recordKeepRelease(id, posting, "nzb-1")

	// The next night's sweep, just over 24 h later, after a restart.
	night2 := newPersistedGuard(t, st, night1.Add(24*time.Hour+3*time.Second))
	if !night2.keepReleaseRepeated(id, posting, "nzb-2") {
		t.Fatal("a new import of the same posting was allowed another re-grab")
	}
	if night2.keepReleaseRepeated(id, posting, "nzb-1") {
		t.Fatal("the original entry (replacement not landed yet) was refused")
	}
	if night2.keepReleaseRepeated(id, "posting:q", "nzb-2") {
		t.Fatal("a different posting was refused")
	}
	if newPersistedGuard(t, st, night1.Add(keepReleaseRetryWindow+time.Hour)).keepReleaseRepeated(id, posting, "nzb-2") {
		t.Fatal("keep-release record did not expire")
	}
}

func testNZB(msgIDs ...string) *storage.NZB {
	nzb := &storage.NZB{}
	for _, id := range msgIDs {
		nzb.Par2Source = append(nzb.Par2Source, storage.PostedFileRef{Segments: []storage.Par2SegmentRef{{MessageID: id}, {MessageID: id + "-2"}}})
	}
	return nzb
}

// The posting identity follows the articles, not the title or file order.
func TestNZBPostingIdentity(t *testing.T) {
	p := nzbPostingIdentity(testNZB("<a@x>", "<b@x>"))
	if p == "" {
		t.Fatal("empty identity")
	}
	if nzbPostingIdentity(testNZB("<b@x>", "<a@x>")) != p {
		t.Error("identity depends on file order")
	}
	if nzbPostingIdentity(testNZB("<a@x>", "<c@x>")) == p {
		t.Error("a differently built posting shares the identity")
	}
	// Logical files are the fallback when no posted-file layout was kept.
	logical := &storage.NZB{Files: []storage.NZBFile{{Segments: []storage.NZBSegment{{MessageID: "<a@x>"}}}}}
	if nzbPostingIdentity(logical) == "" {
		t.Error("no identity from logical files")
	}
	if nzbPostingIdentity(&storage.NZB{}) != "" || nzbPostingIdentity(nil) != "" {
		t.Error("identity for an NZB with no articles")
	}
}

// The sweep path: night 1 re-grabbed away from posting P keeping the release;
// after a restart, night 2 finds the entry still assembled wrong.
func TestSweepKeepReleaseRegrabByPosting(t *testing.T) {
	const name = "8-Bit-Midwinter.2021.REPACK.1080p.WEB.H264-PECULATE"
	identity := regrabIdentityKey("radarr", 7, 0, name)
	postingP := nzbPostingIdentity(testNZB("<p1@x>", "<p2@x>"))
	postingQ := nzbPostingIdentity(testNZB("<q1@x>", "<q2@x>")) // same title, another upload

	// night2 runs the second night's heal for an entry imported as nzbID,
	// whose posting is current.
	night2 := func(t *testing.T, nzbID, current string) (*Repair, *storage.EntryHealth, *Manager) {
		m, _ := newTestManagerForReap(t)
		night1 := NewRepair(m)
		night1.regrabGuard.recordKeepRelease(identity, postingP, "nzb-night1")

		r := NewRepair(m) // a restart: the guard reloads from storage
		r.postingID = func(string) string { return current }
		bf := storage.BrokenFile{FileName: "8bit.mkv", Reason: reasonSplicedVolumes, ArrName: "radarr", MediaID: 7, InfoHash: nzbID}
		h := &storage.EntryHealth{EntryName: name, Status: storage.HealthBroken, FileCount: 1, BrokenFiles: []storage.BrokenFile{bf}}
		r.healBrokenEntryGuarded(context.Background(), &storage.RepairRun{}, &sync.Mutex{}, name, h, false)
		return r, h, m
	}

	t.Run("same posting re-imported: refused", func(t *testing.T) {
		_, _, m := night2(t, "nzb-night2", postingP)
		got, err := m.storage.GetEntryHealth(name)
		if err != nil {
			t.Fatalf("health not saved: %v", err)
		}
		if got.FailureReason != keepReleaseRepeatReason {
			t.Fatalf("FailureReason = %q, want %q", got.FailureReason, keepReleaseRepeatReason)
		}
	})

	t.Run("same title, different posting: re-grabbed", func(t *testing.T) {
		r, h, _ := night2(t, "nzb-night2", postingQ)
		if h.FailureReason == keepReleaseRepeatReason {
			t.Fatal("a differently built posting of the same title was refused")
		}
		if !r.regrabGuard.keepReleaseRepeated(identity, postingQ, "nzb-night3") {
			t.Fatal("the re-grab away from posting Q was not recorded")
		}
	})

	t.Run("original entry still there (replacement not landed): not refused", func(t *testing.T) {
		_, h, _ := night2(t, "nzb-night1", postingP)
		if h.FailureReason == keepReleaseRepeatReason {
			t.Fatal("refused although the Arr has not brought anything back yet")
		}
	})

	t.Run("posting unknown: check does not apply", func(t *testing.T) {
		_, h, _ := night2(t, "nzb-night2", "")
		if h.FailureReason == keepReleaseRepeatReason {
			t.Fatal("refused without knowing the posting")
		}
	})
}
