package usenet

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/fs/reader"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
	"github.com/sirrobot01/decypharr/pkg/usenet/parser"
)

func newTestUsenetWithOverlay(t *testing.T) *Usenet {
	t.Helper()
	store, err := overlay.NewStore(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("overlay.NewStore: %v", err)
	}
	return &Usenet{
		overlay:     store,
		failedFiles: xsync.NewMap[string, failedFileRecord](),
		logger:      zerolog.Nop(),
	}
}

// TestShouldPoisonFailedFile proves commits A and B's central safety
// property: failedFiles (the permanent-failure cache consulted by
// preStreamChecks) must only ever be poisoned by a real playback failure
// against a file the overlay has already given up on (Verdict == Failed),
// never by a verification-only read and never while the file is merely
// Clean (no overlay record yet - padding hasn't had a chance to run) or
// Degraded (padding already covers it, within caps).
func TestShouldPoisonFailedFile(t *testing.T) {
	const nzbID, file = "nzb-1", "movie.mkv"

	t.Run("no overlay record yet (clean) does not poison", func(t *testing.T) {
		u := newTestUsenetWithOverlay(t)
		if got := u.shouldPoisonFailedFile(context.Background(), nzbID, file); got {
			t.Fatalf("shouldPoisonFailedFile() = true, want false (verdict clean)")
		}
	})

	t.Run("degraded (padded, within caps) does not poison", func(t *testing.T) {
		u := newTestUsenetWithOverlay(t)
		u.overlay.SetPolicy(overlay.Policy{MaxRunSegments: 10, MaxTotalSegments: 10, MaxByteRatio: 1.0})
		if _, verdict := u.overlay.Decide(nzbID, file, 0, "<msg1@test>", 1024, 1<<20); verdict != overlay.VerdictDegraded {
			t.Fatalf("setup: Decide verdict = %v, want degraded", verdict)
		}
		if got := u.shouldPoisonFailedFile(context.Background(), nzbID, file); got {
			t.Fatalf("shouldPoisonFailedFile() = true, want false (verdict degraded) - would permanently block the padding path from ever running again")
		}
	})

	t.Run("failed (beyond caps) poisons", func(t *testing.T) {
		u := newTestUsenetWithOverlay(t)
		u.overlay.SetPolicy(overlay.Policy{MaxRunSegments: 1, MaxTotalSegments: 1, MaxByteRatio: 1.0})
		// First segment exhausts the (tiny) caps; second is beyond them.
		_, _ = u.overlay.Decide(nzbID, file, 0, "<msg1@test>", 1024, 1<<20)
		if _, verdict := u.overlay.Decide(nzbID, file, 2, "<msg2@test>", 1024, 1<<20); verdict != overlay.VerdictFailed {
			t.Fatalf("setup: Decide verdict = %v, want failed", verdict)
		}
		if got := u.shouldPoisonFailedFile(context.Background(), nzbID, file); !got {
			t.Fatalf("shouldPoisonFailedFile() = false, want true (verdict failed)")
		}
	})

	t.Run("verification read never poisons, even when verdict is failed", func(t *testing.T) {
		u := newTestUsenetWithOverlay(t)
		u.overlay.SetPolicy(overlay.Policy{MaxRunSegments: 1, MaxTotalSegments: 1, MaxByteRatio: 1.0})
		_, _ = u.overlay.Decide(nzbID, file, 0, "<msg1@test>", 1024, 1<<20)
		_, _ = u.overlay.Decide(nzbID, file, 2, "<msg2@test>", 1024, 1<<20)

		verificationCtx := reader.ContextWithoutPadding(context.Background())
		if got := u.shouldPoisonFailedFile(verificationCtx, nzbID, file); got {
			t.Fatalf("shouldPoisonFailedFile() = true, want false (verification read must never poison)")
		}
	})
}

func testNZBFile(nzbID, name string) *storage.NZBFile {
	return &storage.NZBFile{
		NzbID:    nzbID,
		Name:     name,
		Segments: []storage.NZBSegment{{MessageID: "<seg1@test>", Bytes: 1024}},
	}
}

// TestFailedFileTTLExpires proves commit C's TTL property: a permanent
// failure cached in failedFiles must stop short-circuiting preStreamChecks
// once failedFileTTL has elapsed, so a transient provider-side 430 storm
// can't poison a file for the whole process lifetime.
func TestFailedFileTTLExpires(t *testing.T) {
	u := newTestUsenetWithOverlay(t)
	const nzbID, name = "nzb-1", "movie.mkv"
	file := testNZBFile(nzbID, name)

	u.failedFiles.Store(fsKey(nzbID, name), failedFileRecord{
		err:        errors.New("article not found"),
		recordedAt: time.Now(),
	})
	if err := u.preStreamChecks(file); err == nil {
		t.Fatalf("preStreamChecks() = nil, want short-circuit error while record is fresh")
	}

	// Backdate the record past the TTL instead of sleeping.
	u.failedFiles.Store(fsKey(nzbID, name), failedFileRecord{
		err:        errors.New("article not found"),
		recordedAt: time.Now().Add(-failedFileTTL - time.Second),
	})
	if err := u.preStreamChecks(file); err != nil {
		t.Fatalf("preStreamChecks() = %v, want nil once the record has expired past its TTL", err)
	}
	if _, ok := u.failedFiles.Load(fsKey(nzbID, name)); ok {
		t.Fatalf("expired record was not self-healed (deleted) on read")
	}
}

// TestClearFailedFileRestoresReadability proves ClearFailedFile/
// ClearFailedEntry actually un-poison a cached permanent failure, so a
// successful repair/reclaim/policy change can make a file readable again
// without waiting out the TTL.
func TestClearFailedFileRestoresReadability(t *testing.T) {
	const nzbID, name = "nzb-1", "movie.mkv"
	file := testNZBFile(nzbID, name)

	t.Run("ClearFailedFile", func(t *testing.T) {
		u := newTestUsenetWithOverlay(t)
		u.failedFiles.Store(fsKey(nzbID, name), failedFileRecord{err: errors.New("boom"), recordedAt: time.Now()})
		if err := u.preStreamChecks(file); err == nil {
			t.Fatalf("preStreamChecks() = nil, want short-circuit error before clearing")
		}
		u.ClearFailedFile(nzbID, name)
		if err := u.preStreamChecks(file); err != nil {
			t.Fatalf("preStreamChecks() = %v, want nil after ClearFailedFile", err)
		}
	})

	t.Run("ClearFailedEntry", func(t *testing.T) {
		u := newTestUsenetWithOverlay(t)
		other := testNZBFile(nzbID, "other.mkv")
		u.failedFiles.Store(fsKey(nzbID, name), failedFileRecord{err: errors.New("boom"), recordedAt: time.Now()})
		u.failedFiles.Store(fsKey(nzbID, "other.mkv"), failedFileRecord{err: errors.New("boom"), recordedAt: time.Now()})
		// A different nzbID's record must survive ClearFailedEntry for nzbID.
		u.failedFiles.Store(fsKey("other-nzb", name), failedFileRecord{err: errors.New("boom"), recordedAt: time.Now()})

		u.ClearFailedEntry(nzbID)

		if err := u.preStreamChecks(file); err != nil {
			t.Fatalf("preStreamChecks(%s) = %v, want nil after ClearFailedEntry", name, err)
		}
		if err := u.preStreamChecks(other); err != nil {
			t.Fatalf("preStreamChecks(%s) = %v, want nil after ClearFailedEntry", "other.mkv", err)
		}
		if _, ok := u.failedFiles.Load(fsKey("other-nzb", name)); !ok {
			t.Fatalf("ClearFailedEntry(%s) incorrectly cleared a record under a different nzbID", nzbID)
		}
	})
}

// TestParseWithIDRejectsKnownDeadPosting proves the negative cache actually
// short-circuits ParseWithID: a re-grab of content already marked dead
// within deadPostingTTL is rejected immediately, with no parser or NNTP
// client ever touched (u.nntp is nil here - a real parse attempt would
// nil-pointer panic, which this test would catch).
func TestParseWithIDRejectsKnownDeadPosting(t *testing.T) {
	u := &Usenet{
		logger:       zerolog.Nop(),
		deadPostings: newDeadPostingCache(),
	}
	content := []byte("<nzb><file>dead release</file></nzb>")
	u.deadPostings.Mark(hashNZBContent(content))

	_, _, err := u.ParseWithID(context.Background(), "", "release.nzb", content, "tv")
	if err == nil {
		t.Fatal("ParseWithID() = nil error, want a rejection for a known-dead posting")
	}
	if !errors.Is(err, parser.ErrReleaseUnavailable) {
		t.Errorf("error = %v, want it to wrap parser.ErrReleaseUnavailable", err)
	}
}

// TestParseWithIDAllowsUnmarkedContent proves the negative cache doesn't
// over-reject: content that was never marked dead reaches the real parser
// (which then fails for an unrelated reason - no NNTP client configured in
// this fixture - proving the cache check itself let it through rather than
// rejecting it).
func TestParseWithIDAllowsUnmarkedContent(t *testing.T) {
	u := &Usenet{
		logger:       zerolog.Nop(),
		deadPostings: newDeadPostingCache(),
	}
	content := []byte("<nzb><file>never seen before</file></nzb>")

	_, _, err := u.ParseWithID(context.Background(), "", "release.nzb", content, "tv")
	if err == nil {
		t.Fatal("expected an error (no NNTP client configured), but ParseWithID succeeded")
	}
	if errors.Is(err, parser.ErrReleaseUnavailable) {
		t.Errorf("error = %v, want a plain parse failure, not ErrReleaseUnavailable (content was never marked dead)", err)
	}
}
