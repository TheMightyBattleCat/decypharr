package reader

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/nntp"
)

// The reader is shared per file by playback and verification reads. A slot a
// viewer's read padded must not be handed to a verification read
// (ContextWithoutPadding) as data: it gets the 430 the pad stands for, and its
// dead-segment signal trips.
func TestVerificationReadRefusesCachedPad(t *testing.T) {
	const deadID = "<dead-normal-read@test>"
	t.Setenv("DECYPHARR_FORCE_MISSING_SEGMENTS", deadID+",<dead-verification-read@test>")
	sr, _ := refetchTestReader(t, "nzb-padded-cache", 120, deadID)

	if err := sr.fetcher.Fetch(ContextForPlayback(context.Background()), 120); err != nil {
		t.Fatalf("setup: playback read = %v, want padded", err)
	}

	// Playback keeps getting its pad.
	p := make([]byte, 1024)
	if n, err := sr.ReadAtContext(ContextForPlayback(context.Background()), p, 120*1024); err != nil || n != len(p) {
		t.Fatalf("playback re-read of the pad: n=%d err=%v", n, err)
	}

	sig := NewDeadSegmentSignal()
	vctx := ContextWithDeadSignal(ContextWithoutPadding(context.Background()), sig)
	if _, err := sr.ReadAtContext(vctx, p, 120*1024); err == nil {
		t.Error("verification read of a padded slot succeeded")
	} else if !nntp.IsArticleNotFoundError(err) {
		t.Errorf("verification read error %v, want article-not-found", err)
	}
	if !sig.Detected() {
		t.Error("dead-segment signal not tripped")
	}
}

// A verification read that joins a padding read's in-flight fetch does not
// inherit the pad.
func TestVerificationWaiterRefusesLeaderPad(t *testing.T) {
	const deadID = "<dead-normal-read@test>"
	t.Setenv("DECYPHARR_FORCE_MISSING_SEGMENTS", deadID+",<dead-verification-read@test>")
	sr, _ := refetchTestReader(t, "nzb-padded-inflight", 120, deadID)
	sf := sr.fetcher

	// Hold every connection slot so the leader parks inside doFetch after
	// registering its promise.
	for i := 0; i < cap(sf.semaphore); i++ {
		sf.semaphore <- struct{}{}
	}
	leaderDone := make(chan error, 1)
	go func() { leaderDone <- sf.Fetch(ContextForPlayback(context.Background()), 120) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		sf.inFlightMu.Lock()
		_, ok := sf.inFlight[120]
		sf.inFlightMu.Unlock()
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("leader never registered its promise")
		}
		time.Sleep(time.Millisecond)
	}

	sig := NewDeadSegmentSignal()
	vctx := ContextWithDeadSignal(ContextWithoutPadding(context.Background()), sig)
	waiterDone := make(chan error, 1)
	go func() { waiterDone <- sf.Fetch(vctx, 120) }()
	time.Sleep(50 * time.Millisecond) // let the waiter park on the promise

	for i := 0; i < cap(sf.semaphore); i++ {
		<-sf.semaphore
	}
	if err := <-leaderDone; err != nil {
		t.Fatalf("leader (playback) = %v, want padded", err)
	}
	if werr := <-waiterDone; werr == nil || !sig.Detected() {
		t.Errorf("verification waiter on a padded in-flight fetch: err=%v signal=%v", werr, sig.Detected())
	}
}

// A repair that lands between handleConfirmedMissing's patch check and its
// zero-fill leaves the patch, not the pad, in the slot - and a verification
// read may then use it.
func TestRepairLandingMidPadServesPatch(t *testing.T) {
	const deadID = "<dead-normal-read@test>"
	t.Setenv("DECYPHARR_FORCE_MISSING_SEGMENTS", deadID+",<dead-verification-read@test>")
	sr, store := refetchTestReader(t, "nzb-padded-race", 120, deadID)

	patch := bytes.Repeat([]byte{0xAB}, int(sr.cache.SegmentDataSize(120)))
	landed := false
	// The repair enqueuer runs synchronously inside the window, standing in
	// for a repair finishing on another goroutine at that moment.
	store.SetRepairEnqueuer(func(string, int) {
		if landed {
			return
		}
		landed = true
		if err := store.WritePatch("nzb-padded-race", "movie.mkv", 120, patch); err != nil {
			t.Errorf("WritePatch: %v", err)
		}
		sr.RefetchSegments([]int{120})
	})

	if err := sr.fetcher.Fetch(ContextForPlayback(context.Background()), 120); err != nil {
		t.Fatalf("playback fetch: %v", err)
	}
	if !landed {
		t.Fatal("setup: repair hook never ran")
	}
	p := make([]byte, 1024)
	if n, err := sr.ReadAtContext(ContextForPlayback(context.Background()), p, 120*1024); err != nil || !bytes.Equal(p[:n], patch) {
		t.Errorf("playback read after the repair landed: n=%d err=%v, want the patch", n, err)
	}
	vctx := ContextWithoutPadding(context.Background())
	if n, err := sr.ReadAtContext(vctx, p, 120*1024); err != nil || !bytes.Equal(p[:n], patch) {
		t.Errorf("verification read of the patched slot: n=%d err=%v, want the patch", n, err)
	}
}
