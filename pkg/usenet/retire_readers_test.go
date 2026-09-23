package usenet

import (
	"context"
	"errors"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

// Delete must stop a warm reader from serving the deleted grab: it kept
// fetching the old articles for a player that still held the handle, and
// their 430s escalated to repair by name - against the replacement.
func TestRetireReadersFencesWarmReader(t *testing.T) {
	u := &Usenet{fs: xsync.NewMap[string, *fsEntry](), logger: zerolog.Nop()}
	idle := &fsEntry{}
	busy := &fsEntry{}
	busy.refCount.Store(1) // a stream is mid-read
	other := &fsEntry{}
	u.fs.Store(fsKey("gone", "a.mkv"), idle)
	u.fs.Store(fsKey("gone", "b.mkv"), busy)
	u.fs.Store(fsKey("gone-not", "a.mkv"), other)

	u.retireReaders("gone")

	if _, ok := u.fs.Load(fsKey("gone", "a.mkv")); ok {
		t.Fatal("idle reader of a deleted NZB not torn down")
	}
	if !busy.gone.Load() {
		t.Fatal("busy reader of a deleted NZB not marked gone")
	}
	if other.gone.Load() {
		t.Fatal("a different NZB sharing the ID prefix was marked gone")
	}

	_, _, err := u.getOrCreateEntry(context.Background(), "gone", "b.mkv")
	if !errors.Is(err, ErrEntryGone) {
		t.Fatalf("stream on a gone reader: err = %v, want ErrEntryGone", err)
	}
	if busy.refCount.Load() != 1 {
		t.Fatal("a refused stream took a reference")
	}
}

func TestReviveNZBDropsGoneReaders(t *testing.T) {
	u := &Usenet{fs: xsync.NewMap[string, *fsEntry](), logger: zerolog.Nop()}
	e := &fsEntry{}
	e.gone.Store(true)
	u.fs.Store(fsKey("id", "a.mkv"), e)

	u.reviveNZB("id")
	if _, ok := u.fs.Load(fsKey("id", "a.mkv")); ok {
		t.Fatal("a re-added ID kept its gone reader")
	}
}

func TestRefreshRepairedSegmentsWithoutReader(t *testing.T) {
	u := &Usenet{fs: xsync.NewMap[string, *fsEntry](), logger: zerolog.Nop()}
	if u.RefreshRepairedSegments("id", "a.mkv", []int{1}) {
		t.Fatal("reported a refresh with no open reader")
	}
	u.fs.Store(fsKey("id", "a.mkv"), &fsEntry{}) // multi-volume or not yet read
	if u.RefreshRepairedSegments("id", "a.mkv", []int{1}) {
		t.Fatal("reported a refresh with no streaming reader")
	}
}
