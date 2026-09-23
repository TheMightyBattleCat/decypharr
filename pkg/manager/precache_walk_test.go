package manager

import (
	"testing"
	"time"
)

// A walk's window is the last episode it warms: from+ahead, or -1 for the
// season's end. Extending takes the wider window, and -1 always wins.
func TestForwardWalkWindow(t *testing.T) {
	if got := walkTarget(4, 2); got != 6 {
		t.Fatalf("walkTarget(4, 2) = %d, want 6", got)
	}
	if got := walkTarget(4, -1); got != -1 {
		t.Fatalf("walkTarget(4, -1) = %d, want -1 (season end)", got)
	}

	w := &forwardWalk{target: 6}
	if !w.covers(6) || w.covers(7) {
		t.Fatalf("target 6: covers(6)=%v covers(7)=%v, want true/false", w.covers(6), w.covers(7))
	}
	w.extend(5)
	if w.target != 6 {
		t.Fatalf("extend(5) narrowed the window to %d", w.target)
	}
	w.extend(8)
	if w.target != 8 {
		t.Fatalf("extend(8) = %d, want 8", w.target)
	}
	w.extend(-1)
	if w.target != -1 || !w.covers(99) {
		t.Fatalf("extend(-1) = %d, want season end", w.target)
	}
	w.extend(3)
	if w.target != -1 {
		t.Fatalf("extend(3) after season end = %d, want -1 kept", w.target)
	}
}

// Deciding to stop and leaving p.walks happen together, so a trigger that
// arrives afterwards starts a new walk instead of extending a dead one.
func TestWalkContinuesRemovesFinishedWalk(t *testing.T) {
	p := NewPrecache(&Manager{})
	w := &forwardWalk{target: 3}
	p.walks["k"] = w

	if !p.walkContinues("k", w, 3) {
		t.Fatal("episode 3 is inside the window")
	}
	if _, ok := p.walks["k"]; !ok {
		t.Fatal("a walk still inside its window was removed")
	}
	if p.walkContinues("k", w, 4) {
		t.Fatal("episode 4 is past the window")
	}
	if _, ok := p.walks["k"]; ok {
		t.Fatal("a finished walk stayed registered; the next trigger would extend it and be lost")
	}
}

// Playing a file the walk already burst-cached must still move the window:
// tryMarkWalked answers independently of triggered, once per file.
func TestTryMarkWalkedIndependentOfTriggered(t *testing.T) {
	p := NewPrecache(&Manager{})
	key := "hash:ep2.mkv"
	p.triggered[key] = time.Now() // burst-cached by an earlier walk

	if p.tryMarkTriggered(key) {
		t.Fatal("already-triggered key claimed again")
	}
	if !p.tryMarkWalked(key) {
		t.Fatal("first play of a burst-cached file did not ask for a walk")
	}
	if p.tryMarkWalked(key) {
		t.Fatal("second trigger for the same file asked for another walk")
	}
}

// A burst claimed but skipped for budget or bandwidth is un-claimed, so the
// episode is not hidden from later walks and its own read-ahead for 6 hours.
func TestUntriggerReleasesSkippedBurst(t *testing.T) {
	p := NewPrecache(&Manager{})
	key := "hash:ep3.mkv"
	if !p.tryMarkTriggered(key) {
		t.Fatal("fresh key not claimed")
	}
	p.untrigger(key)
	if !p.tryMarkTriggered(key) {
		t.Fatal("key still claimed after untrigger")
	}
}
