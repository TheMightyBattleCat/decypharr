package overlay

import "testing"

func TestSweepDeadLatch(t *testing.T) {
	s := newTestStore(t)
	const id = "nzb-1"

	// Not sweep-active: MarkSweepDead is a no-op, IsEntrySweepDead is false.
	s.MarkSweepDead(id)
	if s.IsEntrySweepDead(id) {
		t.Fatal("dead latch set for an entry that was never marked sweep-active")
	}

	s.MarkSweepEntry(id)
	if !s.IsEntrySweepActive(id) {
		t.Fatal("entry not sweep-active after MarkSweepEntry")
	}
	if s.IsEntrySweepDead(id) {
		t.Fatal("dead latch set immediately after MarkSweepEntry")
	}

	// A repeat mark must not clear an already-set latch.
	s.MarkSweepDead(id)
	s.MarkSweepEntry(id)
	if !s.IsEntrySweepDead(id) {
		t.Fatal("dead latch lost after a repeat MarkSweepEntry")
	}

	// Clearing the entry drops the latch with it.
	s.ClearSweepEntry(id)
	if s.IsEntrySweepActive(id) || s.IsEntrySweepDead(id) {
		t.Fatal("entry still tracked after ClearSweepEntry")
	}

	// Handle wrappers mirror the store methods.
	h := s.Handle(id)
	s.MarkSweepEntry(id)
	if h.IsSweepDead() {
		t.Fatal("handle reports dead before MarkSweepDead")
	}
	h.MarkSweepDead()
	if !h.IsSweepDead() {
		t.Fatal("handle does not report dead after MarkSweepDead")
	}
}

func TestSweepDeadNilStoreSafe(t *testing.T) {
	var s *Store
	s.MarkSweepDead("x")
	if s.IsEntrySweepDead("x") {
		t.Fatal("nil store reported a dead latch")
	}
	var h *Handle
	h.MarkSweepDead()
	if h.IsSweepDead() {
		t.Fatal("nil handle reported a dead latch")
	}
}
