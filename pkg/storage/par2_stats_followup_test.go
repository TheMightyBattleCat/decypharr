package storage

import (
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// The stats page's PAR2 card reads these keys from "par2_recovery". The test
// below shows which of them the stored attempt records can fill today, and
// which have no source in the fork.
var par2CardFields = []string{
	"repair_attempts", "repair_successes", "repair_failures", "failure_reasons",
	"recovery_payload_bytes",
	"patch_hits", "body_calls", "patch_bytes",
	"store.entries", "store.disk_bytes", "store.recovery_slices", "store.patches",
}

func TestFollowupPar2CardFromAttemptRecords(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)

	s, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Now()
	attempts := []*Par2RepairAttempt{
		{ID: "a1", NzbID: "n1", StartedAt: now.Add(-3 * time.Hour), Outcome: Par2RepairOutcomeCompleted, ReadBytes: 4 << 30, SlicesRepaired: 9, SegmentsPatched: 5},
		{ID: "a2", NzbID: "n2", StartedAt: now.Add(-2 * time.Hour), Outcome: Par2RepairOutcomeFailed, ReadBytes: 1 << 30, FailReason: "more damage than recorded; 70 slices unrecoverable (recovery cap 64)"},
		{ID: "a3", NzbID: "n2", StartedAt: now.Add(-1 * time.Hour), Outcome: Par2RepairOutcomeFailed, ReadBytes: 2 << 30, FailReason: "more damage than recorded; 131 slices unrecoverable (recovery cap 128)"},
		{ID: "a4", NzbID: "n3", StartedAt: now, Outcome: Par2RepairOutcomeUnavailable},
	}
	for _, a := range attempts {
		if err := s.SavePar2RepairAttempt(a); err != nil {
			t.Fatal(err)
		}
	}

	list, err := s.ListPar2RepairAttempts()
	if err != nil {
		t.Fatal(err)
	}
	card := map[string]any{}
	var ok, failed int
	var read int64
	reasons := map[string]int{}
	for _, a := range list {
		switch a.Outcome {
		case Par2RepairOutcomeCompleted:
			ok++
		case Par2RepairOutcomeFailed:
			failed++
			reasons[a.FailReason]++
		}
		read += a.ReadBytes
	}
	card["repair_attempts"] = len(list)
	card["repair_successes"] = ok
	card["repair_failures"] = failed
	card["recovery_payload_bytes"] = read
	card["failure_reasons"] = reasons

	if len(list) != 4 || ok != 1 || failed != 2 || read != 7<<30 {
		t.Fatalf("summary = %d attempts, %d ok, %d failed, %d bytes", len(list), ok, failed, read)
	}
	// The fail reason is free text with the counts of that one repair in it,
	// so the same cause shows as two reasons. The card needs a short code.
	if len(reasons) != 2 {
		t.Fatalf("expected the two cap failures to read as two reasons, got %v", reasons)
	}
	t.Logf("failure_reasons from free text: %d badges for one cause; a reason code is needed", len(reasons))

	// ReadBytes is everything a repair read (intact slices and recovery),
	// not the recovery payload alone, so the card's "Recovery Downloaded"
	// would overstate.
	t.Log("recovery_payload_bytes: only ReadBytes is stored (intact and recovery reads together)")

	var missing []string
	for _, f := range par2CardFields {
		if _, have := card[f]; !have {
			missing = append(missing, f)
		}
	}
	t.Logf("filled from attempt records: %d of %d card fields", len(par2CardFields)-len(missing), len(par2CardFields))
	t.Logf("no source in the attempt records: %v", missing)

	// The records are a history, not counters: clearing it resets the card.
	if err := s.ClearPar2RepairAttempts(); err != nil {
		t.Fatal(err)
	}
	if list, _ := s.ListPar2RepairAttempts(); len(list) != 0 {
		t.Fatalf("history not cleared: %d", len(list))
	}
	t.Log("after 'clear history' every count on the card would read 0")
}
