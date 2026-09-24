package server

import (
	"encoding/json"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// A stored "healthy" record for an entry the overlay holds unrepaired damage
// for is served as "stale" with the pending count (Salem Lord S01E01 read
// healthy for days after a read padded it); the stored record is untouched.
func TestHealthViewServesPendingDamage(t *testing.T) {
	stored := &storage.EntryHealth{EntryName: "Show.S01E01", Status: storage.HealthHealthy}

	if got := healthView(stored, manager.EntryDamage{}); got != any(stored) {
		t.Fatalf("no damage: got %#v, want the stored record unchanged", got)
	}

	raw, err := json.Marshal(healthView(stored, manager.EntryDamage{PendingDeadSegments: 3, Par2Terminal: true}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out["status"] != "stale" || out["entry_name"] != "Show.S01E01" {
		t.Fatalf("served %s, want status stale with the record's fields flattened", raw)
	}
	if out["pending_dead_segments"] != float64(3) || out["par2_terminal"] != true {
		t.Fatalf("served %s, want pending_dead_segments=3 par2_terminal=true", raw)
	}
	if stored.Status != storage.HealthHealthy {
		t.Fatalf("stored record mutated to %q", stored.Status)
	}

	broken := &storage.EntryHealth{EntryName: "Show.S01E02", Status: storage.HealthBroken}
	raw, _ = json.Marshal(healthView(broken, manager.EntryDamage{PendingDeadSegments: 1}))
	if err := json.Unmarshal(raw, &out); err != nil || out["status"] != "broken" {
		t.Fatalf("broken record served %s, want status kept broken", raw)
	}
}
