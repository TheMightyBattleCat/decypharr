package manager

import (
	"testing"
	"time"
)

// TestRegrabGuardTripsOnSecondFailedReplacement proves the loop guard's
// central property: the SAME logical file failing across two automatic
// re-grabs (each producing a fresh nzbID, which is why nzbID-keyed
// registries can't catch this - see repairHandlerRegistry) must trip
// terminal and refuse a third attempt, rather than blocklisting a new
// release forever.
func TestRegrabGuardTripsOnSecondFailedReplacement(t *testing.T) {
	g := newRegrabGuard()
	const identity = "arr:sonarr:42:7"

	allowed, _, firstTrip := g.checkAndRecord(identity)
	if !allowed || firstTrip {
		t.Fatalf("attempt 1: allowed=%v firstTrip=%v, want allowed=true firstTrip=false", allowed, firstTrip)
	}

	allowed, _, firstTrip = g.checkAndRecord(identity)
	if !allowed || firstTrip {
		t.Fatalf("attempt 2: allowed=%v firstTrip=%v, want allowed=true firstTrip=false", allowed, firstTrip)
	}

	allowed, reason, firstTrip := g.checkAndRecord(identity)
	if allowed {
		t.Fatalf("attempt 3: allowed=true, want the guard to refuse a third automatic re-grab")
	}
	if !firstTrip {
		t.Fatalf("attempt 3: firstTrip=false, want true on the call that flips the record terminal")
	}
	if reason == "" {
		t.Fatalf("attempt 3: reason is empty, want a human-readable terminal reason")
	}

	// A fourth attempt must also be refused, but not re-log as a "first trip".
	allowed, _, firstTrip = g.checkAndRecord(identity)
	if allowed || firstTrip {
		t.Fatalf("attempt 4: allowed=%v firstTrip=%v, want allowed=false firstTrip=false (already terminal)", allowed, firstTrip)
	}

	attempts, terminal, _ := g.state(identity)
	if !terminal {
		t.Fatalf("state: terminal=false, want true after tripping")
	}
	if attempts != regrabGuardMaxAttempts {
		t.Fatalf("state: attempts=%d, want %d (attempts stop accumulating once terminal)", attempts, regrabGuardMaxAttempts)
	}
}

// TestRegrabGuardWindowExpiry proves attempts older than regrabGuardWindow
// don't count against the cap - two re-grabs a day apart are unrelated
// incidents, not the same dead-source loop.
func TestRegrabGuardWindowExpiry(t *testing.T) {
	g := newRegrabGuard()
	const identity = "arr:sonarr:42:7"

	start := time.Now()
	g.nowFn = func() time.Time { return start }
	allowed, _, _ := g.checkAndRecord(identity)
	if !allowed {
		t.Fatalf("attempt 1 should be allowed")
	}

	// Advance past the window so the first attempt ages out.
	g.nowFn = func() time.Time { return start.Add(regrabGuardWindow + time.Hour) }
	allowed, _, _ = g.checkAndRecord(identity)
	if !allowed {
		t.Fatalf("attempt after the window elapsed should be allowed - the earlier attempt must have aged out")
	}

	attempts, terminal, _ := g.state(identity)
	if terminal {
		t.Fatalf("state: terminal=true, want false - only one live attempt in the window")
	}
	if attempts != 1 {
		t.Fatalf("state: attempts=%d, want 1 (the expired attempt should have been pruned)", attempts)
	}
}

// TestRegrabGuardClearResetsState proves the manual-override path
// (repairPlaybackFileNow's auto=false branch) fully resets a tripped guard,
// matching "manual research always overrides."
func TestRegrabGuardClearResetsState(t *testing.T) {
	g := newRegrabGuard()
	const identity = "arr:sonarr:42:7"

	for range regrabGuardMaxAttempts + 1 {
		g.checkAndRecord(identity)
	}
	if _, terminal, _ := g.state(identity); !terminal {
		t.Fatalf("setup: expected identity to be terminal before clearing")
	}

	g.clear(identity)

	if attempts, terminal, _ := g.state(identity); terminal || attempts != 0 {
		t.Fatalf("after clear: attempts=%d terminal=%v, want 0/false", attempts, terminal)
	}
	allowed, _, _ := g.checkAndRecord(identity)
	if !allowed {
		t.Fatalf("attempt after clear should be allowed again")
	}
}

func TestRegrabIdentityKey(t *testing.T) {
	if got := regrabIdentityKey("sonarr", 42, 7, "Some.Release.Name"); got == "" {
		t.Fatalf("regrabIdentityKey with a resolved Arr media id returned empty")
	}
	// Two differently-named releases for the SAME episode must map to the
	// same identity - that's the whole point (nzbID/release name changes
	// every re-grab, MediaID/EpisodeID don't).
	a := regrabIdentityKey("sonarr", 42, 7, "Release.One-GRP1")
	b := regrabIdentityKey("sonarr", 42, 7, "Release.Two-GRP2")
	if a != b {
		t.Fatalf("regrabIdentityKey(sonarr,42,7,*) = %q vs %q, want identical regardless of release name", a, b)
	}

	// Without a resolvable Arr media id, falls back to the normalized entry
	// name so at least same-name repeats are still caught.
	fallback := regrabIdentityKey("", 0, 0, "Some.Release.Name")
	if fallback == "" {
		t.Fatalf("regrabIdentityKey fallback returned empty")
	}
	if fallback == a {
		t.Fatalf("fallback identity must not collide with a resolved Arr identity")
	}
}
