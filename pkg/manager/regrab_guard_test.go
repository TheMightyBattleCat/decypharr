package manager

import (
	"fmt"
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

	// Each attempt is a distinct candidate release - a fresh nzbID/release
	// name every re-grab, same as production - so none of these should be
	// deduped against each other.
	allowed, _, firstTrip := g.checkAndRecord(identity, "Release.One-GRP1")
	if !allowed || firstTrip {
		t.Fatalf("attempt 1: allowed=%v firstTrip=%v, want allowed=true firstTrip=false", allowed, firstTrip)
	}

	allowed, _, firstTrip = g.checkAndRecord(identity, "Release.Two-GRP2")
	if !allowed || firstTrip {
		t.Fatalf("attempt 2: allowed=%v firstTrip=%v, want allowed=true firstTrip=false", allowed, firstTrip)
	}

	allowed, reason, firstTrip := g.checkAndRecord(identity, "Release.Three-GRP3")
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
	allowed, _, firstTrip = g.checkAndRecord(identity, "Release.Four-GRP4")
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
	allowed, _, _ := g.checkAndRecord(identity, "Release.One-GRP1")
	if !allowed {
		t.Fatalf("attempt 1 should be allowed")
	}

	// Advance past the window so the first attempt ages out.
	g.nowFn = func() time.Time { return start.Add(regrabGuardWindow + time.Hour) }
	allowed, _, _ = g.checkAndRecord(identity, "Release.Two-GRP2")
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

	for i := range regrabGuardMaxAttempts + 1 {
		g.checkAndRecord(identity, fmt.Sprintf("Release.%d", i))
	}
	if _, terminal, _ := g.state(identity); !terminal {
		t.Fatalf("setup: expected identity to be terminal before clearing")
	}

	g.clear(identity)

	if attempts, terminal, _ := g.state(identity); terminal || attempts != 0 {
		t.Fatalf("after clear: attempts=%d terminal=%v, want 0/false", attempts, terminal)
	}
	allowed, _, _ := g.checkAndRecord(identity, "Release.After-Clear")
	if !allowed {
		t.Fatalf("attempt after clear should be allowed again")
	}
}

// TestRegrabGuardImportPathDedupeAndStrike proves the counting model this
// fix wires RegrabImportGrab and the scheduled sweep's automatic heal into:
// the SAME exact release re-offered inside regrabDedupeWindow doesn't cost a
// strike (an Arr re-serving already-known-dead evidence), but genuinely
// distinct dead releases each strike and still trip terminal at the
// existing after-2 threshold - the live shape from the incident this fix
// closes (five distinct releases, one repeated three times, in one
// nine-minute burst, with the guard stuck at count=1 throughout because
// RegrabImportGrab never consulted it).
func TestRegrabGuardImportPathDedupeAndStrike(t *testing.T) {
	g := newRegrabGuard()
	const identity = "arr:sonarr:42:7"
	start := time.Now()
	now := start
	g.nowFn = func() time.Time { return now }

	// Release A, struck once.
	allowed, _, firstTrip := g.checkAndRecord(identity, "Miracle.Workers.S02E03-QOQ")
	if !allowed || firstTrip {
		t.Fatalf("release A attempt 1: allowed=%v firstTrip=%v, want true/false", allowed, firstTrip)
	}

	// The Arr re-offers the IDENTICAL release seconds later - must not cost
	// a second strike, only be permitted again.
	now = start.Add(5 * time.Second)
	allowed, _, firstTrip = g.checkAndRecord(identity, "Miracle.Workers.S02E03-QOQ")
	if !allowed || firstTrip {
		t.Fatalf("release A attempt 2 (duplicate): allowed=%v firstTrip=%v, want true/false", allowed, firstTrip)
	}
	if attempts, _, _ := g.state(identity); attempts != 1 {
		t.Fatalf("after duplicate release: attempts=%d, want 1 (duplicate must not strike)", attempts)
	}

	// A genuinely different dead release - this is the second distinct
	// candidate and must strike.
	now = start.Add(30 * time.Second)
	allowed, _, firstTrip = g.checkAndRecord(identity, "Miracle.Workers.2019.S02E03.1080p.WEBRip.x264-XLF")
	if !allowed || firstTrip {
		t.Fatalf("release B attempt 1: allowed=%v firstTrip=%v, want true/false", allowed, firstTrip)
	}
	if attempts, _, _ := g.state(identity); attempts != 2 {
		t.Fatalf("after second distinct release: attempts=%d, want 2", attempts)
	}

	// A third distinct dead release exceeds the two-strike budget and must
	// trip terminal.
	now = start.Add(45 * time.Second)
	allowed, reason, firstTrip := g.checkAndRecord(identity, "Miracle.Workers.S02E03.NORDiC.1080p.HMAX.WEB-DL.DD5.1.H.264-DKV")
	if allowed {
		t.Fatalf("release C attempt 1: allowed=true, want the guard to refuse - two distinct dead releases already struck")
	}
	if !firstTrip {
		t.Fatalf("release C attempt 1: firstTrip=false, want true on the call that flips the record terminal")
	}
	if reason == "" {
		t.Fatalf("release C attempt 1: reason is empty, want a human-readable terminal reason")
	}

	attempts, terminal, _ := g.state(identity)
	if !terminal {
		t.Fatalf("state: terminal=false, want true after two distinct dead releases exhausted the budget")
	}
	if attempts != regrabGuardMaxAttempts {
		t.Fatalf("state: attempts=%d, want %d", attempts, regrabGuardMaxAttempts)
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
