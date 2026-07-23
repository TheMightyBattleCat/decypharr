package manager

import (
	"fmt"
	"sync"
	"time"
)

const (
	// regrabGuardMaxAttempts is how many automatic re-grabs a single
	// logical file (see regrabIdentityKey) may go through inside
	// regrabGuardWindow before the guard stops trying and marks it
	// terminal. Two is deliberately tight: a genuinely bad release
	// occasionally needs one re-grab to land a working copy, but a THIRD
	// automatic attempt in the same window means every candidate tried so
	// far shares the same missing articles - almost always the same
	// upload re-posted under different release names, or a source that's
	// simply gone - and retrying a fourth, fifth, ... time only burns one
	// blocklisted release per cycle for no gain.
	regrabGuardMaxAttempts = 2

	// regrabGuardWindow bounds how far back attempts still count against
	// regrabGuardMaxAttempts. Deliberately long (a day, not minutes): the
	// failure mode this guards against (every candidate release sharing
	// the same dead source articles) doesn't resolve on its own on a short
	// timescale - only new data being posted or a human stepping in fixes
	// it, and a day is long enough to distinguish that from "two unrelated
	// re-grabs happened to land in the same hour."
	regrabGuardWindow = 24 * time.Hour

	// regrabGuardTerminalReason is surfaced to the user (via EntryHealth.
	// FailureReason, the existing broken-file GUI surface) when the guard
	// trips - see (*regrabGuard).checkAndRecord.
	regrabGuardTerminalReason = "replacement grabs also missing articles — posting appears dead at source"
)

// regrabAttemptRecord is one logical file's automatic re-grab history.
type regrabAttemptRecord struct {
	attempts []time.Time
	terminal bool
	reason   string
	// loggedTerminal is true once the terminal transition has been logged
	// at WARN - every later call while still terminal only needs a quiet
	// DEBUG, not a repeat of the same warning on every subsequent playback
	// failure (the exact log-storm shape this whole fix is about).
	loggedTerminal bool
}

// regrabGuard tracks automatic re-grab attempts per stable logical-file
// identity (see regrabIdentityKey), independent of nzbID - a re-grab always
// mints a fresh nzbID, which is why neither repairHandlerRegistry nor
// storage.Par2RepairState (both keyed by nzbID) can ever catch a loop across
// re-grab cycles. Only consulted by the automatic path (HandlePlaybackFailure
// via repairPlaybackFileNow's auto=true call); a manual "repair now"/overlay
// "Delete & re-search" always overrides and clears the guard for the
// identity it acts on, matching every other automatic-only gate in this
// package (par2ShouldAutoEnqueue, repairHandlerRegistry.MarkTerminal).
type regrabGuard struct {
	mu      sync.Mutex
	records map[string]*regrabAttemptRecord
	nowFn   func() time.Time
}

func newRegrabGuard() *regrabGuard {
	return &regrabGuard{
		records: make(map[string]*regrabAttemptRecord),
		nowFn:   time.Now,
	}
}

// regrabIdentityKey builds a stable identity for a broken file's underlying
// Arr media record, independent of which nzbID currently backs it. arrName
// and mediaID come from storage.BrokenFile's ArrName/MediaID (the Arr's own
// episode/movie database id - stable across re-grabs of the same episode or
// movie, unlike entry.InfoHash). Falls back to the normalized entry/release
// name when Arr context couldn't be resolved (mediaID == 0) - weaker (a
// differently-named replacement release won't match), but still catches the
// common case of a fast, repeated failure on the SAME entry name.
func regrabIdentityKey(arrName string, mediaID, episodeID int, fallbackEntryName string) string {
	if arrName != "" && mediaID != 0 {
		return fmt.Sprintf("arr:%s:%d:%d", arrName, mediaID, episodeID)
	}
	return "name:" + normalizeCooldownKey(fallbackEntryName)
}

// checkAndRecord reports whether an automatic re-grab for identity may
// proceed. If identity is already marked terminal, or if recording this
// attempt would exceed regrabGuardMaxAttempts within regrabGuardWindow, it
// refuses (marking identity terminal in the latter case) instead of
// recording another attempt. Otherwise it records the attempt and allows it
// to proceed. firstTrip is true only on the call that flips a record from
// non-terminal to terminal, so the caller logs the transition once instead
// of on every subsequent suppressed call.
func (g *regrabGuard) checkAndRecord(identity string) (allowed bool, reason string, firstTrip bool) {
	if identity == "" {
		return true, "", false
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	rec, ok := g.records[identity]
	if !ok {
		rec = &regrabAttemptRecord{}
		g.records[identity] = rec
	}
	if rec.terminal {
		return false, rec.reason, false
	}

	now := g.nowFn()
	cutoff := now.Add(-regrabGuardWindow)
	live := rec.attempts[:0]
	for _, t := range rec.attempts {
		if t.After(cutoff) {
			live = append(live, t)
		}
	}
	rec.attempts = live

	if len(rec.attempts) >= regrabGuardMaxAttempts {
		rec.terminal = true
		rec.reason = regrabGuardTerminalReason
		rec.loggedTerminal = true
		return false, rec.reason, true
	}

	rec.attempts = append(rec.attempts, now)
	return true, "", false
}

// clear removes identity's attempt history and terminal mark entirely - the
// manual-override counterpart to checkAndRecord's automatic gate, called
// when a user-initiated action (manual "repair now", the overlay GUI's
// "Delete & re-search") re-evaluates a file from scratch.
func (g *regrabGuard) clear(identity string) {
	if identity == "" {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.records, identity)
}

// state returns identity's current attempt count and terminal status, for
// tests/observability.
func (g *regrabGuard) state(identity string) (attempts int, terminal bool, reason string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	rec, ok := g.records[identity]
	if !ok {
		return 0, false, ""
	}
	return len(rec.attempts), rec.terminal, rec.reason
}
