package manager

import (
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/storage"
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

	// regrabDedupeWindow bounds how long the guard remembers "this exact
	// release was already struck" before a re-offer of the same release
	// counts as a fresh strike again. Deliberately short and independent of
	// regrabGuardWindow (matches playbackRepairCooldown's scale) - it exists
	// only so an Arr re-offering the identical, already-blocklisted release
	// doesn't burn through the 2-strike budget on duplicate evidence. A
	// genuinely different dead candidate always strikes, no matter how soon
	// after the last one it arrives.
	regrabDedupeWindow = playbackRepairCooldown

	// keepReleaseRetryWindow is how long a re-grab that KEPT a release (a
	// file assembled wrong at import - see keepReleaseReason) blocks another
	// automatic keep-release re-grab of that same release for the same file.
	// Re-importing the same NZB reproduces the same assembly: on a production install
	// 8-Bit Midwinter was re-grabbed every night for five nights, each import
	// byte-identical with the same zero-filled region. regrabGuardWindow
	// cannot stop that - its 24 h window equals the sweep's period, so every
	// night's attempt lands just after the previous one expired. A fix to
	// import assembly reaches such a file through a manual Replace, which
	// clears the guard.
	keepReleaseRetryWindow = 30 * 24 * time.Hour

	// regrabGuardTerminalTTL is how long a tripped guard stays tripped. It
	// used to last only until the next restart; persisted, it needs a bound
	// of its own, so a release posted later can still be grabbed without a
	// manual action.
	regrabGuardTerminalTTL = 7 * 24 * time.Hour

	// keepReleaseRepeatReason is surfaced when the keep-release check refuses.
	keepReleaseRepeatReason = "re-grabbing this release reproduced the same import fault; replace it by hand"
)

// regrabGuardStore persists the guard's records (storage.Storage in
// production) so a restart doesn't wipe a loop's history.
type regrabGuardStore interface {
	SaveRegrabGuardRecord(*storage.RegrabGuardRecord) error
	DeleteRegrabGuardRecord(identity string) error
	ForEachRegrabGuardRecord(func(*storage.RegrabGuardRecord)) error
}

// regrabAttemptRecord is one logical file's automatic re-grab history.
type regrabAttemptRecord struct {
	attempts   []time.Time
	terminal   bool
	terminalAt time.Time
	reason     string
	// loggedTerminal is true once the terminal transition has been logged
	// at WARN - every later call while still terminal only needs a quiet
	// DEBUG, not a repeat of the same warning on every subsequent playback
	// failure (the exact log-storm shape this whole fix is about).
	loggedTerminal bool
	// recentReleases remembers, per distinct release identity (the release
	// name the caller passes to checkAndRecord), the last time that exact
	// release was struck - see regrabDedupeWindow. A release seen again
	// inside the window is still refused/blocklisted by the caller as
	// normal; it just doesn't consume a fresh strike, since it's the same
	// evidence already counted rather than a new distinct dead candidate.
	recentReleases map[string]time.Time
	// keepRelease records, per release name, when a keep-release re-grab of
	// it went ahead - see keepReleaseRetryWindow.
	keepRelease map[string]time.Time
}

func (rec *regrabAttemptRecord) toStorage(identity string) *storage.RegrabGuardRecord {
	return &storage.RegrabGuardRecord{
		Identity:       identity,
		Attempts:       rec.attempts,
		Terminal:       rec.terminal,
		TerminalAt:     rec.terminalAt,
		Reason:         rec.reason,
		RecentReleases: rec.recentReleases,
		KeepRelease:    rec.keepRelease,
	}
}

// empty reports whether rec holds nothing worth keeping.
func (rec *regrabAttemptRecord) empty() bool {
	return len(rec.attempts) == 0 && !rec.terminal && len(rec.keepRelease) == 0
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
	store   regrabGuardStore
	logger  zerolog.Logger
}

func newRegrabGuard() *regrabGuard {
	return &regrabGuard{
		records: make(map[string]*regrabAttemptRecord),
		nowFn:   time.Now,
		logger:  zerolog.Nop(),
	}
}

// attach loads every persisted record from st and persists every later
// change to it. Records with nothing left inside their windows are dropped.
func (g *regrabGuard) attach(st regrabGuardStore, logger zerolog.Logger) {
	if st == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.store = st
	g.logger = logger
	now := g.nowFn()
	var stale []string
	err := st.ForEachRegrabGuardRecord(func(sr *storage.RegrabGuardRecord) {
		rec := &regrabAttemptRecord{
			terminal:       sr.Terminal && now.Sub(sr.TerminalAt) < regrabGuardTerminalTTL,
			terminalAt:     sr.TerminalAt,
			reason:         sr.Reason,
			loggedTerminal: sr.Terminal,

			recentReleases: sr.RecentReleases,
			keepRelease:    sr.KeepRelease,
		}
		for _, t := range sr.Attempts {
			if now.Sub(t) < regrabGuardWindow {
				rec.attempts = append(rec.attempts, t)
			}
		}
		for name, t := range rec.keepRelease {
			if now.Sub(t) >= keepReleaseRetryWindow {
				delete(rec.keepRelease, name)
			}
		}
		if rec.empty() {
			stale = append(stale, sr.Identity)
			return
		}
		g.records[sr.Identity] = rec
	})
	if err != nil {
		logger.Warn().Err(err).Msg("Repair: could not load the re-grab guard's history; starting empty")
	}
	for _, id := range stale {
		_ = st.DeleteRegrabGuardRecord(id)
	}
}

// persist writes identity's record (or removes it when empty). Called with
// g.mu held.
func (g *regrabGuard) persist(identity string, rec *regrabAttemptRecord) {
	if g.store == nil {
		return
	}
	var err error
	if rec == nil || rec.empty() {
		err = g.store.DeleteRegrabGuardRecord(identity)
	} else {
		err = g.store.SaveRegrabGuardRecord(rec.toStorage(identity))
	}
	if err != nil {
		g.logger.Warn().Err(err).Str("identity", identity).Msg("Repair: could not persist the re-grab guard")
	}
}

// keepReleaseRepeated reports whether a keep-release re-grab of releaseName
// for identity already went ahead inside keepReleaseRetryWindow - another
// one would re-import the same NZB into the same fault. Records nothing.
func (g *regrabGuard) keepReleaseRepeated(identity, releaseName string) bool {
	if identity == "" || releaseName == "" {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	rec, ok := g.records[identity]
	if !ok {
		return false
	}
	last, seen := rec.keepRelease[releaseName]
	return seen && g.nowFn().Sub(last) < keepReleaseRetryWindow
}

// recordKeepRelease notes that a keep-release re-grab of releaseName for
// identity is going ahead.
func (g *regrabGuard) recordKeepRelease(identity, releaseName string) {
	if identity == "" || releaseName == "" {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	rec, ok := g.records[identity]
	if !ok {
		rec = &regrabAttemptRecord{}
		g.records[identity] = rec
	}
	now := g.nowFn()
	if rec.keepRelease == nil {
		rec.keepRelease = make(map[string]time.Time)
	}
	for name, t := range rec.keepRelease {
		if now.Sub(t) >= keepReleaseRetryWindow {
			delete(rec.keepRelease, name)
		}
	}
	rec.keepRelease[releaseName] = now
	g.persist(identity, rec)
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
// proceed. releaseName identifies the specific candidate release being
// struck (e.g. the entry/release name) - if the SAME release was already
// struck for this identity inside regrabDedupeWindow, this call is allowed
// without consuming a strike (duplicate evidence, not a new candidate); pass
// "" to skip deduping. Otherwise: if identity is already marked terminal, or
// if recording this attempt would exceed regrabGuardMaxAttempts within
// regrabGuardWindow, it refuses (marking identity terminal in the latter
// case) instead of recording another attempt. Otherwise it records the
// attempt and allows it to proceed. firstTrip is true only on the call that
// flips a record from non-terminal to terminal, so the caller logs the
// transition once instead of on every subsequent suppressed call.
func (g *regrabGuard) checkAndRecord(identity, releaseName string) (allowed bool, reason string, firstTrip bool) {
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
	now := g.nowFn()
	if rec.terminal && now.Sub(rec.terminalAt) >= regrabGuardTerminalTTL {
		rec.terminal, rec.reason, rec.loggedTerminal = false, "", false
		rec.attempts = nil
	}
	if rec.terminal {
		return false, rec.reason, false
	}

	if releaseName != "" {
		if last, seen := rec.recentReleases[releaseName]; seen && now.Sub(last) < regrabDedupeWindow {
			return true, "", false
		}
	}

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
		rec.terminalAt = now
		rec.reason = regrabGuardTerminalReason
		rec.loggedTerminal = true
		g.persist(identity, rec)
		return false, rec.reason, true
	}

	rec.attempts = append(rec.attempts, now)
	if releaseName != "" {
		if rec.recentReleases == nil {
			rec.recentReleases = make(map[string]time.Time)
		}
		// Prune stale entries opportunistically so this map doesn't grow
		// unbounded across every distinct release name seen over the
		// identity's lifetime.
		for name, t := range rec.recentReleases {
			if now.Sub(t) >= regrabDedupeWindow {
				delete(rec.recentReleases, name)
			}
		}
		rec.recentReleases[releaseName] = now
	}
	g.persist(identity, rec)
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
	g.persist(identity, nil)
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
