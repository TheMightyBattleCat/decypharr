package manager

import (
	"sync"
	"time"
)

// repairHandlerKind identifies which repair mechanism currently owns an
// entry's damage, for the cross-mechanism coordination repairHandlerRegistry
// provides. See decideAutoRepairAction (repair_policy.go) for the policy
// that decides which of these an automatic caller should try to acquire.
type repairHandlerKind string

const (
	handlerNone        repairHandlerKind = "none"
	handlerPar2Queued  repairHandlerKind = "par2_queued"
	handlerPar2Running repairHandlerKind = "par2_running"
	handlerRegrab      repairHandlerKind = "regrab"
)

// repairHandlerState is one entry's current handler claim.
type repairHandlerState struct {
	kind     repairHandlerKind
	since    time.Time
	terminal bool
}

// defaultRepairHandlerTTL bounds how long a claimed (non-terminal) handler
// slot survives without being explicitly released, in case a goroutine
// panics or is killed between acquiring the slot and its deferred release -
// this is a safety net, not the normal release path (Release/MarkTerminal
// are). Generous: a real PAR2 pass or Arr delete+re-search round trip can
// legitimately take several minutes.
const defaultRepairHandlerTTL = 30 * time.Minute

// repairHandlerRegistry is the single in-memory source of truth for "is this
// entry already being handled, and by what" - consulted by all four
// auto-repair call sites (playback-failure escalation, the repair sweep, the
// PAR2 worker, and manual repair-now) before acting, so an entry already
// being worked on is never double-queued for PAR2 or double-re-grabbed by a
// second, concurrent caller.
//
// Keyed by nzbID (the storage.Entry.InfoHash a PAR2 pass and a Usenet
// re-grab both ultimately operate on). Not persisted: it exists purely to
// serialize concurrent in-process decisions for the lifetime of one claim: a
// terminal mark, the other durable signal, is mirrored here from
// storage.Par2RepairState (see recordPar2Outcome) so callers can consult one
// place, but storage remains the durable source that survives a restart.
type repairHandlerRegistry struct {
	mu      sync.Mutex
	entries map[string]*repairHandlerState
	ttl     time.Duration
	nowFn   func() time.Time
}

// newRepairHandlerRegistry builds an empty registry. ttl<=0 disables the
// stale-claim safety net (claims only ever end via explicit Release/Set/
// MarkTerminal/ClearTerminal) - tests exercising TTL behavior pass a small
// positive value and a fake nowFn.
func newRepairHandlerRegistry(ttl time.Duration) *repairHandlerRegistry {
	return &repairHandlerRegistry{
		entries: make(map[string]*repairHandlerState),
		ttl:     ttl,
		nowFn:   time.Now,
	}
}

// reapIfStaleLocked drops nzbID's claim if it has exceeded the TTL. Terminal
// claims are sticky - they never expire on their own, only ClearTerminal (a
// manual, explicit action) removes one. Caller must hold reg.mu.
func (reg *repairHandlerRegistry) reapIfStaleLocked(nzbID string) {
	e, ok := reg.entries[nzbID]
	if !ok || e.terminal {
		return
	}
	if reg.ttl <= 0 {
		return
	}
	if reg.nowFn().Sub(e.since) >= reg.ttl {
		delete(reg.entries, nzbID)
	}
}

// TryAcquire claims nzbID for kind if nothing else currently owns it (no
// existing claim, or a stale one past TTL). Returns false when another
// handler (queued/running PAR2, an in-flight re-grab, or a terminal mark)
// already owns the entry - the caller must defer to it and do nothing
// automatic of its own. Empty nzbID always fails (nothing to key on).
func (reg *repairHandlerRegistry) TryAcquire(nzbID string, kind repairHandlerKind) bool {
	if nzbID == "" {
		return false
	}
	reg.mu.Lock()
	defer reg.mu.Unlock()
	reg.reapIfStaleLocked(nzbID)
	if _, exists := reg.entries[nzbID]; exists {
		return false
	}
	reg.entries[nzbID] = &repairHandlerState{kind: kind, since: reg.nowFn()}
	return true
}

// Set unconditionally claims nzbID for kind, overwriting any existing
// (non-terminal) claim and clearing terminal if set. This is the manual
// override primitive: a user-initiated "repair now" / "delete & re-search"
// always proceeds regardless of what an automatic path had claimed, and
// explicitly un-sticks a terminal mark rather than being blocked by it.
func (reg *repairHandlerRegistry) Set(nzbID string, kind repairHandlerKind) {
	if nzbID == "" {
		return
	}
	reg.mu.Lock()
	defer reg.mu.Unlock()
	reg.entries[nzbID] = &repairHandlerState{kind: kind, since: reg.nowFn()}
}

// Transition updates an already-claimed, non-terminal entry's kind in place
// (e.g. par2_queued -> par2_running) without releasing and re-acquiring the
// slot. A no-op if nzbID has no claim or is terminal - callers that need to
// claim unconditionally should use Set.
func (reg *repairHandlerRegistry) Transition(nzbID string, kind repairHandlerKind) {
	if nzbID == "" {
		return
	}
	reg.mu.Lock()
	defer reg.mu.Unlock()
	e, ok := reg.entries[nzbID]
	if !ok || e.terminal {
		return
	}
	e.kind = kind
	e.since = reg.nowFn()
}

// Release clears nzbID's claim (call on completion - success or a transient,
// retryable failure - so a later decision isn't blocked by a stale slot).
// A no-op for a terminal entry - use ClearTerminal to remove that
// deliberately.
func (reg *repairHandlerRegistry) Release(nzbID string) {
	if nzbID == "" {
		return
	}
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if e, ok := reg.entries[nzbID]; ok && e.terminal {
		return
	}
	delete(reg.entries, nzbID)
}

// MarkTerminal marks nzbID as terminally unrepairable by the automatic path
// (PAR2 classified its own failure as terminal - see classifyPar2Failure).
// Sticky: only ClearTerminal removes it, so no automatic caller races back
// in while the GUI surfaces this file for a manual "Delete & re-search".
func (reg *repairHandlerRegistry) MarkTerminal(nzbID string) {
	if nzbID == "" {
		return
	}
	reg.mu.Lock()
	defer reg.mu.Unlock()
	reg.entries[nzbID] = &repairHandlerState{kind: handlerNone, since: reg.nowFn(), terminal: true}
}

// ClearTerminal removes nzbID's claim entirely, terminal or not - the manual
// override counterpart to MarkTerminal, called when a user-initiated action
// (manual PAR2 repair-now, or the overlay GUI's "Delete & re-search")
// re-evaluates an entry from scratch.
func (reg *repairHandlerRegistry) ClearTerminal(nzbID string) {
	if nzbID == "" {
		return
	}
	reg.mu.Lock()
	defer reg.mu.Unlock()
	delete(reg.entries, nzbID)
}

// IsTerminal reports whether nzbID is currently marked terminal.
func (reg *repairHandlerRegistry) IsTerminal(nzbID string) bool {
	if nzbID == "" {
		return false
	}
	reg.mu.Lock()
	defer reg.mu.Unlock()
	e, ok := reg.entries[nzbID]
	return ok && e.terminal
}

// State returns nzbID's current claim, if any (reaping it first if stale).
// exists is false when there is no live claim - the entry is free.
func (reg *repairHandlerRegistry) State(nzbID string) (kind repairHandlerKind, terminal bool, exists bool) {
	if nzbID == "" {
		return handlerNone, false, false
	}
	reg.mu.Lock()
	defer reg.mu.Unlock()
	reg.reapIfStaleLocked(nzbID)
	e, ok := reg.entries[nzbID]
	if !ok {
		return handlerNone, false, false
	}
	return e.kind, e.terminal, true
}

// ReapStale walks every claim and drops the ones past TTL (terminal claims
// are always skipped - see reapIfStaleLocked). This is the periodic-sweep
// counterpart to the lazy, per-key reaping every other method already does
// on access: it exists so a claim for an entry nothing queries again (e.g.
// its owning goroutine panicked before Release) doesn't linger in the map
// forever just because nothing happens to look it up. Returns the number of
// entries reaped, for tests/observability.
func (reg *repairHandlerRegistry) ReapStale() int {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if reg.ttl <= 0 {
		return 0
	}
	now := reg.nowFn()
	reaped := 0
	for id, e := range reg.entries {
		if e.terminal {
			continue
		}
		if now.Sub(e.since) >= reg.ttl {
			delete(reg.entries, id)
			reaped++
		}
	}
	return reaped
}
