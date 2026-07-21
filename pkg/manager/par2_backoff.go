package manager

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

const (
	// par2BackoffBase is the first retry delay after a transient failure,
	// doubling each subsequent consecutive failure (5m, 10m, 20m, ...) up to
	// par2BackoffCap - replaces the previous behavior of re-attempting on
	// every triggering event (a playback read of the still-padded segment,
	// or the next repair sweep pass), which for a job that itself takes
	// ~15-20 minutes to fail (a full sequential streaming pass over the
	// release before concluding there's nothing usable) meant a near-constant
	// hammer of provider bandwidth for an attempt very likely to fail the
	// same way again immediately.
	par2BackoffBase = 5 * time.Minute
	// par2BackoffCap bounds how long automatic retries ever back off to.
	par2BackoffCap = 6 * time.Hour
	// par2BackoffMaxShift bounds the doubling exponent so the shift itself
	// never overflows time.Duration - par2BackoffBase<<11 already exceeds
	// par2BackoffCap by a wide margin, so anything beyond this just clamps.
	par2BackoffMaxShift = 11
)

// par2BackoffDuration returns how long to wait before the next automatic
// retry, given attemptCount consecutive failures (1 = the first failure).
func par2BackoffDuration(attemptCount int) time.Duration {
	if attemptCount <= 0 {
		return 0
	}
	shift := attemptCount - 1
	if shift > par2BackoffMaxShift {
		shift = par2BackoffMaxShift
	}
	d := par2BackoffBase * time.Duration(uint64(1)<<uint(shift))
	if d <= 0 || d > par2BackoffCap {
		return par2BackoffCap
	}
	return d
}

// par2FailureClass is the outcome of classifyPar2Failure.
type par2FailureClass struct {
	// terminal means retrying can never succeed without external state
	// changing (new PAR2 data being posted, the Arr re-acquiring the
	// release) - the automatic path must stop re-enqueuing entirely, and the
	// overlay management API surfaces this file as "unrepairable (reason)"
	// with only a manual retry allowed (which always re-evaluates from
	// scratch - see Par2Repair.Enqueue).
	terminal bool
	// reason is the human-readable classification, surfaced verbatim in the
	// persisted Par2RepairState.TerminalReason and the overlay list.
	reason string
}

// par2TerminalSubstrings are runRepair failure messages that can never
// resolve themselves on a bare retry: no PAR2 data was ever retained for
// this release, every retained PAR2 file failed to fetch (recovery data
// confirmed gone, not merely slow), or the PAR2 index/posted-file mapping
// is structurally inconsistent with what's on disk. This is deliberately a
// substring match against runRepair's existing error messages rather than a
// parallel set of typed sentinel errors threaded through every return in
// that function - runRepair has no other error-classification consumer
// today, and inventing one across a dozen call sites for a single new
// caller isn't worth the churn. Anything NOT matched here defaults to
// transient (backed off, not given up on) - a false "transient" costs an
// extra backed-off retry; a false "terminal" would silently stop ever
// trying again, which is the worse mistake to risk.
var par2TerminalSubstrings = []string{
	"no PAR2 data available",
	"no PAR2 data and backfill failed",
	"no PAR2 recovery volumes retained",
	"failed to fetch any PAR2 file",
	"no posted file matched the PAR2 recovery set",
	"match posted files:",
	"is not part of any matched posted file",
	"map dead segment",
	"exceeds the",
	"recovery slices fetched/available",
	"recovery slice",
	"parse PAR2 index",
}

// classifyPar2Failure decides whether err (a runRepair failure) should back
// off and retry automatically, or stop trying entirely until a manual
// repair-now. A context deadline/cancellation - the job simply ran out of
// time or was shut down mid-pass, proving nothing about whether the data is
// actually available - is always transient.
func classifyPar2Failure(err error) par2FailureClass {
	if err == nil {
		return par2FailureClass{}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return par2FailureClass{}
	}
	if errors.Is(err, par2.ErrChecksumMismatch) {
		return par2FailureClass{terminal: true, reason: "checksum verification failed (CRC canary) - retained recovery/intact data can't be trusted"}
	}
	msg := err.Error()
	for _, s := range par2TerminalSubstrings {
		if strings.Contains(msg, s) {
			return par2FailureClass{terminal: true, reason: msg}
		}
	}
	return par2FailureClass{}
}

// par2ShouldAutoEnqueue reports whether nzbID's persisted repair state
// permits an automatic (non-manual) enqueue right now: no state yet, or the
// last outcome wasn't terminal and any backoff window has elapsed. Only
// consulted by Enqueue - the automatic-trigger primitive - never by RunNow,
// so a manual "repair now" always overrides and re-evaluates from scratch
// regardless of prior terminal/backoff state.
func (p *Par2Repair) par2ShouldAutoEnqueue(nzbID string) bool {
	state, err := p.manager.storage.GetPar2RepairState(nzbID)
	if err != nil || state == nil {
		return true
	}
	if state.Terminal {
		p.logger.Debug().Str("entry", nzbID).Str("reason", state.TerminalReason).
			Msg("par2 repair: skipping auto-enqueue, marked unrepairable (terminal) - use manual repair now to retry")
		return false
	}
	if !state.NextRetryAt.IsZero() && time.Now().Before(state.NextRetryAt) {
		p.logger.Debug().Str("entry", nzbID).Time("next_retry_at", state.NextRetryAt).
			Msg("par2 repair: skipping auto-enqueue, still in backoff window")
		return false
	}
	return true
}

// recordPar2Outcome updates nzbID's persisted repair state after one
// attempt (automatic or manual - RunNow's attempts update this exactly the
// same way, so a manual retry's own outcome is what determines whether
// future AUTOMATIC attempts resume). A nil err (success) clears all
// backoff/terminal state entirely - best-effort, logged not returned, since
// a state-tracking failure must never affect the repair pass itself.
func (p *Par2Repair) recordPar2Outcome(nzbID string, err error) {
	if err == nil {
		if derr := p.manager.storage.DeletePar2RepairState(nzbID); derr != nil {
			p.logger.Debug().Err(derr).Str("entry", nzbID).Msg("par2 repair: failed to clear repair state after success")
		}
		return
	}

	state, gerr := p.manager.storage.GetPar2RepairState(nzbID)
	if gerr != nil || state == nil {
		state = &storage.Par2RepairState{NzbID: nzbID}
	}
	state.LastAttempt = time.Now()
	state.AttemptCount++
	state.LastError = err.Error()

	class := classifyPar2Failure(err)
	state.Terminal = class.terminal
	if class.terminal {
		state.TerminalReason = class.reason
		state.NextRetryAt = time.Time{}
	} else {
		state.TerminalReason = ""
		state.NextRetryAt = time.Now().Add(par2BackoffDuration(state.AttemptCount))
	}

	if serr := p.manager.storage.SavePar2RepairState(state); serr != nil {
		p.logger.Debug().Err(serr).Str("entry", nzbID).Msg("par2 repair: failed to persist repair state")
	}
}
