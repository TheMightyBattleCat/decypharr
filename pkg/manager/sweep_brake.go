package manager

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/notifications"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// sweepBrakeLimit is how many Usenet files in a row may end with a failed read
// or an unreadable container before a sweep stops healing. An entry's
// unreadable containers count once between them (see observe).
const sweepBrakeLimit = 5

// sweepBrake stops a sweep's healing once its verdicts have all turned bad at
// once. When no news server can be reached (the box's connection is down, DNS
// fails, every provider refuses or is over its quota), every file the sweep
// reads comes back short, and nothing in a single verdict tells that apart
// from a damaged file. Five such files in a row stops every delete,
// blocklist and re-search for the rest of the sweep; probing goes on, so the
// health records still show what was seen.
//
// A STAT check no news server answered at all (Usenet.checkAvailability's
// ErrAvailabilityUnchecked, "No eligible providers" included) leaves the file
// Unknown, unread and unhealed: neutral here, it neither counts nor resets.
// What counts is an outage the STAT check got through and the body fetch
// behind the ffprobe read did not.
type sweepBrake struct {
	mu      sync.Mutex
	inARow  int
	tripped bool
	onTrip  func(reason string)
}

type sweepBrakeKey struct{}

// brakeSignal is what one probed file says to the brake.
type brakeSignal int

const (
	brakeNeutral brakeSignal = iota // says nothing about reachability
	brakeCount                      // the read failed, or the container could not be opened
	brakeReset                      // a provider answered: read clean, or reported the article gone
)

// sweepBrakeSignal classifies one probed file. Torrent files are neutral: a
// debrid check says nothing about the news servers, and counting them as
// healthy would let a mixed library reset the count during an outage.
func sweepBrakeSignal(res fileResult) brakeSignal {
	if res.protocol == config.ProtocolTorrent {
		return brakeNeutral
	}
	switch {
	case res.readFailed:
		return brakeCount
	case res.broken && strings.HasPrefix(res.reason, ffprobeReasonUnreadable):
		return brakeCount
	case realMissingArticleReason(res.reason):
		return brakeReset
	case res.healthy:
		return brakeReset
	}
	return brakeNeutral
}

// realMissingArticleReason reports a verdict a news server gave: an article
// reported missing (430), not a read that stopped.
func realMissingArticleReason(reason string) bool {
	switch reason {
	case "usenet_segment_missing", "usenet_segment_missing_deferred",
		"sweep_dead_segment", "sweep_dead_segment_ffprobe_override",
		ffprobeReasonDeadSegment:
		return true
	}
	return false
}

// unreadableContainer reports an ffprobe verdict that the container cannot
// be parsed, with no failed read behind it: permanent, never retried.
func unreadableContainer(res fileResult) bool {
	return !res.readFailed && strings.HasPrefix(res.reason, ffprobeReasonUnreadable) &&
		!strings.Contains(res.reason, ffprobeReadFailedTag)
}

// observe folds one entry's file results into the count, in file order. Once
// tripped the brake holds for the rest of the sweep; onTrip runs once.
//
// An entry's unreadable containers add 1 between them: one fake or
// wrong-password pack gives that verdict for every file, and on its own
// says nothing about the news servers. Failed reads count per file, and a
// run of different entries with unreadable files still trips.
func (b *sweepBrake) observe(results []fileResult) {
	if b == nil {
		return
	}
	reason := ""
	countedUnreadable := false
	b.mu.Lock()
	for _, res := range results {
		if b.tripped {
			break
		}
		switch sweepBrakeSignal(res) {
		case brakeCount:
			if unreadableContainer(res) {
				if countedUnreadable {
					continue
				}
				countedUnreadable = true
			}
			b.inARow++
			if b.inARow >= sweepBrakeLimit {
				b.tripped = true
				reason = fmt.Sprintf("%d files in a row could not be read (no news server answered, or every provider refused); the last was %q", b.inARow, res.name)
			}
		case brakeReset:
			b.inARow = 0
		}
	}
	b.mu.Unlock()
	if reason != "" && b.onTrip != nil {
		b.onTrip(reason)
	}
}

// holding reports whether the brake has tripped.
func (b *sweepBrake) holding() bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.tripped
}

func sweepBrakeFrom(ctx context.Context) *sweepBrake {
	if ctx == nil {
		return nil
	}
	b, _ := ctx.Value(sweepBrakeKey{}).(*sweepBrake)
	return b
}

// withSweepBrake gives run's probes a brake; autoRepairStillOn reads it.
func (r *Repair) withSweepBrake(ctx context.Context, run *storage.RepairRun) context.Context {
	b := &sweepBrake{onTrip: func(reason string) { r.sweepBrakeTripped(run, reason) }}
	return context.WithValue(ctx, sweepBrakeKey{}, b)
}

// sweepBrakeTripped logs the stop and sends the repair notification with its
// reason.
func (r *Repair) sweepBrakeTripped(run *storage.RepairRun, reason string) {
	runID := ""
	if run != nil {
		runID = run.ID
	}
	r.logger.Warn().Str("run_id", runID).Str("reason", reason).
		Msg("Sweep: healing stopped for the rest of this sweep; files are still checked, nothing more is deleted or re-searched")
	if r.manager == nil || r.manager.Notifications == nil {
		return
	}
	r.manager.Notifications.Notify(notifications.Event{
		Type:   config.EventRepairFailed,
		Status: "warning",
		Message: fmt.Sprintf("\n**Run**: %s\n**Healing stopped**: %s\nThe sweep goes on checking files but deletes and re-searches nothing more. Check the news servers, then run Fix broken.",
			runID, reason),
	})
}
