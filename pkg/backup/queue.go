package backup

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// RunQueueFile in the data folder holds the run in progress and the runs
// waiting for the run slot, so neither a restart nor a crash loses one.
const RunQueueFile = "backup-queue.json"

const (
	// queueSkipped is a scheduled run that found another backup running.
	queueSkipped = "skipped"
	// queueInterrupted is a run that a restart, crash or power cut ended.
	queueInterrupted = "interrupted"
)

// maxRunAttempts is how many times one run is started before an
// interrupted run stops being resumed (a run that crashes the process would
// otherwise restart it for ever).
const maxRunAttempts = 3

// resumeDelay is how long after start the queue is first looked at, so a
// resumed run does not compete with the rest of start-up.
var resumeDelay = 2 * time.Minute

// kindCheck marks a queued run of the scheduled check rather than a job.
const kindCheck = "check"

// queuedRun is one run waiting for the run slot, or the run holding it.
type queuedRun struct {
	// Kind is "" for a backup job and kindCheck for the check.
	Kind    string `json:"kind,omitempty"`
	Job     string `json:"job,omitempty"`
	Trigger string `json:"trigger"` // "schedule:<job>" or "manual:<job>"
	Reason  string `json:"reason,omitempty"`
	// Since is when the run was queued, or started for the run in progress.
	Since time.Time `json:"since"`
	// Attempts counts the starts of this run, interrupted ones included.
	Attempts int `json:"attempts,omitempty"`
}

// key tells runs apart: one entry per job, and one for the check.
func (r queuedRun) key() string {
	if r.Kind == kindCheck {
		return kindCheck + ":"
	}
	return "job:" + r.Job
}

// label names the run for messages.
func (r queuedRun) label() string {
	if r.Kind == kindCheck {
		return "backup check"
	}
	return fmt.Sprintf("backup job %q", r.Job)
}

type runQueueDoc struct {
	// Running is written when a run starts and removed when it ends on its
	// own (finished, failed or stopped from the page). A start that still
	// finds it knows the run was cut short and queues it again.
	Running *queuedRun  `json:"running,omitempty"`
	Queue   []queuedRun `json:"queue,omitempty"`
}

// saveQueueLocked writes the queue file; the caller holds qmu.
func (s *BackupService) saveQueueLocked() {
	if s.deps.DataDir == "" {
		return
	}
	path := filepath.Join(s.deps.DataDir, RunQueueFile)
	if s.running == nil && len(s.queue) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			s.log.Warn().Err(err).Msg("Backup queue file was not removed")
		}
		return
	}
	if err := writeJSONFile(path, runQueueDoc{Running: s.running, Queue: s.queue}); err != nil {
		s.log.Warn().Err(err).Msg("Backup queue was not saved; a restart may lose a waiting run")
	}
}

// recoverQueue loads the queue file at start. A run still marked as running
// was cut short: it goes to the front of the queue, unless it has already
// been started maxRunAttempts times.
func (s *BackupService) recoverQueue() {
	if s.deps.DataDir == "" {
		return
	}
	data, err := os.ReadFile(filepath.Join(s.deps.DataDir, RunQueueFile))
	if err != nil {
		if !os.IsNotExist(err) {
			s.log.Warn().Err(err).Msg("Backup queue file is unreadable; starting with an empty queue")
		}
		return
	}
	var doc runQueueDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		s.log.Warn().Err(err).Msg("Backup queue file is unreadable; starting with an empty queue")
		return
	}
	s.qmu.Lock()
	defer s.qmu.Unlock()
	s.queue = doc.Queue
	if r := doc.Running; r != nil {
		if r.Attempts >= maxRunAttempts {
			s.log.Error().Str("job", r.Job).Int("attempts", r.Attempts).
				Msg("Backup was interrupted again and will not be resumed; it runs at its next scheduled time")
			if s.cfg().Notify.OnFailure {
				msg := fmt.Sprintf("The %s was interrupted %d times in a row and will not be resumed again", r.label(), r.Attempts)
				go s.notify(config.EventBackupFailed, "error", msg, nil)
			}
		} else {
			r.Reason = queueInterrupted
			s.queue = append([]queuedRun{*r}, without(s.queue, r.key())...)
			s.log.Info().Str("job", r.Job).Str("kind", r.Kind).Msg("Backup was interrupted by a restart; it resumes shortly")
		}
	}
	s.running = nil
	s.saveQueueLocked()
}

// enqueue adds a run that could not get the slot. A job is queued once; an
// interrupted run goes to the front.
func (s *BackupService) enqueue(r queuedRun) {
	s.qmu.Lock()
	defer s.qmu.Unlock()
	for _, q := range s.queue {
		if q.key() == r.key() {
			return
		}
	}
	if r.Since.IsZero() {
		r.Since = s.now()
	}
	if r.Reason == queueInterrupted {
		s.queue = append([]queuedRun{r}, s.queue...)
	} else {
		s.queue = append(s.queue, r)
	}
	s.saveQueueLocked()
}

// markRunning records the run that now holds the slot and drops the job
// from the queue: this run covers what was waiting.
func (s *BackupService) markRunning(r queuedRun) {
	s.qmu.Lock()
	defer s.qmu.Unlock()
	s.queue = without(s.queue, r.key())
	s.running = &r
	s.saveQueueLocked()
}

// clearRunning ends the run in progress. A run ended by a shutdown leaves its
// record so the next start resumes it.
func (s *BackupService) clearRunning() {
	s.qmu.Lock()
	defer s.qmu.Unlock()
	s.running = nil
	s.saveQueueLocked()
}

// queuedFor reports why a job is waiting, or "" when it is not.
func (s *BackupService) queuedFor(job string) string {
	return s.queuedReason(queuedRun{Job: job}.key())
}

func (s *BackupService) queuedReason(key string) string {
	s.qmu.Lock()
	defer s.qmu.Unlock()
	for _, q := range s.queue {
		if q.key() == key {
			return q.Reason
		}
	}
	return ""
}

func without(q []queuedRun, key string) []queuedRun {
	out := q[:0:0]
	for _, r := range q {
		if r.key() != key {
			out = append(out, r)
		}
	}
	return out
}

// stopping reports whether Stop has been called.
func (s *BackupService) stopping() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ctx != nil && s.ctx.Err() != nil
}

// kick starts the first queued run if the slot is free. It runs when a run
// releases the slot and once shortly after start. A scheduled run that comes
// up inside quiet hours waits for them to end.
func (s *BackupService) kick() {
	if s.stopping() {
		return
	}
	next, ok := s.nextQueued()
	if !ok {
		return
	}
	if strings.HasPrefix(next.Trigger, "schedule:") {
		if end, in := quietUntil(s.now(), s.cfg().QuietHours); in {
			s.kickAt(end)
			return
		}
	}
	if next.Kind == kindCheck {
		release, err := s.claim(checkLabel)
		if err != nil {
			return // the run holding the slot kicks again when it ends
		}
		go func() {
			defer release()
			s.runCheck(s.runContext(), next.Trigger, next.Attempts)
		}()
		return
	}
	job, ok := s.jobByName(next.Job)
	if !ok {
		return
	}
	release, err := s.claim("job:" + next.Job)
	if err != nil {
		return // the run holding the slot kicks again when it ends
	}
	go func() {
		defer release()
		s.runJob(s.runContext(), job, next.Trigger, next.Attempts)
	}()
}

// nextQueued is the first queued run whose job can still run; entries for
// jobs that were removed, and scheduled entries of disabled jobs, are dropped.
func (s *BackupService) nextQueued() (queuedRun, bool) {
	s.qmu.Lock()
	defer s.qmu.Unlock()
	changed := false
	defer func() {
		if changed {
			s.saveQueueLocked()
		}
	}()
	for len(s.queue) > 0 {
		r := s.queue[0]
		if r.Kind == kindCheck {
			if s.cfg().Check.Enabled || !strings.HasPrefix(r.Trigger, "schedule:") {
				return r, true
			}
		} else if job, ok := s.jobByName(r.Job); ok && (job.Enabled || r.Trigger != "schedule:"+r.Job) {
			return r, true
		}
		s.queue = s.queue[1:]
		changed = true
	}
	return queuedRun{}, false
}

// kickAt kicks the queue at t, once, unless the service stops first.
func (s *BackupService) kickAt(t time.Time) {
	s.mu.Lock()
	ctx := s.ctx
	if s.kickTimer != nil {
		s.kickTimer.Stop()
	}
	s.kickTimer = time.AfterFunc(time.Until(t), func() {
		if ctx == nil || ctx.Err() == nil {
			s.kick()
		}
	})
	s.mu.Unlock()
}
