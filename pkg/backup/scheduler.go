package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-co-op/gocron/v2"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
)

const (
	scheduleTag = "backup"
	watchTag    = "backup-watch"
	linksTag    = "backup-links"
	// staleCheckEvery is how often the stale check looks at the snapshots.
	staleCheckEvery = 30 * time.Minute
)

// Start registers the jobs on the scheduler and watches the settings, so a
// save reschedules without a restart even when no code calls ApplyConfig.
// The scheduler itself is started by the manager.
func (s *BackupService) Start(ctx context.Context) error {
	s.mu.Lock()
	s.ctx, s.cancel = context.WithCancel(ctx)
	runCtx := s.ctx
	s.mu.Unlock()
	// Runs a restart or crash cut short, and scheduled runs that were
	// waiting for the slot, start shortly after start-up.
	s.recoverQueue()
	s.removeDrills()
	go func() {
		t := time.NewTimer(resumeDelay)
		defer t.Stop()
		select {
		case <-runCtx.Done():
		case <-t.C:
			s.kick()
			s.uploadIfBehind(runCtx)
		}
	}()
	if s.deps.Scheduler != nil {
		_, err := s.deps.Scheduler.NewJob(gocron.DurationJob(time.Minute),
			gocron.NewTask(s.reconcile), gocron.WithTags(watchTag), gocron.WithName("backup-watch"))
		if err != nil {
			return err
		}
	}
	return s.ApplyConfig()
}

// stopWait is how long Stop waits for a cancelled run to wind down, so the
// manager does not close the database under it.
var stopWait = 10 * time.Second

// Stop removes every backup job, cancels a run in progress and waits (up to
// stopWait) for it to finish.
func (s *BackupService) Stop() {
	if s.deps.Scheduler != nil {
		s.deps.Scheduler.RemoveByTags(scheduleTag, watchTag, linksTag, checkTag)
	}
	s.mu.Lock()
	cancel := s.cancel
	s.jobs = map[string]gocron.Job{}
	if s.kickTimer != nil {
		s.kickTimer.Stop()
	}
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	deadline := time.Now().Add(stopWait)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		idle := s.runningJob == ""
		s.mu.Unlock()
		if idle {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.log.Warn().Msg("Backup run did not stop in time; carrying on with shutdown")
}

// scheduleSignature is what ApplyConfig depends on.
func scheduleSignature(cfg config.Backup) string {
	b, _ := json.Marshal(struct {
		Dir   string
		Jobs  []config.BackupJob
		Link  config.BackupLinkCheck
		Quiet config.BackupQuietHours
		Check config.BackupCheck
	}{cfg.Dir, cfg.Jobs, cfg.LinkCheck, cfg.QuietHours, cfg.Check})
	return string(b)
}

// reconcile re-applies the schedule when the settings changed and runs the
// stale check.
func (s *BackupService) reconcile() {
	cfg := s.cfg()
	s.mu.Lock()
	changed := scheduleSignature(cfg) != s.applied
	s.mu.Unlock()
	if changed {
		if err := s.ApplyConfig(); err != nil {
			s.log.Warn().Err(err).Msg("Backup schedule not applied")
		}
	}
	s.checkStale(context.Background())
}

// ApplyConfig re-registers the jobs from the live settings. Call it after a
// settings save; a run in progress is not interrupted. A bad schedule only
// skips its own job and is returned.
func (s *BackupService) ApplyConfig() error {
	cfg := s.cfg()
	if s.deps.Scheduler == nil {
		s.mu.Lock()
		s.applied = scheduleSignature(cfg)
		s.mu.Unlock()
		return nil
	}
	sched := s.deps.Scheduler
	sched.RemoveByTags(scheduleTag, linksTag, checkTag)
	jobs := map[string]gocron.Job{}
	var checkJob gocron.Job
	var errs []error
	if strings.TrimSpace(cfg.Dir) != "" {
		for _, j := range cfg.Jobs {
			if !j.Enabled {
				continue
			}
			jd, err := utils.ConvertToClockOrCronJobDef(j.Schedule)
			if err != nil {
				errs = append(errs, fmt.Errorf("backup job %q: %w", j.Name, err))
				continue
			}
			name := j.Name
			job, err := sched.NewJob(jd, gocron.NewTask(func() { s.fire(name) }),
				gocron.WithTags(scheduleTag), gocron.WithName("backup-"+name))
			if err != nil {
				errs = append(errs, fmt.Errorf("backup job %q: %w", j.Name, err))
				continue
			}
			jobs[name] = job
		}
	}
	if strings.TrimSpace(cfg.Dir) != "" && cfg.Check.Enabled {
		jd, err := utils.ConvertToClockOrCronJobDef(cfg.Check.Schedule)
		if err == nil {
			checkJob, err = sched.NewJob(jd, gocron.NewTask(s.fireCheck),
				gocron.WithTags(checkTag), gocron.WithName("backup-check"))
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("backup check: %w", err))
		}
	}
	if cfg.LinkCheck.Enabled && strings.TrimSpace(cfg.LinkCheck.Schedule) != "" {
		jd, err := utils.ConvertToClockOrCronJobDef(cfg.LinkCheck.Schedule)
		if err == nil {
			_, err = sched.NewJob(jd, gocron.NewTask(s.scheduledLinkCheck),
				gocron.WithTags(linksTag), gocron.WithName("backup-links"))
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("link check: %w", err))
		}
	}
	s.mu.Lock()
	s.jobs = jobs
	s.checkJob = checkJob
	s.applied = scheduleSignature(cfg)
	s.mu.Unlock()
	return errors.Join(errs...)
}

func (s *BackupService) scheduledLinkCheck() {
	if _, err := s.LinksCheck(s.runContext()); err != nil && !errors.Is(err, ErrBusy) {
		s.log.Warn().Err(err).Msg("Scheduled link check failed")
	}
}

// fire is one scheduled firing of a job. Inside quiet hours it waits for the
// window to end, then runs once.
func (s *BackupService) fire(name string) {
	ctx := s.runContext()
	if end, in := quietUntil(s.now(), s.cfg().QuietHours); in {
		s.mu.Lock()
		if s.deferred[name] {
			s.mu.Unlock()
			return
		}
		s.deferred[name] = true
		s.mu.Unlock()
		s.log.Info().Str("job", name).Time("resumes", end).Msg("Scheduled backup waits for quiet hours to end")
		go func() {
			t := time.NewTimer(time.Until(end))
			defer t.Stop()
			select {
			case <-ctx.Done():
			case <-t.C:
				s.mu.Lock()
				delete(s.deferred, name)
				s.mu.Unlock()
				s.runScheduled(ctx, name)
			}
		}()
		return
	}
	s.runScheduled(ctx, name)
}

// quietUntil reports whether now is inside the window, and when it ends.
// The window may cross midnight; an unset or equal start and end is no
// window.
func quietUntil(now time.Time, q config.BackupQuietHours) (end time.Time, inside bool) {
	start, ok1 := parseHM(q.Start)
	stop, ok2 := parseHM(q.End)
	if !ok1 || !ok2 || start == stop {
		return time.Time{}, false
	}
	mins := now.Hour()*60 + now.Minute()
	day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	endToday := day.Add(time.Duration(stop) * time.Minute)
	if start < stop {
		if mins >= start && mins < stop {
			return endToday, true
		}
		return time.Time{}, false
	}
	// Crosses midnight.
	switch {
	case mins >= start:
		return endToday.AddDate(0, 0, 1), true
	case mins < stop:
		return endToday, true
	}
	return time.Time{}, false
}

func parseHM(v string) (int, bool) {
	t, err := time.Parse("15:04", strings.TrimSpace(v))
	if err != nil {
		return 0, false
	}
	return t.Hour()*60 + t.Minute(), true
}

// checkStale notifies backup_stale for a job with no success for the
// configured number of hours, at most once per that long.
func (s *BackupService) checkStale(ctx context.Context) {
	cfg := s.cfg()
	hours := cfg.Notify.StaleHours
	if hours <= 0 || strings.TrimSpace(cfg.Dir) == "" {
		return
	}
	now := s.now()
	s.mu.Lock()
	if now.Sub(s.staleChecked) < staleCheckEvery {
		s.mu.Unlock()
		return
	}
	s.staleChecked = now
	s.mu.Unlock()
	r, err := s.repo()
	if err != nil {
		return
	}
	snaps, err := r.ListSnapshots(ctx)
	if err != nil {
		return
	}
	limit := time.Duration(hours) * time.Hour
	for _, j := range cfg.Jobs {
		if !j.Enabled {
			continue
		}
		// A job with no snapshot yet counts from when this process started.
		last := s.startedAt
		for _, sn := range snaps {
			if jobOfTrigger(sn.Trigger) == j.Name && sn.Result != ResultPartial {
				last = sn.CreatedAt
				break
			}
		}
		if now.Sub(last) < limit {
			continue
		}
		s.mu.Lock()
		due := now.Sub(s.staleNotice[j.Name]) >= limit
		if due {
			s.staleNotice[j.Name] = now
		}
		s.mu.Unlock()
		if due {
			s.notify(config.EventBackupStale, "warning",
				fmt.Sprintf("Backup job %q has had no successful run for %d hours", j.Name, hours), nil)
		}
	}
}
