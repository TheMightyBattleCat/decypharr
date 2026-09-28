package utils

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/robfig/cron/v3"
)

// ConvertToJobDef converts a string interval to a gocron.JobDefinition.
func ConvertToJobDef(interval string) (gocron.JobDefinition, error) {
	// Parse the interval string
	// Interval could be in the format "1h", "30m", "15s" or "1h30m" or "04:05"
	var jd gocron.JobDefinition

	if jd, ok := clockOrCronJobDef(interval); ok {
		return jd, nil
	}

	if dur, err := ParseDuration(interval); err == nil {
		return gocron.DurationJob(dur), nil
	}

	return jd, fmt.Errorf("invalid interval format: %s", interval)
}

// ConvertToClockOrCronJobDef is ConvertToJobDef without intervals: schedule
// must be a clock time ("12:00") or a cron expression ("0 3 * * *"). An
// interval ("6h", "@every 6h") counts from when the job is registered, and
// jobs are re-registered on every settings save and restart, so one could
// keep restarting and never fire.
func ConvertToClockOrCronJobDef(schedule string) (gocron.JobDefinition, error) {
	s := strings.TrimSpace(schedule)
	// "@every" can follow a time zone prefix ("TZ=UTC @every 1m"), so look
	// for it anywhere rather than only at the start.
	if !strings.Contains(s, "@every") {
		if jd, ok := clockOrCronJobDef(s); ok {
			return jd, nil
		}
	}
	return nil, fmt.Errorf("%q is not a time (12:00) or a cron expression (0 3 * * *)", schedule)
}

func clockOrCronJobDef(s string) (gocron.JobDefinition, bool) {
	if t, ok := parseClockTime(s); ok {
		return gocron.DailyJob(1, gocron.NewAtTimes(
			gocron.NewAtTime(uint(t.Hour()), uint(t.Minute()), uint(t.Second())),
		)), true
	}
	sched, err := parseCron(s)
	if err != nil {
		return nil, false
	}
	// A valid expression that never matches a real date ("0 0 30 2 *", the
	// 30th of February) has no next run. gocron refuses to register such a
	// job, so accepting it here would save a schedule that silently never
	// runs.
	if sched.Next(time.Now()).IsZero() {
		return nil, false
	}
	return gocron.CronJob(s, false), true
}

// parseCron is cron.ParseStandard with its panic turned into an error. The
// cron library reads a "TZ=" or "CRON_TZ=" prefix up to the first space, so
// a schedule that is only a time zone ("TZ=Europe/London", no fields after
// it) panics with a slice-bounds error. At startup that panic stopped every
// service; on a settings save it became a 500 instead of a clear refusal.
func parseCron(s string) (sched cron.Schedule, err error) {
	defer func() {
		if r := recover(); r != nil {
			sched, err = nil, fmt.Errorf("invalid cron expression %q", s)
		}
	}()
	return cron.ParseStandard(s)
}

func parseClockTime(s string) (time.Time, bool) {
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return time.Time{}, false
	}
	h, err := strconv.Atoi(parts[0])
	if err != nil || h < 0 || h > 23 {
		return time.Time{}, false
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil || m < 0 || m > 59 {
		return time.Time{}, false
	}
	now := time.Now()
	// build a time.Time for today at h:m:00 in the local zone
	t := time.Date(
		now.Year(), now.Month(), now.Day(),
		h, m, 0, 0,
		time.Local,
	)
	return t, true
}
