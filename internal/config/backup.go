package config

import (
	"fmt"
	"strings"
	"time"
)

// Backup job names created by default.
const (
	BackupJobQuick = "quick"
	BackupJobFull  = "full"
)

// Backup component names a job can list. They match pkg/backup.Component;
// the list is repeated here because config cannot import pkg/backup.
var backupComponents = []string{"db", "config", "state", "nzb", "overlay", "links", "hearsay"}

// BackupRetention is a job's grandfather-father-son retention.
type BackupRetention struct {
	Last    int `json:"last"`
	Daily   int `json:"daily"`
	Weekly  int `json:"weekly"`
	Monthly int `json:"monthly"`
}

// BackupJob is one scheduled backup.
type BackupJob struct {
	Name       string   `json:"name"`
	Enabled    bool     `json:"enabled"`
	Components []string `json:"components"`
	// Schedule is a clock time ("04:00") or a cron expression
	// ("0 */6 * * *"); see utils.ConvertToClockOrCronJobDef.
	Schedule  string          `json:"schedule"`
	Retention BackupRetention `json:"retention"`
}

// BackupLinkCheck schedules the Library Links check.
type BackupLinkCheck struct {
	Enabled  bool   `json:"enabled"`
	Schedule string `json:"schedule"`
	// DroppedPercent notifies links_dropped when the live link count falls
	// by more than this share since the last check.
	DroppedPercent int `json:"dropped_percent,omitempty"`
}

// BackupPreAction controls the snapshot taken before risky actions.
type BackupPreAction struct {
	Enabled    bool `json:"enabled"`
	Keep       int  `json:"keep"`         // keep at most this many
	MaxAgeDays int  `json:"max_age_days"` // and none older than this
	// Refuse lists actions that are refused when the snapshot fails,
	// instead of carrying on.
	Refuse []string `json:"refuse,omitempty"`
}

// BackupAutoRecreate is the opt-in automatic link re-creation.
type BackupAutoRecreate struct {
	Enabled bool `json:"enabled"`
	// Limit is the most links one check re-creates; over it nothing is.
	Limit  int  `json:"limit"`
	Rescan bool `json:"rescan,omitempty"`
}

// BackupQuietHours holds scheduled jobs back inside a daily window.
type BackupQuietHours struct {
	Start string `json:"start,omitempty"` // "HH:MM"
	End   string `json:"end,omitempty"`
}

// BackupNotify is the backup notification settings.
type BackupNotify struct {
	OnFailure bool `json:"on_failure"`
	// StaleHours notifies backup_stale when a job has had no success for
	// this long; 0 turns it off.
	StaleHours int `json:"stale_hours"`
}

// Backup is the backup feature's settings (JSON key "backup").
type Backup struct {
	// Dir is the destination folder. Empty means backups are not set up.
	Dir          string `json:"dir,omitempty"`
	RcloneRemote string `json:"rclone_remote,omitempty"`

	Jobs []BackupJob `json:"jobs,omitempty"`

	// ExtraLinkFolders are walked for symlinks besides the Arr root folders.
	ExtraLinkFolders []string `json:"extra_link_folders,omitempty"`
	// ArrPathMaps maps an Arr's own path prefix to the local one, by Arr name.
	ArrPathMaps map[string]map[string]string `json:"arr_path_maps,omitempty"`

	LinkCheck    BackupLinkCheck    `json:"link_check"`
	PreAction    BackupPreAction    `json:"pre_action"`
	AutoRecreate BackupAutoRecreate `json:"auto_recreate"`
	QuietHours   BackupQuietHours   `json:"quiet_hours"`
	Notify       BackupNotify       `json:"notify"`

	ThrottleMBps        int  `json:"throttle_mbps"`
	PauseWhileStreaming bool `json:"pause_while_streaming"`
	ConfigHistoryMax    int  `json:"config_history_max,omitempty"`
}

// DefaultBackup returns the settings of an install that has never configured
// backups. Backups stay off until Dir is set.
func DefaultBackup() Backup {
	return Backup{
		Jobs: []BackupJob{
			{
				Name: BackupJobQuick, Enabled: true,
				Components: []string{"db", "config", "state", "links", "hearsay"},
				Schedule:   "0 */6 * * *",
				Retention:  BackupRetention{Last: 8, Daily: 14, Weekly: 8, Monthly: 6},
			},
			{
				Name: BackupJobFull, Enabled: true,
				Components: []string{"db", "config", "state", "links", "hearsay", "nzb", "overlay"},
				Schedule:   "04:00",
				Retention:  BackupRetention{Last: 3, Daily: 14, Weekly: 8, Monthly: 12},
			},
		},
		LinkCheck:           BackupLinkCheck{Enabled: true, Schedule: "0 * * * *", DroppedPercent: 10},
		PreAction:           BackupPreAction{Enabled: true, Keep: 20, MaxAgeDays: 14},
		AutoRecreate:        BackupAutoRecreate{Enabled: false, Limit: 25},
		Notify:              BackupNotify{OnFailure: true, StaleHours: 48},
		ThrottleMBps:        20,
		PauseWhileStreaming: true,
		ConfigHistoryMax:    200,
	}
}

// setBackupDefaults fills the settings of an install that has none. Once any
// job exists the user's values stand, including switches turned off.
func (c *Config) setBackupDefaults() {
	b := &c.Backup
	if len(b.Jobs) == 0 && b.LinkCheck == (BackupLinkCheck{}) && b.PreAction.Keep == 0 {
		dir, remote, extra, maps := b.Dir, b.RcloneRemote, b.ExtraLinkFolders, b.ArrPathMaps
		*b = DefaultBackup()
		b.Dir, b.RcloneRemote, b.ExtraLinkFolders, b.ArrPathMaps = dir, remote, extra, maps
		return
	}
	if b.ConfigHistoryMax <= 0 {
		b.ConfigHistoryMax = 200
	}
	if b.AutoRecreate.Limit <= 0 {
		b.AutoRecreate.Limit = 25
	}
	if b.PreAction.Keep <= 0 {
		b.PreAction.Keep = 20
	}
	if b.PreAction.MaxAgeDays <= 0 {
		b.PreAction.MaxAgeDays = 14
	}
}

// Validate checks the settings. checkSchedule reports whether a schedule is a
// clock time or cron expression; callers pass a wrapper around
// utils.ConvertToClockOrCronJobDef (config cannot import utils).
func (b *Backup) Validate(checkSchedule func(string) error) error {
	b.Dir = strings.TrimSpace(b.Dir)
	seen := map[string]bool{}
	for i := range b.Jobs {
		j := &b.Jobs[i]
		j.Name = strings.TrimSpace(j.Name)
		j.Schedule = strings.TrimSpace(j.Schedule)
		if j.Name == "" {
			return fmt.Errorf("backup job %d has no name", i+1)
		}
		if seen[strings.ToLower(j.Name)] {
			return fmt.Errorf("backup job %q is listed twice", j.Name)
		}
		seen[strings.ToLower(j.Name)] = true
		if len(j.Components) == 0 {
			return fmt.Errorf("backup job %q has no components", j.Name)
		}
		for _, c := range j.Components {
			if !validBackupComponent(c) {
				return fmt.Errorf("backup job %q: unknown component %q", j.Name, c)
			}
		}
		r := j.Retention
		if r.Last < 0 || r.Daily < 0 || r.Weekly < 0 || r.Monthly < 0 {
			return fmt.Errorf("backup job %q: retention counts cannot be negative", j.Name)
		}
		if j.Enabled {
			if r.Last+r.Daily+r.Weekly+r.Monthly == 0 {
				return fmt.Errorf("backup job %q: retention would keep nothing", j.Name)
			}
			if err := checkSchedule(j.Schedule); err != nil {
				return fmt.Errorf("backup job %q: schedule %w", j.Name, err)
			}
		}
	}
	b.LinkCheck.Schedule = strings.TrimSpace(b.LinkCheck.Schedule)
	if b.LinkCheck.Enabled {
		if err := checkSchedule(b.LinkCheck.Schedule); err != nil {
			return fmt.Errorf("link check schedule %w", err)
		}
	}
	if b.ThrottleMBps < 0 {
		return fmt.Errorf("backup I/O limit cannot be negative")
	}
	if b.PreAction.Keep < 0 || b.PreAction.MaxAgeDays < 0 {
		return fmt.Errorf("pre-action retention cannot be negative")
	}
	if b.AutoRecreate.Limit < 0 {
		return fmt.Errorf("automatic link re-creation limit cannot be negative")
	}
	if b.Notify.StaleHours < 0 {
		return fmt.Errorf("backup stale threshold cannot be negative")
	}
	if (b.QuietHours.Start == "") != (b.QuietHours.End == "") {
		return fmt.Errorf("quiet hours need both a start and an end")
	}
	for _, t := range []string{b.QuietHours.Start, b.QuietHours.End} {
		if t == "" {
			continue
		}
		if _, err := time.Parse("15:04", t); err != nil {
			return fmt.Errorf("quiet hours %q is not a time like 02:30", t)
		}
	}
	return nil
}

func validBackupComponent(c string) bool {
	for _, k := range backupComponents {
		if c == k {
			return true
		}
	}
	return false
}

// OnSaved is called by Config.Save with the bytes it just wrote to
// config.json. The backup service sets it at start-up, before any save it
// should see; it is nil otherwise. It runs under the config lock, so it must
// not call Get or Update.
var OnSaved func(data []byte)
