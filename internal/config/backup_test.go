package config

import (
	"errors"
	"strings"
	"testing"
)

func okSchedule(s string) error {
	if s == "" || strings.Contains(s, "bad") {
		return errors.New("is not a time or cron expression")
	}
	return nil
}

func TestBackupDefaults(t *testing.T) {
	c := &Config{}
	c.setBackupDefaults()
	b := c.Backup
	if len(b.Jobs) != 2 || b.Jobs[0].Name != "quick" || b.Jobs[0].Schedule != "0 */6 * * *" || b.Jobs[1].Schedule != "04:00" {
		t.Fatalf("jobs = %+v", b.Jobs)
	}
	if !b.PreAction.Enabled || b.AutoRecreate.Enabled || b.ThrottleMBps != 20 || !b.PauseWhileStreaming || !b.LinkCheck.Enabled {
		t.Fatalf("defaults = %+v", b)
	}
	if err := b.Validate(okSchedule); err != nil {
		t.Fatal(err)
	}
}

func TestBackupDefaultsKeepUserChoices(t *testing.T) {
	c := &Config{Backup: DefaultBackup()}
	c.Backup.PauseWhileStreaming = false
	c.Backup.PreAction.Enabled = false
	c.setBackupDefaults()
	if c.Backup.PauseWhileStreaming || c.Backup.PreAction.Enabled {
		t.Fatal("a switch the user turned off was reset")
	}
}

func TestBackupValidate(t *testing.T) {
	cases := map[string]func(b *Backup){
		"no components":  func(b *Backup) { b.Jobs[0].Components = nil },
		"unknown comp":   func(b *Backup) { b.Jobs[0].Components = []string{"nope"} },
		"bad schedule":   func(b *Backup) { b.Jobs[0].Schedule = "bad" },
		"duplicate name": func(b *Backup) { b.Jobs[1].Name = "Quick" },
		"keeps nothing":  func(b *Backup) { b.Jobs[0].Retention = BackupRetention{} },
		"negative":       func(b *Backup) { b.ThrottleMBps = -1 },
		"half quiet":     func(b *Backup) { b.QuietHours.Start = "01:00" },
		"bad quiet":      func(b *Backup) { b.QuietHours = BackupQuietHours{Start: "25:00", End: "02:00"} },
		"bad link check": func(b *Backup) { b.LinkCheck.Schedule = "bad" },
	}
	for name, mutate := range cases {
		b := DefaultBackup()
		mutate(&b)
		if err := b.Validate(okSchedule); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	b := DefaultBackup()
	b.Jobs[0].Enabled, b.Jobs[0].Schedule = false, "bad"
	if err := b.Validate(okSchedule); err != nil {
		t.Errorf("a disabled job's schedule is not checked: %v", err)
	}
}
