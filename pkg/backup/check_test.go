package backup_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/backup"
)

// waitStatus polls the status until ok says so.
func waitStatus(t *testing.T, svc *backup.BackupService, what string, ok func(backup.StatusResponse) bool) backup.StatusResponse {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		st, err := svc.Status(context.Background())
		if err == nil && ok(st) {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s: %+v", what, st)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCheckReadsBlobsBackAndTestRestoresTheDatabase(t *testing.T) {
	st := newSourceStore(t)
	repoDir := filepath.Join(t.TempDir(), "repo")
	var mu sync.Mutex
	var events []config.NotificationEvent
	svc := backup.NewService(backup.Deps{
		DataDir: t.TempDir(),
		Config: func() config.Backup {
			return config.Backup{
				Dir:    repoDir,
				Jobs:   []config.BackupJob{{Name: "quick", Enabled: true, Components: []string{"db", "config"}, Retention: config.BackupRetention{Last: 5}}},
				Check:  config.BackupCheck{Enabled: true, Schedule: config.DefaultBackupCheckSchedule, RestoreTest: true},
				Notify: config.BackupNotify{OnFailure: true},
			}
		},
		DBExport: backup.NewDBExporter(st),
		Notify: func(ev config.NotificationEvent, _, _ string, _ error) {
			mu.Lock()
			events = append(events, ev)
			mu.Unlock()
		},
	})
	if _, err := svc.RunJob(context.Background(), "quick"); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, svc, "the backup", func(s backup.StatusResponse) bool { return s.Snapshots == 1 && !s.Jobs[0].Running })

	if err := svc.RunCheck(); err != nil {
		t.Fatal(err)
	}
	s := waitStatus(t, svc, "the check", func(s backup.StatusResponse) bool {
		return s.Check != nil && s.Check.Last != nil && !s.Check.Running
	})
	last := s.Check.Last
	// Records counts every store (entries, items, ...); the two entries are
	// among them, and importAndOpen already matched the snapshot's total.
	if !last.OK || last.BlobsChecked == 0 || last.RestoreTest == nil || !last.RestoreTest.OK || last.RestoreTest.Records < 2 {
		t.Fatalf("check of a good repository: %+v %+v", last, last.RestoreTest)
	}
	if b, err := os.ReadFile(filepath.Join(repoDir, "index", "check.json")); err != nil || !strings.Contains(string(b), `"ok": true`) {
		t.Fatalf("check result not kept in the repository: %v %s", err, b)
	}
	if left, _ := filepath.Glob(filepath.Join(repoDir, "tmp", "drill-*")); len(left) != 0 {
		t.Fatalf("restore test left %v behind", left)
	}

	// Lose one blob: the next check fails and says so.
	blobs, _ := filepath.Glob(filepath.Join(repoDir, "blobs", "*", "*"))
	if len(blobs) == 0 {
		t.Fatal("no blobs on disk")
	}
	if err := os.Remove(blobs[0]); err != nil {
		t.Fatal(err)
	}
	at := last.At
	if err := svc.RunCheck(); err != nil {
		t.Fatal(err)
	}
	s = waitStatus(t, svc, "the second check", func(s backup.StatusResponse) bool {
		return s.Check != nil && s.Check.Last != nil && !s.Check.Running && s.Check.Last.At != at
	})
	if s.Check.Last.OK || s.Check.Last.Missing+s.Check.Last.Corrupt == 0 || len(s.Check.Last.Problems) == 0 {
		t.Fatalf("check with a missing blob: %+v", s.Check.Last)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(events) != 1 || events[0] != config.EventBackupFailed {
		t.Fatalf("want one failure notice, got %v", events)
	}
}
