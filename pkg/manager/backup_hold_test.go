package manager

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/backup"
	"github.com/sirrobot01/decypharr/pkg/backup/backuptest"
)

// holdOn points the config path at a temp folder and, when on, leaves the
// marker a database swap writes.
func holdOn(t *testing.T, on bool) {
	t.Helper()
	prev := config.GetMainPath()
	dir := t.TempDir()
	config.SetConfigPath(dir)
	t.Cleanup(func() { config.SetConfigPath(prev) })
	if on {
		if err := os.WriteFile(filepath.Join(dir, backup.PostRestoreHoldFile), []byte(`{}`), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPostRestoreHoldFollowsTheMarker(t *testing.T) {
	m := &Manager{}
	holdOn(t, false)
	if m.PostRestoreHold() {
		t.Fatal("hold on without a marker")
	}
	holdOn(t, true)
	if !m.PostRestoreHold() {
		t.Fatal("hold off with a marker")
	}
	if err := backup.ReleasePostRestoreHold(config.GetMainPath()); err != nil {
		t.Fatal(err)
	}
	if m.PostRestoreHold() {
		t.Fatal("hold still on after release")
	}
}

func TestHoldBlocksWebhookDeleteCleanup(t *testing.T) {
	m := &Manager{}
	holdOn(t, true)
	action, entry, _ := m.HandleArrWebhookCleanup(ArrWebhookEvent{DownloadID: "abc", FileName: "x.mkv"})
	if action != "held" || entry != "" {
		t.Fatalf("got %q %q", action, entry)
	}
}

func TestHoldBlocksStaleNZBCleanup(t *testing.T) {
	m := &Manager{}
	holdOn(t, true)
	r := &Repair{manager: m}
	_, err := r.CleanupStaleNZBs(context.Background(), []string{"e1"}, nil)
	if err == nil {
		t.Fatal("cleanup ran during the hold")
	}
}

func TestHoldBlocksAutomaticRepair(t *testing.T) {
	m := &Manager{}
	holdOn(t, true)
	r := &Repair{manager: m}
	out, err := r.handleAutoDamage(context.Background(), RepairSourcePlayback, "e", "f.mkv", 0)
	if err != nil || out.acted || out.reason != "post-restore hold" {
		t.Fatalf("got %+v %v", out, err)
	}
}

func TestBackupServiceSwapIsSafe(t *testing.T) {
	m := &Manager{}
	if _, err := m.Backup().Status(context.Background()); !errors.Is(err, backup.ErrNotImplemented) {
		t.Fatalf("default: %v", err)
	}
	fake := backuptest.NewService()
	m.SetBackup(fake)
	if m.Backup() != backup.Service(fake) {
		t.Fatal("installed service not returned")
	}
	m.SetBackup(nil)
	if _, err := m.Backup().Status(context.Background()); !errors.Is(err, backup.ErrNotImplemented) {
		t.Fatalf("after nil: %v", err)
	}
}

func TestHoldPausesScheduledSweepAutoRepair(t *testing.T) {
	m := &Manager{}
	r := &Repair{manager: m}
	holdOn(t, false)
	if !r.autoRepairStillOn(context.Background(), true) {
		t.Fatal("auto-repair off without a hold")
	}
	holdOn(t, true)
	if r.autoRepairStillOn(context.Background(), true) {
		t.Fatal("sweep auto-repair ran during the hold")
	}
}
