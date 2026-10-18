//go:build !windows

package backup

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

func (a *autoEnv) runCLI(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := RunCLI(context.Background(), args, CLIEnv{
		Out: &out, Err: &errb,
		NewService: func(string, string) (*BackupService, func(), error) { return a.svc, func() {}, nil },
	})
	return code, out.String(), errb.String()
}

func (a *autoEnv) snapshotID(t *testing.T) string {
	t.Helper()
	repo, err := OpenRepo(a.repoDir)
	if err != nil {
		t.Fatal(err)
	}
	infos, err := repo.ListSnapshots(context.Background())
	if err != nil || len(infos) != 1 {
		t.Fatalf("snapshots = %d, %v", len(infos), err)
	}
	return infos[0].ID
}

// Quick job -> snapshot -> links plan, end to end over a temp data folder.
func TestEndToEndQuickJobSnapshotLinksPlan(t *testing.T) {
	a := newAutoEnv(t, config.BackupAutoRecreate{})
	plan, err := a.svc.PlanLinksRestore(context.Background(), a.snapshotID(t), LinksPlanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	creates := 0
	for _, r := range plan.Rows {
		if r.Path == a.e4 || r.Path == a.e5 {
			creates++
		}
	}
	if creates != 2 {
		t.Fatalf("plan rows = %+v", plan.Rows)
	}
	if a.exists(a.e4) || a.exists(a.e5) {
		t.Fatal("planning created links")
	}
}

func TestCLILinksPlanThenApply(t *testing.T) {
	a := newAutoEnv(t, config.BackupAutoRecreate{})
	id := a.snapshotID(t)
	data := t.TempDir()

	code, out, e := a.runCLI(t, "restore", "--repo", a.repoDir, "--snapshot", id, "--config", data, "--only", "links", "--links-plan")
	if code != 0 || !strings.Contains(out, "link plan written") {
		t.Fatalf("plan: %d %q %q", code, out, e)
	}
	if _, err := os.Stat(filepath.Join(data, "links-plan-"+id+".json")); err != nil {
		t.Fatal("no plan file written")
	}
	if a.exists(a.e4) || a.exists(a.e5) {
		t.Fatal("--links-plan created links")
	}

	code, out, e = a.runCLI(t, "restore", "--repo", a.repoDir, "--snapshot", id, "--config", data, "--only", "links", "--links-apply")
	if code != 0 || !strings.Contains(out, "links: 2 created") {
		t.Fatalf("apply: %d %q %q", code, out, e)
	}
	if !a.exists(a.e4) || !a.exists(a.e5) {
		t.Fatal("--links-apply did not create the links")
	}
}

// The scheduled link check reaches automatic re-creation.
func TestScheduledLinkCheckAutoRecreates(t *testing.T) {
	a := newAutoEnv(t, config.BackupAutoRecreate{Enabled: true, Limit: 25})
	if err := markLinkRestoreApplied(a.data, a.svc.now()); err != nil {
		t.Fatal(err)
	}
	a.svc.scheduledLinkCheck()
	if !a.exists(a.e4) || !a.exists(a.e5) {
		t.Fatal("scheduled check did not re-create the links")
	}
}

// --links-plan and --links-apply on their own touch only the links.
func TestCLILinksFlagsImplyOnlyLinks(t *testing.T) {
	a := newAutoEnv(t, config.BackupAutoRecreate{})
	id := a.snapshotID(t)
	data := t.TempDir()
	cfgPath := filepath.Join(data, "config.json")
	if err := os.WriteFile(cfgPath, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	code, out, e := a.runCLI(t, "restore", "--repo", a.repoDir, "--snapshot", id, "--config", data, "--links-plan")
	if code != 0 || !strings.Contains(out, "link plan written") {
		t.Fatalf("plan: %d %q %q", code, out, e)
	}
	if !strings.Contains(out, "same command with --links-apply") {
		t.Fatalf("plan text does not say what to run next: %q", out)
	}
	if m, _ := filepath.Glob(cfgPath + ".before-restore-*"); len(m) != 0 {
		t.Fatal("--links-plan re-ran the config restore")
	}
	if PendingRestoreExists(data) || PostRestoreHoldActive(data) {
		t.Fatal("--links-plan re-ran the database restore")
	}
}

// warnTimeFS refuses to set link times, so an apply reports warnings.
type warnTimeFS struct{ LinkFS }

func (warnTimeFS) SetLinkTime(string, time.Time) error { return errors.New("denied") }

// A link made without its recorded time is a warning the CLI prints.
func TestCLILinksApplyPrintsWarnings(t *testing.T) {
	a := newAutoEnv(t, config.BackupAutoRecreate{})
	id := a.snapshotID(t)
	a.svc.deps.FS = warnTimeFS{a.svc.deps.FS}
	data := t.TempDir()

	code, out, e := a.runCLI(t, "restore", "--repo", a.repoDir, "--snapshot", id, "--config", data, "--only", "links", "--links-apply")
	if code != 0 || !strings.Contains(out, "links: 2 created") {
		t.Fatalf("apply: %d %q %q", code, out, e)
	}
	if !strings.Contains(e, "warning:") || !strings.Contains(e, "could not set the modification time") {
		t.Fatalf("warnings not printed: out %q err %q", out, e)
	}
}
