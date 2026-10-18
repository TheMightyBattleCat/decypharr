//go:build !windows

package backup_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/backup"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func cliRepo(t *testing.T) (dir, id string) {
	t.Helper()
	dir = t.TempDir()
	repo, err := backup.OpenRepo(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	st := newSourceStore(t)
	dbEntry, err := backup.NewDBExporter(st)(ctx, repo, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, _, err := repo.PutBlob(ctx, strings.NewReader(`{"port":"8282"}`), backup.CompressDefault)
	if err != nil {
		t.Fatal(err)
	}
	snap := &backup.Snapshot{
		ID: "20260101-000000-abcd", CreatedAt: time.Now(), Trigger: "manual:full",
		Decypharr: backup.BuildInfo{DBFormat: storage.DBFormat},
		Components: map[backup.Component]backup.ComponentEntry{
			backup.ComponentDB:     dbEntry,
			backup.ComponentConfig: {Files: map[string]string{"config.json": ref.SHA256}},
		},
	}
	if err := repo.WriteSnapshot(ctx, snap); err != nil {
		t.Fatal(err)
	}
	return dir, snap.ID
}

func runCLI(args ...string) (int, string, string) {
	var out, errb bytes.Buffer
	code := backup.RunCLI(context.Background(), args, backup.CLIEnv{Out: &out, Err: &errb})
	return code, out.String(), errb.String()
}

func TestCLIListVerifyRestore(t *testing.T) {
	repo, id := cliRepo(t)

	if code, out, e := runCLI("list", "--repo", repo); code != 0 || !strings.Contains(out, id) {
		t.Fatalf("list: %d %q %q", code, out, e)
	}
	if code, out, e := runCLI("verify", "--repo", repo, "--full"); code != 0 || !strings.Contains(out, "result: ok") {
		t.Fatalf("verify: %d %q %q", code, out, e)
	}

	data := t.TempDir()
	if err := os.WriteFile(filepath.Join(data, "config.json"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	code, out, e := runCLI("restore", "--repo", repo, "--snapshot", "latest", "--config", data, "--only", "config,db")
	if code != 0 {
		t.Fatalf("restore: %d %q %q", code, out, e)
	}
	got, _ := os.ReadFile(filepath.Join(data, "config.json"))
	if string(got) != `{"port":"8282"}` {
		t.Fatalf("config.json = %q", got)
	}
	if m, _ := filepath.Glob(filepath.Join(data, "config.json.before-restore-*")); len(m) != 1 {
		t.Fatal("the old config.json was not kept")
	}
	if !backup.PostRestoreHoldActive(data) || backup.PendingRestoreExists(data) {
		t.Fatal("database was not swapped in with the hold on")
	}
	st, err := storage.NewStorage(filepath.Join(data, "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if st.StoreCounts()["entries"] != 2 {
		t.Fatalf("restored counts = %v", st.StoreCounts())
	}
}

func TestCLIRestoreRefusesWhileStoresLocked(t *testing.T) {
	repo, _ := cliRepo(t)
	data := t.TempDir()
	if err := os.MkdirAll(filepath.Join(data, "db"), 0o755); err != nil {
		t.Fatal(err)
	}
	lock, err := os.OpenFile(filepath.Join(data, "db", "entries.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}

	code, _, e := runCLI("restore", "--repo", repo, "--snapshot", "latest", "--config", data, "--only", "config,db")
	if code == 0 || !strings.Contains(e, "running Decypharr") {
		t.Fatalf("restore went ahead: %d %q", code, e)
	}
	if _, err := os.Stat(filepath.Join(data, "config.json")); err == nil {
		t.Fatal("restore wrote files while the stores were locked")
	}
	if _, err := backup.CheckStoresFree(data); !errors.Is(err, backup.ErrStoreLocked) {
		t.Fatalf("CheckStoresFree = %v", err)
	}
}

func TestCLIRestoreSetsConfigPathBeforeStorage(t *testing.T) {
	repo, _ := cliRepo(t)
	data := t.TempDir()
	cwd := t.TempDir()
	t.Chdir(cwd)
	config.Reset()
	t.Cleanup(config.Reset)

	code, out, e := runCLI("restore", "--repo", repo, "--snapshot", "latest", "--config", data)
	if code != 0 {
		t.Fatalf("restore: %d %q %q", code, out, e)
	}
	if entries, _ := os.ReadDir(cwd); len(entries) != 0 {
		t.Fatalf("restore created files in the working folder: %v", entries)
	}
	if got := config.GetMainPath(); got != data {
		t.Fatalf("config path = %q, want %q", got, data)
	}
}

func TestRestoreRefusedDatabaseLeavesOtherFilesUntouched(t *testing.T) {
	repoDir, id := cliRepo(t)
	repo, err := backup.OpenRepo(repoDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	snap, err := repo.ReadSnapshot(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	newer := *snap
	newer.ID = "20260102-000000-abcd"
	newer.Decypharr.DBFormat = storage.DBFormat + 1
	if err := repo.WriteSnapshot(ctx, &newer); err != nil {
		t.Fatal(err)
	}

	cases := map[string]struct {
		snapshot string
		setup    func(data string)
	}{
		"newer database format": {newer.ID, func(string) {}},
		"restore already staged": {id, func(data string) {
			_ = os.WriteFile(filepath.Join(data, backup.PendingRestoreFile), []byte("{}"), 0o600)
		}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			data := t.TempDir()
			cfgPath := filepath.Join(data, "config.json")
			if err := os.WriteFile(cfgPath, []byte("{}"), 0o600); err != nil {
				t.Fatal(err)
			}
			tc.setup(data)
			_, err := backup.RestoreToDataDir(ctx, repo, backup.RestoreOptions{SnapshotID: tc.snapshot, DataDir: data})
			if err == nil {
				t.Fatal("restore went ahead")
			}
			if got, _ := os.ReadFile(cfgPath); string(got) != "{}" {
				t.Fatalf("config.json was replaced: %q", got)
			}
			if m, _ := filepath.Glob(cfgPath + ".before-restore-*"); len(m) != 0 {
				t.Fatalf("config.json was moved aside: %v", m)
			}
		})
	}
}

func TestCLIReadPathsNeverCreateARepository(t *testing.T) {
	root := t.TempDir()
	missing := filepath.Join(root, "typo", "backups")
	empty := filepath.Join(root, "empty")
	if err := os.Mkdir(empty, 0o755); err != nil {
		t.Fatal(err)
	}
	data := t.TempDir()
	for _, dir := range []string{missing, empty} {
		for _, args := range [][]string{
			{"list", "--repo", dir},
			{"verify", "--repo", dir},
			{"restore", "--repo", dir, "--snapshot", "latest", "--config", data},
		} {
			if code, _, e := runCLI(args...); code == 0 || !strings.Contains(e, "not a backup repository") {
				t.Fatalf("%v: %d %q", args, code, e)
			}
		}
	}
	if _, err := os.Stat(filepath.Join(root, "typo")); !os.IsNotExist(err) {
		t.Fatalf("a folder was created for the missing path: %v", err)
	}
	if entries, _ := os.ReadDir(empty); len(entries) != 0 {
		t.Fatalf("the empty folder was turned into a repository: %v", entries)
	}
}

// The store locks stay held through the restore and the swap, and are
// released when the command has finished.
func TestCLIRestoreHoldsStoreLocksUntilSwapDone(t *testing.T) {
	repo, _ := cliRepo(t)
	data := t.TempDir()
	if err := os.MkdirAll(filepath.Join(data, "db"), 0o755); err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(data, "db", "entries.lock")
	if err := os.WriteFile(lockPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	seen := false
	backup.SetDuringRestoreHook(func(dir string) {
		seen = true
		moved, _ := filepath.Glob(filepath.Join(dir, "db.before-restore-*", "entries.lock"))
		if len(moved) != 1 {
			t.Errorf("the hook ran before the database swap finished")
			return
		}
		if busy, err := backup.LockHeld(moved[0]); err != nil || !busy {
			t.Errorf("store lock free during the restore: busy=%v err=%v", busy, err)
		}
	})
	t.Cleanup(func() { backup.SetDuringRestoreHook(nil) })

	code, out, e := runCLI("restore", "--repo", repo, "--snapshot", "latest", "--config", data, "--only", "db")
	if code != 0 || !seen {
		t.Fatalf("restore: %d seen=%v %q %q", code, seen, out, e)
	}
	moved, _ := filepath.Glob(filepath.Join(data, "db.before-restore-*", "entries.lock"))
	if len(moved) != 1 {
		t.Fatalf("old db folder = %v", moved)
	}
	if busy, err := backup.LockHeld(moved[0]); err != nil || busy {
		t.Fatalf("store lock still held after the command: busy=%v err=%v", busy, err)
	}
}
