package backup

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// fakeRclone answers the rclone calls an upload makes and records them.
type fakeRclone struct {
	mu        sync.Mutex
	calls     []string            // verb and the remote path or list contents
	repoJSON  []byte              // nil: the remote has no repo.json
	listings  map[string][]string // "snapshots"/"blobs" -> lsf output
	copied    [][]string          // the --files-from lists of each copy
	deleted   [][]string          // the --files-from lists of each delete
	failVerbs map[string]bool
}

func (f *fakeRclone) run(_ context.Context, args []string, _ func(string)) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	verb := args[0]
	readList := func() []string {
		for i, a := range args {
			if a == "--files-from-raw" {
				b, _ := os.ReadFile(args[i+1])
				return strings.Fields(string(b))
			}
		}
		return nil
	}
	f.calls = append(f.calls, verb+" "+args[len(args)-1])
	if f.failVerbs[verb] {
		return nil, &rcloneError{Code: 1, Tail: []string{verb + " failed"}}
	}
	switch verb {
	case "cat":
		if f.repoJSON == nil {
			return nil, &rcloneError{Code: 3, Tail: []string{"directory not found"}}
		}
		return f.repoJSON, nil
	case "copy":
		f.copied = append(f.copied, readList())
	case "delete":
		f.deleted = append(f.deleted, readList())
	case "lsf":
		for k, v := range f.listings {
			if strings.HasSuffix(args[len(args)-1], "/"+k) {
				return []byte(strings.Join(v, "\n")), nil
			}
		}
		return nil, &rcloneError{Code: 3}
	case "listremotes":
		return []byte("bk: crypt\n"), nil
	}
	return nil, nil
}

func (f *fakeRclone) verbs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, c := range f.calls {
		out = append(out, strings.Fields(c)[0])
	}
	return out
}

// offsiteEnv is a test env with one complete full snapshot and an rclone
// remote set.
func offsiteEnv(t *testing.T) (*testEnv, *fakeRclone) {
	t.Helper()
	e := newEnv(t, nil)
	e.svc.runScheduled(context.Background(), "full")
	if len(e.snapshots()) != 1 {
		t.Fatal("no snapshot to copy")
	}
	e.setCfg(func(c *config.Backup) { c.RcloneRemote = "bk:decypharr" })
	f := &fakeRclone{listings: map[string][]string{}}
	e.svc.rclone = f.run
	return e, f
}

func TestOffsiteFirstUploadCopiesBlobsBeforeManifests(t *testing.T) {
	e, f := offsiteEnv(t)
	res := e.svc.uploadOnce(context.Background())
	if res == nil || !res.OK {
		t.Fatalf("upload: %+v", res)
	}
	want := []string{"cat", "copyto", "copy", "copy", "lsf", "lsf"}
	if got := f.verbs(); strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("calls %v, want %v", got, want)
	}
	if len(f.copied) != 2 || len(f.copied[0]) == 0 || !strings.HasPrefix(f.copied[0][0], "blobs/") {
		t.Fatalf("first copy should be the blobs: %v", f.copied)
	}
	if len(f.copied[1]) != 1 || !strings.HasPrefix(f.copied[1][0], "snapshots/") {
		t.Fatalf("second copy should be the one manifest: %v", f.copied[1])
	}
	if res.Snapshots != 1 || res.Blobs != len(f.copied[0]) || res.Deleted != 0 {
		t.Fatalf("result: %+v", res)
	}
}

func TestOffsiteRefusesARemoteHoldingAnotherRepository(t *testing.T) {
	e, f := offsiteEnv(t)
	f.repoJSON = []byte(`{"format":2,"created_at":"2020-01-01T00:00:00Z"}`)
	res := e.svc.uploadOnce(context.Background())
	if res == nil || res.OK || !strings.Contains(res.Error, "different backup repository") {
		t.Fatalf("upload to a foreign remote: %+v", res)
	}
	for _, v := range f.verbs() {
		if v == "copy" || v == "copyto" || v == "delete" {
			t.Fatalf("nothing may be written to a foreign remote: %v", f.verbs())
		}
	}
}

func TestOffsiteDeletesWhatTheLocalRepositoryDropped(t *testing.T) {
	e, f := offsiteEnv(t)
	mine, _ := os.ReadFile(filepath.Join(e.repo.Dir(), "repo.json"))
	f.repoJSON = mine
	snap := e.snapshots()[0].ID
	f.listings["snapshots"] = []string{snap + ".json.zst", "20200101T000000Z-dead.json.zst"}
	f.listings["blobs"] = []string{"ff/" + strings.Repeat("f", 64) + ".xz"}
	res := e.svc.uploadOnce(context.Background())
	if res == nil || !res.OK || res.Deleted != 2 {
		t.Fatalf("upload: %+v", res)
	}
	if len(f.deleted) != 2 || f.deleted[0][0] != "snapshots/20200101T000000Z-dead.json.zst" ||
		f.deleted[1][0] != "blobs/ff/"+strings.Repeat("f", 64)+".xz" {
		t.Fatalf("deletes (manifests first, then blobs): %v", f.deleted)
	}
}

func TestOffsiteCapsSnapshotDeletes(t *testing.T) {
	e, f := offsiteEnv(t)
	mine, _ := os.ReadFile(filepath.Join(e.repo.Dir(), "repo.json"))
	f.repoJSON = mine
	for i := 0; i <= maxOffsiteSnapshotDeletes; i++ {
		f.listings["snapshots"] = append(f.listings["snapshots"], fmt.Sprintf("20200101T0000%02dZ-%04d.json.zst", i%60, i))
	}
	res := e.svc.uploadOnce(context.Background())
	if res == nil || res.OK || !strings.Contains(res.Error, "deletes at most") {
		t.Fatalf("upload over the delete cap: %+v", res)
	}
	if len(f.deleted) != 0 {
		t.Fatalf("nothing may be deleted over the cap: %v", f.deleted)
	}
}

func TestOffsiteLeavesPartialSnapshotsBehind(t *testing.T) {
	e, f := offsiteEnv(t)
	// A full run stopped during its walks leaves a partial snapshot.
	e.advance(2 * time.Hour)
	startPausedFull(t, e)
	if err := e.svc.StopRun(); err != nil {
		t.Fatal(err)
	}
	e.waitIdle()
	if len(e.snapshots()) != 2 {
		t.Fatalf("want the complete and the partial snapshot: %+v", e.snapshots())
	}
	e.svc.rclone = f.run
	res := e.svc.uploadOnce(context.Background())
	if res == nil || !res.OK || res.Snapshots != 1 {
		t.Fatalf("upload: %+v", res)
	}
}

func TestOffsiteFailureIsRecordedAndAlerted(t *testing.T) {
	e, f := offsiteEnv(t)
	f.failVerbs = map[string]bool{"copy": true}
	res := e.svc.uploadOnce(context.Background())
	if res == nil || res.OK || !strings.Contains(res.Error, "copy failed") {
		t.Fatalf("upload: %+v", res)
	}
	e.svc.recordUpload(*res)
	b, err := os.ReadFile(filepath.Join(e.repo.Dir(), "index", offsiteFile))
	if err != nil || !strings.Contains(string(b), "copy failed") {
		t.Fatalf("result not kept: %v %s", err, b)
	}
	failed := false
	for _, ev := range e.events() {
		failed = failed || ev == config.EventBackupFailed
	}
	if !failed {
		t.Fatal("a failed upload should send a failure notice")
	}
}

// TestOffsiteWithRealRclone copies to a local folder with the real rclone,
// then drops a snapshot locally and checks the next upload removes it and
// its blobs from the copy. It needs rclone on the PATH.
func TestOffsiteWithRealRclone(t *testing.T) {
	if _, err := exec.LookPath("rclone"); err != nil {
		t.Skip("rclone is not installed")
	}
	e := newEnv(t, nil)
	e.svc.runScheduled(context.Background(), "quick")
	e.advance(2 * time.Hour)
	e.svc.runScheduled(context.Background(), "full")
	remote := t.TempDir()
	e.setCfg(func(c *config.Backup) {
		c.RcloneRemote = remote
		// An empty config file keeps the test away from the user's remotes.
		c.RcloneConfig = filepath.Join(t.TempDir(), "rclone.conf")
	})
	_ = os.WriteFile(e.cfg.RcloneConfig, nil, 0o600)
	if res := e.svc.uploadOnce(context.Background()); res == nil || !res.OK {
		t.Fatalf("first upload: %+v", res)
	}
	snaps := e.snapshots()
	for _, sn := range snaps {
		if _, err := os.Stat(filepath.Join(remote, "snapshots", sn.ID+".json.zst")); err != nil {
			t.Fatalf("snapshot %s not copied: %v", sn.ID, err)
		}
	}
	if _, err := os.Stat(filepath.Join(remote, "repo.json")); err != nil {
		t.Fatal("repo.json not copied")
	}

	// Drop the quick snapshot locally (retention would); the next upload
	// removes it from the copy, with the blobs only it used.
	var quick string
	for _, sn := range snaps {
		if sn.Trigger == "schedule:quick" {
			quick = sn.ID
		}
	}
	plan, err := e.repo.PrunePlan(context.Background(), RetentionPolicy{Last: 1, TriggerPrefix: "schedule:quick"})
	if err != nil {
		t.Fatal(err)
	}
	plan.DeleteSnapshot = []SnapshotInfo{{ID: quick}}
	if _, err := e.repo.Prune(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	res := e.svc.uploadOnce(context.Background())
	if res == nil || !res.OK || res.Deleted == 0 {
		t.Fatalf("second upload: %+v", res)
	}
	if _, err := os.Stat(filepath.Join(remote, "snapshots", quick+".json.zst")); !os.IsNotExist(err) {
		t.Fatalf("dropped snapshot still on the copy: %v", err)
	}
	// What is left on the copy verifies as a repository of its own.
	copyRepo, err := OpenExistingRepo(remote)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := copyRepo.Verify(context.Background(), VerifyOptions{Full: true}, nil)
	if err != nil || !rep.OK {
		t.Fatalf("the copy does not verify: %v %+v", err, rep)
	}
}
