package backup

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
)

type autoEnv struct {
	*linkEnv
	svc     *BackupService
	data    string
	repoDir string
	cfg     config.Backup
	mu      sync.Mutex
	notes   []config.NotificationEvent
	e4      string // links that are in the snapshot but gone from disk
	e5      string
}

// newAutoEnv takes a snapshot that holds two links, then removes them from
// the disk so the next check finds them missing.
func newAutoEnv(t *testing.T, ar config.BackupAutoRecreate) *autoEnv {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	le := newLinkEnv(t)
	a := &autoEnv{linkEnv: le, data: t.TempDir()}
	a.e4 = filepath.Join(le.tv, "Show", "S01E04.mkv")
	a.e5 = filepath.Join(le.tv, "Show", "S01E05.mkv")
	mustLink(t, le.mount+"/__all__/EntryNew/c.mkv", a.e4)
	mustLink(t, le.mount+"/__all__/EntryA/a.mkv", a.e5)

	fa, src := le.arrSrc()
	fa.files = append(fa.files, arr.MediaFile{Path: "/arrtv/Show/S01E05.mkv", FileID: 15, ItemID: 7, Title: "Show"})
	a.repoDir = filepath.Join(t.TempDir(), "repo")
	repo, err := OpenRepo(a.repoDir)
	if err != nil {
		t.Fatal(err)
	}
	a.cfg = config.Backup{
		Dir:          "repo",
		Jobs:         []config.BackupJob{{Name: "quick", Enabled: true, Components: []string{"links"}, Schedule: "0 */6 * * *"}},
		AutoRecreate: ar,
	}
	a.svc = NewService(Deps{
		DataDir:   a.data,
		Config:    func() config.Backup { a.mu.Lock(); defer a.mu.Unlock(); return a.cfg },
		OpenRepo:  func(string) (Repo, error) { return repo, nil },
		FS:        le.fs,
		Lookup:    le.lk,
		MountRoot: func() string { return le.mount },
		Arrs:      func() []ArrSource { return []ArrSource{src} },
		ExtraRoots: func() []string {
			return []string{le.movies, le.unused, le.tv + "/"}
		},
		Notify: func(ev config.NotificationEvent, _, _ string, _ error) {
			a.mu.Lock()
			a.notes = append(a.notes, ev)
			a.mu.Unlock()
		},
		Now: func() time.Time { return time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC) },
	})
	if _, err := a.svc.RunJob(context.Background(), "quick"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		a.svc.mu.Lock()
		idle := a.svc.runningJob == ""
		a.svc.mu.Unlock()
		if idle {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("snapshot did not finish")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if infos, _ := repo.ListSnapshots(context.Background()); len(infos) != 1 {
		t.Fatalf("snapshots = %d", len(infos))
	}
	for _, p := range []string{a.e4, a.e5} {
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
	}
	return a
}

func (a *autoEnv) exists(p string) bool {
	_, err := os.Lstat(p)
	return err == nil
}

func (a *autoEnv) dropped() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, ev := range a.notes {
		if ev == config.EventLinksDropped {
			n++
		}
	}
	return n
}

func TestAutoRecreateOffByDefault(t *testing.T) {
	a := newAutoEnv(t, config.BackupAutoRecreate{Limit: 25})
	if err := markLinkRestoreApplied(a.data, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := a.svc.LinksCheck(context.Background()); err != nil {
		t.Fatal(err)
	}
	if a.exists(a.e4) || a.exists(a.e5) {
		t.Fatal("links were re-created while the setting is off")
	}
}

func TestAutoRecreateWaitsForFirstManualApply(t *testing.T) {
	a := newAutoEnv(t, config.BackupAutoRecreate{Enabled: true, Limit: 25})
	if _, err := a.svc.LinksCheck(context.Background()); err != nil {
		t.Fatal(err)
	}
	if a.exists(a.e4) || a.exists(a.e5) {
		t.Fatal("links were re-created before any manual apply")
	}
	if err := markLinkRestoreApplied(a.data, time.Now()); err != nil {
		t.Fatal(err)
	}
	st, err := a.svc.LinksCheck(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !a.exists(a.e4) || !a.exists(a.e5) {
		t.Fatal("links were not re-created once a manual apply was recorded")
	}
	if len(st.Recreated) != 2 || st.Recreated[0].SnapshotID == "" {
		t.Fatalf("recreated = %+v", st.Recreated)
	}
	if got, _ := a.svc.LinksStatus(context.Background()); len(got.Recreated) != 2 {
		t.Fatalf("status recreated = %+v", got.Recreated)
	}
	if a.dropped() != 0 {
		t.Fatal("links_dropped sent under the limit")
	}
	want := a.mount + "/__all__/EntryNew/c.mkv"
	if got, _ := os.Readlink(a.e4); got != want {
		t.Fatalf("target = %q", got)
	}
}

func TestAutoRecreateOverLimitCreatesNothingAndNotifies(t *testing.T) {
	a := newAutoEnv(t, config.BackupAutoRecreate{Enabled: true, Limit: 1})
	if err := markLinkRestoreApplied(a.data, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := a.svc.LinksCheck(context.Background()); err != nil {
		t.Fatal(err)
	}
	if a.exists(a.e4) || a.exists(a.e5) {
		t.Fatal("links were re-created over the limit")
	}
	if a.dropped() != 1 {
		t.Fatalf("links_dropped sent %d times", a.dropped())
	}
}

func TestAutoRecreateNeverRetargetsOrReplaces(t *testing.T) {
	a := newAutoEnv(t, config.BackupAutoRecreate{Enabled: true, Limit: 25})
	if err := markLinkRestoreApplied(a.data, time.Now()); err != nil {
		t.Fatal(err)
	}
	// A file where one link used to be is never replaced; the other
	// link comes back.
	if err := os.WriteFile(a.e4, []byte("real file"), 0o600); err != nil {
		t.Fatal(err)
	}
	// An existing link, even one the snapshot would retarget, is left alone.
	existing := filepath.Join(a.tv, "Show", "S01E01.mkv")
	before, _ := os.Readlink(existing)
	other := a.mount + "/__all__/EntryB/b.mkv"
	_ = os.Remove(existing)
	mustLink(t, other, existing)
	if _, err := a.svc.LinksCheck(context.Background()); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(a.e4); string(b) != "real file" {
		t.Fatal("a regular file was replaced")
	}
	if got, _ := os.Readlink(existing); got != other || got == before {
		t.Fatalf("existing link changed to %q", got)
	}
	if !a.exists(a.e5) {
		t.Fatal("missing link was not re-created")
	}
}
