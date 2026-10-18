package backup

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-co-op/gocron/v2"

	"github.com/sirrobot01/decypharr/internal/config"
)

type testEnv struct {
	t      *testing.T
	svc    *BackupService
	repo   *FileRepo
	data   string
	mu     sync.Mutex
	cfg    config.Backup
	clock  time.Time
	notes  []config.NotificationEvent
	fail   atomic.Bool
	writes atomic.Int64
	block  chan struct{}
}

func (e *testEnv) setCfg(f func(*config.Backup)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	f(&e.cfg)
}

func (e *testEnv) advance(d time.Duration) {
	e.mu.Lock()
	e.clock = e.clock.Add(d)
	e.mu.Unlock()
}

func (e *testEnv) events() []config.NotificationEvent {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]config.NotificationEvent(nil), e.notes...)
}

func newEnv(t *testing.T, sched gocron.Scheduler) *testEnv {
	t.Helper()
	e := &testEnv{t: t, data: t.TempDir(), clock: time.Now().UTC().Truncate(time.Hour)}
	repo, err := OpenRepo(filepath.Join(t.TempDir(), "repo"))
	if err != nil {
		t.Fatal(err)
	}
	e.repo = repo
	e.cfg = config.Backup{
		Dir: "repo",
		Jobs: []config.BackupJob{
			{Name: "quick", Enabled: true, Components: []string{"db", "config", "state", "links"}, Schedule: "0 */6 * * *",
				Retention: config.BackupRetention{Last: 2}},
			{Name: "full", Enabled: true, Components: []string{"db", "config", "state", "nzb", "overlay", "hearsay"},
				Schedule: "04:00", Retention: config.BackupRetention{Last: 3}},
		},
		PreAction: config.BackupPreAction{Enabled: true, Keep: 2, MaxAgeDays: 14},
		Notify:    config.BackupNotify{OnFailure: true},
	}
	write := func(rel, body string) {
		p := filepath.Join(e.data, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("config.json", `{"a":1}`)
	write("usenet/meta/one.meta", "one")
	write("usenet/overlay/x/f", "overlay")
	e.svc = NewService(Deps{
		DataDir:  e.data,
		DBWrites: e.writes.Load,
		Config:   func() config.Backup { e.mu.Lock(); defer e.mu.Unlock(); return e.cfg },
		OpenRepo: func(string) (Repo, error) { return repo, nil },
		DBExport: func(ctx context.Context, bs BlobStore, _ Progress) (ComponentEntry, error) {
			if e.block != nil {
				<-e.block
			}
			if e.fail.Load() {
				return ComponentEntry{}, errors.New("export broke")
			}
			// The lock must already be held by the run.
			if _, err := repo.Lock(ctx); !errors.Is(err, ErrBusy) {
				t.Errorf("repo lock not held during the export: %v", err)
			}
			ref, _, err := bs.PutBlob(ctx, stringsReader("db"), CompressDefault)
			return ComponentEntry{StoreBlobs: map[string]string{"entries": ref.SHA256}}, err
		},
		Notify: func(ev config.NotificationEvent, _, _ string, _ error) {
			e.mu.Lock()
			e.notes = append(e.notes, ev)
			e.mu.Unlock()
		},
		Lookup:    noEntries{},
		Scheduler: sched,
		Now:       func() time.Time { e.mu.Lock(); defer e.mu.Unlock(); return e.clock },
	})
	return e
}

func (e *testEnv) waitIdle() {
	e.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		e.svc.mu.Lock()
		idle := e.svc.runningJob == ""
		e.svc.mu.Unlock()
		if idle {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	e.t.Fatal("run did not finish")
}

func (e *testEnv) snapshots() []SnapshotInfo {
	e.t.Helper()
	l, err := e.repo.ListSnapshots(context.Background())
	if err != nil {
		e.t.Fatal(err)
	}
	return l
}

func TestRunJobWritesSnapshotWithComponentsAndHoldsLock(t *testing.T) {
	e := newEnv(t, nil)
	if _, err := e.svc.RunJob(context.Background(), "full"); err != nil {
		t.Fatal(err)
	}
	e.waitIdle()
	snaps := e.snapshots()
	if len(snaps) != 1 || snaps[0].Trigger != "manual:full" {
		t.Fatalf("snapshots: %+v", snaps)
	}
	snap, err := e.repo.ReadSnapshot(context.Background(), snaps[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []Component{ComponentDB, ComponentConfig, ComponentState, ComponentNZB, ComponentOverlay, ComponentHearsay} {
		if _, ok := snap.Components[c]; !ok {
			t.Errorf("component %s missing", c)
		}
	}
	if snap.Components[ComponentNZB].FileCount != 1 || snap.Components[ComponentOverlay].FileCount != 1 {
		t.Errorf("walk counts: %+v", snap.Components)
	}
	if _, ok := snap.Components[ComponentConfig].Files["config.json"]; !ok {
		t.Error("config.json not stored")
	}
	// The lock is released afterwards.
	unlock, err := e.repo.Lock(context.Background())
	if err != nil {
		t.Fatalf("lock still held: %v", err)
	}
	unlock()
	st, _ := e.svc.Status(context.Background())
	if !st.Configured || st.Snapshots != 1 || st.Jobs[1].LastSuccess == nil {
		t.Errorf("status: %+v", st)
	}
}

func TestRunJobUnknownAndBusy(t *testing.T) {
	e := newEnv(t, nil)
	if _, err := e.svc.RunJob(context.Background(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown job: %v", err)
	}
	e.block = make(chan struct{})
	if _, err := e.svc.RunJob(context.Background(), "quick"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.RunJob(context.Background(), "full"); !errors.Is(err, ErrBusy) {
		t.Fatalf("second run: %v", err)
	}
	st, _ := e.svc.Status(context.Background())
	if !st.Jobs[0].Running || st.Jobs[1].Running {
		t.Errorf("running flags: %+v", st.Jobs)
	}
	close(e.block)
	e.waitIdle()
}

func TestScheduledRunSkipsWhenAnotherIsRunning(t *testing.T) {
	e := newEnv(t, nil)
	e.block = make(chan struct{})
	if _, err := e.svc.RunJob(context.Background(), "quick"); err != nil {
		t.Fatal(err)
	}
	e.svc.runScheduled(context.Background(), "full") // returns at once, queues nothing
	close(e.block)
	e.waitIdle()
	if n := len(e.snapshots()); n != 1 {
		t.Fatalf("want only the manual run, got %d snapshots", n)
	}
}

func TestRunPrunesByJobPolicy(t *testing.T) {
	e := newEnv(t, nil) // quick keeps the last 2
	for i := 0; i < 4; i++ {
		e.advance(time.Hour)
		e.svc.runScheduled(context.Background(), "quick")
	}
	snaps := e.snapshots()
	if len(snaps) != 2 {
		t.Fatalf("want 2 after pruning, got %d", len(snaps))
	}
	for _, s := range snaps {
		if s.Trigger != "schedule:quick" {
			t.Errorf("trigger %q", s.Trigger)
		}
	}
}

func TestFailedRunNotifiesAndWritesNoSnapshot(t *testing.T) {
	e := newEnv(t, nil)
	e.fail.Store(true)
	e.svc.runScheduled(context.Background(), "quick")
	if n := len(e.snapshots()); n != 0 {
		t.Fatalf("snapshots after failure: %d", n)
	}
	if ev := e.events(); len(ev) != 1 || ev[0] != config.EventBackupFailed {
		t.Fatalf("events: %v", ev)
	}
	st, _ := e.svc.Status(context.Background())
	if st.Jobs[0].LastError == "" {
		t.Error("last error not shown")
	}
	// The failed run released its lock and slot.
	e.fail.Store(false)
	e.svc.runScheduled(context.Background(), "quick")
	if n := len(e.snapshots()); n != 1 {
		t.Fatalf("retry: %d", n)
	}
}

func TestScheduleParsingAndReschedule(t *testing.T) {
	sched, err := gocron.NewScheduler()
	if err != nil {
		t.Fatal(err)
	}
	sched.Start()
	t.Cleanup(func() { _ = sched.Shutdown() })
	e := newEnv(t, sched)
	if err := e.svc.ApplyConfig(); err != nil {
		t.Fatal(err)
	}
	st, _ := e.svc.Status(context.Background())
	if st.Jobs[0].NextRun == nil || st.Jobs[1].NextRun == nil {
		t.Fatalf("next runs: %+v", st.Jobs)
	}
	first := *st.Jobs[1].NextRun

	// Reschedule through the settings watcher, no explicit ApplyConfig.
	e.setCfg(func(c *config.Backup) {
		c.Jobs[1].Schedule = "23:30"
		c.Jobs[0].Enabled = false
	})
	e.svc.reconcile()
	st, _ = e.svc.Status(context.Background())
	if st.Jobs[0].NextRun != nil {
		t.Error("a disabled job still has a next run")
	}
	if st.Jobs[1].NextRun == nil || st.Jobs[1].NextRun.Equal(first) || st.Jobs[1].NextRun.Hour() != 23 {
		t.Errorf("not rescheduled: was %v now %v", first, st.Jobs[1].NextRun)
	}

	// A bad schedule skips only its own job.
	e.setCfg(func(c *config.Backup) {
		c.Jobs[0].Enabled = true
		c.Jobs[0].Schedule = "every tuesday"
	})
	if err := e.svc.ApplyConfig(); err == nil {
		t.Fatal("bad schedule accepted")
	}
	st, _ = e.svc.Status(context.Background())
	if st.Jobs[0].NextRun != nil || st.Jobs[1].NextRun == nil {
		t.Errorf("after bad schedule: %+v", st.Jobs)
	}
}

func TestQuietHours(t *testing.T) {
	at := func(h, m int) time.Time { return time.Date(2026, 3, 1, h, m, 0, 0, time.UTC) }
	cases := []struct {
		q      config.BackupQuietHours
		now    time.Time
		inside bool
		end    time.Time
	}{
		{config.BackupQuietHours{Start: "18:00", End: "23:00"}, at(19, 0), true, at(23, 0)},
		{config.BackupQuietHours{Start: "18:00", End: "23:00"}, at(23, 0), false, time.Time{}},
		{config.BackupQuietHours{Start: "22:00", End: "06:00"}, at(23, 30), true, at(6, 0).AddDate(0, 0, 1)},
		{config.BackupQuietHours{Start: "22:00", End: "06:00"}, at(5, 0), true, at(6, 0)},
		{config.BackupQuietHours{Start: "22:00", End: "06:00"}, at(12, 0), false, time.Time{}},
		{config.BackupQuietHours{}, at(12, 0), false, time.Time{}},
	}
	for i, c := range cases {
		end, in := quietUntil(c.now, c.q)
		if in != c.inside || (in && !end.Equal(c.end)) {
			t.Errorf("case %d: got %v %v", i, in, end)
		}
	}
}

func TestPreActionReusesWithinTenMinutes(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()
	id1, err := e.svc.PreAction(ctx, ActionDeleteEntry)
	if err != nil || id1 == "" {
		t.Fatalf("first: %q %v", id1, err)
	}
	e.advance(9 * time.Minute)
	id2, err := e.svc.PreAction(ctx, ActionDeleteEntries)
	if err != nil || id2 != id1 {
		t.Fatalf("within 10 minutes should reuse: %q vs %q (%v)", id2, id1, err)
	}
	if n := len(e.snapshots()); n != 1 {
		t.Fatalf("snapshots: %d", n)
	}
	e.advance(2 * time.Minute)
	id3, err := e.svc.PreAction(ctx, ActionDeleteEntry)
	if err != nil || id3 == id1 {
		t.Fatalf("after 10 minutes want a new snapshot: %q %v", id3, err)
	}
	snaps := e.snapshots()
	if snaps[0].Trigger != "pre:delete_entry" {
		t.Errorf("trigger %q", snaps[0].Trigger)
	}
	// Quick set only: no nzb or overlay.
	snap, _ := e.repo.ReadSnapshot(ctx, id3)
	if _, ok := snap.Components[ComponentNZB]; ok {
		t.Error("pre-action snapshot walked the NZB records")
	}
}

func TestPreActionReuseNeedsNothingElseSince(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()
	id1, _ := e.svc.PreAction(ctx, ActionDeleteEntry)
	e.advance(time.Minute)
	e.svc.runScheduled(ctx, "quick")
	id2, _ := e.svc.PreAction(ctx, ActionDeleteEntry)
	if id1 == id2 || id2 == "" {
		t.Fatalf("a backup ran in between, so the old snapshot is not current: %q %q", id1, id2)
	}
}

func TestPreActionReuseNeedsNoDatabaseWriteSince(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()
	id1, _ := e.svc.PreAction(ctx, ActionDeleteEntry)
	id2, _ := e.svc.PreAction(ctx, ActionDeleteEntries)
	if id1 == "" || id2 != id1 {
		t.Fatalf("no write in between should reuse: %q %q", id1, id2)
	}
	e.writes.Add(1) // an entry was written
	id3, _ := e.svc.PreAction(ctx, ActionDeleteEntry)
	if id3 == "" || id3 == id1 {
		t.Fatalf("a database write means a new snapshot: %q %q", id1, id3)
	}
	if n := len(e.snapshots()); n != 2 {
		t.Fatalf("snapshots: %d", n)
	}
}

func TestPreActionRetention(t *testing.T) {
	e := newEnv(t, nil) // keep 2
	for i := 0; i < 4; i++ {
		e.advance(11 * time.Minute)
		if _, err := e.svc.PreAction(context.Background(), ActionClearRepairState); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(e.snapshots()); n != 2 {
		t.Fatalf("want 2 pre-action snapshots kept, got %d", n)
	}
}

func TestPreActionFailureCarriesOnUnlessRefused(t *testing.T) {
	e := newEnv(t, nil)
	e.fail.Store(true)
	ctx := context.Background()
	id, err := e.svc.PreAction(ctx, ActionDeleteEntry)
	if err == nil || errors.Is(err, ErrPreActionRefused) || id != "" {
		t.Fatalf("carry-on case: %q %v", id, err)
	}
	e.setCfg(func(c *config.Backup) { c.PreAction.Refuse = []string{ActionDeleteEntry} })
	if _, err := e.svc.PreAction(ctx, ActionDeleteEntry); !errors.Is(err, ErrPreActionRefused) {
		t.Fatalf("refused case: %v", err)
	}
	if n := len(e.events()); n != 2 {
		t.Errorf("each failure should notify: %v", e.events())
	}
}

func TestPreActionOffOrUnconfigured(t *testing.T) {
	e := newEnv(t, nil)
	e.setCfg(func(c *config.Backup) { c.PreAction.Enabled = false })
	if id, err := e.svc.PreAction(context.Background(), ActionDeleteEntry); id != "" || err != nil {
		t.Fatalf("off: %q %v", id, err)
	}
	e.setCfg(func(c *config.Backup) { c.PreAction.Enabled = true; c.Dir = "" })
	if id, err := e.svc.PreAction(context.Background(), ActionDeleteEntry); id != "" || err != nil {
		t.Fatalf("unconfigured: %q %v", id, err)
	}
}

func TestPostRestoreHoldFlagAndRelease(t *testing.T) {
	e := newEnv(t, nil)
	var hr HoldReleaser = e.svc
	if hr.HoldActive() {
		t.Fatal("hold on without a marker")
	}
	if err := writeJSONFile(filepath.Join(e.data, PostRestoreHoldFile), PostRestoreHold{SnapshotID: "s"}); err != nil {
		t.Fatal(err)
	}
	st, _ := e.svc.Status(context.Background())
	if !st.PostRestore || !hr.HoldActive() {
		t.Fatal("marker should switch the hold on")
	}
	if err := hr.ReleaseHold(); err != nil {
		t.Fatal(err)
	}
	st, _ = e.svc.Status(context.Background())
	if st.PostRestore || PostRestoreHoldActive(e.data) {
		t.Fatal("hold still on after release")
	}
	if err := hr.ReleaseHold(); err != nil {
		t.Fatalf("second release: %v", err)
	}
}

func TestStaleBackupNotifiesOncePerThreshold(t *testing.T) {
	e := newEnv(t, nil)
	e.setCfg(func(c *config.Backup) { c.Notify.StaleHours = 24; c.Jobs = c.Jobs[:1] })
	ctx := context.Background()
	e.svc.checkStale(ctx)
	if len(e.events()) != 0 {
		t.Fatal("stale right after start")
	}
	e.advance(25 * time.Hour)
	e.svc.checkStale(ctx)
	e.advance(time.Hour)
	e.svc.checkStale(ctx) // inside the threshold since the last notice
	if ev := e.events(); len(ev) != 1 || ev[0] != config.EventBackupStale {
		t.Fatalf("events: %v", ev)
	}
}

func TestDeleteSnapshotFreesBlobs(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()
	e.svc.runScheduled(ctx, "quick")
	e.advance(time.Hour)
	e.svc.runScheduled(ctx, "quick")
	snaps := e.snapshots()
	if len(snaps) != 2 {
		t.Fatal(len(snaps))
	}
	plan, err := e.svc.RetentionPreview(ctx, RetentionPreviewRequest{SnapshotID: snaps[0].ID})
	if err != nil || len(plan.DeleteSnapshot) != 1 || plan.KeepSnapshots != 1 {
		t.Fatalf("preview: %+v %v", plan, err)
	}
	if err := e.svc.DeleteSnapshot(ctx, snaps[0].ID); err != nil {
		t.Fatal(err)
	}
	if n := len(e.snapshots()); n != 1 {
		t.Fatalf("after delete: %d", n)
	}
}

func TestWP6ServicesDefaultToNotImplemented(t *testing.T) {
	e := newEnv(t, nil)
	if _, err := e.svc.ListConfigHistory(context.Background()); !errors.Is(err, ErrNotImplemented) {
		t.Fatal(err)
	}
	if _, err := e.svc.ListRemoved(context.Background(), RemovedQuery{}); !errors.Is(err, ErrNotImplemented) {
		t.Fatal(err)
	}
}

func TestUnknownLinkPlansAreRefused(t *testing.T) {
	e := newEnv(t, nil)
	if _, err := e.svc.ApplyLinksRestore(context.Background(), "x", LinksApplyRequest{PlanID: "nope"}); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := e.svc.ApplyLinksCleanup(context.Background(), LinksCleanupApplyRequest{PlanID: "nope"}); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}

func stringsReader(s string) *strings.Reader { return strings.NewReader(s) }

type noEntries struct{}

func (noEntries) EntryExists(string) bool         { return false }
func (noEntries) FileInEntry(_, _ string) bool    { return false }
func (noEntries) EntryByID(string) (string, bool) { return "", false }
func (noEntries) EntryID(string) string           { return "" }

func TestStopWaitsForARunToFinish(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	e := newEnv(t, nil)
	release, err := e.svc.claim("job")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { e.svc.Stop(); close(done) }()
	select {
	case <-done:
		t.Fatal("Stop returned while a run still held the claim")
	case <-time.After(100 * time.Millisecond):
	}
	release()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return after the run finished")
	}
}

func TestStopGivesUpOnAStuckRun(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	e := newEnv(t, nil)
	prev := stopWait
	stopWait = 50 * time.Millisecond
	t.Cleanup(func() { stopWait = prev })
	if _, err := e.svc.claim("job"); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	e.svc.Stop()
	if time.Since(start) > 2*time.Second {
		t.Fatal("Stop waited past its bound")
	}
}

func writeLinkSnapshot(t *testing.T, e *testEnv, paths ...string) string {
	t.Helper()
	var buf bytes.Buffer
	m := &Manifest{}
	for _, p := range paths {
		m.Rows = append(m.Rows, LinkRow{Path: p, State: LinkLive})
	}
	if err := EncodeManifest(&buf, m); err != nil {
		t.Fatal(err)
	}
	ref, _, err := e.repo.PutBlob(context.Background(), &buf, CompressDefault)
	if err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.clock = e.clock.Add(time.Hour)
	at := e.clock
	e.mu.Unlock()
	snap := &Snapshot{CreatedAt: at, Trigger: "manual:quick", Result: "ok",
		Components: map[Component]ComponentEntry{ComponentLinks: {Manifest: ref.SHA256, Rows: len(paths)}}}
	if err := e.repo.WriteSnapshot(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	return snap.ID
}

func TestLinksMissingNamesTheNewestSnapshotWithTheLink(t *testing.T) {
	e := newEnv(t, nil)
	older := writeLinkSnapshot(t, e, "/tv/a.mkv", "/tv/b.mkv")
	newer := writeLinkSnapshot(t, e, "/tv/a.mkv")
	e.svc.linkMu.Lock()
	e.svc.lastManifest = &Manifest{Missing: []MissingLink{
		{Path: "/tv/a.mkv", Arr: "sonarr"}, {Path: "/tv/b.mkv", Arr: "sonarr"}, {Path: "/tv/c.mkv", Arr: "sonarr"}}}
	e.svc.linkMu.Unlock()
	got, err := e.svc.LinksMissing(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		id string
		ok bool
	}{{newer, true}, {older, true}, {"", false}}
	for i, w := range want {
		if got[i].SnapshotID != w.id || got[i].Restorable != w.ok {
			t.Errorf("row %d = %+v, want snapshot %q restorable %v", i, got[i], w.id, w.ok)
		}
	}
}

func TestLinkRestoreMarker(t *testing.T) {
	dir := t.TempDir()
	if LinkRestoreApplied(dir) || LinkRestoreApplied("") {
		t.Fatal("marker reported before it was written")
	}
	if err := markLinkRestoreApplied(dir, time.Now()); err != nil {
		t.Fatal(err)
	}
	if !LinkRestoreApplied(dir) {
		t.Fatal("marker not seen")
	}
	e := newEnv(t, nil)
	if st, _ := e.svc.Status(context.Background()); st.LinkRestoreApplied {
		t.Error("status reports a restore before one ran")
	}
	if err := markLinkRestoreApplied(e.data, time.Now()); err != nil {
		t.Fatal(err)
	}
	if st, _ := e.svc.Status(context.Background()); !st.LinkRestoreApplied {
		t.Error("status does not report the marker")
	}
}

func TestApplyLinksRestoreWritesMarkerOnlyWhenSomethingChanged(t *testing.T) {
	e := newEnv(t, nil)
	e.svc.linkPlans["empty"] = LinkPlan{PlanID: "empty"}
	if _, err := e.svc.ApplyLinksRestore(context.Background(), "x", LinksApplyRequest{PlanID: "empty"}); err != nil {
		t.Fatal(err)
	}
	if LinkRestoreApplied(e.data) {
		t.Fatal("marker written for an empty plan")
	}
}

func TestApplyLinksRestoreMarksAManualRestore(t *testing.T) {
	e := newEnv(t, nil)
	path := filepath.Join(t.TempDir(), "tv", "a.mkv")
	e.svc.linkPlans["p"] = LinkPlan{PlanID: "p", Rows: []LinkPlanRow{{Path: path, Class: PlanCreate, Target: "/nowhere/a.mkv"}}}
	res, err := e.svc.ApplyLinksRestore(context.Background(), "x", LinksApplyRequest{PlanID: "p"})
	if err != nil || res.Created != 1 {
		t.Fatalf("res = %+v, err = %v", res, err)
	}
	if !LinkRestoreApplied(e.data) {
		t.Fatal("marker missing after a link was created")
	}
}

func TestDirChangeAppliesWithoutRebuildingService(t *testing.T) {
	data := t.TempDir()
	if err := os.WriteFile(filepath.Join(data, "config.json"), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	dirA, dirB := filepath.Join(t.TempDir(), "a"), filepath.Join(t.TempDir(), "b")
	var mu sync.Mutex
	cfg := config.Backup{
		Dir:  dirA,
		Jobs: []config.BackupJob{{Name: "j", Enabled: true, Components: []string{"config"}, Schedule: "04:00", Retention: config.BackupRetention{Last: 3}}},
	}
	svc := NewService(Deps{
		DataDir: data,
		Config:  func() config.Backup { mu.Lock(); defer mu.Unlock(); return cfg },
		Lookup:  noEntries{},
	})
	run := func() {
		t.Helper()
		if _, err := svc.RunJob(context.Background(), "j"); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			svc.mu.Lock()
			idle := svc.runningJob == ""
			svc.mu.Unlock()
			if idle {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatal("run did not finish")
	}
	count := func(dir string) int {
		r, err := OpenRepo(dir)
		if err != nil {
			t.Fatal(err)
		}
		l, err := r.ListSnapshots(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return len(l)
	}
	run()
	mu.Lock()
	cfg.Dir = dirB
	mu.Unlock()
	st, err := svc.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.Snapshots != 0 {
		t.Errorf("status still reads the old folder: %d snapshots", st.Snapshots)
	}
	run()
	if count(dirA) != 1 || count(dirB) != 1 {
		t.Errorf("snapshots: old %d, new %d; want 1 and 1", count(dirA), count(dirB))
	}
}

func TestStatusCarriesHeartbeatAndServerClock(t *testing.T) {
	e := newEnv(t, nil)
	release, err := e.svc.claim("job:quick")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	e.svc.report(ProgressUpdate{Component: ComponentNZB, Phase: "walk", FilesDone: 1, FilesTotal: 2})
	st, err := e.svc.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.Now.IsZero() || st.Running == nil || st.Running.UpdatedAt == nil || st.Running.RunStarted == nil {
		t.Fatalf("status: now=%v running=%+v", st.Now, st.Running)
	}
}

// startPausedFull starts the full job with something always streaming and
// waits until its walk reports the pause.
func startPausedFull(t *testing.T, e *testEnv) {
	t.Helper()
	e.setCfg(func(c *config.Backup) { c.PauseWhileStreaming = true })
	e.svc.deps.Streaming = func() bool { return true }
	if _, err := e.svc.RunJob(context.Background(), "full"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := e.svc.Status(context.Background())
		if st.Running != nil && st.Running.Paused != "" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the walk never reported a pause")
}

func TestStopRunEndsAPausedBackupWithoutASnapshot(t *testing.T) {
	e := newEnv(t, nil)
	startPausedFull(t, e)
	if err := e.svc.StopRun(); err != nil {
		t.Fatal(err)
	}
	e.waitIdle()
	if n := len(e.snapshots()); n != 0 {
		t.Fatalf("a stopped run wrote %d snapshots", n)
	}
	st, _ := e.svc.Status(context.Background())
	for _, j := range st.Jobs {
		if j.Name == "full" && j.LastError != "Stopped from the Backups page" {
			t.Fatalf("full job last error %q", j.LastError)
		}
	}
	for _, ev := range e.events() {
		if ev == config.EventBackupFailed {
			t.Fatal("stopping a backup should not send a failure notice")
		}
	}
	if err := e.svc.StopRun(); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stop with nothing running: %v", err)
	}
}

func TestTurningPauseOffLetsAPausedBackupFinish(t *testing.T) {
	e := newEnv(t, nil)
	startPausedFull(t, e)
	e.setCfg(func(c *config.Backup) { c.PauseWhileStreaming = false })
	e.waitIdle()
	if n := len(e.snapshots()); n != 1 {
		t.Fatalf("want the run to finish with 1 snapshot, got %d", n)
	}
}
