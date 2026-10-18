package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
)

// Deps is everything the Service needs from the rest of Decypharr. Function
// fields keep this package free of the manager.
type Deps struct {
	// DataDir is the data folder (config.json, db/, usenet/ live under it).
	DataDir string
	// Config returns the live backup settings.
	Config func() config.Backup
	// Build records which build wrote a snapshot.
	Build BuildInfo
	// DBExport streams the database stores into the repository.
	DBExport DBExporter
	// DBWrites returns a count of database writes that only goes up; nil
	// means the database is assumed to change (a pre-action snapshot is
	// then never reused).
	DBWrites func() int64
	// OpenRepo opens the repository in dir; nil means OpenRepo.
	OpenRepo func(dir string) (Repo, error)

	// Link checks.
	FS         LinkFS
	Lookup     EntryLookup
	MountRoot  func() string
	Arrs       func() []ArrSource
	ExtraRoots func() []string
	// Rescanner asks an Arr to re-read an item after links were restored.
	Rescanner func(ctx context.Context, arrName string, itemID int) error

	// Streaming reports whether anyone is playing a file; the walks pause
	// while it is true and the setting is on.
	Streaming func() bool
	// Notify sends a backup notification; nil sends nothing.
	Notify func(ev config.NotificationEvent, status, msg string, err error)
	// Alerts reports whether Notify can reach anyone; nil is not reported.
	Alerts func() AlertState
	// Scheduler is the manager's gocron scheduler; nil means no schedule.
	Scheduler gocron.Scheduler
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

// maxKeptPlans bounds the link plans the service remembers, matching the
// cleanup plan store in linkcheck.go so both forget together.
const maxKeptPlans = maxRememberedPlans

// BackupService is the real backup Service. Methods the work packages did not build
// yet fall through to the not-implemented base.
type BackupService struct {
	notImplemented

	// History and Undelete are WP6's services; nil returns ErrNotImplemented.
	History  HistoryService
	Undelete UndeleteService

	deps Deps
	log  zerolog.Logger

	mu         sync.Mutex
	runningJob string // label of the run holding the claim; "" when idle
	runStarted time.Time
	progress   *ProgressUpdate
	// runCancel cancels the backup job in progress (nil when none), and
	// stopRequested records that StopRun asked for it.
	runCancel     context.CancelFunc
	stopRequested bool
	lastErr       map[string]string
	deferred      map[string]bool
	repoDir       string
	repoVal       Repo
	startedAt     time.Time

	// plans
	linkPlans    map[string]LinkPlan
	linkPlanIDs  []string
	cleanPlans   map[string]CleanupPlan
	cleanPlanIDs []string

	// last link check
	linkMu       sync.Mutex
	checking     bool
	lastManifest *Manifest
	lastLinks    LinksStatusResponse
	lastLive     int
	trend        []TrendPoint
	recreated    []AutoRecreated

	// scheduler state
	ctx          context.Context
	cancel       context.CancelFunc
	jobs         map[string]gocron.Job
	applied      string
	staleChecked time.Time
	staleNotice  map[string]time.Time

	preMu sync.Mutex
	// preSnap and preWrites are the newest pre-action snapshot this process
	// took and the database write count when it started; preMu guards them.
	preSnap   string
	preWrites int64

	sizeAt    time.Time
	sizeBytes int64

	// The run queue (queue.go): runs waiting for the slot and the run holding
	// it, mirrored in RunQueueFile. qmu guards them.
	qmu       sync.Mutex
	queue     []queuedRun
	running   *queuedRun
	kickTimer *time.Timer

	// The last check (check.go), read from the repository once, and its
	// scheduler job; mu guards them.
	lastCheck   *CheckResult
	checkLoaded bool
	checkJob    gocron.Job

	// The off-box copy (offsite.go); offMu guards it. rclone is nil outside
	// tests (execRclone).
	offMu      sync.Mutex
	offRunning bool
	offAgain   bool
	offPhase   string
	offLast    *OffsiteResult
	offLoaded  bool
	offTypes   map[string]string
	rclone     rcloneRunner
}

var _ Service = (*BackupService)(nil)

// NewService builds the service. It schedules nothing until Start.
func NewService(d Deps) *BackupService {
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.OpenRepo == nil {
		d.OpenRepo = func(dir string) (Repo, error) { return OpenRepo(dir) }
	}
	if d.Config == nil {
		d.Config = func() config.Backup { return config.Backup{} }
	}
	return &BackupService{
		deps:        d,
		log:         logger.New("backup"),
		lastErr:     map[string]string{},
		deferred:    map[string]bool{},
		linkPlans:   map[string]LinkPlan{},
		cleanPlans:  map[string]CleanupPlan{},
		jobs:        map[string]gocron.Job{},
		staleNotice: map[string]time.Time{},
		offTypes:    map[string]string{},
		startedAt:   d.Now(),
	}
}

func (s *BackupService) now() time.Time { return s.deps.Now() }

func (s *BackupService) cfg() config.Backup { return s.deps.Config() }

// repo opens (and keeps) the repository of the configured folder. One
// instance per folder matters: the writer lock is held per instance.
func (s *BackupService) repo() (Repo, error) {
	dir := strings.TrimSpace(s.cfg().Dir)
	if dir == "" {
		return nil, ErrNotConfigured
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.repoVal != nil && s.repoDir == dir {
		return s.repoVal, nil
	}
	r, err := s.deps.OpenRepo(dir)
	if err != nil {
		return nil, err
	}
	s.repoDir, s.repoVal = dir, r
	return r, nil
}

// claim takes the single run slot. A second claim gets ErrBusy.
func (s *BackupService) claim(label string) (release func(), err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.runningJob != "" {
		return nil, ErrBusy
	}
	s.runningJob = label
	s.runStarted = s.now()
	s.progress = nil
	return func() {
		s.mu.Lock()
		s.runningJob = ""
		s.progress = nil
		s.mu.Unlock()
		// A run waiting for the slot can start now.
		go s.kick()
	}, nil
}

func (s *BackupService) report(p ProgressUpdate) {
	now := s.now()
	p.UpdatedAt = &now
	s.mu.Lock()
	if !s.runStarted.IsZero() {
		started := s.runStarted
		p.RunStarted = &started
	}
	s.progress = &p
	s.mu.Unlock()
}

func (s *BackupService) notify(ev config.NotificationEvent, status, msg string, err error) {
	if s.deps.Notify != nil {
		s.deps.Notify(ev, status, msg, err)
	}
}

// ---- status ----

// Status builds the Backups page cards.
func (s *BackupService) Status(ctx context.Context) (StatusResponse, error) {
	cfg := s.cfg()
	st := StatusResponse{
		Configured:  strings.TrimSpace(cfg.Dir) != "",
		Dir:         strings.TrimSpace(cfg.Dir),
		PostRestore: s.HoldActive(),
		PendingSwap: PendingRestoreExists(s.deps.DataDir),
		Jobs:        []JobStatus{},
		Now:         s.now(),
		ServerTZ:    serverZone(s.now()),

		LinkRestoreApplied: LinkRestoreApplied(s.deps.DataDir),
	}
	s.mu.Lock()
	running := s.runningJob
	if s.progress != nil {
		p := *s.progress
		st.Running = &p
	}
	jobs := make(map[string]gocron.Job, len(s.jobs))
	for k, v := range s.jobs {
		jobs[k] = v
	}
	checkJob := s.checkJob
	lastErr := make(map[string]string, len(s.lastErr))
	for k, v := range s.lastErr {
		lastErr[k] = v
	}
	s.mu.Unlock()

	var snaps []SnapshotInfo
	if st.Configured {
		if r, err := s.repo(); err == nil {
			snaps, _ = r.ListSnapshots(ctx)
			st.Snapshots = len(snaps)
			for _, sn := range snaps {
				if sn.Result != ResultPartial {
					st.LastNewBytes = sn.NewBytes
					break
				}
			}
			st.RepoBytes = s.repoBytes(r)
		}
		st.FreeBytes, st.SameDisk = diskInfo(st.Dir, s.deps.DataDir)
	}
	if st.Configured {
		cs := &CheckStatus{Enabled: cfg.Check.Enabled, Schedule: cfg.Check.Schedule,
			Running: running == checkLabel, Queued: s.queuedReason(queuedRun{Kind: kindCheck}.key())}
		if r, err := s.repo(); err == nil {
			cs.Last = s.loadCheck(r)
		}
		if checkJob != nil && cfg.Check.Enabled {
			if t, err := checkJob.NextRun(); err == nil && !t.IsZero() {
				cs.NextRun = &t
			}
		}
		st.Check = cs
		var repo Repo
		if r, err := s.repo(); err == nil {
			repo = r
		}
		st.Offsite = s.offsiteStatus(cfg, repo)
	}
	if s.deps.Alerts != nil && (cfg.Notify.OnFailure || cfg.Notify.StaleHours > 0) {
		a := s.deps.Alerts()
		st.Alerts = &a
	}

	for _, j := range cfg.Jobs {
		js := JobStatus{Name: j.Name, Enabled: j.Enabled, Schedule: j.Schedule,
			Running: running == "job:"+j.Name, LastError: lastErr[j.Name], Queued: s.queuedFor(j.Name)}
		for _, c := range j.Components {
			js.Components = append(js.Components, Component(c))
		}
		for _, sn := range snaps {
			// A partial snapshot (a full run that did not finish its walks)
			// is not a success.
			if jobOfTrigger(sn.Trigger) == j.Name && sn.Result != ResultPartial {
				t := sn.CreatedAt
				js.LastSuccess = &t
				break
			}
		}
		if job, ok := jobs[j.Name]; ok && j.Enabled {
			if t, err := job.NextRun(); err == nil && !t.IsZero() {
				js.NextRun = &t
			}
		}
		st.Jobs = append(st.Jobs, js)
	}
	return st, nil
}

// serverZone names the zone job schedules are read in: the scheduler runs on
// the server's local time.
func serverZone(t time.Time) string {
	name, _ := t.In(time.Local).Zone()
	return name
}

// jobOfTrigger returns the job name of a "schedule:<job>" or "manual:<job>"
// trigger, or "" for any other.
func jobOfTrigger(trigger string) string {
	for _, p := range []string{"schedule:", "manual:"} {
		if strings.HasPrefix(trigger, p) {
			return strings.TrimPrefix(trigger, p)
		}
	}
	return ""
}

// repoBytes sums the repository folder, at most once a minute (a walk of
// thousands of blobs is not free on every status poll).
func (s *BackupService) repoBytes(r Repo) int64 {
	fr, ok := r.(*FileRepo)
	if !ok {
		return 0
	}
	s.mu.Lock()
	if s.now().Sub(s.sizeAt) < time.Minute && !s.sizeAt.IsZero() {
		b := s.sizeBytes
		s.mu.Unlock()
		return b
	}
	s.mu.Unlock()
	var total int64
	_ = filepath.WalkDir(fr.Dir(), func(_ string, d os.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() {
			if info, ierr := d.Info(); ierr == nil {
				total += info.Size()
			}
		}
		return nil
	})
	s.mu.Lock()
	s.sizeAt, s.sizeBytes = s.now(), total
	s.mu.Unlock()
	return total
}

// ---- snapshots ----

// ListSnapshots lists snapshots, newest first.
func (s *BackupService) ListSnapshots(ctx context.Context) ([]SnapshotInfo, error) {
	r, err := s.repo()
	if err != nil {
		return nil, err
	}
	return r.ListSnapshots(ctx)
}

// GetSnapshot reads one manifest.
func (s *BackupService) GetSnapshot(ctx context.Context, id string) (*Snapshot, error) {
	r, err := s.repo()
	if err != nil {
		return nil, err
	}
	return r.ReadSnapshot(ctx, id)
}

// keepAll is a policy that deletes no snapshot; pruning with it removes only
// blobs no snapshot references.
var keepAll = RetentionPolicy{Last: 1 << 30}

// DeleteSnapshot removes a snapshot and then the blobs only it used. It runs
// in the single run slot so it cannot overlap a backup.
func (s *BackupService) DeleteSnapshot(ctx context.Context, id string) error {
	r, err := s.repo()
	if err != nil {
		return err
	}
	release, err := s.claim("delete")
	if err != nil {
		return err
	}
	defer release()
	unlock, err := r.Lock(ctx)
	if err != nil {
		return err
	}
	derr := r.DeleteSnapshot(ctx, id)
	unlock()
	if derr != nil {
		return derr
	}
	return s.prune(ctx, r, keepAll)
}

// VerifySnapshot checks a snapshot's blobs; full re-hashes them.
func (s *BackupService) VerifySnapshot(ctx context.Context, id string, full bool) (VerifyReport, error) {
	r, err := s.repo()
	if err != nil {
		return VerifyReport{}, err
	}
	return r.Verify(ctx, VerifyOptions{SnapshotID: id, Full: full}, nil)
}

// RestoreDB stages the snapshot's database; the swap happens at the next
// start.
func (s *BackupService) RestoreDB(ctx context.Context, id string) (RestoreDBResponse, error) {
	r, err := s.repo()
	if err != nil {
		return RestoreDBResponse{}, err
	}
	if _, err := r.ReadSnapshot(ctx, id); err != nil {
		return RestoreDBResponse{}, err
	}
	return StageDBRestore(r, id, s.deps.DataDir)
}

// RetentionPreview shows what a policy (or one deletion) would remove.
func (s *BackupService) RetentionPreview(ctx context.Context, req RetentionPreviewRequest) (PrunePlan, error) {
	r, err := s.repo()
	if err != nil {
		return PrunePlan{}, err
	}
	if req.SnapshotID != "" {
		return s.previewDelete(ctx, r, req.SnapshotID)
	}
	policy := req.Policy
	if req.Job != "" {
		found := false
		for _, j := range s.cfg().Jobs {
			if j.Name == req.Job {
				policy, found = jobPolicy(j, "schedule:"+j.Name), true
			}
		}
		if !found {
			return PrunePlan{}, fmt.Errorf("%w: no job named %q", ErrNotFound, req.Job)
		}
	}
	return r.PrunePlan(ctx, policy)
}

// previewDelete lists the blobs only one snapshot uses.
func (s *BackupService) previewDelete(ctx context.Context, r Repo, id string) (PrunePlan, error) {
	infos, err := r.ListSnapshots(ctx)
	if err != nil {
		return PrunePlan{}, err
	}
	plan := PrunePlan{Policy: RetentionPolicy{}}
	var target *Snapshot
	others := map[string]struct{}{}
	for _, info := range infos {
		snap, err := r.ReadSnapshot(ctx, info.ID)
		if err != nil {
			return PrunePlan{}, err
		}
		if info.ID == id {
			target = snap
			plan.DeleteSnapshot = append(plan.DeleteSnapshot, info)
			continue
		}
		plan.KeepSnapshots++
		if err := blobRefs(ctx, r, snap, others); err != nil {
			return PrunePlan{}, err
		}
	}
	if target == nil {
		return PrunePlan{}, fmt.Errorf("%w: snapshot %s", ErrNotFound, id)
	}
	mine := map[string]struct{}{}
	if err := blobRefs(ctx, r, target, mine); err != nil {
		return PrunePlan{}, err
	}
	for sha := range mine {
		if _, shared := others[sha]; !shared {
			plan.DeleteBlobs = append(plan.DeleteBlobs, sha)
		}
	}
	sort.Strings(plan.DeleteBlobs)
	plan.BlobCount = len(plan.DeleteBlobs)
	return plan, nil
}

// blobRefs adds every blob sha snapshot s names to set.
func blobRefs(ctx context.Context, bs BlobStore, s *Snapshot, set map[string]struct{}) error {
	for _, e := range s.Components {
		for _, sha := range e.StoreBlobs {
			set[sha] = struct{}{}
		}
		for _, sha := range e.Files {
			set[sha] = struct{}{}
		}
		if e.Manifest != "" {
			set[e.Manifest] = struct{}{}
		}
		if e.Tree != "" {
			set[e.Tree] = struct{}{}
			t, err := ReadTree(ctx, bs, e.Tree)
			if err != nil {
				return err
			}
			for _, sha := range t.Files {
				set[sha] = struct{}{}
			}
		}
	}
	return nil
}

// ---- WP6 delegation ----

func (s *BackupService) ListRemoved(ctx context.Context, q RemovedQuery) ([]RemovedEntry, error) {
	if s.Undelete == nil {
		return nil, ErrNotImplemented
	}
	return s.Undelete.ListRemoved(ctx, q)
}

func (s *BackupService) RestoreRemoved(ctx context.Context, key string, req RestoreRemovedRequest) (RestoreRemovedResponse, error) {
	if s.Undelete == nil {
		return RestoreRemovedResponse{}, ErrNotImplemented
	}
	return s.Undelete.Restore(ctx, key, req)
}

func (s *BackupService) ListConfigHistory(ctx context.Context) ([]ConfigVersion, error) {
	if s.History == nil {
		return nil, ErrNotImplemented
	}
	return s.History.List(ctx)
}

func (s *BackupService) DiffConfigHistory(ctx context.Context, id, against string) (ConfigDiff, error) {
	if s.History == nil {
		return ConfigDiff{}, ErrNotImplemented
	}
	return s.History.Diff(ctx, id, against)
}

func (s *BackupService) RevertConfig(ctx context.Context, id string) (RevertResponse, error) {
	if s.History == nil {
		return RevertResponse{}, ErrNotImplemented
	}
	return s.History.Revert(ctx, id)
}

// ---- links ----

// buildManifest walks the library roots.
func (s *BackupService) buildManifest(ctx context.Context, p Progress) (*Manifest, error) {
	d := s.deps
	if d.Lookup == nil {
		return nil, fmt.Errorf("%w: link checks are not wired", ErrNotImplemented)
	}
	fsys := d.FS
	if fsys == nil {
		fsys = OSLinkFS{}
	}
	var arrs []ArrSource
	if d.Arrs != nil {
		arrs = d.Arrs()
	}
	extra := s.cfg().ExtraLinkFolders
	if d.ExtraRoots != nil {
		extra = d.ExtraRoots()
	}
	mount := ""
	if d.MountRoot != nil {
		mount = d.MountRoot()
	}
	return BuildManifest(ctx, ManifestInput{
		FS: fsys, Lookup: d.Lookup, MountRoot: mount,
		Roots: DiscoverRoots(ctx, arrs, extra), Arrs: arrs,
	}, p)
}

// recordCheck keeps a manifest as the latest check and notifies on a fall in
// live links or links the Arrs list but the disk lacks.
func (s *BackupService) recordCheck(m *Manifest) LinksStatusResponse {
	res := Summarise(m, s.now().UTC())
	st := res.Status()
	s.linkMu.Lock()
	prevLive, hadPrev := s.lastLive, s.lastManifest != nil
	prevMissing := s.lastLinks.Missing
	s.lastManifest = m
	s.lastLive = res.Live
	s.trend = append(s.trend, res.Trend())
	if len(s.trend) > 500 {
		s.trend = s.trend[len(s.trend)-500:]
	}
	st.Trend = append([]TrendPoint(nil), s.trend...)
	s.lastLinks = st
	s.linkMu.Unlock()

	pct := s.cfg().LinkCheck.DroppedPercent
	if hadPrev && pct > 0 && prevLive > 0 && res.Live < prevLive && (prevLive-res.Live)*100 > prevLive*pct {
		s.notify(config.EventLinksDropped, "warning",
			fmt.Sprintf("Live library links fell from %d to %d since the last check", prevLive, res.Live), nil)
	}
	if res.Missing > 0 && res.Missing != prevMissing {
		s.notify(config.EventBackupLinksMissing, "warning",
			fmt.Sprintf("%d library files have no symlink on disk", res.Missing), nil)
	}
	return st
}

// LinksStatus returns the last check's summary.
func (s *BackupService) LinksStatus(context.Context) (LinksStatusResponse, error) {
	s.linkMu.Lock()
	defer s.linkMu.Unlock()
	st := s.lastLinks
	st.Running = s.checking
	st.Trend = append([]TrendPoint(nil), s.trend...)
	st.Recreated = s.recreatedList()
	return st, nil
}

// LinksCheck runs a check now.
func (s *BackupService) LinksCheck(ctx context.Context) (LinksStatusResponse, error) {
	s.linkMu.Lock()
	if s.checking {
		s.linkMu.Unlock()
		return LinksStatusResponse{}, ErrBusy
	}
	s.checking = true
	s.linkMu.Unlock()
	defer func() {
		s.linkMu.Lock()
		s.checking = false
		s.linkMu.Unlock()
	}()
	m, err := s.buildManifest(ctx, nil)
	if err != nil {
		return LinksStatusResponse{}, err
	}
	st := s.recordCheck(m)
	s.autoRecreate(ctx, m)
	s.linkMu.Lock()
	st.Recreated = s.recreatedList()
	s.lastLinks.Recreated = st.Recreated
	s.linkMu.Unlock()
	return st, nil
}

func (s *BackupService) currentManifest(ctx context.Context) (*Manifest, error) {
	s.linkMu.Lock()
	m := s.lastManifest
	s.linkMu.Unlock()
	if m != nil {
		return m, nil
	}
	if _, err := s.LinksCheck(ctx); err != nil {
		return nil, err
	}
	s.linkMu.Lock()
	defer s.linkMu.Unlock()
	return s.lastManifest, nil
}

// ListLinks pages the last check's rows.
func (s *BackupService) ListLinks(ctx context.Context, q LinksQuery) (LinksPage, error) {
	m, err := s.currentManifest(ctx)
	if err != nil {
		return LinksPage{}, err
	}
	size := q.PageSize
	if size <= 0 {
		size = 100
	}
	page := max(q.Page, 1)
	needle := strings.ToLower(q.Q)
	var rows []LinkRow
	for _, r := range m.Rows {
		if q.State != "" && r.State != q.State {
			continue
		}
		if q.Root != "" && r.Root != q.Root {
			continue
		}
		if needle != "" && !matchesQuery(r, needle) {
			continue
		}
		rows = append(rows, r)
	}
	total := len(rows)
	lo := min((page-1)*size, total)
	hi := min(lo+size, total)
	return LinksPage{Rows: append([]LinkRow{}, rows[lo:hi]...), Total: total, Page: page, PageSize: size}, nil
}

// LinksMissing lists files the Arrs know that have no symlink. Each row
// carries the newest snapshot whose link manifest has a link at that path,
// when there is one.
func (s *BackupService) LinksMissing(ctx context.Context) ([]MissingLink, error) {
	m, err := s.currentManifest(ctx)
	if err != nil {
		return nil, err
	}
	out := append([]MissingLink{}, m.Missing...)
	if len(out) == 0 {
		return out, nil
	}
	r, err := s.repo()
	if err != nil {
		return out, nil // no destination: nothing is restorable
	}
	infos, err := r.ListSnapshots(ctx)
	if err != nil {
		return out, nil
	}
	left := len(out)
	for _, info := range infos { // newest first
		if left == 0 {
			break
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		sm, err := s.loadSnapshotManifest(ctx, r, info.ID)
		if err != nil {
			continue
		}
		have := make(map[string]struct{}, len(sm.Rows))
		for _, row := range sm.Rows {
			have[row.Path] = struct{}{}
		}
		for i := range out {
			if out[i].SnapshotID != "" {
				continue
			}
			if _, ok := have[out[i].Path]; ok {
				out[i].SnapshotID, out[i].Restorable = info.ID, true
				left--
			}
		}
	}
	return out, nil
}

// loadSnapshotManifest reads the link manifest of a snapshot.
func (s *BackupService) loadSnapshotManifest(ctx context.Context, r Repo, id string) (*Manifest, error) {
	snap, err := r.ReadSnapshot(ctx, id)
	if err != nil {
		return nil, err
	}
	e, ok := snap.Components[ComponentLinks]
	if !ok || e.Manifest == "" {
		return nil, fmt.Errorf("%w: snapshot %s has no link manifest", ErrInvalid, id)
	}
	b, err := readBlob(ctx, r, e.Manifest)
	if err != nil {
		return nil, err
	}
	return DecodeManifest(bytes.NewReader(b))
}

// PlanLinksRestore is the dry run of a link restore.
func (s *BackupService) PlanLinksRestore(ctx context.Context, id string, req LinksPlanRequest) (LinkPlan, error) {
	r, err := s.repo()
	if err != nil {
		return LinkPlan{}, err
	}
	m, err := s.loadSnapshotManifest(ctx, r, id)
	if err != nil {
		return LinkPlan{}, err
	}
	fsys := s.deps.FS
	if fsys == nil {
		fsys = OSLinkFS{}
	}
	mount := ""
	if s.deps.MountRoot != nil {
		mount = s.deps.MountRoot()
	}
	plan := BuildLinkPlan(LinkPlanInput{FS: fsys, Lookup: s.deps.Lookup, MountRoot: mount,
		Rows: m.Rows, Scope: req.Scope, SnapshotID: id, Now: s.now().UTC()})
	s.mu.Lock()
	s.linkPlans[plan.PlanID] = plan
	s.linkPlanIDs = append(s.linkPlanIDs, plan.PlanID)
	for len(s.linkPlanIDs) > maxKeptPlans {
		delete(s.linkPlans, s.linkPlanIDs[0])
		s.linkPlanIDs = s.linkPlanIDs[1:]
	}
	s.mu.Unlock()
	return plan, nil
}

// ApplyLinksRestore carries out a plan this process built.
func (s *BackupService) ApplyLinksRestore(ctx context.Context, _ string, req LinksApplyRequest) (LinksApplyResponse, error) {
	s.mu.Lock()
	plan, ok := s.linkPlans[req.PlanID]
	s.mu.Unlock()
	if !ok {
		return LinksApplyResponse{}, fmt.Errorf("%w: plan %s is unknown; build a new plan", ErrNotFound, req.PlanID)
	}
	fsys := s.deps.FS
	if fsys == nil {
		fsys = OSLinkFS{}
	}
	res := ApplyLinkPlan(ctx, fsys, plan, LinkApplyOptions{Retarget: req.Retarget, Dangling: req.Dangling,
		Rescan: req.Rescan, Rescanner: s.deps.Rescanner})
	for _, w := range res.Warnings {
		s.log.Warn().Str("plan", req.PlanID).Msg(w)
	}
	if res.Created+res.Retarget > 0 {
		if err := markLinkRestoreApplied(s.deps.DataDir, s.now().UTC()); err != nil {
			s.log.Warn().Err(err).Msg("Could not record that a link restore was applied")
		}
	}
	return res, nil
}

// PlanLinksCleanup is the dry run of a dead-link cleanup.
func (s *BackupService) PlanLinksCleanup(ctx context.Context, req LinksCleanupPlanRequest) (CleanupPlan, error) {
	m, err := s.currentManifest(ctx)
	if err != nil {
		return CleanupPlan{}, err
	}
	plan := BuildCleanupPlan(m.Rows, req.Filter, s.now().UTC())
	s.mu.Lock()
	s.cleanPlans[plan.PlanID] = plan
	s.cleanPlanIDs = append(s.cleanPlanIDs, plan.PlanID)
	for len(s.cleanPlanIDs) > maxKeptPlans {
		delete(s.cleanPlans, s.cleanPlanIDs[0])
		s.cleanPlanIDs = s.cleanPlanIDs[1:]
	}
	s.mu.Unlock()
	return plan, nil
}

// ApplyLinksCleanup removes the ticked groups after a pre-action snapshot.
func (s *BackupService) ApplyLinksCleanup(ctx context.Context, req LinksCleanupApplyRequest) (CleanupResult, error) {
	s.mu.Lock()
	plan, ok := s.cleanPlans[req.PlanID]
	s.mu.Unlock()
	if !ok {
		return CleanupResult{}, fmt.Errorf("%w: plan %s is unknown; build a new plan", ErrNotFound, req.PlanID)
	}
	var snapID string
	if len(req.Groups) > 0 {
		id, err := s.PreAction(ctx, ActionLinksCleanup)
		if err != nil && errors.Is(err, ErrPreActionRefused) {
			return CleanupResult{}, err
		}
		if err != nil {
			s.log.Warn().Err(err).Msg("Links cleanup carries on without a safety snapshot")
		}
		snapID = id
	}
	fsys := s.deps.FS
	if fsys == nil {
		fsys = OSLinkFS{}
	}
	res := ApplyCleanup(ctx, fsys, plan, req.Groups)
	res.SnapshotID = snapID
	s.dropRemovedLinks(res.RemovedPaths)
	return res, nil
}

// dropRemovedLinks takes links that a cleanup deleted out of the stored
// check, so the page and the next plan match the disk without a new walk.
func (s *BackupService) dropRemovedLinks(paths []string) {
	if len(paths) == 0 {
		return
	}
	gone := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		gone[p] = struct{}{}
	}
	s.linkMu.Lock()
	defer s.linkMu.Unlock()
	old := s.lastManifest
	if old == nil {
		return
	}
	// A fresh manifest: readers hold the old one without the lock.
	m := *old
	m.Rows = make([]LinkRow, 0, len(old.Rows))
	for _, r := range old.Rows {
		if _, ok := gone[r.Path]; !ok {
			m.Rows = append(m.Rows, r)
		}
	}
	at := s.now().UTC()
	if s.lastLinks.LastCheck != nil {
		at = *s.lastLinks.LastCheck
	}
	res := Summarise(&m, at)
	s.lastManifest = &m
	s.lastLive = res.Live
	s.lastLinks.Roots = res.Roots
	if n := len(s.trend); n > 0 {
		s.trend[n-1].Live, s.trend[n-1].Dead = res.Live, res.Dead
	}
}

// Repo returns the repository of the configured folder (ErrNotConfigured
// when none is set). Other services built on the same destination use it
// through LiveRepo.
func (s *BackupService) Repo() (Repo, error) { return s.repo() }
