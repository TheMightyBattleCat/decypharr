package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/sirrobot01/decypharr/internal/config"
)

// runOptions describes one snapshot run.
type runOptions struct {
	trigger    string
	components []Component
	// walks decides whether the large walks honour the throttle and the
	// pause-while-streaming setting. Pre-action snapshots have no walks.
	walks bool
}

// countingStore counts what a run writes. With fast set every blob uses
// CompressFast, for snapshots the user is waiting on.
type countingStore struct {
	BlobStore
	fast  bool
	blobs atomic.Int64
	bytes atomic.Int64
}

func (c *countingStore) PutBlob(ctx context.Context, r io.Reader, level CompressionLevel) (BlobRef, bool, error) {
	if c.fast {
		level = CompressFast
	}
	ref, created, err := c.BlobStore.PutBlob(ctx, r, level)
	if err == nil && created {
		c.blobs.Add(1)
		stored := ref.Stored
		if stored == 0 {
			stored = ref.Size
		}
		c.bytes.Add(stored)
	}
	return ref, created, err
}

type fileCacheRepo interface {
	LoadFileCache() *FileCache
	SaveFileCache(*FileCache) error
}

// orderComponents puts wanted components in the stable AllComponents order,
// with the two walks (NZB records and the overlay) last, so a full run can
// save everything else before it starts on them.
func orderComponents(want []Component) []Component {
	set := map[Component]bool{}
	for _, c := range want {
		set[c] = true
	}
	var out, walks []Component
	for _, c := range AllComponents() {
		switch {
		case !set[c]:
		case isWalk(c):
			walks = append(walks, c)
		default:
			out = append(out, c)
		}
	}
	return append(out, walks...)
}

// isWalk reports whether c is stored by walking a folder of files.
func isWalk(c Component) bool { return c == ComponentNZB || c == ComponentOverlay }

// snapshot takes one snapshot. The caller holds the run slot. The repository
// lock is held from before the first blob is written until the manifest is
// stored, so a prune cannot delete a blob the run is about to reference.
func (s *BackupService) snapshot(ctx context.Context, repo Repo, opts runOptions) (*Snapshot, error) {
	unlock, err := repo.Lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()

	cfg := s.cfg()
	started := s.now()
	// Pre-action snapshots (no walks) run while the user waits on the
	// action, so they trade some size for speed.
	store := &countingStore{BlobStore: repo, fast: !opts.walks}
	snap := &Snapshot{
		ID:         newSnapshotID(started),
		CreatedAt:  started.UTC(),
		Trigger:    opts.trigger,
		Decypharr:  s.deps.Build,
		Components: map[Component]ComponentEntry{},
		Result:     ResultOK,
	}
	cache := NewFileCache()
	fcr, hasCache := repo.(fileCacheRepo)
	if hasCache {
		cache = fcr.LoadFileCache()
		defer func() {
			if err := fcr.SaveFileCache(cache); err != nil {
				s.log.Warn().Err(err).Msg("Backup file cache was not saved; the next run reads every file again")
			}
		}()
	}

	var rate int64
	var pause func() bool
	if opts.walks {
		rate = int64(cfg.ThrottleMBps) << 20
		if s.deps.Streaming != nil {
			// The setting is read on every poll, so turning it off in
			// Settings lets a paused backup carry on straight away.
			pause = func() bool { return s.cfg().PauseWhileStreaming && s.deps.Streaming() }
		}
	}
	progress := Progress(func(u ProgressUpdate) { s.report(u) })
	w := walkSettings{cache: cache, rate: rate, pause: pause, progress: progress}

	stats := func() SnapshotStats {
		return SnapshotStats{
			NewBlobs:   int(store.blobs.Load()),
			NewBytes:   store.bytes.Load(),
			DurationMS: s.now().Sub(started).Milliseconds(),
		}
	}
	partial := false
	for _, c := range orderComponents(opts.components) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if isWalk(c) && opts.walks && !partial && len(snap.Components) > 0 {
			// Save what is done before the walks, which can take hours on a
			// first run: if they are stopped or interrupted, the database,
			// settings and links are still backed up. The finished snapshot
			// replaces this one under the same id.
			snap.Result, snap.Stats = ResultPartial, stats()
			if err := repo.WriteSnapshot(ctx, snap); err != nil {
				return nil, err
			}
			partial = true
			s.log.Info().Str("snapshot", snap.ID).Msg("Backup saved everything but the NZB records and overlay; storing those now")
		}
		s.report(ProgressUpdate{Component: c, Phase: "start"})
		entry, err := s.capture(ctx, store, c, w)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", c, err)
		}
		snap.Components[c] = entry
	}
	snap.Result, snap.Stats = ResultOK, stats()
	if err := repo.WriteSnapshot(ctx, snap); err != nil {
		return nil, err
	}
	return snap, nil
}

// componentFiles names the files each file-based component captures, relative
// to the data folder. The archive restore accepts only these.
var componentFiles = map[Component][]string{
	ComponentConfig:  {"config.json", "auth.json"},
	ComponentState:   {"arr_library_paths.json", "plex_reap_jobs.json", "usenet_bandwidth.json"},
	ComponentHearsay: {"hearsay/identity.key"},
}

type walkSettings struct {
	cache    *FileCache
	rate     int64
	pause    func() bool
	progress Progress
}

// capture stores one component.
func (s *BackupService) capture(ctx context.Context, bs BlobStore, c Component, w walkSettings) (ComponentEntry, error) {
	dir := s.deps.DataDir
	switch c {
	case ComponentDB:
		if s.deps.DBExport == nil {
			return ComponentEntry{}, fmt.Errorf("%w: no database exporter", ErrNotImplemented)
		}
		return s.deps.DBExport(ctx, bs, w.progress)
	case ComponentConfig:
		files, err := putFiles(ctx, bs, dir, componentFiles[ComponentConfig]...)
		return ComponentEntry{Files: files}, err
	case ComponentState:
		files, err := putFiles(ctx, bs, dir, componentFiles[ComponentState]...)
		return ComponentEntry{Files: files}, err
	case ComponentHearsay:
		files, err := putFiles(ctx, bs, dir, componentFiles[ComponentHearsay]...)
		return ComponentEntry{Files: files}, err
	case ComponentNZB:
		res, err := WalkTree(ctx, bs, s.walkOpts(filepath.Join(dir, "usenet", "meta"), c, w, func(rel string) bool {
			return strings.HasSuffix(rel, ".meta")
		}))
		if err != nil {
			return ComponentEntry{}, err
		}
		return ComponentEntry{Tree: res.Tree.SHA256, FileCount: res.FileCount, Bytes: res.Bytes}, nil
	case ComponentOverlay:
		return s.captureOverlay(ctx, bs, w)
	case ComponentLinks:
		m, err := s.buildManifest(ctx, w.progress)
		if err != nil {
			return ComponentEntry{}, err
		}
		var buf bytes.Buffer
		if err := EncodeManifest(&buf, m); err != nil {
			return ComponentEntry{}, err
		}
		ref, _, err := bs.PutBlob(ctx, &buf, CompressBest)
		if err != nil {
			return ComponentEntry{}, err
		}
		s.recordCheck(m)
		return ComponentEntry{Manifest: ref.SHA256, Rows: len(m.Rows), Roots: m.Roots}, nil
	}
	return ComponentEntry{}, fmt.Errorf("%w: unknown component %q", ErrInvalid, c)
}

func (s *BackupService) walkOpts(root string, c Component, w walkSettings, match func(string) bool) WalkOptions {
	return WalkOptions{Root: root, Component: c, Cache: w.cache, Level: CompressDefault,
		BytesPerSec: w.rate, Pause: w.pause, PauseReason: "waiting for playback to end", Match: match, Progress: w.progress}
}

// captureOverlay walks the overlay and the volume-order backups into one
// tree, each under its own folder name.
func (s *BackupService) captureOverlay(ctx context.Context, bs BlobStore, w walkSettings) (ComponentEntry, error) {
	merged := Tree{Files: map[string]string{}}
	var entry ComponentEntry
	for _, sub := range []string{"overlay", "volume-order-backup"} {
		res, err := WalkTree(ctx, bs, s.walkOpts(filepath.Join(s.deps.DataDir, "usenet", sub), ComponentOverlay, w, nil))
		if err != nil {
			return ComponentEntry{}, err
		}
		t, err := ReadTree(ctx, bs, res.Tree.SHA256)
		if err != nil {
			return ComponentEntry{}, err
		}
		for rel, sha := range t.Files {
			merged.Files[sub+"/"+rel] = sha
		}
		entry.FileCount += res.FileCount
		entry.Bytes += res.Bytes
	}
	b, err := json.Marshal(merged)
	if err != nil {
		return ComponentEntry{}, err
	}
	ref, _, err := bs.PutBlob(ctx, bytes.NewReader(b), CompressBest)
	if err != nil {
		return ComponentEntry{}, err
	}
	entry.Tree = ref.SHA256
	return entry, nil
}

// putFiles stores the named files under dir; one that does not exist is
// skipped. Names are slash-separated and become the map keys.
func putFiles(ctx context.Context, bs BlobStore, dir string, names ...string) (map[string]string, error) {
	out := map[string]string{}
	for _, n := range names {
		b, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(n)))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		ref, _, err := bs.PutBlob(ctx, bytes.NewReader(b), CompressDefault)
		if err != nil {
			return nil, err
		}
		out[n] = ref.SHA256
	}
	return out, nil
}

// jobPolicy converts a job's retention. prefix limits it to snapshots of one
// trigger kind.
func jobPolicy(j config.BackupJob, prefix string) RetentionPolicy {
	return RetentionPolicy{Last: j.Retention.Last, Daily: j.Retention.Daily,
		Weekly: j.Retention.Weekly, Monthly: j.Retention.Monthly, TriggerPrefix: prefix}
}

// prune deletes what the policy drops. It takes the repository lock itself
// (through Repo.Prune), so callers must have released theirs.
func (s *BackupService) prune(ctx context.Context, r Repo, policy RetentionPolicy) error {
	plan, err := r.PrunePlan(ctx, policy)
	if err != nil {
		return err
	}
	if len(plan.DeleteSnapshot) == 0 && len(plan.DeleteBlobs) == 0 {
		return nil
	}
	res, err := r.Prune(ctx, plan)
	if err != nil {
		return err
	}
	s.log.Info().Int("snapshots", res.DeletedSnapshots).Int("blobs", res.DeletedBlobs).
		Int64("freed_bytes", res.FreedBytes).Msg("Backup retention applied")
	return nil
}

func (s *BackupService) jobByName(name string) (config.BackupJob, bool) {
	for _, j := range s.cfg().Jobs {
		if j.Name == name {
			return j, true
		}
	}
	return config.BackupJob{}, false
}

func jobComponents(j config.BackupJob) []Component {
	var out []Component
	for _, c := range j.Components {
		if comp := Component(c); comp.Valid() {
			out = append(out, comp)
		}
	}
	return out
}

// RunJob starts a job now in the background. Another run in progress is
// ErrBusy; progress is on Status.
func (s *BackupService) RunJob(_ context.Context, name string) (RunJobResponse, error) {
	job, ok := s.jobByName(name)
	if !ok {
		return RunJobResponse{}, fmt.Errorf("%w: no job named %q", ErrNotFound, name)
	}
	if _, err := s.repo(); err != nil {
		return RunJobResponse{}, err
	}
	release, err := s.claim("job:" + name)
	if err != nil {
		return RunJobResponse{}, err
	}
	go func() {
		defer release()
		s.runJob(s.runContext(), job, "manual:"+name, 0)
	}()
	return RunJobResponse{Job: name, Started: true}, nil
}

// runContext is the context background runs use; it ends at Stop.
func (s *BackupService) runContext() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

// runJob runs a job in a claimed slot: snapshot, then prune, then notify on
// failure. attempts is how many times this run was started before (an
// interrupted run being resumed). The run is recorded in the queue file while
// it lasts, so a restart, crash or power cut in the middle resumes it at the
// next start; a run a shutdown cancels keeps that record on purpose.
func (s *BackupService) runJob(ctx context.Context, job config.BackupJob, trigger string, attempts int) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.mu.Lock()
	s.runCancel, s.stopRequested = cancel, false
	s.mu.Unlock()
	s.markRunning(queuedRun{Job: job.Name, Trigger: trigger, Since: s.now(), Attempts: attempts + 1})

	err := func() error {
		repo, err := s.repo()
		if err != nil {
			return err
		}
		snap, err := s.snapshot(ctx, repo, runOptions{trigger: trigger, components: jobComponents(job), walks: true})
		if err != nil {
			return err
		}
		s.log.Info().Str("job", job.Name).Str("snapshot", snap.ID).Int("new_blobs", snap.Stats.NewBlobs).
			Int64("new_bytes", snap.Stats.NewBytes).Msg("Backup finished")
		kind := trigger[:strings.Index(trigger, ":")+1]
		if perr := s.prune(ctx, repo, jobPolicy(job, kind+job.Name)); perr != nil {
			s.log.Warn().Err(perr).Str("job", job.Name).Msg("Backup retention could not run")
		}
		return nil
	}()
	s.mu.Lock()
	cancelled := errors.Is(err, context.Canceled)
	stopped := s.stopRequested && cancelled
	interrupted := !stopped && cancelled && s.ctx != nil && s.ctx.Err() != nil
	s.runCancel, s.stopRequested = nil, false
	switch {
	case stopped:
		s.lastErr[job.Name] = "Stopped from the Backups page"
	case interrupted:
		s.lastErr[job.Name] = "Interrupted by a shutdown; it resumes after the next start"
	case err != nil:
		s.lastErr[job.Name] = err.Error()
	default:
		delete(s.lastErr, job.Name)
		delete(s.staleNotice, job.Name)
	}
	s.mu.Unlock()
	if interrupted {
		// The run record stays in the queue file; recoverQueue resumes it.
		s.log.Warn().Str("job", job.Name).Msg("Backup interrupted by a shutdown; it resumes after the next start")
		return
	}
	s.clearRunning()
	if err == nil && strings.TrimSpace(s.cfg().RcloneRemote) != "" {
		s.upload() // outside the run slot
	}
	if stopped {
		s.log.Info().Str("job", job.Name).Msg("Backup stopped by the user")
		return
	}
	if err != nil {
		s.log.Error().Err(err).Str("job", job.Name).Msg("Backup failed")
		if s.cfg().Notify.OnFailure {
			s.notify(config.EventBackupFailed, "error", fmt.Sprintf("Backup job %q failed", job.Name), err)
		}
	}
}

// runScheduled is a scheduled firing. When another backup holds the run slot
// the run is queued and starts as soon as the slot is free.
func (s *BackupService) runScheduled(ctx context.Context, name string) {
	job, ok := s.jobByName(name)
	if !ok || !job.Enabled {
		return
	}
	if _, err := s.repo(); err != nil {
		return // not set up
	}
	release, err := s.claim("job:" + name)
	if err != nil {
		s.log.Info().Str("job", name).Msg("Scheduled backup waits for the running backup to finish")
		s.enqueue(queuedRun{Job: name, Trigger: "schedule:" + name, Reason: queueSkipped})
		return
	}
	defer release()
	s.runJob(ctx, job, "schedule:"+name, 0)
}
