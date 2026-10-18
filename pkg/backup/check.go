package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// The check reads every blob the snapshots refer to back, decompresses and
// re-hashes it, then restores the newest database backup into a scratch
// folder in the repository and opens it: proof the backups can be restored,
// not only that they are on disk. It runs on its own schedule (weekly by
// default) in the run slot, with the backup I/O limit and the
// pause-while-streaming setting.

const (
	checkTag   = "backup-check"
	checkLabel = "check"
	// checkFile in the repository's index folder holds the last result.
	checkFile = "check.json"
	// drillPrefix names the restore test's scratch folders in tmp/.
	drillPrefix = "drill-"
	// maxCheckProblems is how many missing or damaged blobs a result lists.
	maxCheckProblems = 10
)

// CheckResult is the outcome of the last check.
type CheckResult struct {
	At           time.Time          `json:"at"`
	DurationMS   int64              `json:"duration_ms"`
	Trigger      string             `json:"trigger"`
	OK           bool               `json:"ok"`
	Snapshots    int                `json:"snapshots"`
	BlobsChecked int                `json:"blobs_checked"`
	Missing      int                `json:"missing,omitempty"`
	Corrupt      int                `json:"corrupt,omitempty"`
	Problems     []string           `json:"problems,omitempty"`
	RestoreTest  *RestoreTestResult `json:"restore_test,omitempty"`
	Error        string             `json:"error,omitempty"`
}

// RestoreTestResult is the restore test of one snapshot's database.
type RestoreTestResult struct {
	Snapshot string `json:"snapshot,omitempty"`
	Records  int    `json:"records"`
	OK       bool   `json:"ok"`
	Error    string `json:"error,omitempty"`
}

// CheckRunner is implemented by the real Service; the API starts a check
// through it (the Service contract has no such method).
type CheckRunner interface {
	RunCheck() error
}

var _ CheckRunner = (*BackupService)(nil)

// RunCheck starts a check now in the background. Another run in progress is
// ErrBusy.
func (s *BackupService) RunCheck() error {
	if _, err := s.repo(); err != nil {
		return err
	}
	release, err := s.claim(checkLabel)
	if err != nil {
		return err
	}
	go func() {
		defer release()
		s.runCheck(s.runContext(), "manual:check", 0)
	}()
	return nil
}

// fireCheck is a scheduled firing of the check: queued when the slot is busy
// or inside quiet hours.
func (s *BackupService) fireCheck() {
	if _, err := s.repo(); err != nil {
		return // not set up
	}
	if _, in := quietUntil(s.now(), s.cfg().QuietHours); in {
		s.enqueue(queuedRun{Kind: kindCheck, Trigger: "schedule:check", Reason: queueSkipped})
		s.kick()
		return
	}
	release, err := s.claim(checkLabel)
	if err != nil {
		s.log.Info().Msg("Backup check waits for the running backup to finish")
		s.enqueue(queuedRun{Kind: kindCheck, Trigger: "schedule:check", Reason: queueSkipped})
		return
	}
	defer release()
	s.runCheck(s.runContext(), "schedule:check", 0)
}

// runCheck runs the check in a claimed slot, records the result and notifies
// when it found a problem. Like a job it is recorded in the queue file while
// it runs, so a restart resumes it.
func (s *BackupService) runCheck(ctx context.Context, trigger string, attempts int) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.mu.Lock()
	s.runCancel, s.stopRequested = cancel, false
	s.mu.Unlock()
	s.markRunning(queuedRun{Kind: kindCheck, Trigger: trigger, Since: s.now(), Attempts: attempts + 1})

	res := s.check(ctx, trigger)

	s.mu.Lock()
	cancelled := ctx.Err() != nil
	stopped := cancelled && s.stopRequested
	interrupted := cancelled && !stopped && s.ctx != nil && s.ctx.Err() != nil
	s.runCancel, s.stopRequested = nil, false
	s.mu.Unlock()
	if interrupted {
		s.log.Warn().Msg("Backup check interrupted by a shutdown; it resumes after the next start")
		return
	}
	s.clearRunning()
	if stopped {
		s.log.Info().Msg("Backup check stopped by the user")
		return
	}
	s.saveCheck(res)
	if res.OK {
		s.log.Info().Int("blobs", res.BlobsChecked).Int64("ms", res.DurationMS).Msg("Backup check passed")
		return
	}
	s.log.Error().Str("error", res.Error).Int("missing", res.Missing).Int("corrupt", res.Corrupt).Msg("Backup check found a problem")
	if s.cfg().Notify.OnFailure {
		s.notify(config.EventBackupFailed, "error", "The backup check found a problem: "+checkSummary(res), nil)
	}
}

// check does the work and never fails: what went wrong is in the result.
func (s *BackupService) check(ctx context.Context, trigger string) CheckResult {
	started := s.now()
	res := CheckResult{At: started.UTC(), Trigger: trigger}
	defer func() { res.DurationMS = s.now().Sub(started).Milliseconds() }()
	repo, err := s.repo()
	if err != nil {
		res.Error = err.Error()
		return res
	}
	cfg := s.cfg()
	var pause func() bool
	if s.deps.Streaming != nil {
		pause = func() bool { return s.cfg().PauseWhileStreaming && s.deps.Streaming() }
	}
	rep, err := repo.Verify(ctx, VerifyOptions{Full: true, BytesPerSec: int64(cfg.ThrottleMBps) << 20, Pause: pause},
		Progress(func(u ProgressUpdate) { s.report(u) }))
	if err != nil {
		res.Error = err.Error()
		return res
	}
	res.Snapshots, res.BlobsChecked = rep.Snapshots, rep.BlobsChecked
	res.Missing, res.Corrupt = len(rep.Missing), len(rep.Corrupt)
	for _, p := range append(prefixed("missing ", rep.Missing), prefixed("damaged ", rep.Corrupt)...) {
		if len(res.Problems) == maxCheckProblems {
			break
		}
		res.Problems = append(res.Problems, p)
	}
	if cfg.Check.RestoreTest {
		s.report(ProgressUpdate{Phase: "restore test"})
		rt := s.restoreTest(ctx, repo)
		res.RestoreTest = &rt
	}
	res.OK = rep.OK && (res.RestoreTest == nil || res.RestoreTest.OK)
	return res
}

// restoreTest restores the newest snapshot's database into a scratch folder
// and opens it, the same checks a real restore makes before staging.
func (s *BackupService) restoreTest(ctx context.Context, repo Repo) RestoreTestResult {
	infos, err := repo.ListSnapshots(ctx)
	if err != nil {
		return RestoreTestResult{Error: err.Error()}
	}
	var snap *Snapshot
	for _, info := range infos {
		if !hasComponent(info.Components, ComponentDB) {
			continue
		}
		if snap, err = repo.ReadSnapshot(ctx, info.ID); err != nil {
			return RestoreTestResult{Snapshot: info.ID, Error: err.Error()}
		}
		break
	}
	if snap == nil {
		return RestoreTestResult{Error: "no snapshot holds a database"}
	}
	entry := snap.Components[ComponentDB]
	scratch, err := os.MkdirTemp(s.scratchDir(repo), drillPrefix)
	if err != nil {
		return RestoreTestResult{Snapshot: snap.ID, Error: err.Error()}
	}
	defer os.RemoveAll(scratch)
	_, counts, err := importAndOpen(scratch, entry.Records, func(store string) (io.ReadCloser, error) {
		sha, ok := entry.StoreBlobs[store]
		if !ok {
			return nil, nil
		}
		return repo.OpenBlob(ctx, sha)
	})
	if err != nil {
		return RestoreTestResult{Snapshot: snap.ID, Error: err.Error()}
	}
	total := 0
	for _, n := range counts {
		total += n
	}
	return RestoreTestResult{Snapshot: snap.ID, Records: total, OK: true}
}

// scratchDir is the repository's tmp folder, so the restore test does not
// write to the disk the live database is on; the system temp folder for a
// repository that is not a folder.
func (s *BackupService) scratchDir(repo Repo) string {
	if fr, ok := repo.(*FileRepo); ok {
		return filepath.Join(fr.Dir(), "tmp")
	}
	return os.TempDir()
}

// removeDrills deletes restore-test folders a crash left in tmp/.
func (s *BackupService) removeDrills() {
	repo, err := s.repo()
	if err != nil {
		return
	}
	fr, ok := repo.(*FileRepo)
	if !ok {
		return
	}
	dir := filepath.Join(fr.Dir(), "tmp")
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), drillPrefix) {
			_ = os.RemoveAll(filepath.Join(dir, e.Name()))
		}
	}
}

// saveCheck keeps the result in memory and in the repository.
func (s *BackupService) saveCheck(res CheckResult) {
	s.mu.Lock()
	s.lastCheck = &res
	s.checkLoaded = true
	s.mu.Unlock()
	repo, err := s.repo()
	if err != nil {
		return
	}
	if fr, ok := repo.(*FileRepo); ok {
		if err := writeJSONFile(filepath.Join(fr.Dir(), "index", checkFile), res); err != nil {
			s.log.Warn().Err(err).Msg("Backup check result was not saved")
		}
	}
}

// loadCheck returns the last result, reading it from the repository once.
func (s *BackupService) loadCheck(repo Repo) *CheckResult {
	s.mu.Lock()
	if s.checkLoaded {
		res := s.lastCheck
		s.mu.Unlock()
		return res
	}
	s.mu.Unlock()
	var res *CheckResult
	if fr, ok := repo.(*FileRepo); ok {
		var r CheckResult
		data, err := os.ReadFile(filepath.Join(fr.Dir(), "index", checkFile))
		switch {
		case err == nil && json.Unmarshal(data, &r) == nil:
			res = &r
		case err != nil && !errors.Is(err, os.ErrNotExist):
			s.log.Warn().Err(err).Msg("Backup check result is unreadable")
		}
	}
	s.mu.Lock()
	s.lastCheck, s.checkLoaded = res, true
	s.mu.Unlock()
	return res
}

// checkSummary is one line for a notification.
func checkSummary(r CheckResult) string {
	var parts []string
	if r.Error != "" {
		parts = append(parts, r.Error)
	}
	if r.Missing > 0 {
		parts = append(parts, fmt.Sprintf("%d blobs missing", r.Missing))
	}
	if r.Corrupt > 0 {
		parts = append(parts, fmt.Sprintf("%d blobs damaged", r.Corrupt))
	}
	if t := r.RestoreTest; t != nil && !t.OK {
		parts = append(parts, "restore test failed: "+t.Error)
	}
	if len(parts) == 0 {
		return "unknown problem"
	}
	return strings.Join(parts, "; ")
}

func prefixed(p string, xs []string) []string {
	out := make([]string, len(xs))
	for i, x := range xs {
		out[i] = p + x
	}
	return out
}

func hasComponent(cs []Component, c Component) bool {
	for _, x := range cs {
		if x == c {
			return true
		}
	}
	return false
}
