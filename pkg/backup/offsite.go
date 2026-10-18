package backup

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// The off-box copy: after each successful backup job the backup folder is
// copied to an rclone remote (Settings → Backups → Rclone remote), so losing
// the machine does not lose the backups. It runs outside the run slot, so a
// long first upload never holds up a backup or a safety snapshot.
//
// The remote mirrors the repository without ever overwriting a file on it:
//
//  1. its repo.json must be this repository's (or there is none yet);
//  2. the blobs the local snapshots use are copied, then the manifests, so a
//     manifest never arrives before its blobs;
//  3. manifests the local repository no longer has are deleted, then blobs
//     no copied manifest uses.
//
// Only the snapshots that existed when the upload started are copied; one
// written meanwhile goes with the next upload.

const (
	// offsiteFile in the repository's index folder holds the last upload.
	offsiteFile = "offsite.json"
	// maxOffsiteSnapshotDeletes caps the manifests one upload deletes from
	// the remote; more means something is wrong locally, and nothing goes.
	maxOffsiteSnapshotDeletes = 50
	// offsiteStats is how often rclone reports progress.
	offsiteStats = "10s"
)

// OffsiteResult is the last upload.
type OffsiteResult struct {
	At         time.Time `json:"at"`
	DurationMS int64     `json:"duration_ms"`
	Remote     string    `json:"remote"`
	OK         bool      `json:"ok"`
	Snapshots  int       `json:"snapshots"`
	Blobs      int       `json:"blobs"`
	Deleted    int       `json:"deleted,omitempty"`
	Error      string    `json:"error,omitempty"`
}

// OffsiteStatus is the off-box copy's line on the Backups page.
type OffsiteStatus struct {
	Remote string `json:"remote"`
	// RemoteType is the rclone backend of the remote ("crypt", "drive", ...),
	// or empty while unknown.
	RemoteType string         `json:"remote_type,omitempty"`
	Running    bool           `json:"running"`
	Phase      string         `json:"phase,omitempty"`
	Last       *OffsiteResult `json:"last,omitempty"`
}

// OffsiteRunner is implemented by the real Service; the API starts an upload
// through it (the Service contract has no such method).
type OffsiteRunner interface {
	StartUpload() error
}

var _ OffsiteRunner = (*BackupService)(nil)

// rcloneRunner runs rclone with args and returns its standard output. Each
// line rclone writes to standard error is passed to stderr. A failed run's
// error is an *rcloneError.
type rcloneRunner func(ctx context.Context, args []string, stderr func(string)) ([]byte, error)

// rcloneError is a failed rclone run.
type rcloneError struct {
	Code int
	Tail []string // the last lines rclone wrote to standard error
	Err  error
}

func (e *rcloneError) Error() string {
	if len(e.Tail) > 0 {
		return "rclone: " + strings.Join(e.Tail, " | ")
	}
	return "rclone: " + e.Err.Error()
}

func (e *rcloneError) Unwrap() error { return e.Err }

// rcloneNotFound reports whether err is rclone saying the path does not
// exist (exit code 3: directory not found, 4: file not found).
func rcloneNotFound(err error) bool {
	var re *rcloneError
	return errors.As(err, &re) && (re.Code == 3 || re.Code == 4)
}

// execRclone runs the rclone on the PATH.
func execRclone(ctx context.Context, args []string, stderr func(string)) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "rclone", args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	pipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, &rcloneError{Code: -1, Err: err}
	}
	var tail []string
	sc := bufio.NewScanner(pipe)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if stderr != nil {
			stderr(line)
		}
		if tail = append(tail, line); len(tail) > 3 {
			tail = tail[1:]
		}
	}
	if err := cmd.Wait(); err != nil {
		code := -1
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			code = ee.ExitCode()
		}
		return out.Bytes(), &rcloneError{Code: code, Tail: tail, Err: err}
	}
	return out.Bytes(), nil
}

// StartUpload starts an upload in the background now, or right after the
// one in progress.
func (s *BackupService) StartUpload() error {
	if strings.TrimSpace(s.cfg().RcloneRemote) == "" {
		return fmt.Errorf("%w: no rclone remote is set", ErrNotConfigured)
	}
	if _, err := s.repo(); err != nil {
		return err
	}
	s.upload()
	return nil
}

// upload runs uploads in the background, one at a time: a request while
// one runs makes it run once more when it ends.
func (s *BackupService) upload() {
	s.offMu.Lock()
	if s.offRunning {
		s.offAgain = true
		s.offMu.Unlock()
		return
	}
	s.offRunning, s.offAgain = true, false
	s.offMu.Unlock()
	go func() {
		for {
			res := s.uploadOnce(s.runContext())
			if res != nil {
				s.recordUpload(*res)
			}
			s.offMu.Lock()
			if !s.offAgain || s.stopping() {
				s.offRunning, s.offPhase = false, ""
				s.offMu.Unlock()
				return
			}
			s.offAgain = false
			s.offMu.Unlock()
		}
	}()
}

func (s *BackupService) setUploadPhase(p string) {
	s.offMu.Lock()
	s.offPhase = p
	s.offMu.Unlock()
}

// uploadOnce copies the repository to the remote. It returns nil when there
// is nothing to report (no remote, or the service is stopping).
func (s *BackupService) uploadOnce(ctx context.Context) *OffsiteResult {
	cfg := s.cfg()
	remote := strings.TrimSpace(cfg.RcloneRemote)
	if remote == "" || ctx.Err() != nil {
		return nil
	}
	started := s.now()
	res := &OffsiteResult{At: started.UTC(), Remote: remote}
	err := s.mirror(ctx, cfg, remote, res)
	res.DurationMS = s.now().Sub(started).Milliseconds()
	if err != nil {
		if ctx.Err() != nil {
			return nil // a shutdown; the next start or job uploads again
		}
		res.Error = err.Error()
		return res
	}
	res.OK = true
	return res
}

func (s *BackupService) mirror(ctx context.Context, cfg config.Backup, remote string, res *OffsiteResult) error {
	repo, err := s.repo()
	if err != nil {
		return err
	}
	fr, ok := repo.(*FileRepo)
	if !ok {
		return errors.New("the off-box copy needs a backup folder")
	}
	run := s.rclone
	if run == nil {
		run = execRclone
	}
	base := []string{}
	if c := strings.TrimSpace(cfg.RcloneConfig); c != "" {
		base = append(base, "--config", c)
	}
	rpath := func(sub string) string {
		if strings.HasSuffix(remote, ":") || strings.HasSuffix(remote, "/") {
			return remote + sub
		}
		return remote + "/" + sub
	}
	rclone := func(phase string, args ...string) ([]byte, error) {
		s.setUploadPhase(phase)
		return run(ctx, append(append([]string{}, base...), args...), func(line string) {
			if i := strings.Index(line, "Transferred:"); i >= 0 {
				s.setUploadPhase(phase + ": " + strings.TrimSpace(line[i+len("Transferred:"):]))
			}
		})
	}

	// 1. The remote holds this repository, or none yet.
	local, err := os.ReadFile(filepath.Join(fr.Dir(), "repo.json"))
	if err != nil {
		return err
	}
	var mine repoMeta
	if err := json.Unmarshal(local, &mine); err != nil {
		return fmt.Errorf("repo.json is unreadable: %w", err)
	}
	theirs, err := rclone("checking the remote", "cat", rpath("repo.json"))
	switch {
	case rcloneNotFound(err):
		if _, err := rclone("starting the remote copy", "copyto", filepath.Join(fr.Dir(), "repo.json"), rpath("repo.json")); err != nil {
			return err
		}
	case err != nil:
		return err
	default:
		var m repoMeta
		if json.Unmarshal(theirs, &m) != nil || !m.CreatedAt.Equal(mine.CreatedAt) {
			return fmt.Errorf("%s already holds a different backup repository; choose an empty folder for the off-box copy", remote)
		}
	}

	// 2. What to copy: the snapshots there are now and the blobs they use.
	snaps, _, err := fr.loadSnapshots(ctx, true)
	if err != nil {
		return err
	}
	live := map[string]struct{}{}
	var snapFiles []string
	for _, sn := range snaps {
		if sn.Result == ResultPartial {
			// A run in progress rewrites its partial manifest when it
			// finishes, and the remote copy never overwrites: only
			// complete snapshots go.
			continue
		}
		if err := fr.snapshotShas(ctx, sn, live); err != nil {
			return err
		}
		p, err := fr.snapshotPath(sn.ID)
		if err != nil {
			return err
		}
		snapFiles = append(snapFiles, relSlash(fr.Dir(), p))
	}
	var blobFiles []string
	for sha := range live {
		p, _, err := fr.blobPath(sha)
		if err != nil {
			return fmt.Errorf("the local backup folder is missing blob %s; run a check before copying it off-box", sha)
		}
		blobFiles = append(blobFiles, relSlash(fr.Dir(), p))
	}
	sort.Strings(blobFiles)
	res.Snapshots, res.Blobs = len(snapFiles), len(blobFiles)

	copyList := func(phase string, files []string) error {
		list, cleanup, err := writeList(files)
		if err != nil {
			return err
		}
		defer cleanup()
		args := []string{"copy", fr.Dir(), remote, "--files-from-raw", list, "--ignore-existing",
			"--stats", offsiteStats, "--stats-one-line", "--stats-log-level", "NOTICE"}
		if bw := strings.TrimSpace(cfg.RcloneBwlimit); bw != "" {
			args = append(args, "--bwlimit", bw)
		}
		_, err = rclone(phase, args...)
		return err
	}
	if err := copyList("uploading blobs", blobFiles); err != nil {
		return err
	}
	if err := copyList("uploading snapshot list", snapFiles); err != nil {
		return err
	}

	// 3. Delete what the local repository no longer has: manifests first,
	// then blobs no copied manifest uses.
	remoteSnaps, err := rcloneList(rclone, rpath("snapshots"))
	if err != nil {
		return err
	}
	keepSnap := map[string]bool{}
	for _, f := range snapFiles {
		keepSnap[strings.TrimPrefix(f, "snapshots/")] = true
	}
	var delSnaps []string
	for _, f := range remoteSnaps {
		if !keepSnap[f] {
			delSnaps = append(delSnaps, "snapshots/"+f)
		}
	}
	if len(delSnaps) > maxOffsiteSnapshotDeletes {
		return fmt.Errorf("the off-box copy would delete %d snapshots from %s; it deletes at most %d at once, so nothing was deleted. Check the local backup folder", len(delSnaps), remote, maxOffsiteSnapshotDeletes)
	}
	remoteBlobs, err := rcloneList(rclone, rpath("blobs"))
	if err != nil {
		return err
	}
	var delBlobs []string
	for _, f := range remoteBlobs {
		name := filepath.Base(f)
		sha := strings.TrimSuffix(name, filepath.Ext(name))
		if _, ok := live[sha]; !ok {
			delBlobs = append(delBlobs, "blobs/"+f)
		}
	}
	deleteList := func(phase string, files []string) error {
		if len(files) == 0 {
			return nil
		}
		list, cleanup, err := writeList(files)
		if err != nil {
			return err
		}
		defer cleanup()
		_, err = rclone(phase, "delete", remote, "--files-from-raw", list)
		return err
	}
	if err := deleteList("removing deleted snapshots", delSnaps); err != nil {
		return err
	}
	if err := deleteList("removing unused blobs", delBlobs); err != nil {
		return err
	}
	res.Deleted = len(delSnaps) + len(delBlobs)
	return nil
}

// rcloneList lists the files under path, recursively; a path that does not
// exist is empty.
func rcloneList(rclone func(string, ...string) ([]byte, error), path string) ([]string, error) {
	out, err := rclone("listing the remote", "lsf", "-R", "--files-only", path)
	if rcloneNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var files []string
	for _, line := range strings.Split(string(out), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			files = append(files, line)
		}
	}
	return files, nil
}

// writeList writes one path per line to a temporary file for --files-from.
func writeList(files []string) (string, func(), error) {
	f, err := os.CreateTemp("", "decypharr-offsite-*.txt")
	if err != nil {
		return "", nil, err
	}
	_, werr := f.WriteString(strings.Join(files, "\n") + "\n")
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		_ = os.Remove(f.Name())
		return "", nil, werr
	}
	return f.Name(), func() { _ = os.Remove(f.Name()) }, nil
}

func relSlash(base, p string) string {
	rel, err := filepath.Rel(base, p)
	if err != nil {
		return p
	}
	return filepath.ToSlash(rel)
}

// recordUpload keeps the result in memory and in the repository, and
// notifies a failure.
func (s *BackupService) recordUpload(res OffsiteResult) {
	s.offMu.Lock()
	s.offLast, s.offLoaded = &res, true
	s.offMu.Unlock()
	if repo, err := s.repo(); err == nil {
		if fr, ok := repo.(*FileRepo); ok {
			if err := writeJSONFile(filepath.Join(fr.Dir(), "index", offsiteFile), res); err != nil {
				s.log.Warn().Err(err).Msg("Off-box copy result was not saved")
			}
		}
	}
	if res.OK {
		s.log.Info().Str("remote", res.Remote).Int("snapshots", res.Snapshots).Int("deleted", res.Deleted).
			Int64("ms", res.DurationMS).Msg("Backup copied off-box")
		return
	}
	s.log.Error().Str("remote", res.Remote).Str("error", res.Error).Msg("Off-box copy failed")
	if s.cfg().Notify.OnFailure {
		s.notify(config.EventBackupFailed, "error", "The off-box copy to "+res.Remote+" failed: "+res.Error, nil)
	}
}

// loadUpload returns the last upload, reading it from the repository once.
func (s *BackupService) loadUpload(repo Repo) *OffsiteResult {
	s.offMu.Lock()
	if s.offLoaded {
		r := s.offLast
		s.offMu.Unlock()
		return r
	}
	s.offMu.Unlock()
	var res *OffsiteResult
	if fr, ok := repo.(*FileRepo); ok {
		if data, err := os.ReadFile(filepath.Join(fr.Dir(), "index", offsiteFile)); err == nil {
			var r OffsiteResult
			if json.Unmarshal(data, &r) == nil {
				res = &r
			}
		}
	}
	s.offMu.Lock()
	s.offLast, s.offLoaded = res, true
	s.offMu.Unlock()
	return res
}

// offsiteStatus builds the Backups page line; nil when no remote is set.
func (s *BackupService) offsiteStatus(cfg config.Backup, repo Repo) *OffsiteStatus {
	remote := strings.TrimSpace(cfg.RcloneRemote)
	if remote == "" {
		return nil
	}
	st := &OffsiteStatus{Remote: remote, RemoteType: s.remoteType(cfg, remote)}
	if repo != nil {
		st.Last = s.loadUpload(repo)
	}
	s.offMu.Lock()
	st.Running, st.Phase = s.offRunning, s.offPhase
	s.offMu.Unlock()
	return st
}

// remoteType is the rclone backend of the remote, looked up in the
// background once per remote and config file (the status must not wait on
// rclone).
func (s *BackupService) remoteType(cfg config.Backup, remote string) string {
	name, _, ok := strings.Cut(remote, ":")
	if !ok || name == "" {
		return "local"
	}
	key := strings.TrimSpace(cfg.RcloneConfig) + "\x00" + name
	s.offMu.Lock()
	defer s.offMu.Unlock()
	if t, ok := s.offTypes[key]; ok {
		return t
	}
	s.offTypes[key] = ""
	go func() {
		run := s.rclone
		if run == nil {
			run = execRclone
		}
		args := []string{"listremotes", "--long"}
		if c := strings.TrimSpace(cfg.RcloneConfig); c != "" {
			args = append([]string{"--config", c}, args...)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		out, err := run(ctx, args, nil)
		t := "unknown"
		if err == nil {
			for _, line := range strings.Split(string(out), "\n") {
				f := strings.Fields(line)
				if len(f) >= 2 && strings.TrimSuffix(f[0], ":") == name {
					t = f[1]
				}
			}
		}
		s.offMu.Lock()
		s.offTypes[key] = t
		s.offMu.Unlock()
	}()
	return ""
}

// uploadIfBehind starts an upload after start-up when the last one failed,
// or is older than the newest snapshot.
func (s *BackupService) uploadIfBehind(ctx context.Context) {
	if strings.TrimSpace(s.cfg().RcloneRemote) == "" {
		return
	}
	repo, err := s.repo()
	if err != nil {
		return
	}
	infos, err := repo.ListSnapshots(ctx)
	if err != nil || len(infos) == 0 {
		return
	}
	last := s.loadUpload(repo)
	if last == nil || !last.OK || last.At.Before(infos[0].CreatedAt) {
		s.upload()
	}
}
