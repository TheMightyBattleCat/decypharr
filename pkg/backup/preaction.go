package backup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// Pre-action names passed by the server's call sites.
const (
	ActionClearRepairState        = "clear_repair_state"
	ActionClearDecodeVerification = "clear_decode_verification"
	ActionClearRepairRuns         = "clear_repair_runs"
	ActionClearPar2History        = "clear_par2_history"
	ActionStaleNZBCleanup         = "stale_nzb_cleanup"
	ActionReplaceUnverified       = "replace_unverified"
	ActionDeleteEntry             = "delete_entry"
	ActionDeleteEntries           = "delete_entries"
	ActionLinksCleanup            = "links_cleanup"
	ActionUpgrade                 = "upgrade"
)

// PreActionFunc takes the snapshot before a risky action.
type PreActionFunc func(ctx context.Context, action string) (snapshotID string, err error)

var preActionHook atomic.Pointer[PreActionFunc]

// SetPreActionHook installs the function PreAction uses when it is given no
// service. Passing nil removes it.
func SetPreActionHook(f PreActionFunc) {
	if f == nil {
		preActionHook.Store(nil)
		return
	}
	preActionHook.Store(&f)
}

// PreAction takes a pre-action snapshot through svc, or through the package
// hook when svc is nil. With neither it does nothing and returns "". Callers
// log a failure and carry on with the action (the spec default).
func PreAction(ctx context.Context, svc Service, action string) (string, error) {
	if svc != nil {
		if _, stub := svc.(notImplemented); !stub {
			return svc.PreAction(ctx, action)
		}
	}
	if h := preActionHook.Load(); h != nil {
		return (*h)(ctx, action)
	}
	return "", nil
}

// preActionReuse is how long a pre-action snapshot stays good for the next
// risky action.
const preActionReuse = 10 * time.Minute

// preActionWait is how long a pre-action waits for a backup in progress.
const preActionWait = 20 * time.Second

// ErrPreActionRefused wraps the failure when the snapshot before an action
// failed and settings say that action must then be refused. Callers check it
// with errors.Is; any other PreAction error means carry on.
var ErrPreActionRefused = errors.New("backup: the action was refused because its safety snapshot failed")

// preActionComponents is what a pre-action snapshot holds: the Quick set
// without Hearsay.
var preActionComponents = []Component{ComponentDB, ComponentConfig, ComponentState, ComponentLinks}

func refusesAction(cfg config.BackupPreAction, action string) bool {
	for _, a := range cfg.Refuse {
		if a == action {
			return true
		}
	}
	return false
}

// PreAction takes a Quick snapshot tagged pre:<action> before a risky action
// and returns its id. A snapshot taken less than ten minutes ago, with no
// other snapshot since, is reused, so a bulk delete costs one. With backups
// off or not set up it does nothing, unless the action is on the refuse list.
// A failure is logged and notified; it returns an error that wraps
// ErrPreActionRefused only for refused actions.
func (s *BackupService) PreAction(ctx context.Context, action string) (string, error) {
	cfg := s.cfg()
	if !cfg.PreAction.Enabled {
		return "", nil
	}
	if strings.TrimSpace(cfg.Dir) == "" {
		if refusesAction(cfg.PreAction, action) {
			return "", fmt.Errorf("%w: %w", ErrPreActionRefused, ErrNotConfigured)
		}
		return "", nil
	}
	s.preMu.Lock() // one pre-action at a time; the second finds the first's snapshot
	defer s.preMu.Unlock()

	id, err := s.preActionSnapshot(ctx, cfg, action)
	if err == nil {
		return id, nil
	}
	s.log.Error().Err(err).Str("action", action).Msg("Safety snapshot before an action failed")
	if cfg.Notify.OnFailure {
		s.notify(config.EventBackupFailed, "error", "Safety snapshot before "+action+" failed", err)
	}
	if refusesAction(cfg.PreAction, action) {
		return "", fmt.Errorf("%w: %w", ErrPreActionRefused, err)
	}
	return "", err
}

func (s *BackupService) preActionSnapshot(ctx context.Context, cfg config.Backup, action string) (string, error) {
	repo, err := s.repo()
	if err != nil {
		return "", err
	}
	if infos, err := repo.ListSnapshots(ctx); err == nil && len(infos) > 0 {
		newest := infos[0]
		if strings.HasPrefix(newest.Trigger, "pre:") && s.now().Sub(newest.CreatedAt) < preActionReuse &&
			s.databaseUnchangedSince(newest.ID) {
			return newest.ID, nil
		}
	}
	writes := s.dbWrites()
	release, err := s.claimWithWait(ctx, "pre:"+action)
	if err != nil {
		return "", err
	}
	defer release()
	snap, err := s.snapshot(ctx, repo, runOptions{trigger: "pre:" + action, components: preActionComponents})
	if err != nil {
		return "", err
	}
	s.preSnap, s.preWrites = snap.ID, writes
	policy := RetentionPolicy{Last: cfg.PreAction.Keep, MaxAgeDays: cfg.PreAction.MaxAgeDays, TriggerPrefix: "pre:"}
	if perr := s.prune(ctx, repo, policy); perr != nil {
		s.log.Warn().Err(perr).Msg("Pre-action retention could not run")
	}
	return snap.ID, nil
}

func (s *BackupService) dbWrites() int64 {
	if s.deps.DBWrites == nil {
		return -1
	}
	return s.deps.DBWrites()
}

// databaseUnchangedSince reports whether snapshot id is the pre-action
// snapshot this process took and the database has taken no write since it
// started. A snapshot from an earlier run of the program is never reused.
// The caller holds preMu.
func (s *BackupService) databaseUnchangedSince(id string) bool {
	return s.deps.DBWrites != nil && s.preSnap == id && s.dbWrites() == s.preWrites
}

// claimWithWait is claim that gives a backup in progress a short while to
// finish, so a delete during a scheduled run still gets its snapshot.
func (s *BackupService) claimWithWait(ctx context.Context, label string) (func(), error) {
	deadline := time.Now().Add(preActionWait)
	for {
		release, err := s.claim(label)
		if err == nil || !errors.Is(err, ErrBusy) || time.Now().After(deadline) {
			return release, err
		}
		if err := sleepCtx(ctx, time.Second); err != nil {
			return nil, err
		}
	}
}
