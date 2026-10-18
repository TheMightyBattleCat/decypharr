package backuptest

import (
	"context"
	"sync"

	"github.com/sirrobot01/decypharr/pkg/backup"
)

// Service is a backup.Service fake. Every method returns
// backup.ErrNotImplemented unless a matching *Fn field is set, so a test sets
// only what it exercises. PreAction records its calls.
type Service struct {
	backup.Service // not-implemented base

	StatusFn         func(context.Context) (backup.StatusResponse, error)
	RunJobFn         func(context.Context, string) (backup.RunJobResponse, error)
	ListSnapshotsFn  func(context.Context) ([]backup.SnapshotInfo, error)
	GetSnapshotFn    func(context.Context, string) (*backup.Snapshot, error)
	DeleteSnapshotFn func(context.Context, string) error
	LinksStatusFn    func(context.Context) (backup.LinksStatusResponse, error)
	PreActionFn      func(context.Context, string) (string, error)

	mu         sync.Mutex
	preActions []string
}

var _ backup.Service = (*Service)(nil)

// NewService returns a fake with nothing implemented.
func NewService() *Service { return &Service{Service: backup.NotImplemented()} }

// PreActions returns the action names PreAction was called with.
func (f *Service) PreActions() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.preActions...)
}

func (f *Service) Status(ctx context.Context) (backup.StatusResponse, error) {
	if f.StatusFn != nil {
		return f.StatusFn(ctx)
	}
	return f.Service.Status(ctx)
}

func (f *Service) RunJob(ctx context.Context, name string) (backup.RunJobResponse, error) {
	if f.RunJobFn != nil {
		return f.RunJobFn(ctx, name)
	}
	return f.Service.RunJob(ctx, name)
}

func (f *Service) ListSnapshots(ctx context.Context) ([]backup.SnapshotInfo, error) {
	if f.ListSnapshotsFn != nil {
		return f.ListSnapshotsFn(ctx)
	}
	return f.Service.ListSnapshots(ctx)
}

func (f *Service) GetSnapshot(ctx context.Context, id string) (*backup.Snapshot, error) {
	if f.GetSnapshotFn != nil {
		return f.GetSnapshotFn(ctx, id)
	}
	return f.Service.GetSnapshot(ctx, id)
}

func (f *Service) DeleteSnapshot(ctx context.Context, id string) error {
	if f.DeleteSnapshotFn != nil {
		return f.DeleteSnapshotFn(ctx, id)
	}
	return f.Service.DeleteSnapshot(ctx, id)
}

func (f *Service) LinksStatus(ctx context.Context) (backup.LinksStatusResponse, error) {
	if f.LinksStatusFn != nil {
		return f.LinksStatusFn(ctx)
	}
	return f.Service.LinksStatus(ctx)
}

func (f *Service) PreAction(ctx context.Context, action string) (string, error) {
	f.mu.Lock()
	f.preActions = append(f.preActions, action)
	f.mu.Unlock()
	if f.PreActionFn != nil {
		return f.PreActionFn(ctx, action)
	}
	return "", nil
}
