package backup

import (
	"context"
	"io"
)

// LiveRepo is a Repo that opens the repository of the configured folder on
// every call, so a service built before the destination is chosen (or after
// it changes) always reads the current one. Open returns ErrNotConfigured
// while no folder is set.
type LiveRepo struct {
	Open func() (Repo, error)
}

var _ Repo = LiveRepo{}

func (l LiveRepo) PutBlob(ctx context.Context, r io.Reader, level CompressionLevel) (BlobRef, bool, error) {
	rp, err := l.Open()
	if err != nil {
		return BlobRef{}, false, err
	}
	return rp.PutBlob(ctx, r, level)
}

func (l LiveRepo) OpenBlob(ctx context.Context, sha string) (io.ReadCloser, error) {
	rp, err := l.Open()
	if err != nil {
		return nil, err
	}
	return rp.OpenBlob(ctx, sha)
}

func (l LiveRepo) HasBlob(ctx context.Context, sha string) (bool, error) {
	rp, err := l.Open()
	if err != nil {
		return false, err
	}
	return rp.HasBlob(ctx, sha)
}

func (l LiveRepo) Lock(ctx context.Context) (func(), error) {
	rp, err := l.Open()
	if err != nil {
		return nil, err
	}
	return rp.Lock(ctx)
}

func (l LiveRepo) WriteSnapshot(ctx context.Context, s *Snapshot) error {
	rp, err := l.Open()
	if err != nil {
		return err
	}
	return rp.WriteSnapshot(ctx, s)
}

func (l LiveRepo) ListSnapshots(ctx context.Context) ([]SnapshotInfo, error) {
	rp, err := l.Open()
	if err != nil {
		return nil, err
	}
	return rp.ListSnapshots(ctx)
}

func (l LiveRepo) ReadSnapshot(ctx context.Context, id string) (*Snapshot, error) {
	rp, err := l.Open()
	if err != nil {
		return nil, err
	}
	return rp.ReadSnapshot(ctx, id)
}

func (l LiveRepo) DeleteSnapshot(ctx context.Context, id string) error {
	rp, err := l.Open()
	if err != nil {
		return err
	}
	return rp.DeleteSnapshot(ctx, id)
}

func (l LiveRepo) PrunePlan(ctx context.Context, policy RetentionPolicy) (PrunePlan, error) {
	rp, err := l.Open()
	if err != nil {
		return PrunePlan{}, err
	}
	return rp.PrunePlan(ctx, policy)
}

func (l LiveRepo) Prune(ctx context.Context, plan PrunePlan) (PruneResult, error) {
	rp, err := l.Open()
	if err != nil {
		return PruneResult{}, err
	}
	return rp.Prune(ctx, plan)
}

func (l LiveRepo) Verify(ctx context.Context, opts VerifyOptions, p Progress) (VerifyReport, error) {
	rp, err := l.Open()
	if err != nil {
		return VerifyReport{}, err
	}
	return rp.Verify(ctx, opts, p)
}
