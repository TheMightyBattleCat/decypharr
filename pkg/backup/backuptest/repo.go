// Package backuptest has an in-memory backup.Repo and a backup.Service fake
// for other packages' tests. It is a normal package so they can import it.
package backuptest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/sirrobot01/decypharr/pkg/backup"
)

// MemRepo is an in-memory backup.Repo. Blobs are stored uncompressed.
type MemRepo struct {
	mu        sync.Mutex
	blobs     map[string][]byte
	snapshots map[string]*backup.Snapshot
	locked    bool
}

var _ backup.Repo = (*MemRepo)(nil)

// NewMemRepo returns an empty repository.
func NewMemRepo() *MemRepo {
	return &MemRepo{blobs: map[string][]byte{}, snapshots: map[string]*backup.Snapshot{}}
}

func (r *MemRepo) PutBlob(_ context.Context, rd io.Reader, _ backup.CompressionLevel) (backup.BlobRef, bool, error) {
	data, err := io.ReadAll(rd)
	if err != nil {
		return backup.BlobRef{}, false, err
	}
	sum := sha256.Sum256(data)
	sha := hex.EncodeToString(sum[:])
	r.mu.Lock()
	defer r.mu.Unlock()
	_, existed := r.blobs[sha]
	if !existed {
		r.blobs[sha] = data
	}
	n := int64(len(data))
	return backup.BlobRef{SHA256: sha, Size: n, Stored: n}, !existed, nil
}

func (r *MemRepo) OpenBlob(_ context.Context, sha string) (io.ReadCloser, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	data, ok := r.blobs[sha]
	if !ok {
		return nil, fmt.Errorf("blob %s: %w", sha, backup.ErrNotFound)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (r *MemRepo) HasBlob(_ context.Context, sha string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.blobs[sha]
	return ok, nil
}

func (r *MemRepo) Lock(context.Context) (func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.locked {
		return nil, backup.ErrBusy
	}
	r.locked = true
	return func() {
		r.mu.Lock()
		r.locked = false
		r.mu.Unlock()
	}, nil
}

func (r *MemRepo) WriteSnapshot(_ context.Context, s *backup.Snapshot) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := *s
	r.snapshots[s.ID] = &cp
	return nil
}

func (r *MemRepo) sorted() []*backup.Snapshot {
	out := make([]*backup.Snapshot, 0, len(r.snapshots))
	for _, s := range r.snapshots {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out
}

func (r *MemRepo) ListSnapshots(context.Context) ([]backup.SnapshotInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []backup.SnapshotInfo
	for _, s := range r.sorted() {
		out = append(out, s.Info())
	}
	return out, nil
}

func (r *MemRepo) ReadSnapshot(_ context.Context, id string) (*backup.Snapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.snapshots[id]
	if !ok {
		return nil, fmt.Errorf("snapshot %s: %w", id, backup.ErrNotFound)
	}
	cp := *s
	return &cp, nil
}

func (r *MemRepo) DeleteSnapshot(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.snapshots[id]; !ok {
		return fmt.Errorf("snapshot %s: %w", id, backup.ErrNotFound)
	}
	delete(r.snapshots, id)
	return nil
}

// refs lists every blob sha a snapshot points at.
func refs(s *backup.Snapshot) []string {
	var out []string
	for _, c := range s.Components {
		for _, v := range c.StoreBlobs {
			out = append(out, v)
		}
		for _, v := range c.Files {
			out = append(out, v)
		}
		for _, v := range []string{c.Tree, c.Manifest} {
			if v != "" {
				out = append(out, v)
			}
		}
	}
	return out
}

// PrunePlan keeps the newest policy.Last snapshots and, for the daily,
// weekly and monthly counts, the newest snapshot in each of that many
// distinct days, ISO weeks and months. It is a simple model for tests, not
// the real repository's code.
func (r *MemRepo) PrunePlan(_ context.Context, p backup.RetentionPolicy) (backup.PrunePlan, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	keep := map[string]bool{}
	var cand []*backup.Snapshot
	for _, s := range r.sorted() {
		if p.TriggerPrefix == "" || len(s.Trigger) >= len(p.TriggerPrefix) && s.Trigger[:len(p.TriggerPrefix)] == p.TriggerPrefix {
			cand = append(cand, s)
		} else {
			keep[s.ID] = true
		}
	}
	for i, s := range cand {
		if i < p.Last {
			keep[s.ID] = true
		}
	}
	bucket := func(n int, key func(s *backup.Snapshot) string) {
		seen := map[string]bool{}
		for _, s := range cand {
			k := key(s)
			if seen[k] {
				continue
			}
			if len(seen) >= n {
				break
			}
			seen[k] = true
			keep[s.ID] = true
		}
	}
	bucket(p.Daily, func(s *backup.Snapshot) string { return s.CreatedAt.Format("2006-01-02") })
	bucket(p.Weekly, func(s *backup.Snapshot) string { y, w := s.CreatedAt.ISOWeek(); return fmt.Sprint(y, w) })
	bucket(p.Monthly, func(s *backup.Snapshot) string { return s.CreatedAt.Format("2006-01") })

	plan := backup.PrunePlan{Policy: p}
	live := map[string]bool{}
	for _, s := range r.snapshots {
		if keep[s.ID] {
			for _, sha := range refs(s) {
				live[sha] = true
			}
		}
	}
	for _, s := range cand {
		if keep[s.ID] {
			plan.KeepSnapshots++
		} else {
			plan.DeleteSnapshot = append(plan.DeleteSnapshot, s.Info())
		}
	}
	for sha, data := range r.blobs {
		if !live[sha] {
			plan.DeleteBlobs = append(plan.DeleteBlobs, sha)
			plan.FreedBytes += int64(len(data))
		}
	}
	sort.Strings(plan.DeleteBlobs)
	plan.BlobCount = len(plan.DeleteBlobs)
	return plan, nil
}

func (r *MemRepo) Prune(_ context.Context, plan backup.PrunePlan) (backup.PruneResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var res backup.PruneResult
	for _, s := range plan.DeleteSnapshot {
		if _, ok := r.snapshots[s.ID]; ok {
			delete(r.snapshots, s.ID)
			res.DeletedSnapshots++
		}
	}
	live := map[string]bool{}
	for _, s := range r.snapshots {
		for _, sha := range refs(s) {
			live[sha] = true
		}
	}
	for _, sha := range plan.DeleteBlobs {
		if data, ok := r.blobs[sha]; ok && !live[sha] {
			res.FreedBytes += int64(len(data))
			delete(r.blobs, sha)
			res.DeletedBlobs++
		}
	}
	return res, nil
}

func (r *MemRepo) Verify(_ context.Context, opts backup.VerifyOptions, _ backup.Progress) (backup.VerifyReport, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rep := backup.VerifyReport{OK: true}
	for _, s := range r.snapshots {
		if opts.SnapshotID != "" && s.ID != opts.SnapshotID {
			continue
		}
		rep.Snapshots++
		for _, sha := range refs(s) {
			rep.BlobsChecked++
			data, ok := r.blobs[sha]
			switch {
			case !ok:
				rep.Missing = append(rep.Missing, sha)
			case opts.Full:
				sum := sha256.Sum256(data)
				if hex.EncodeToString(sum[:]) != sha {
					rep.Corrupt = append(rep.Corrupt, sha)
				}
			}
		}
	}
	rep.OK = len(rep.Missing) == 0 && len(rep.Corrupt) == 0
	return rep, nil
}
