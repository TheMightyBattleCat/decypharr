package backup

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// ---- prune ----

type blobFile struct {
	path string
	size int64
}

// listBlobs returns every blob file on disk keyed by sha.
func (r *FileRepo) listBlobs(ctx context.Context) (map[string]blobFile, error) {
	out := map[string]blobFile{}
	root := filepath.Join(r.dir, "blobs")
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		ext := filepath.Ext(name)
		sha := strings.TrimSuffix(name, ext)
		if (ext != extZstd && ext != extRaw) || !validSHA(sha) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		out[sha] = blobFile{path: p, size: info.Size()}
		return nil
	})
	return out, err
}

// hasPrefix reports whether trigger s falls under a policy prefix. A prefix
// ending in a colon ("pre:") names a whole family of triggers. Any other
// prefix ("schedule:daily") names one job and matches only exactly, so job
// "daily" never reaches the snapshots of "daily-full".
func hasPrefix(s, prefix string) bool {
	if strings.HasSuffix(prefix, ":") {
		return strings.HasPrefix(s, prefix)
	}
	return s == prefix
}

// PrunePlan decides which snapshots the policy drops and which blobs only
// they referenced. Snapshots whose trigger does not match the policy's
// prefix are never touched and their blobs stay. The newest matching
// snapshot is always kept. Nothing is deleted.
func (r *FileRepo) PrunePlan(ctx context.Context, p RetentionPolicy) (PrunePlan, error) {
	snaps, _, err := r.loadSnapshots(ctx, true)
	if err != nil {
		return PrunePlan{}, err
	}
	keep := map[string]bool{}
	var cand []*Snapshot
	for _, s := range snaps {
		if p.TriggerPrefix == "" || hasPrefix(s.Trigger, p.TriggerPrefix) {
			cand = append(cand, s)
		} else {
			keep[s.ID] = true
		}
	}
	for i, s := range cand {
		if i == 0 || i < p.Last {
			keep[s.ID] = true
		}
	}
	bucket := func(n int, key func(t time.Time) string) {
		if n <= 0 {
			return
		}
		seen := map[string]bool{}
		for _, s := range cand {
			k := key(s.CreatedAt.UTC())
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
	bucket(p.Daily, func(t time.Time) string { return t.Format("2006-01-02") })
	bucket(p.Weekly, func(t time.Time) string { y, w := t.ISOWeek(); return fmt.Sprint(y, "-", w) })
	bucket(p.Monthly, func(t time.Time) string { return t.Format("2006-01") })
	if p.MaxAgeDays > 0 {
		cutoff := r.now().Add(-time.Duration(p.MaxAgeDays) * 24 * time.Hour)
		for i, s := range cand {
			if i > 0 && s.CreatedAt.Before(cutoff) {
				keep[s.ID] = false
			}
		}
	}

	plan := PrunePlan{Policy: p}
	live := map[string]struct{}{}
	for _, s := range snaps {
		if keep[s.ID] {
			if err := r.snapshotShas(ctx, s, live); err != nil {
				return PrunePlan{}, err
			}
		}
	}
	for _, s := range snaps {
		if keep[s.ID] {
			plan.KeepSnapshots++
		} else {
			plan.DeleteSnapshot = append(plan.DeleteSnapshot, s.Info())
		}
	}
	// Blobs referenced by the snapshots being deleted only; blobs on disk
	// that no snapshot refers to (aborted runs) go too.
	onDisk, err := r.listBlobs(ctx)
	if err != nil {
		return PrunePlan{}, err
	}
	for sha, bf := range onDisk {
		if _, ok := live[sha]; !ok {
			plan.DeleteBlobs = append(plan.DeleteBlobs, sha)
			plan.FreedBytes += bf.size
		}
	}
	sort.Strings(plan.DeleteBlobs)
	plan.BlobCount = len(plan.DeleteBlobs)
	return plan, nil
}

// Prune deletes the plan's snapshots and then sweeps: every blob in the
// plan that no remaining snapshot refers to is removed. The references are
// recomputed under the lock, so a snapshot written since the plan was made
// keeps its blobs. Prune takes the repository lock itself; the caller must
// not hold it.
func (r *FileRepo) Prune(ctx context.Context, plan PrunePlan) (PruneResult, error) {
	unlock, err := r.Lock(ctx)
	if err != nil {
		return PruneResult{}, err
	}
	defer unlock()

	var res PruneResult
	// Check every remaining manifest and tree first; a failure here must
	// not leave half a prune behind.
	del := map[string]bool{}
	for _, s := range plan.DeleteSnapshot {
		del[s.ID] = true
	}
	snaps, _, err := r.loadSnapshots(ctx, true)
	if err != nil {
		return res, err
	}
	live := map[string]struct{}{}
	for _, s := range snaps {
		if !del[s.ID] {
			if err := r.snapshotShas(ctx, s, live); err != nil {
				return res, err
			}
		}
	}
	for _, s := range plan.DeleteSnapshot {
		if err := r.DeleteSnapshot(ctx, s.ID); err == nil {
			res.DeletedSnapshots++
		} else if !errors.Is(err, ErrNotFound) {
			return res, err
		}
	}
	for _, sha := range plan.DeleteBlobs {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		if !validSHA(sha) {
			continue
		}
		if _, ok := live[sha]; ok {
			continue
		}
		deleted := false
		for _, ext := range []string{extZstd, extRaw} {
			p := r.blobBase(sha) + ext
			st, err := os.Stat(p)
			if err != nil {
				continue
			}
			if err := os.Remove(p); err != nil {
				return res, err
			}
			res.FreedBytes += st.Size()
			deleted = true
		}
		if deleted {
			res.DeletedBlobs++
		}
	}
	r.cleanTemp()
	return res, nil
}

// cleanTemp removes spool files left by a crashed run.
func (r *FileRepo) cleanTemp() {
	dir := filepath.Join(r.dir, "tmp")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	cutoff := r.now().Add(-24 * time.Hour)
	for _, e := range entries {
		if info, err := e.Info(); err == nil && !e.IsDir() && info.ModTime().Before(cutoff) {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
}
