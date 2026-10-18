package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
)

// Verify checks the repository. Without Full it checks that every blob the
// chosen snapshots refer to is present; with Full it also reads each blob,
// decompresses it and re-hashes it. When no snapshot is named, a quick check
// looks at the newest snapshot only and a full check at all of them.
func (r *FileRepo) Verify(ctx context.Context, opts VerifyOptions, p Progress) (VerifyReport, error) {
	snaps, bad, err := r.loadSnapshots(ctx, false)
	if err != nil {
		return VerifyReport{}, err
	}
	var chosen []*Snapshot
	switch {
	case opts.SnapshotID != "":
		for _, s := range snaps {
			if s.ID == opts.SnapshotID {
				chosen = append(chosen, s)
			}
		}
		if len(chosen) == 0 {
			for _, b := range bad {
				if b == opts.SnapshotID {
					return VerifyReport{Snapshots: 1, Corrupt: []string{"snapshot:" + b}, CheckedAt: r.now().UTC()}, nil
				}
			}
			return VerifyReport{}, fmt.Errorf("snapshot %s: %w", opts.SnapshotID, ErrNotFound)
		}
	case opts.Full:
		chosen = snaps
	case len(snaps) > 0:
		chosen = snaps[:1]
	}

	rep := VerifyReport{Snapshots: len(chosen), CheckedAt: r.now().UTC()}
	if opts.SnapshotID == "" {
		for _, b := range bad {
			rep.Corrupt = append(rep.Corrupt, "snapshot:"+b)
		}
	}
	set := map[string]struct{}{}
	for _, s := range chosen {
		if err := r.snapshotShas(ctx, s, set); err != nil {
			if ctx.Err() != nil {
				return rep, ctx.Err()
			}
			rep.Corrupt = append(rep.Corrupt, fmt.Sprintf("snapshot:%s (%v)", s.ID, err))
		}
	}
	shas := make([]string, 0, len(set))
	for sha := range set {
		shas = append(shas, sha)
	}
	sort.Strings(shas)

	for i, sha := range shas {
		if err := ctx.Err(); err != nil {
			return rep, err
		}
		rep.BlobsChecked++
		p.Report(ProgressUpdate{Phase: "verify", FilesDone: i, FilesTotal: len(shas)})
		if !validSHA(sha) {
			rep.Corrupt = append(rep.Corrupt, sha)
			continue
		}
		if ok, _ := r.HasBlob(ctx, sha); !ok {
			rep.Missing = append(rep.Missing, sha)
			continue
		}
		if !opts.Full {
			continue
		}
		if !r.blobHashes(ctx, sha) {
			if ctx.Err() != nil {
				return rep, ctx.Err()
			}
			rep.Corrupt = append(rep.Corrupt, sha)
		}
	}
	p.Report(ProgressUpdate{Phase: "verify", FilesDone: len(shas), FilesTotal: len(shas)})
	rep.OK = len(rep.Missing) == 0 && len(rep.Corrupt) == 0
	return rep, nil
}

// blobHashes reports whether the blob decompresses and matches its name.
func (r *FileRepo) blobHashes(ctx context.Context, sha string) bool {
	rc, err := r.OpenBlob(ctx, sha)
	if err != nil {
		return false
	}
	defer rc.Close()
	h := sha256.New()
	if _, err := io.Copy(h, &ctxReader{ctx: ctx, r: rc}); err != nil {
		return false
	}
	return hex.EncodeToString(h.Sum(nil)) == sha
}
