package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Tree is the content of a tree blob: slash-separated relative path to the
// sha256 of the file's blob. Go sorts map keys when encoding, so the same
// folder always produces the same tree blob.
type Tree struct {
	Files map[string]string `json:"files"`
}

// ReadTree loads a tree blob.
func ReadTree(ctx context.Context, bs BlobStore, sha string) (*Tree, error) {
	b, err := readBlob(ctx, bs, sha)
	if err != nil {
		return nil, err
	}
	var t Tree
	if err := json.Unmarshal(b, &t); err != nil {
		return nil, fmt.Errorf("backup: tree %s is unreadable: %w", sha, err)
	}
	if t.Files == nil {
		t.Files = map[string]string{}
	}
	return &t, nil
}

// WalkOptions configures WalkTree.
type WalkOptions struct {
	// Root is the folder to walk. A missing folder gives an empty tree.
	Root string
	// Component labels progress updates.
	Component Component
	// Cache lets unchanged files skip hashing; nil hashes everything. It is
	// updated in place, and the caller saves it.
	Cache *FileCache
	// Level is the compression for file blobs (the tree uses the best).
	Level CompressionLevel
	// BytesPerSec throttles reading; 0 is unlimited.
	BytesPerSec int64
	// Pause is polled before each file; while it returns true the walk waits,
	// reporting PauseReason with each poll.
	Pause       func() bool
	PauseReason string
	// PauseInterval is how often a paused walk polls again (default 1s).
	PauseInterval time.Duration
	// Match picks files by slash-separated relative path; nil takes all
	// regular files.
	Match func(rel string) bool
	// Progress is called after each file.
	Progress Progress
}

// rateWindow measures speed over the last span of a walk's unpaused time, so
// the time left follows what the walk is doing now: files the cache skips go
// by far faster than files that are read and compressed.
type rateWindow struct {
	span    time.Duration
	samples []rateSample
}

type rateSample struct {
	active time.Duration // unpaused time since the walk started
	done   int64
}

func (w *rateWindow) add(active time.Duration, done int64) {
	w.samples = append(w.samples, rateSample{active, done})
	// Keep one sample at or before the start of the window.
	drop := 0
	for drop+1 < len(w.samples) && w.samples[drop+1].active <= active-w.span {
		drop++
	}
	if drop > 0 {
		w.samples = append(w.samples[:0], w.samples[drop:]...)
	}
}

// rate is bytes per second across the window, or 0 until it covers a second.
func (w *rateWindow) rate() int64 {
	if len(w.samples) < 2 {
		return 0
	}
	first, last := w.samples[0], w.samples[len(w.samples)-1]
	d := last.active - first.active
	if d < time.Second {
		return 0
	}
	return int64(float64(last.done-first.done) / d.Seconds())
}

// WalkResult is what a walk stored.
type WalkResult struct {
	Tree      BlobRef
	FileCount int
	Bytes     int64 // total size of the files in the tree
	NewBlobs  int   // blobs written by this walk, the tree included
	NewBytes  int64 // their stored size
	Reused    int   // files taken from the cache without being read
}

type walkFile struct {
	rel, abs string
	size     int64
	mtime    int64
}

// WalkTree stores every regular file under opts.Root as a blob and returns
// the tree blob listing them. Symbolic links and special files are skipped
// and never followed. A file whose size and mtime match the cache, and whose
// blob is still in the store, is not read.
func WalkTree(ctx context.Context, bs BlobStore, opts WalkOptions) (WalkResult, error) {
	var res WalkResult
	root := filepath.Clean(opts.Root)
	var files []walkFile
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				if p == root {
					return fs.SkipAll
				}
				return nil
			}
			return err
		}
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if !d.Type().IsRegular() {
			return nil // directories are descended into; links and specials skipped
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if opts.Match != nil && !opts.Match(rel) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		files = append(files, walkFile{rel: rel, abs: p, size: info.Size(), mtime: info.ModTime().UnixNano()})
		return nil
	})
	if err != nil {
		return res, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].rel < files[j].rel })

	interval := opts.PauseInterval
	if interval <= 0 {
		interval = time.Second
	}
	th := &throttle{rate: opts.BytesPerSec, start: time.Now()}
	tree := Tree{Files: make(map[string]string, len(files))}
	seen := make(map[string]struct{}, len(files))
	var written, done, total int64
	for _, f := range files {
		total += f.size
	}
	start := time.Now()
	var pausedFor time.Duration
	var pausedAt time.Time // start of the pause in progress; zero when running
	activeTime := func() time.Duration {
		idle := pausedFor
		if !pausedAt.IsZero() {
			idle += time.Since(pausedAt)
		}
		return time.Since(start) - idle
	}
	window := rateWindow{span: time.Minute}
	update := func(filesDone int) ProgressUpdate {
		u := ProgressUpdate{Component: opts.Component, Phase: "walk", FilesDone: filesDone, FilesTotal: len(files),
			BytesWritten: written, BytesDone: done, BytesTotal: total}
		u.RateBPS = window.rate()
		if active := activeTime(); u.RateBPS == 0 && active >= time.Second && done > 0 {
			u.RateBPS = int64(float64(done) / active.Seconds())
		}
		if u.RateBPS > 0 {
			u.ETASeconds = (total - done) / u.RateBPS
		}
		return u
	}
	opts.Progress.Report(update(0))

	for i, f := range files {
		if opts.Pause != nil && opts.Pause() {
			since := time.Now()
			pausedAt = since
			reason := opts.PauseReason
			if reason == "" {
				reason = "paused"
			}
			for opts.Pause() {
				u := update(i)
				u.Paused, u.PausedSince = reason, &since
				opts.Progress.Report(u)
				if err := sleepCtx(ctx, interval); err != nil {
					return res, err
				}
			}
			pausedFor += time.Since(since)
			pausedAt = time.Time{}
		}
		if err := ctx.Err(); err != nil {
			return res, err
		}
		sha, skipped, newRef, err := storeFile(ctx, bs, opts, th, f)
		if err != nil {
			return res, err
		}
		done += f.size
		window.add(activeTime(), done)
		if skipped {
			continue // vanished while walking
		}
		seen[f.abs] = struct{}{}
		tree.Files[f.rel] = sha
		res.FileCount++
		res.Bytes += f.size
		if newRef != nil {
			res.NewBlobs++
			res.NewBytes += newRef.Stored
			written += newRef.Stored
		} else {
			res.Reused++
		}
		opts.Progress.Report(update(i + 1))
	}
	if opts.Cache != nil {
		opts.Cache.retainUnder(root, seen)
	}

	b, err := json.Marshal(tree)
	if err != nil {
		return res, err
	}
	ref, created, err := bs.PutBlob(ctx, strings.NewReader(string(b)), CompressBest)
	if err != nil {
		return res, err
	}
	res.Tree = ref
	if created {
		res.NewBlobs++
		res.NewBytes += ref.Stored
	}
	return res, nil
}

// storeFile returns the file's blob sha. skipped is true when the file
// disappeared; newRef is set when a new blob was written.
func storeFile(ctx context.Context, bs BlobStore, opts WalkOptions, th *throttle, f walkFile) (sha string, skipped bool, newRef *BlobRef, err error) {
	if opts.Cache != nil {
		if cached, ok := opts.Cache.Lookup(f.abs, f.size, f.mtime); ok {
			if has, herr := bs.HasBlob(ctx, cached); herr == nil && has {
				return cached, false, nil, nil
			}
		}
	}
	fh, err := os.Open(f.abs)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", true, nil, nil
		}
		return "", false, nil, err
	}
	defer fh.Close()
	ref, created, err := bs.PutBlob(ctx, &throttledReader{ctx: ctx, r: fh, th: th}, opts.Level)
	if err != nil {
		return "", false, nil, fmt.Errorf("backup: storing %s: %w", f.rel, err)
	}
	if created {
		newRef = &ref
	}
	if opts.Cache != nil {
		// Only trust the stat taken before the read if the file did not
		// change while it was being read.
		if st, serr := fh.Stat(); serr == nil && st.Size() == f.size && st.ModTime().UnixNano() == f.mtime && ref.Size == f.size {
			opts.Cache.Put(f.abs, f.size, f.mtime, ref.SHA256)
		} else {
			opts.Cache.forget(f.abs)
		}
	}
	if newRef == nil {
		// Blob already existed (dedup): nothing new written.
		return ref.SHA256, false, nil, nil
	}
	return ref.SHA256, false, newRef, nil
}
