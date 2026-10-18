package backup

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeFile(t *testing.T, path, content string, mtime time.Time) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatal(err)
	}
}

func TestWalkTreeDedupAndFileCache(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()
	src := t.TempDir()
	mt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	writeFile(t, filepath.Join(src, "a.meta"), "same content\n", mt)
	writeFile(t, filepath.Join(src, "sub", "b.meta"), "same content\n", mt) // duplicate of a
	writeFile(t, filepath.Join(src, "sub", "c.meta"), "different\n", mt)
	_ = os.Symlink("/nonexistent/target", filepath.Join(src, "link.meta"))

	cache := r.LoadFileCache()
	res, err := WalkTree(ctx, r, WalkOptions{Root: src, Cache: cache})
	if err != nil {
		t.Fatal(err)
	}
	if res.FileCount != 3 {
		t.Fatalf("symlink should be skipped: %+v", res)
	}
	// Two distinct file blobs plus the tree: the identical files share one.
	if res.NewBlobs != 3 {
		t.Fatalf("want 3 new blobs, got %d", res.NewBlobs)
	}
	tr, err := ReadTree(ctx, r, res.Tree.SHA256)
	if err != nil || tr.Files["a.meta"] != tr.Files["sub/b.meta"] || tr.Files["sub/c.meta"] == tr.Files["a.meta"] {
		t.Fatalf("tree: %v %+v", err, tr)
	}

	// Second walk with a saved cache reads nothing and writes nothing.
	if err := r.SaveFileCache(cache); err != nil {
		t.Fatal(err)
	}
	cache2 := r.LoadFileCache()
	if cache2.Len() != 3 {
		t.Fatalf("cache reloaded with %d entries", cache2.Len())
	}
	res2, err := WalkTree(ctx, r, WalkOptions{Root: src, Cache: cache2})
	if err != nil {
		t.Fatal(err)
	}
	if res2.Reused != 3 || res2.NewBlobs != 0 || res2.Tree.SHA256 != res.Tree.SHA256 {
		t.Fatalf("second walk: %+v", res2)
	}

	// Prove the cache is what skipped the read: change the content but keep
	// size and mtime, and the stale hash is reused.
	writeFile(t, filepath.Join(src, "sub", "c.meta"), "DIFFERENT\n", mt)
	res3, _ := WalkTree(ctx, r, WalkOptions{Root: src, Cache: cache2})
	if res3.Tree.SHA256 != res.Tree.SHA256 || res3.Reused != 3 {
		t.Fatalf("cache hit should skip the read: %+v", res3)
	}
	// A new mtime makes it re-read, and a deleted file leaves the tree and cache.
	writeFile(t, filepath.Join(src, "sub", "c.meta"), "DIFFERENT\n", mt.Add(time.Hour))
	_ = os.Remove(filepath.Join(src, "a.meta"))
	res4, _ := WalkTree(ctx, r, WalkOptions{Root: src, Cache: cache2})
	if res4.Reused != 1 || res4.FileCount != 2 || res4.NewBlobs != 2 {
		t.Fatalf("after change: %+v", res4)
	}
	if cache2.Len() != 2 {
		t.Fatalf("cache should forget deleted file: %d", cache2.Len())
	}
}

func TestWalkTreeRereadsWhenBlobPruned(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "a.meta"), "payload payload payload\n", time.Unix(1_700_000_000, 0))
	cache := NewFileCache()
	res, _ := WalkTree(ctx, r, WalkOptions{Root: src, Cache: cache})
	tr, _ := ReadTree(ctx, r, res.Tree.SHA256)
	p, _, _ := r.blobPath(tr.Files["a.meta"])
	_ = os.Remove(p)
	res2, err := WalkTree(ctx, r, WalkOptions{Root: src, Cache: cache})
	if err != nil || res2.Reused != 0 {
		t.Fatalf("%v %+v", err, res2)
	}
	if ok, _ := r.HasBlob(ctx, tr.Files["a.meta"]); !ok {
		t.Fatal("blob not restored")
	}
}

func TestWalkTreeMatchMissingRootPauseAndCancel(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()
	empty, err := WalkTree(ctx, r, WalkOptions{Root: filepath.Join(t.TempDir(), "nope")})
	if err != nil || empty.FileCount != 0 || empty.Tree.SHA256 == "" {
		t.Fatalf("missing root: %v %+v", err, empty)
	}
	src := t.TempDir()
	mt := time.Unix(1_700_000_000, 0)
	writeFile(t, filepath.Join(src, "a.meta"), "a", mt)
	writeFile(t, filepath.Join(src, "b.txt"), "b", mt)
	res, _ := WalkTree(ctx, r, WalkOptions{Root: src, Match: func(rel string) bool { return filepath.Ext(rel) == ".meta" }})
	if res.FileCount != 1 {
		t.Fatalf("match: %+v", res)
	}

	polls := 0
	res, err = WalkTree(ctx, r, WalkOptions{Root: src, PauseInterval: time.Millisecond, Pause: func() bool { polls++; return polls <= 3 }})
	if err != nil || res.FileCount != 2 || polls < 4 {
		t.Fatalf("pause: polls=%d %v %+v", polls, err, res)
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := WalkTree(cctx, r, WalkOptions{Root: src}); err == nil {
		t.Fatal("cancelled walk should fail")
	}
}

func TestWalkTreeThrottle(t *testing.T) {
	r := newTestRepo(t)
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "big"), string(make([]byte, 20_000)), time.Unix(1_700_000_000, 0))
	start := time.Now()
	if _, err := WalkTree(context.Background(), r, WalkOptions{Root: src, BytesPerSec: 100_000}); err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el < 150*time.Millisecond {
		t.Fatalf("20 kB at 100 kB/s should take ~200ms, took %v", el)
	}
}
