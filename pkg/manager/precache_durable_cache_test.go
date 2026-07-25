package manager

import (
	"context"
	"testing"
)

// fakeWrite records one WriteCachedRange call.
type fakeWrite struct {
	entryName, filename string
	fileSize            int64
	p                   []byte
	off                 int64
}

// recordingWriter satisfies both MountManager and dfsCacheRangeWriter, like
// pkg/mount/dfs.Manager does in production - a scriptable fake mirroring
// fakeCacheReader's role for the read seam (par2_cache_source_test.go),
// proving cacheWriter() resolves the write seam via the same
// MountManager() type-assertion pattern without needing a real DFS mount.
type recordingWriter struct {
	writes []fakeWrite
}

func (r *recordingWriter) Start(ctx context.Context) error { return nil }
func (r *recordingWriter) Stop() error                     { return nil }
func (r *recordingWriter) Stats() map[string]any           { return nil }
func (r *recordingWriter) IsReady() bool                   { return true }
func (r *recordingWriter) Type() string                    { return "fake" }
func (r *recordingWriter) Refresh(dirs []string) error     { return nil }

func (r *recordingWriter) WriteCachedRange(entryName, filename string, fileSize int64, p []byte, off int64) error {
	cp := make([]byte, len(p))
	copy(cp, p)
	r.writes = append(r.writes, fakeWrite{entryName: entryName, filename: filename, fileSize: fileSize, p: cp, off: off})
	return nil
}

func TestPrecacheCacheWriterResolvesViaMountManagerTypeAssertion(t *testing.T) {
	m := &Manager{}
	p := &Precache{manager: m}

	if got := p.cacheWriter(); got != nil {
		t.Fatalf("cacheWriter() with no MountManager set = %v, want nil", got)
	}

	fw := &recordingWriter{}
	m.SetMountManager(fw)
	got := p.cacheWriter()
	if got == nil {
		t.Fatalf("cacheWriter() = nil, want a resolved dfsCacheRangeWriter")
	}
	if err := got.WriteCachedRange("Entry", "file.mkv", 100, []byte("abc"), 10); err != nil {
		t.Fatalf("WriteCachedRange: %v", err)
	}
	if len(fw.writes) != 1 {
		t.Fatalf("expected 1 recorded write, got %d", len(fw.writes))
	}
	w := fw.writes[0]
	if w.entryName != "Entry" || w.filename != "file.mkv" || w.fileSize != 100 || w.off != 10 || string(w.p) != "abc" {
		t.Fatalf("recorded write = %+v, want {Entry file.mkv 100 abc 10}", w)
	}
}

// TestPrecacheCacheWriterNilWhenMountManagerDoesntImplementIt proves a
// MountManager that doesn't satisfy dfsCacheRangeWriter (e.g. rclone mode's
// stub) resolves to nil rather than panicking - the same nil-safe contract
// dfsCacheRangeReader relies on.
func TestPrecacheCacheWriterNilWhenMountManagerDoesntImplementIt(t *testing.T) {
	m := &Manager{}
	p := &Precache{manager: m}
	m.SetMountManager(NewStubMountManager())

	if got := p.cacheWriter(); got != nil {
		t.Fatalf("cacheWriter() with a non-writer MountManager = %v, want nil", got)
	}
}
