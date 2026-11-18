package manager

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// A season entry split out of a multi-season NZB is not stored until
// markAsCompleted, and its own name is no WebDAV folder before then. The
// import gate opens its files on the internal path by the InfoHash they
// carry (the parent's NZB ID), with no folder name at all, and that path
// resolves to the stored parent, which holds the files.
func TestFFProbeImportGate_SeasonEntryProbesByInfoHash(t *testing.T) {
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	cfg := config.Get()
	cfg.Repair.FFProbeOnImport = true

	dir := t.TempDir()
	argLog := filepath.Join(dir, "args")
	script := "#!/bin/sh\n" +
		"echo \"$@\" >> " + argLog + "\n" +
		"cat <<'EOF'\n" + `{"format":{"duration":"3600.000000"},"streams":[{"codec_type":"video"},{"codec_type":"audio"}]}` + "\nEOF\n"
	bin := filepath.Join(dir, "ffprobe")
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	const base = "http://127.0.0.1:1/webdav/"
	checker := &ffprobeChecker{
		binPath:      bin,
		timeout:      5 * time.Second,
		baseURL:      base,
		logger:       zerolog.Nop(),
		tailIntactFn: func(context.Context, string, string, time.Duration) bool { return true },
	}

	store, err := storage.NewStorage(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	m := &Manager{
		storage: store, config: cfg, ctx: t.Context(), arr: arr.NewStorage(), logger: zerolog.Nop(),
		clients: xsync.NewMap[string, debrid.Client](), processingEntries: xsync.NewMap[string, struct{}](),
	}
	d := NewDownloadManager(m)
	d.ffprobeImportOnce.Do(func() { d.ffprobeImportChecker = checker })

	const nzbID = "parent-nzb-id"
	s1 := &storage.File{Name: "Show.S01E01.mkv", Size: 1 << 20, InfoHash: nzbID}
	s2 := &storage.File{Name: "Show.S02E01.mkv", Size: 1 << 20, InfoHash: nzbID}
	parent := &storage.Entry{
		InfoHash: nzbID, Name: "Show.S01-S02.1080p", Protocol: config.ProtocolNZB,
		Files:     map[string]*storage.File{s1.Name: s1, s2.Name: s2},
		Providers: map[string]*storage.ProviderEntry{},
	}
	// processor.go writes the parent to the main store before download runs.
	if err := store.AddOrUpdate(parent); err != nil {
		t.Fatal(err)
	}
	seasons := convertToMultiSeason(parent, []SeasonInfo{
		{SeasonNumber: 1, Files: []*storage.File{s1}, InfoHash: "season-1-id", Name: "Show.S01.1080p"},
		{SeasonNumber: 2, Files: []*storage.File{s2}, InfoHash: "season-2-id", Name: "Show.S02.1080p"},
	})
	parentFolder := parent.GetFolder()
	season := seasons[0]
	if season.GetFolder() == parentFolder {
		t.Fatalf("test setup: season folder %q equals the parent's", parentFolder)
	}

	if err := d.ffprobeImportGate(season); err != nil {
		t.Fatalf("gate rejected a healthy season file: %v", err)
	}

	raw, err := os.ReadFile(argLog)
	if err != nil {
		t.Fatalf("ffprobe never ran: %v", err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		fields := strings.Fields(line)
		target := fields[len(fields)-1]
		hash, file := internalTargetParts(t, base, target)
		if hash != nzbID || file != s1.Name {
			t.Fatalf("probe opened %s, want %s__internal__/%s/%s", target, base, nzbID, s1.Name)
		}
		for _, folder := range []string{parentFolder, season.GetFolder()} {
			if strings.Contains(target, folder) {
				t.Fatalf("probe URL %s still names folder %q", target, folder)
			}
		}
		entry, f, err := m.InternalFile(hash, file)
		if err != nil {
			t.Fatalf("probe %s does not resolve: %v", target, err)
		}
		if entry.InfoHash != nzbID || f.Name != s1.Name {
			t.Fatalf("probe %s resolved to entry %q file %q", target, entry.InfoHash, f.Name)
		}
	}
}

// internalTargetParts splits a probe URL into the InfoHash and file name of
// the internal WebDAV path, failing the test for any other shape. The path
// is spelled out rather than built from the constant, so the test also pins
// the route the WebDAV handler serves.
func internalTargetParts(t *testing.T, base, target string) (hash, file string) {
	t.Helper()
	rest, ok := strings.CutPrefix(target, base+"__internal__/")
	if !ok {
		t.Fatalf("probe URL %s is not on the internal path %s__internal__/", target, base)
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 2 {
		t.Fatalf("probe URL %s: want __internal__/<infohash>/<file>", target)
	}
	hash, err := url.PathUnescape(parts[0])
	if err != nil {
		t.Fatal(err)
	}
	file, err = url.PathUnescape(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	return hash, file
}

// ffprobe -v error prints only this line for a 404 from our own WebDAV. That
// says nothing about the file: the import is admitted and flagged for the
// next sweep, never rejected and re-grabbed.
func TestVerifyImportFile_WebDAV404IsInconclusive(t *testing.T) {
	saved := ffprobeImportRetryBackoffs
	ffprobeImportRetryBackoffs = []time.Duration{time.Millisecond, time.Millisecond}
	t.Cleanup(func() { ffprobeImportRetryBackoffs = saved })

	for _, stderr := range []string{
		"http://127.0.0.1/webdav/__all__/Show.S01.1080p/f.mkv: Server returned 404 Not Found",
		"http://127.0.0.1/webdav/__all__/Show.S01.1080p/f.mkv: Server returned 401 Unauthorized (authorization failed)",
	} {
		f, _ := failingFFProbe(t, stderr, 1000)
		d := &Downloader{logger: zerolog.Nop()}
		ok, conclusive, reason := d.verifyImportFile(context.Background(), f, &storage.Entry{Name: "E"}, "f.mkv", "webdav-4xx-hash",
			expectedRuntime{Seconds: 3600, Bytes: 4 << 30})
		if !ok || conclusive || reason != "" {
			t.Fatalf("%q: got ok=%v conclusive=%v reason=%q, want admitted but unverified", stderr, ok, conclusive, reason)
		}
	}
}
