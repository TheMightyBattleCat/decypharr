package manager

import (
	"context"
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

// A season entry split out of a multi-season NZB is not a WebDAV folder
// until markAsCompleted writes it to the main store; its files are served
// from the parent's folder. The import gate must probe them there.
func TestFFProbeImportGate_SeasonEntryProbesParentFolder(t *testing.T) {
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
	checker := &ffprobeChecker{
		binPath:      bin,
		timeout:      5 * time.Second,
		baseURL:      "http://127.0.0.1:1/",
		logger:       zerolog.Nop(),
		tailIntactFn: func(context.Context, string, string, time.Duration) bool { return true },
	}

	m := &Manager{
		config: cfg, ctx: t.Context(), arr: arr.NewStorage(), logger: zerolog.Nop(),
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
	seasons := convertToMultiSeason(parent, []SeasonInfo{
		{SeasonNumber: 1, Files: []*storage.File{s1}, InfoHash: "season-1-id", Name: "Show.S01.1080p"},
		{SeasonNumber: 2, Files: []*storage.File{s2}, InfoHash: "season-2-id", Name: "Show.S02.1080p"},
	})
	parentFolder := parent.GetFolder()
	season := seasons[0]
	if season.GetFolder() == parentFolder {
		t.Fatalf("test setup: season folder %q equals the parent's", parentFolder)
	}

	if err := d.ffprobeImportGate(season, parentFolder); err != nil {
		t.Fatalf("gate rejected a healthy season file: %v", err)
	}

	raw, err := os.ReadFile(argLog)
	if err != nil {
		t.Fatalf("ffprobe never ran: %v", err)
	}
	want := checker.baseURL + EntryAllFolder + "/" + parentFolder + "/" + s1.Name
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if !strings.Contains(line, want) {
			t.Fatalf("probe did not open the parent's served folder:\n  args: %s\n  want URL: %s", line, want)
		}
		if strings.Contains(line, "/"+season.GetFolder()+"/") {
			t.Fatalf("probe opened the season entry's own folder, which WebDAV does not serve yet: %s", line)
		}
	}
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
		ok, conclusive, reason := d.verifyImportFile(context.Background(), f, &storage.Entry{Name: "E"}, "E", "f.mkv", "webdav-4xx-hash",
			expectedRuntime{Seconds: 3600, Bytes: 4 << 30})
		if !ok || conclusive || reason != "" {
			t.Fatalf("%q: got ok=%v conclusive=%v reason=%q, want admitted but unverified", stderr, ok, conclusive, reason)
		}
	}
}
