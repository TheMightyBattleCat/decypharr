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
	"github.com/sirrobot01/decypharr/pkg/notifications"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// A multi-season NZB is split into one entry per season, and each season's
// files pass the ffprobe import gate when that season completes. The parent
// is then completed only to leave the queue: its files are the seasons'
// files, already gated, so they must not be probed a second time (every
// probe reads through Usenet).
func TestDownload_SplitParentDoesNotReprobeSeasonFiles(t *testing.T) {
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
		storage: store, config: cfg, ctx: t.Context(), queue: newQueue(store, ""),
		arr: arr.NewStorage(), logger: zerolog.Nop(),
		Notifications: notifications.New(&cfg.Notifications, zerolog.Nop()),
		clients:       xsync.NewMap[string, debrid.Client](), processingEntries: xsync.NewMap[string, struct{}](),
	}
	d := NewDownloadManager(m)
	m.downloader = d
	d.ffprobeImportOnce.Do(func() { d.ffprobeImportChecker = checker })

	const nzbID = "parent-nzb-id"
	s1 := &storage.File{Name: "Show.S01E01.mkv", Size: 1 << 20, InfoHash: nzbID}
	s2 := &storage.File{Name: "Show.S02E01.mkv", Size: 1 << 20, InfoHash: nzbID}
	parent := &storage.Entry{
		InfoHash: nzbID, Name: "Show.S01-S02.1080p", Protocol: config.ProtocolNZB,
		State: storage.EntryStateDownloading, Action: config.DownloadActionNone, SavePath: t.TempDir(),
		Files:     map[string]*storage.File{s1.Name: s1, s2.Name: s2},
		Providers: map[string]*storage.ProviderEntry{},
	}
	if err := store.AddOrUpdate(parent); err != nil {
		t.Fatal(err)
	}
	if err := m.queue.Add(parent); err != nil {
		t.Fatal(err)
	}

	// Baseline: how many ffprobe runs one gate pass over one file costs
	// (metadata and decode passes), measured on a single-season entry.
	single := &storage.Entry{
		InfoHash: "single-id", Name: "Show.S01.1080p", Protocol: config.ProtocolNZB,
		Files:     map[string]*storage.File{"Show.S01E01.mkv": {Name: "Show.S01E01.mkv", Size: 1 << 20, InfoHash: nzbID}},
		Providers: map[string]*storage.ProviderEntry{},
	}
	if err := d.ffprobeImportGate(single); err != nil {
		t.Fatalf("baseline gate: %v", err)
	}
	baseline := len(probeLog(t, argLog))
	if baseline == 0 {
		t.Fatal("baseline gate ran no probe")
	}
	if err := os.Remove(argLog); err != nil {
		t.Fatal(err)
	}

	if err := d.download(parent); err != nil {
		t.Fatalf("download: %v", err)
	}

	probes := map[string]int{}
	for _, target := range probeLog(t, argLog) {
		_, file := internalTargetParts(t, base, target)
		probes[file]++
	}
	for _, name := range []string{s1.Name, s2.Name} {
		if probes[name] != baseline {
			t.Errorf("%s probed %d times, want %d (one gate pass, under its season only)", name, probes[name], baseline)
		}
	}
	if len(probes) != 2 {
		t.Errorf("probed files %v, want exactly the two season files", probes)
	}
}

// probeLog returns the URL each logged ffprobe run opened.
func probeLog(t *testing.T, argLog string) []string {
	t.Helper()
	raw, err := os.ReadFile(argLog)
	if err != nil {
		t.Fatalf("ffprobe never ran: %v", err)
	}
	var targets []string
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		fields := strings.Fields(line)
		targets = append(targets, fields[len(fields)-1])
	}
	return targets
}
