package manager

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/parser"
)

var errNoEligibleProviders = errors.New("no eligible providers available")

// parseWaitFixture is a queued NZB entry with a staged file, a manager whose
// usenet parse and provider check are stubs, and a job queue that only
// records retries (its timers are far longer than the test).
type parseWaitFixture struct {
	m        *Manager
	job      *Job
	retries  chan *Job
	parses   int
	parseErr func(call int) error
	ready    func() (bool, string)
}

func newParseWaitFixture(t *testing.T) *parseWaitFixture {
	t.Helper()
	store, err := storage.NewStorage(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	staged := filepath.Join(t.TempDir(), "release.nzb")
	if err := os.WriteFile(staged, []byte("<nzb/>"), 0o600); err != nil {
		t.Fatalf("stage NZB: %v", err)
	}
	entry := &storage.Entry{
		InfoHash:  "nzb-1",
		Name:      "release",
		Protocol:  config.ProtocolNZB,
		Category:  "sonarr",
		Magnet:    staged,
		Status:    debridTypes.TorrentStatusQueued,
		State:     storage.EntryStateDownloading,
		Providers: map[string]*storage.ProviderEntry{},
		Files:     map[string]*storage.File{},
	}
	m := &Manager{queue: newQueue(store, ""), logger: zerolog.Nop()}
	if err := m.queue.Add(entry); err != nil {
		t.Fatalf("queue entry: %v", err)
	}
	f := &parseWaitFixture{m: m, retries: make(chan *Job, 16)}
	origDelay := nzbProviderWaitDelay
	nzbProviderWaitDelay = time.Millisecond
	t.Cleanup(func() { nzbProviderWaitDelay = origDelay })
	m.jobQueue = NewJobQueue(t.Context(), 1, func(_ context.Context, job *Job) { f.retries <- job })
	t.Cleanup(m.jobQueue.Close)

	f.job = &Job{
		ID:      entry.InfoHash,
		Type:    JobTypeNZB,
		Entry:   entry,
		Request: &ImportRequest{Name: "release", Arr: &arr.Arr{Name: "sonarr"}},
	}
	f.ready = providersReady
	f.parseErr = func(int) error { return errNoEligibleProviders }
	stubNZBProviders(t, func() (bool, string) { return f.ready() })
	orig := parseQueuedNZB
	parseQueuedNZB = func(*Manager, context.Context, string, string, []byte, string) (*storage.NZB, map[string]*parser.FileGroup, error) {
		f.parses++
		return nil, nil, f.parseErr(f.parses)
	}
	t.Cleanup(func() { parseQueuedNZB = orig })
	return f
}

// expectRetry waits for the job to come back through the job queue.
func (f *parseWaitFixture) expectRetry(t *testing.T, what string) {
	t.Helper()
	select {
	case job := <-f.retries:
		if job != f.job {
			t.Fatalf("%s: a different job was resubmitted", what)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: the job was not scheduled again; it would stay queued with nothing to parse it", what)
	}
}

func (f *parseWaitFixture) state(t *testing.T) storage.TorrentState {
	t.Helper()
	saved, err := f.m.queue.GetTorrent(f.job.Entry.InfoHash)
	if err != nil {
		t.Fatalf("read entry back: %v", err)
	}
	return saved.State
}

// While no provider can take the parse STAT (every primary over its quota,
// every provider cooling down after failed dials), the queued NZB waits with
// backoff and is never failed: past the three parse attempts that used to
// fail it, and without counting the waits as attempts.
func TestQueuedNZBWaitsWhileNoProviderCanServe(t *testing.T) {
	f := newParseWaitFixture(t)
	checks := 0
	f.ready = func() (bool, string) {
		checks++
		if checks <= 5 {
			return false, "every primary provider is cooling down after failed connections"
		}
		return true, ""
	}
	for i := 1; i <= 5; i++ {
		f.m.processJob(t.Context(), f.job)
		if st := f.state(t); st == storage.EntryStateError {
			t.Fatalf("wait %d: entry failed (%s) while no provider could be asked; the Arr would blocklist it", i, f.job.Entry.LastError)
		}
		if f.job.ProviderWaits != i || f.job.ParseAttempts != 0 {
			t.Fatalf("wait %d: ProviderWaits=%d ParseAttempts=%d, want %d and 0", i, f.job.ProviderWaits, f.job.ParseAttempts, i)
		}
		if f.parses != 0 {
			t.Fatalf("wait %d: parsed %d time(s) with no provider able to answer", i, f.parses)
		}
		f.expectRetry(t, fmt.Sprintf("wait %d", i))
	}
	// A provider can serve again: the sixth run parses. Its error is an
	// ordinary one now (the provider check passes), so it counts as today.
	f.parseErr = func(int) error { return errors.New("NNTP CONNECTION (code 0): connection reset") }
	f.m.processJob(t.Context(), f.job)
	if f.parses != 1 {
		t.Fatalf("sixth run parsed %d time(s), want 1", f.parses)
	}
	if f.job.ParseAttempts != 1 || f.job.ProviderWaits != 5 {
		t.Fatalf("after a counted error: ParseAttempts=%d ProviderWaits=%d, want 1 and 5", f.job.ParseAttempts, f.job.ProviderWaits)
	}
	if st := f.state(t); st == storage.EntryStateError {
		t.Fatalf("entry failed on its first counted parse error")
	}
}

// A parse that fails because no provider could be asked (the check passed a
// moment before, then every pool went into its dial cooldown) waits rather
// than spending one of the three attempts; once they are spent the next
// such failure still waits instead of failing the download.
func TestParseErrorWithNoProviderLeftWaitsUncounted(t *testing.T) {
	f := newParseWaitFixture(t)
	up := true
	f.ready = func() (bool, string) {
		if up {
			up = false // pre-parse check passes, the one after the error fails
			return true, ""
		}
		return false, "no primary provider can take requests"
	}
	f.job.ParseAttempts = nzbParseMaxAttempts - 1 // the next counted error would fail it
	for i := 1; i <= 3; i++ {
		up = true
		f.m.processJob(t.Context(), f.job)
		if st := f.state(t); st == storage.EntryStateError {
			t.Fatalf("run %d: entry failed (%s) on a provider-level error", i, f.job.Entry.LastError)
		}
		if f.parses != i || f.job.ProviderWaits != i || f.job.ParseAttempts != nzbParseMaxAttempts-1 {
			t.Fatalf("run %d: parses=%d ProviderWaits=%d ParseAttempts=%d", i, f.parses, f.job.ProviderWaits, f.job.ParseAttempts)
		}
		f.expectRetry(t, fmt.Sprintf("run %d", i))
	}
}

// Controls: what the parse says about the release itself still fails at
// once, and an ordinary error with a provider able to serve still counts
// toward the three attempts and then fails.
func TestParseVerdictsStillFailWithProvidersDown(t *testing.T) {
	t.Run("complete 430 rejects", func(t *testing.T) {
		f := newParseWaitFixture(t)
		f.parseErr = func(int) error {
			return fmt.Errorf("failed to stat segment: %w", parser.ErrReleaseUnavailable)
		}
		f.m.processJob(t.Context(), f.job)
		if st := f.state(t); st != storage.EntryStateError {
			t.Fatalf("state = %s, want the failed state the Arr blocklists", st)
		}
		if f.job.ProviderWaits != 0 {
			t.Fatalf("ProviderWaits = %d, want 0", f.job.ProviderWaits)
		}
	})
	t.Run("invalid NZB fails", func(t *testing.T) {
		f := newParseWaitFixture(t)
		calls := 0
		f.ready = func() (bool, string) {
			calls++
			return calls == 1, "no primary provider can take requests"
		}
		f.parseErr = func(int) error { return fmt.Errorf("%w: missing <nzb> tag", usenet.ErrInvalidNZB) }
		f.m.processJob(t.Context(), f.job)
		if st := f.state(t); st != storage.EntryStateError {
			t.Fatalf("state = %s, want failed", st)
		}
		if f.job.ProviderWaits != 0 {
			t.Fatalf("ProviderWaits = %d, want 0", f.job.ProviderWaits)
		}
	})
	t.Run("ordinary error with providers up counts", func(t *testing.T) {
		f := newParseWaitFixture(t)
		for i := 1; i < nzbParseMaxAttempts; i++ {
			f.m.processJob(t.Context(), f.job)
			if f.job.ParseAttempts != i {
				t.Fatalf("parse %d: ParseAttempts = %d", i, f.job.ParseAttempts)
			}
		}
		f.m.processJob(t.Context(), f.job)
		if st := f.state(t); st != storage.EntryStateError {
			t.Fatalf("state after %d parses = %s, want failed", nzbParseMaxAttempts, st)
		}
		if f.job.ProviderWaits != 0 {
			t.Fatalf("ProviderWaits = %d, want 0", f.job.ProviderWaits)
		}
	})
}

func TestNZBProviderWaitDoublesToTheCap(t *testing.T) {
	want := []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 15 * time.Minute, 15 * time.Minute}
	for waits, w := range want {
		if got := nzbProviderWait(waits); got != w {
			t.Errorf("nzbProviderWait(%d) = %s, want %s", waits, got, w)
		}
	}
	if got := nzbProviderWait(1000); got != nzbProviderWaitCap {
		t.Errorf("nzbProviderWait(1000) = %s, want the cap", got)
	}
}
