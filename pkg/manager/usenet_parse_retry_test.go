package manager

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/parser"
)

func TestRetryNZBParseOnlyRetriesErrorsThatMayPass(t *testing.T) {
	m := &Manager{logger: zerolog.Nop()}
	m.jobQueue = NewJobQueue(t.Context(), 1, func(context.Context, *Job) {})
	t.Cleanup(m.jobQueue.Close)

	newJob := func() *Job {
		return &Job{ID: "nzb-1", Type: JobTypeNZB, Entry: &storage.Entry{InfoHash: "nzb-1", Name: "release"}}
	}
	transient := errors.New("NNTP CONNECTION (code 0): connection failed")

	cases := []struct {
		name string
		err  error
		ctx  func() context.Context
	}{
		{"dead release", fmt.Errorf("stat: %w", parser.ErrReleaseUnavailable), t.Context},
		{"invalid NZB", fmt.Errorf("%w: missing <nzb> tag", usenet.ErrInvalidNZB), t.Context},
		{"stopping", transient, func() context.Context {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			return ctx
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			job := newJob()
			if m.retryNZBParse(tc.ctx(), job, tc.err) {
				t.Fatalf("retryNZBParse(%v) = true, want false", tc.err)
			}
			if job.ParseAttempts != 0 {
				t.Fatalf("ParseAttempts = %d, want 0", job.ParseAttempts)
			}
		})
	}

	job := newJob()
	for attempt := 1; attempt < nzbParseMaxAttempts; attempt++ {
		if !m.retryNZBParse(t.Context(), job, transient) {
			t.Fatalf("parse %d: retryNZBParse = false, want a retry", attempt)
		}
		if job.ParseAttempts != attempt {
			t.Fatalf("parse %d: ParseAttempts = %d", attempt, job.ParseAttempts)
		}
	}
	if m.retryNZBParse(t.Context(), job, transient) {
		t.Fatalf("retryNZBParse after %d parses = true, want the download to fail", nzbParseMaxAttempts)
	}
}

func TestRejectDamagedNZBFailsTheQueuedEntry(t *testing.T) {
	store, err := storage.NewStorage(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	m := &Manager{queue: newQueue(store, ""), logger: zerolog.Nop()}
	entry := &storage.Entry{
		InfoHash:  "nzb-1",
		Name:      "release",
		Protocol:  config.ProtocolNZB,
		Category:  "sonarr",
		Status:    debridTypes.TorrentStatusQueued,
		State:     storage.EntryStateDownloading,
		Providers: map[string]*storage.ProviderEntry{},
		Files:     map[string]*storage.File{},
	}
	if err := m.queue.Add(entry); err != nil {
		t.Fatalf("queue entry: %v", err)
	}

	cause := fmt.Errorf("stat: %w", parser.ErrReleaseUnavailable)
	m.rejectDamagedNZB(&Job{ID: entry.InfoHash, Type: JobTypeNZB, Entry: entry}, cause)

	saved, err := m.queue.GetTorrent(entry.InfoHash)
	if err != nil {
		t.Fatalf("read entry back: %v", err)
	}
	if saved.State != storage.EntryStateError || saved.Status != debridTypes.TorrentStatusError {
		t.Fatalf("entry state/status = %s/%s, want the failed state the Arr blocklists", saved.State, saved.Status)
	}
	if saved.LastError != cause.Error() {
		t.Fatalf("LastError = %q, want %q", saved.LastError, cause.Error())
	}
}
