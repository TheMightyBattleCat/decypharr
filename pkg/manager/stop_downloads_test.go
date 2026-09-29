package manager

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func newStopDownloadsManager() *Manager {
	m := &Manager{logger: zerolog.Nop()}
	m.ctx, m.cancelDownloads = context.WithCancel(context.Background())
	return m
}

// Shutdown cancels downloads and imports, and waits for them, before the
// HTTP server goes away: an import's ffprobe reads through that server.
func TestStopDownloadsCancelsAndWaitsForTasks(t *testing.T) {
	m := newStopDownloadsManager()
	var finished atomic.Bool
	started := make(chan struct{})
	if !m.startDownloadTask(func() {
		close(started)
		<-m.ctx.Done()
		time.Sleep(50 * time.Millisecond) // winding down after the cancel
		finished.Store(true)
	}) {
		t.Fatal("task refused before shutdown")
	}
	<-started

	m.StopDownloads(5 * time.Second)
	if m.ctx.Err() == nil {
		t.Fatal("download context not cancelled")
	}
	if !finished.Load() {
		t.Fatal("StopDownloads returned before the task ended")
	}
	if m.startDownloadTask(func() {}) {
		t.Fatal("a new task started after StopDownloads")
	}
}

func TestStopDownloadsGivesUpAfterTimeout(t *testing.T) {
	m := newStopDownloadsManager()
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	m.startDownloadTask(func() { <-release }) // ignores the cancel

	start := time.Now()
	m.StopDownloads(100 * time.Millisecond)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("StopDownloads waited %v past its timeout", elapsed)
	}
}
