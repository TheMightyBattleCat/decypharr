package utils

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDownloadFileTimesOutOnSilentServer(t *testing.T) {
	old := downloadTimeout
	downloadTimeout = 200 * time.Millisecond
	defer func() { downloadTimeout = old }()

	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	defer srv.Close()
	defer close(release)

	start := time.Now()
	_, _, err := DownloadFile(context.Background(), srv.URL+"/a.nzb")
	if err == nil {
		t.Fatal("expected a timeout error")
	}
	if time.Since(start) > 3*time.Second {
		t.Fatalf("took %v, timeout not applied", time.Since(start))
	}
}

func TestDownloadFileCancelEndsAtOnce(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	defer srv.Close()
	defer close(release)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(100 * time.Millisecond); cancel() }()
	start := time.Now()
	_, _, err := DownloadFile(ctx, srv.URL+"/a.nzb")
	if err == nil || time.Since(start) > 3*time.Second {
		t.Fatalf("cancel not honoured: err=%v after %v", err, time.Since(start))
	}
}

func TestDownloadFileSizeCap(t *testing.T) {
	old := maxNZBBytes
	maxNZBBytes = 1 << 20
	defer func() { maxNZBBytes = old }()

	chunk := []byte(strings.Repeat("x", 64<<10))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for r.Context().Err() == nil {
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	_, _, err := DownloadFile(context.Background(), srv.URL+"/a.nzb")
	if !errors.Is(err, ErrDownloadTooLarge) || !strings.Contains(err.Error(), "NZB larger than 1 MiB") {
		t.Fatalf("want size error, got %v", err)
	}
}

func TestDownloadFileSendsUserAgent(t *testing.T) {
	got := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.UserAgent()
		_, _ = w.Write([]byte("<nzb/>"))
	}))
	defer srv.Close()
	_, _, err := DownloadFile(context.Background(), srv.URL+"/a.nzb", UserAgentOption("my-agent/1"))
	if err != nil {
		t.Fatal(err)
	}
	if ua := <-got; ua != "my-agent/1" {
		t.Fatalf("User-Agent = %q", ua)
	}
}

func TestOpenMagnetHttpURLSizeCapAndCancel(t *testing.T) {
	old := maxTorrentBytes
	maxTorrentBytes = 1 << 20
	defer func() { maxTorrentBytes = old }()

	chunk := []byte(strings.Repeat("x", 64<<10))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for r.Context().Err() == nil {
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
	}))
	defer srv.Close()
	_, err := OpenMagnetHttpURL(context.Background(), srv.URL+"/t.torrent", false)
	if !errors.Is(err, ErrDownloadTooLarge) {
		t.Fatalf("want size error, got %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := OpenMagnetHttpURL(ctx, srv.URL+"/t.torrent", false); err == nil {
		t.Fatal("cancelled context should fail")
	}
}
