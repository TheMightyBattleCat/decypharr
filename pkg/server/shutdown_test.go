package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/stats"
)

// startTestServer runs Server.Start on a free port with a /stream endpoint
// that writes until the client goes away. It returns the address, the cancel
// func and the channel Start's result arrives on.
func startTestServer(t *testing.T) (string, context.CancelFunc, chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	dir := t.TempDir()
	cfg := fmt.Sprintf(`{"bind_address":"127.0.0.1","port":"%d"}`, port)
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(cfg), 0600); err != nil {
		t.Fatal(err)
	}
	config.Reset()
	config.SetConfigPath(dir)
	t.Cleanup(config.Reset)

	r := chi.NewRouter()
	r.Get("/stream", func(w http.ResponseWriter, req *http.Request) {
		fl, _ := w.(http.Flusher)
		buf := make([]byte, 1024)
		for req.Context().Err() == nil {
			if _, err := w.Write(buf); err != nil {
				return
			}
			if fl != nil {
				fl.Flush()
			}
			time.Sleep(20 * time.Millisecond)
		}
	})
	r.Get("/ping", func(w http.ResponseWriter, req *http.Request) {})
	s := &Server{router: r, logger: zerolog.Nop(), stats: &stats.Collector{}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Start(ctx) }()
	for i := 0; i < 100; i++ {
		resp, err := http.Get("http://" + addr + "/ping")
		if err == nil {
			_ = resp.Body.Close()
			return addr, cancel, done
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("server did not come up")
	return "", nil, nil
}

func TestShutdownDoesNotWaitForeverForOpenStream(t *testing.T) {
	old := shutdownGrace
	shutdownGrace = 300 * time.Millisecond
	t.Cleanup(func() { shutdownGrace = old })

	addr, cancel, done := startTestServer(t)
	resp, err := http.Get("http://" + addr + "/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if _, err := io.ReadFull(resp.Body, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(shutdownGrace + 700*time.Millisecond):
		t.Fatal("Start still blocked on the open stream after the grace period")
	}
	// The viewer's read ends once the connection is closed.
	readDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		close(readDone)
	}()
	select {
	case <-readDone:
	case <-time.After(2 * time.Second):
		t.Fatal("the client's read did not end after shutdown")
	}
}

func TestShutdownWithNoOpenResponseReturnsAtOnce(t *testing.T) {
	old := shutdownGrace
	shutdownGrace = 5 * time.Second
	t.Cleanup(func() { shutdownGrace = old })

	_, cancel, done := startTestServer(t)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Start did not return promptly with no open response")
	}
}
