package sabnzbd

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
)

func TestAddNZBURLLogAndErrorHaveNoKey(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	var buf bytes.Buffer
	s := &SABnzbd{logger: zerolog.New(&buf)}
	_, err = s.addNZBURL(context.Background(), "http://"+addr+"/dl?apikey=SECRET&r=SECRET2", nil, config.DownloadAction(""), "")
	if err == nil {
		t.Fatal("expected an error")
	}
	if strings.Contains(err.Error(), "SECRET") || strings.Contains(buf.String(), "SECRET") {
		t.Fatalf("key leaked: err=%v log=%s", err, buf.String())
	}
	if !strings.Contains(buf.String(), "Failed to download NZB") {
		t.Fatalf("expected the failure to be logged, got %q", buf.String())
	}
}
