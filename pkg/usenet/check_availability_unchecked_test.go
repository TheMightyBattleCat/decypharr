package usenet

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/nntp"
)

// A BatchStat that fails as a whole (here: the client is closed) asked no
// server anything: checkAvailability reports the file unchecked, neither
// available (nil) nor missing.
func TestCheckAvailabilityTopLevelStatErrorIsUnchecked(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	cfg := config.Get()
	oldUsenet := cfg.Usenet
	cfg.Usenet = config.Usenet{
		Providers: []config.UsenetProvider{
			{Host: "127.0.0.1", Port: ln.Addr().(*net.TCPAddr).Port, MaxConnections: 1, Priority: 1},
		},
		MaxConnections: 1,
	}
	t.Cleanup(func() { cfg.Usenet = oldUsenet })
	client, err := nntp.NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	_ = client.Close()
	u := &Usenet{nntp: client, logger: zerolog.Nop()}

	if _, statErr := client.BatchStat(context.Background(), []string{"<a@test>"}); statErr == nil {
		t.Fatalf("BatchStat on a closed client returned no error; the test no longer reaches the top-level error branch")
	}
	err = u.checkAvailability(context.Background(), "file.mkv", []string{"<a@test>", "<b@test>"})
	if !errors.Is(err, ErrAvailabilityUnchecked) {
		t.Fatalf("checkAvailability = %v, want ErrAvailabilityUnchecked", err)
	}
	if errors.Is(err, customerror.UsenetSegmentMissingError) {
		t.Fatalf("checkAvailability = %v: an unasked file must not read as missing", err)
	}
}
