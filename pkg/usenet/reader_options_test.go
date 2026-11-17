package usenet

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/fs/reader"
)

// A PAR2 patch is the file's real bytes, so the reader a file is opened with
// serves it whatever playback_padding says. Only the zero-fill follows the
// setting.
func TestReaderOptionsServePatchWithPaddingOff(t *testing.T) {
	const deadID = "<dead-normal-read@test>"
	t.Setenv("DECYPHARR_FORCE_MISSING_SEGMENTS", deadID+",<dead-verification-read@test>")

	for _, padding := range []bool{false, true} {
		t.Run(fmt.Sprintf("padding=%v", padding), func(t *testing.T) {
			config.Reset()
			config.SetConfigPath(t.TempDir())
			t.Cleanup(config.Reset)
			config.Get().Repair.PlaybackPadding = &padding

			u := newTestUsenetWithOverlay(t)
			file := &storage.NZBFile{NzbID: "nzb-opts", Name: "movie.mkv"}

			cfg := reader.DefaultConfig()
			for _, opt := range u.readerOptions(file) {
				opt(&cfg)
			}
			if cfg.Overlay == nil {
				t.Error("reader built without the overlay handle")
			}
			if cfg.PaddingOff == padding {
				t.Errorf("PaddingOff = %v with playback_padding = %v", cfg.PaddingOff, padding)
			}

			segments := make([]reader.SegmentMeta, 50)
			for i := range segments {
				segments[i] = reader.SegmentMeta{MessageID: fmt.Sprintf("<opts-%d@test>", i), Number: i + 1, Bytes: 1024}
			}
			segments[10].MessageID = deadID
			patch := bytes.Repeat([]byte{0x5A}, 1024)
			if err := u.overlay.WritePatch(file.NzbID, file.Name, 10, patch); err != nil {
				t.Fatalf("WritePatch: %v", err)
			}

			cfg.DiskPath = t.TempDir()
			cfg.PrefetchAhead = 0
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stats := &reader.ReaderStats{}
			cache, err := reader.NewSegmentCache(ctx, segments, cfg, stats, zerolog.Nop())
			if err != nil {
				t.Fatalf("NewSegmentCache: %v", err)
			}
			defer func() { _ = cache.Close() }()
			// nil NNTP client: a fetch that reaches a provider panics.
			sf := reader.NewSegmentFetcher(ctx, nil, cache, cfg, stats, zerolog.Nop())
			defer sf.Close()

			if err := sf.Fetch(reader.ContextForPlayback(context.Background()), 10); err != nil {
				t.Fatalf("read of the patched segment = %v, want the patch", err)
			}
			if data, ok := cache.Get(10); !ok || !bytes.Equal(data, patch) {
				t.Fatalf("segment 10: cached=%v, want the patch bytes", ok)
			}
		})
	}
}
