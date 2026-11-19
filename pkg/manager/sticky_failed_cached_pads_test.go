package manager

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// cacheRangeRecorder is a DFS MountManager that records every
// ForgetCachedRange call.
type cacheRangeRecorder struct {
	mu     sync.Mutex
	forgot []forgotRange
}

type forgotRange struct {
	folder, file string
	off, length  int64
}

func (r *cacheRangeRecorder) Start(context.Context) error { return nil }
func (r *cacheRangeRecorder) Stop() error                 { return nil }
func (r *cacheRangeRecorder) Stats() map[string]any       { return nil }
func (r *cacheRangeRecorder) IsReady() bool               { return true }
func (r *cacheRangeRecorder) Type() string                { return "test" }
func (r *cacheRangeRecorder) Refresh([]string) error      { return nil }
func (r *cacheRangeRecorder) ForgetCachedRange(folder, file string, off, length int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.forgot = append(r.forgot, forgotRange{folder, file, off, length})
}

func (r *cacheRangeRecorder) snapshot() []forgotRange {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := append([]forgotRange(nil), r.forgot...)
	sort.Slice(out, func(i, j int) bool { return out[i].off < out[j].off })
	return out
}

// A sticky-failed file whose articles came back is cleared by the sweep.
// The zeros served for its padded segments are in the DFS cache with no
// marker, and the clear deletes the only record of them. The clear must
// forget the cached range of every padded segment (widened to the AES
// block chain for an encrypted file) and leave the patched one alone.
func TestClearStickyFailedForgetsCachedPads(t *testing.T) {
	cases := []struct {
		name      string
		encrypted bool
		want      [][2]int64 // [off, length)
	}{
		{name: "plain", want: [][2]int64{{100, 100}, {300, 100}}},
		// [100,200) -> [96,224); [300,400) -> [288,416), clamped to Size 400.
		{name: "encrypted", encrypted: true, want: [][2]int64{{96, 128}, {288, 112}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config.SetConfigPath(t.TempDir())
			t.Cleanup(config.Reset)

			const (
				nzbID = "nzb-s3"
				name  = "Show.S01E01"
				file  = "Show.S01E01.mkv"
			)
			strg, err := storage.NewStorage(t.TempDir())
			if err != nil {
				t.Fatalf("storage.NewStorage: %v", err)
			}
			t.Cleanup(func() { _ = strg.Close() })
			store, err := overlay.NewStore(t.TempDir(), zerolog.Nop())
			if err != nil {
				t.Fatalf("overlay.NewStore: %v", err)
			}
			u, err := usenet.NewWithNZBStorageForTest(store)
			if err != nil {
				t.Fatalf("NewWithNZBStorageForTest: %v", err)
			}

			nf := storage.NZBFile{Name: file, Size: 400, IsStored: true, IsEncrypted: tc.encrypted}
			for i := 0; i < 4; i++ {
				nf.Segments = append(nf.Segments, storage.NZBSegment{
					Number: i + 1, MessageID: "<s" + string(rune('0'+i)) + ">", Bytes: 100,
					StartOffset: int64(i) * 100, EndOffset: int64(i)*100 + 99,
				})
			}
			if err := u.AddNZBForTest(&storage.NZB{ID: nzbID, Name: name, Files: []storage.NZBFile{nf}}); err != nil {
				t.Fatalf("AddNZBForTest: %v", err)
			}
			// Segments 1 and 3 padded, segment 2 padded then patched. The
			// policy's file size is large so three pads stay under its caps.
			for _, i := range []int{1, 2, 3} {
				if d, _ := store.Decide(nzbID, file, i, nf.Segments[i].MessageID, 100, 1_000_000, 0); d != overlay.DecisionPad {
					t.Fatalf("fixture: Decide(%d) = %v, want DecisionPad", i, d)
				}
			}
			if err := store.WritePatch(nzbID, file, 2, sizedPatch("recovered", 100)); err != nil {
				t.Fatalf("WritePatch: %v", err)
			}

			rec := &cacheRangeRecorder{}
			m := &Manager{storage: strg, usenet: u, mountManager: rec}
			m.par2Repair = &Par2Repair{manager: m, logger: zerolog.Nop()}
			r := &Repair{manager: m, logger: zerolog.Nop()}

			r.clearStickyFailed(&storage.Entry{Name: name}, file, nzbID)

			if mf, _ := store.GetManifest(nzbID); mf == nil || mf.Files[file] == nil || len(mf.Files[file].DeadSegments) != 1 {
				t.Fatal("fixture: the clear should leave only the patched record")
			}
			got := rec.snapshot()
			if len(got) != len(tc.want) {
				t.Fatalf("ForgetCachedRange calls = %+v, want ranges %v (both pads, not the patch)", got, tc.want)
			}
			for i, w := range tc.want {
				g := got[i]
				if g.folder != name || g.file != file || g.off != w[0] || g.length != w[1] {
					t.Errorf("forget %d = %+v, want %s/%s [%d,+%d)", i, g, name, file, w[0], w[1])
				}
			}
		})
	}
}
