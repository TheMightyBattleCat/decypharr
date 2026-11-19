package manager

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// padForgetRecorder is a DFS MountManager that records ForgetCachedRange
// and ForgetPaddedRanges calls.
type padForgetRecorder struct {
	cacheRangeRecorder
	mu     sync.Mutex
	padded [][2]string // folder, file
}

func (r *padForgetRecorder) ForgetPaddedRanges(folder, file string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.padded = append(r.padded, [2]string{folder, file})
	return 0
}

func (r *padForgetRecorder) paddedCalls() [][2]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][2]string(nil), r.padded...)
}

const (
	padNZB  = "nzb-pad-marks"
	padName = "Show.S01E02"
	padFile = "Show.S01E02.mkv"
)

// newPadMarksManager stores a 4-segment NZB (100 bytes each) and its entry,
// pads segments 1-3 and patches segment 2, and wires the manager's
// damage-cleared notifier as manager.go does.
func newPadMarksManager(t *testing.T) (*Manager, *overlay.Store, *padForgetRecorder, *storage.Entry) {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)

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
	nf := storage.NZBFile{Name: padFile, Size: 400, IsStored: true}
	for i := 0; i < 4; i++ {
		nf.Segments = append(nf.Segments, storage.NZBSegment{
			Number: i + 1, MessageID: "<p" + string(rune('0'+i)) + ">", Bytes: 100,
			StartOffset: int64(i) * 100, EndOffset: int64(i)*100 + 99,
		})
	}
	if err := u.AddNZBForTest(&storage.NZB{ID: padNZB, Name: padName, Files: []storage.NZBFile{nf}}); err != nil {
		t.Fatalf("AddNZBForTest: %v", err)
	}
	for _, i := range []int{1, 2, 3} {
		if d, _ := store.Decide(padNZB, padFile, i, nf.Segments[i].MessageID, 100, 1_000_000, 0); d != overlay.DecisionPad {
			t.Fatalf("fixture: Decide(%d) = %v, want DecisionPad", i, d)
		}
	}
	if err := store.WritePatch(padNZB, padFile, 2, sizedPatch("recovered", 100)); err != nil {
		t.Fatalf("WritePatch: %v", err)
	}
	entry := &storage.Entry{
		InfoHash: padNZB,
		Name:     padName,
		Protocol: config.ProtocolNZB,
		Files: map[string]*storage.File{
			padFile: {Name: padFile, InfoHash: padNZB, Size: 400, AddedOn: time.Now()},
		},
	}
	if err := strg.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate: %v", err)
	}

	rec := &padForgetRecorder{}
	m := &Manager{storage: strg, usenet: u, mountManager: rec, logger: zerolog.Nop()}
	m.par2Repair = &Par2Repair{manager: m, logger: zerolog.Nop()}
	u.SetOverlayDamageClearedNotifier(m.onOverlayDamageCleared)
	return m, store, rec, entry
}

// OverlayPadRanges reports the dead/padded (not patched) segments' bytes
// within the asked window, clipped to it; nothing for a file with no
// records or an entry that isn't usenet.
func TestOverlayPadRanges(t *testing.T) {
	m, _, _, entry := newPadMarksManager(t)

	if got, want := m.OverlayPadRanges(entry, padFile, 0, 400), [][2]int64{{100, 200}, {300, 400}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("whole file = %v, want %v (segments 1 and 3, not the patched 2)", got, want)
	}
	if got, want := m.OverlayPadRanges(entry, padFile, 150, 350), [][2]int64{{150, 200}, {300, 350}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("window [150,350) = %v, want %v", got, want)
	}
	if got := m.OverlayPadRanges(entry, padFile, 0, 100); got != nil {
		t.Fatalf("clean window = %v, want nil", got)
	}
	if got := m.OverlayPadRanges(entry, "other.mkv", 0, 400); got != nil {
		t.Fatalf("file with no records = %v, want nil", got)
	}
	torrent := *entry
	torrent.Protocol = config.ProtocolTorrent
	if got := m.OverlayPadRanges(&torrent, padFile, 0, 400); got != nil {
		t.Fatalf("torrent entry = %v, want nil", got)
	}
}

// Removing a file's overlay records (here the reclaim/reap path,
// OverlayDeleteFile) forgets the DFS cache's marked zero-fill for it, under
// the entry's cache folder, and invalidates the removed pads' ranges as a
// PAR2 repair does. The patched segment is real bytes and is left alone.
func TestOverlayRecordsRemovedForgetCachedPads(t *testing.T) {
	cases := []struct {
		name   string
		remove func(t *testing.T, m *Manager)
	}{
		{"DeleteFile", func(t *testing.T, m *Manager) {
			if removed, err := m.usenet.OverlayDeleteFile(padNZB, padFile); err != nil || !removed {
				t.Fatalf("OverlayDeleteFile = %v, %v", removed, err)
			}
		}},
		{"ClearFileDamage", func(t *testing.T, m *Manager) {
			if _, err := m.usenet.OverlayClearFileDamage(padNZB, padFile); err != nil {
				t.Fatalf("OverlayClearFileDamage: %v", err)
			}
		}},
		{"DeleteEntry", func(t *testing.T, m *Manager) {
			if err := m.usenet.OverlayDeleteEntry(padNZB); err != nil {
				t.Fatalf("OverlayDeleteEntry: %v", err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, _, rec, entry := newPadMarksManager(t)
			tc.remove(t, m)

			if got, want := rec.paddedCalls(), [][2]string{{entry.GetFolder(), padFile}}; !reflect.DeepEqual(got, want) {
				t.Fatalf("ForgetPaddedRanges calls = %v, want %v", got, want)
			}
			got := rec.snapshot()
			want := []forgotRange{{padName, padFile, 100, 100}, {padName, padFile, 300, 100}}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("ForgetCachedRange calls = %+v, want %+v (both pads, not the patch)", got, want)
			}
		})
	}
}

// The per-file range cache is rebuilt when the record set changes: a pad
// recorded after the first lookup is reported by the next one.
func TestOverlayPadRangesFollowsNewRecords(t *testing.T) {
	m, store, _, entry := newPadMarksManager(t)
	if got, want := m.OverlayPadRanges(entry, padFile, 0, 400), [][2]int64{{100, 200}, {300, 400}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("first lookup = %v, want %v", got, want)
	}
	if d, _ := store.Decide(padNZB, padFile, 0, "<p0>", 100, 1_000_000, 0); d != overlay.DecisionPad {
		t.Fatalf("fixture: Decide(0) = %v, want DecisionPad", d)
	}
	if got, want := m.OverlayPadRanges(entry, padFile, 0, 400), [][2]int64{{0, 100}, {100, 200}, {300, 400}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("after a new pad = %v, want %v", got, want)
	}
}

// countPadManifestReads swaps padMarksManifest for one that counts its
// calls, for the length of the test.
func countPadManifestReads(t *testing.T) *atomic.Int32 {
	t.Helper()
	var n atomic.Int32
	orig := padMarksManifest
	padMarksManifest = func(u *usenet.Usenet, nzbID string) (*overlay.Manifest, error) {
		n.Add(1)
		return orig(u, nzbID)
	}
	t.Cleanup(func() { padMarksManifest = orig })
	return &n
}

// A DFS stream asks for a damaged file's pad ranges after every chunk. With
// no record change between two chunks the manifest is read once; a record
// written between them is read and reported by the next chunk.
func TestOverlayPadRangesReadsManifestOnlyOnChange(t *testing.T) {
	m, store, _, entry := newPadMarksManager(t)
	reads := countPadManifestReads(t)

	if got, want := m.OverlayPadRanges(entry, padFile, 0, 200), [][2]int64{{100, 200}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("chunk 1 = %v, want %v", got, want)
	}
	if got, want := m.OverlayPadRanges(entry, padFile, 200, 400), [][2]int64{{300, 400}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("chunk 2 = %v, want %v", got, want)
	}
	if got := reads.Load(); got != 1 {
		t.Fatalf("manifest reads over two chunks with no record change = %d, want 1", got)
	}
	// A file of the same release with no records is cached too.
	for i := 0; i < 2; i++ {
		if got := m.OverlayPadRanges(entry, "other.mkv", 0, 400); got != nil {
			t.Fatalf("file with no records = %v, want nil", got)
		}
	}
	if got := reads.Load(); got != 2 {
		t.Fatalf("manifest reads after two chunks of a clean file = %d, want 2", got)
	}

	if d, _ := store.Decide(padNZB, padFile, 0, "<p0>", 100, 1_000_000, 0); d != overlay.DecisionPad {
		t.Fatalf("fixture: Decide(0) = %v, want DecisionPad", d)
	}
	if got, want := m.OverlayPadRanges(entry, padFile, 0, 200), [][2]int64{{0, 100}, {100, 200}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("chunk after a new pad = %v, want %v", got, want)
	}
	if got := reads.Load(); got != 3 {
		t.Fatalf("manifest reads after a record change = %d, want 3", got)
	}
	if got, want := m.OverlayPadRanges(entry, padFile, 200, 400), [][2]int64{{300, 400}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("next chunk = %v, want %v", got, want)
	}
	if got := reads.Load(); got != 3 {
		t.Fatalf("manifest reads after the next unchanged chunk = %d, want 3", got)
	}
}
