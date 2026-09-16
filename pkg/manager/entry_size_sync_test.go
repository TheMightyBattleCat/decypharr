package manager

import (
	"fmt"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

type fakeMetaSource map[string]*storage.NZB

func (f fakeMetaSource) GetNZBHeader(id string) (*storage.NZB, error) { return f.GetNZB(id) }

func (f fakeMetaSource) GetNZB(id string) (*storage.NZB, error) {
	if n, ok := f[id]; ok {
		return n, nil
	}
	return nil, fmt.Errorf("nzb not found: %s", id)
}

// metaFile builds a meta file of n articles of per bytes each, contiguous from 0.
func metaFile(name string, n int, per int64) storage.NZBFile {
	f := storage.NZBFile{Name: name, Size: int64(n) * per}
	for i := 0; i < n; i++ {
		start := int64(i) * per
		f.Segments = append(f.Segments, storage.NZBSegment{Number: i + 1, Bytes: per, StartOffset: start, EndOffset: start + per - 1})
	}
	return f
}

func newEntrySizeSyncStorage(t *testing.T) *storage.Storage {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	s, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("storage.NewStorage: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func addNZBEntry(t *testing.T, s *storage.Storage, hash, name string, protocol config.Protocol, sizes map[string]int64) *storage.EntryItem {
	t.Helper()
	added := time.Date(2026, 5, 17, 10, 59, 13, 0, time.UTC)
	e := &storage.Entry{InfoHash: hash, Name: name, Protocol: protocol, AddedOn: added, Files: map[string]*storage.File{}}
	for fn, size := range sizes {
		e.Files[fn] = &storage.File{Name: fn, InfoHash: hash, Size: size, AddedOn: added}
	}
	if err := s.AddOrUpdate(e); err != nil {
		t.Fatalf("AddOrUpdate: %v", err)
	}
	item, err := s.GetEntryItem(e.GetFolder())
	if err != nil {
		t.Fatalf("GetEntryItem: %v", err)
	}
	return item
}

// Tide on Sark S02 on a production install: the entry held one RAR volume's size while
// the meta held every volume. The sync must fix the entry and the folder
// item that WebDAV and the mount serve.
func TestSyncEntrySizesFromMetaCorrectsOneVolumeSize(t *testing.T) {
	s := newEntrySizeSyncStorage(t)
	const hash = "9a78d2cf"
	full := metaFile("e01.mkv", 85, 1000)
	other := metaFile("e02.mkv", 85, 1000)
	src := fakeMetaSource{hash: {ID: hash, Files: []storage.NZBFile{full, other}}}
	item := addNZBEntry(t, s, hash, "Tide.On.Sark.S02", config.ProtocolNZB, map[string]int64{"e01.mkv": 1000, "e02.mkv": other.Size})

	if !syncEntrySizesFromMeta(s, src, item, zerolog.Nop()) {
		t.Fatal("sync reported no change for a stale size")
	}

	entry, err := s.Get(hash)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := entry.Files["e01.mkv"].Size; got != full.Size {
		t.Errorf("entry size = %d, want %d", got, full.Size)
	}
	got, err := s.GetEntryItem(item.Name)
	if err != nil {
		t.Fatalf("GetEntryItem: %v", err)
	}
	if size := got.Files["e01.mkv"].Size; size != full.Size {
		t.Errorf("item size = %d, want %d", size, full.Size)
	}
	if size := got.Files["e02.mkv"].Size; size != other.Size {
		t.Errorf("untouched file size = %d, want %d", size, other.Size)
	}
	if storage.EntryItemRepairFingerprint(got) == storage.EntryItemRepairFingerprint(item) {
		t.Error("fingerprint unchanged, so a decode stamp on the short file would carry over")
	}

	// A second pass has nothing to do.
	if syncEntrySizesFromMeta(s, src, got, zerolog.Nop()) {
		t.Error("second sync reported a change")
	}
}

// The meta size is only trusted when its articles cover exactly that size.
func TestSyncEntrySizesFromMetaLeavesUncoveredLayout(t *testing.T) {
	cases := map[string]func(f *storage.NZBFile){
		"gap":            func(f *storage.NZBFile) { f.Segments[3].StartOffset++ },
		"short layout":   func(f *storage.NZBFile) { f.Size += 10 },
		"no articles":    func(f *storage.NZBFile) { f.Segments = nil },
		"offset nonzero": func(f *storage.NZBFile) { f.Segments[0].StartOffset = 5 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s := newEntrySizeSyncStorage(t)
			const hash = "h1"
			f := metaFile("a.mkv", 10, 1000)
			mutate(&f)
			src := fakeMetaSource{hash: {ID: hash, Files: []storage.NZBFile{f}}}
			item := addNZBEntry(t, s, hash, "Entry", config.ProtocolNZB, map[string]int64{"a.mkv": 4000})

			if syncEntrySizesFromMeta(s, src, item, zerolog.Nop()) {
				t.Fatal("sync changed a file whose meta layout does not cover its size")
			}
			entry, _ := s.Get(hash)
			if got := entry.Files["a.mkv"].Size; got != 4000 {
				t.Errorf("entry size = %d, want 4000", got)
			}
		})
	}
}

func TestSyncEntrySizesFromMetaIgnoresTorrentsAndMatchingSizes(t *testing.T) {
	s := newEntrySizeSyncStorage(t)
	f := metaFile("a.mkv", 10, 1000)
	src := fakeMetaSource{
		"torrent": {ID: "torrent", Files: []storage.NZBFile{f}},
		"same":    {ID: "same", Files: []storage.NZBFile{f}},
	}
	torrentItem := addNZBEntry(t, s, "torrent", "Torrent.Entry", config.ProtocolTorrent, map[string]int64{"a.mkv": 4000})
	sameItem := addNZBEntry(t, s, "same", "Same.Entry", config.ProtocolNZB, map[string]int64{"a.mkv": f.Size})

	if syncEntrySizesFromMeta(s, src, torrentItem, zerolog.Nop()) {
		t.Error("sync changed a torrent entry")
	}
	if syncEntrySizesFromMeta(s, src, sameItem, zerolog.Nop()) {
		t.Error("sync reported a change for matching sizes")
	}
}
