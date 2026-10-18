package storage

import (
	"bytes"
	"io"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

type memStreams map[string]*bytes.Buffer

func (m memStreams) open(store string) (io.Writer, error) {
	b := &bytes.Buffer{}
	m[store] = b
	return b, nil
}

func (m memStreams) reader(store string) (io.ReadCloser, error) {
	b, ok := m[store]
	if !ok {
		return nil, nil
	}
	return io.NopCloser(bytes.NewReader(b.Bytes())), nil
}

func newExportEntry(hash, name, category string, status debridTypes.TorrentStatus) *Entry {
	return &Entry{InfoHash: hash, Name: name, Category: category, Status: status}
}

func seedEntry(t *testing.T, s *Storage, e *Entry) string {
	t.Helper()
	if err := s.AddOrUpdate(e); err != nil {
		t.Fatal(err)
	}
	return e.GetFolder()
}

func TestExportImportRoundTripRebuildsAttributes(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	src, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	seedEntry(t, src, newExportEntry("aaa", "One", "movies", "downloaded"))
	seedEntry(t, src, newExportEntry("bbb", "Two", "tv", "downloaded"))
	seedEntry(t, src, newExportEntry("ccc", "Three", "tv", "error"))
	folder := newExportEntry("aaa", "One", "movies", "downloaded").GetFolder()
	if err := src.SaveEntryHealth(&EntryHealth{EntryName: folder, Status: HealthStatus("healthy")}); err != nil {
		t.Fatal(err)
	}
	if err := src.SaveMigrationStatus(&SystemMigrationStatus{}); err != nil {
		t.Fatal(err)
	}

	streams := memStreams{}
	counts, err := src.ExportStores(streams.open, nil)
	if err != nil {
		t.Fatal(err)
	}
	if counts["entries"] != 4 { // three plus __migration_status__
		t.Fatalf("entries exported = %d", counts["entries"])
	}

	dir := t.TempDir()
	rep, err := ImportStores(dir, streams.reader)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OrphanItems != 0 || rep.EntriesWithoutItems != 0 {
		t.Fatalf("unexpected report %+v", rep)
	}
	dst, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	if got := len(dst.entries.KeysBy(attributeCategory, "tv")); got != 2 {
		t.Fatalf("KeysBy category tv = %d", got)
	}
	if got := len(dst.entries.KeysBy(attributeStatus, "error")); got != 1 {
		t.Fatalf("KeysBy status error = %d", got)
	}
	if got := len(dst.repairState.KeysBy(attributeStatus, "healthy")); got != 1 {
		t.Fatalf("repair_state KeysBy status = %d", got)
	}
	if _, err := dst.GetMigrationStatus(); err != nil {
		t.Fatalf("raw __ key lost: %v", err)
	}
	for n, c := range src.StoreCounts() {
		if dst.StoreCounts()[n] != c {
			t.Fatalf("store %s: %d records, want %d", n, dst.StoreCounts()[n], c)
		}
	}
}

func TestImportDropsOrphansAndFlagsEntriesWithoutItems(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	src, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	seedEntry(t, src, newExportEntry("aaa", "One", "movies", "downloaded"))
	folder := seedEntry(t, src, newExportEntry("bbb", "Two", "movies", "downloaded"))
	if err := src.UpdateItem(&EntryItem{Name: "ghost folder", Files: map[string]*File{}}); err != nil {
		t.Fatal(err)
	}
	if err := src.SaveEntryHealth(&EntryHealth{EntryName: "ghost folder", Status: HealthStatus("healthy")}); err != nil {
		t.Fatal(err)
	}
	// Remove the item of one real entry.
	if err := src.entryItems.Delete(folder); err != nil {
		t.Fatal(err)
	}

	streams := memStreams{}
	if _, err := src.ExportStores(streams.open, nil); err != nil {
		t.Fatal(err)
	}
	rep, err := ImportStores(t.TempDir(), streams.reader)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OrphanItems != 1 || rep.OrphanRepairState != 1 || rep.EntriesWithoutItems != 1 {
		t.Fatalf("report = %+v", rep)
	}
}

func TestExportRepeatsEntriesAndItemsWhenEntriesChange(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	src, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	seedEntry(t, src, newExportEntry("aaa", "One", "movies", "downloaded"))

	added := false
	exportAfterPass = func(store string) {
		if store == "entries" && !added {
			added = true
			seedEntry(t, src, newExportEntry("bbb", "Two", "movies", "downloaded"))
		}
	}
	t.Cleanup(func() { exportAfterPass = nil })

	opens := map[string]int{}
	streams := memStreams{}
	counts, err := src.ExportStores(func(store string) (io.Writer, error) {
		opens[store]++
		return streams.open(store)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if opens["entries"] != 2 || opens["items"] != 2 || opens["queue"] != 1 {
		t.Fatalf("passes = %v", opens)
	}
	if counts["entries"] != 2 || counts["items"] != 2 {
		t.Fatalf("counts = %v", counts)
	}
	rep, err := ImportStores(t.TempDir(), streams.reader)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OrphanItems != 0 || rep.EntriesWithoutItems != 0 {
		t.Fatalf("report = %+v", rep)
	}
}

func TestExportRepeatsEntriesAndItemsWhenEntriesChangeDuringItemsPass(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	src, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	seedEntry(t, src, newExportEntry("aaa", "One", "movies", "downloaded"))

	added := false
	exportAfterPass = func(store string) {
		if store == "items" && !added {
			added = true
			seedEntry(t, src, newExportEntry("bbb", "Two", "movies", "downloaded"))
		}
	}
	t.Cleanup(func() { exportAfterPass = nil })

	opens := map[string]int{}
	streams := memStreams{}
	counts, err := src.ExportStores(func(store string) (io.Writer, error) {
		opens[store]++
		return streams.open(store)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if opens["entries"] != 2 || opens["items"] != 2 || opens["queue"] != 1 {
		t.Fatalf("passes = %v", opens)
	}
	if counts["entries"] != 2 || counts["items"] != 2 {
		t.Fatalf("counts = %v", counts)
	}
	rep, err := ImportStores(t.TempDir(), streams.reader)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OrphanItems != 0 || rep.EntriesWithoutItems != 0 {
		t.Fatalf("report = %+v", rep)
	}
}

func TestImportRejectsCorruptStream(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	src, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	seedEntry(t, src, newExportEntry("aaa", "One", "movies", "downloaded"))
	streams := memStreams{}
	if _, err := src.ExportStores(streams.open, nil); err != nil {
		t.Fatal(err)
	}
	b := streams["entries"].Bytes()
	b[len(b)-40] ^= 0xff // inside the last record or trailer
	if _, err := ImportStores(t.TempDir(), streams.reader); err == nil {
		t.Fatal("corrupt stream was accepted")
	}
}
