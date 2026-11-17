package storage

import (
	"reflect"
	"sort"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
	"google.golang.org/protobuf/proto"
)

func setFolderNaming(t *testing.T, naming config.WebDavFolderNaming) {
	t.Helper()
	if _, err := config.Update(func(c *config.Config) error {
		c.FolderNaming = naming
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func storeKeys(t *testing.T, s *Storage, name string) []string {
	t.Helper()
	var keys []string
	if err := s.storeByName(name).ForEach(func(k string, _ []byte) error {
		keys = append(keys, k)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	sort.Strings(keys)
	return keys
}

// A database backed up on an install with a non-default folder_naming is
// imported while the running config still has the default: a restore stages
// the database before the snapshot's config is moved into place. Every item
// and health record must be kept, and after the restart under the restored
// config each entry is listed once, under the folder it had.
func TestImportUnderAnotherFolderNamingKeepsItemsAndHealth(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	setFolderNaming(t, config.WebdavUseHash)

	src, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	entries := []*Entry{
		{InfoHash: "aaa", Name: "One", Category: "movies", Status: "downloaded", Protocol: config.ProtocolTorrent,
			Files: map[string]*File{"one.mkv": {Name: "one.mkv", InfoHash: "aaa", Size: 1}}},
		{InfoHash: "bbb", Name: "Two", Category: "tv", Status: "downloaded", Protocol: config.ProtocolNZB,
			Files: map[string]*File{"two.mkv": {Name: "two.mkv", InfoHash: "bbb", Size: 1}}},
		// A season entry split out of a pack: its files keep the pack's
		// InfoHash, and the pack itself is not in the snapshot.
		{InfoHash: "ccc", Name: "Show S02", Category: "tv", Status: "downloaded", Protocol: config.ProtocolNZB,
			Files: map[string]*File{"s02e01.mkv": {Name: "s02e01.mkv", InfoHash: "pack", Size: 1}}},
	}
	for _, e := range entries {
		if err := src.AddOrUpdate(e); err != nil {
			t.Fatal(err)
		}
		if err := src.SaveEntryHealth(&EntryHealth{
			EntryName:   e.GetFolder(),
			Status:      HealthStatus("broken"),
			BrokenFiles: []BrokenFile{{EntryName: e.GetFolder(), FileName: "x.mkv", Reason: "dead"}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"aaa", "bbb", "ccc"}
	if got := storeKeys(t, src, "items"); !reflect.DeepEqual(got, want) {
		t.Fatalf("source items = %v, want %v", got, want)
	}
	streams := memStreams{}
	if _, err := src.ExportStores(streams.open, nil); err != nil {
		t.Fatal(err)
	}

	// The restore runs under the default naming.
	setFolderNaming(t, "")
	dir := t.TempDir()
	rep, err := ImportStores(dir, streams.reader)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OrphanItems != 0 || rep.OrphanRepairState != 0 || rep.EntriesWithoutItems != 0 {
		t.Errorf("import report = %+v, want nothing dropped or flagged", rep)
	}
	// The restore reopens the staged folder the way startup will.
	staged, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := staged.Close(); err != nil {
		t.Fatal(err)
	}

	// The snapshot's config is moved into place and the app restarts.
	setFolderNaming(t, config.WebdavUseHash)
	again, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	for _, name := range []string{"items", "repair_state"} {
		if got := storeKeys(t, again, name); !reflect.DeepEqual(got, want) {
			t.Errorf("%s keys after restart = %v, want %v (each folder once)", name, got, want)
		}
	}
	for _, k := range want {
		h, err := again.GetEntryHealth(k)
		if err != nil {
			t.Errorf("health %s: %v", k, err)
			continue
		}
		if h.Status != HealthStatus("broken") || len(h.BrokenFiles) != 1 || h.BrokenFiles[0].FileName != "x.mkv" {
			t.Errorf("health %s = status %q broken %v, want the backed-up record", k, h.Status, h.BrokenFiles)
		}
	}
}

// Two seasons split out of one pack carry the pack's InfoHash on their
// files. When one season's entry is missing from the snapshot (deleted while
// the export ran), its item and health record are orphans even though the
// other season shares the pack's InfoHash.
func TestImportDropsMissingSeasonOfSharedPack(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	setFolderNaming(t, config.WebdavUseHash)

	src, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	for _, e := range []*Entry{
		{InfoHash: "s01", Name: "Show S01", Category: "tv", Status: "downloaded", Protocol: config.ProtocolNZB,
			Files: map[string]*File{"s01e01.mkv": {Name: "s01e01.mkv", InfoHash: "pack", Size: 1}}},
		{InfoHash: "s02", Name: "Show S02", Category: "tv", Status: "downloaded", Protocol: config.ProtocolNZB,
			Files: map[string]*File{"s02e01.mkv": {Name: "s02e01.mkv", InfoHash: "pack", Size: 1}}},
	} {
		if err := src.AddOrUpdate(e); err != nil {
			t.Fatal(err)
		}
		if err := src.SaveEntryHealth(&EntryHealth{EntryName: e.GetFolder(), Status: HealthStatus("healthy")}); err != nil {
			t.Fatal(err)
		}
	}
	// The season's entry is gone; its item and health record are not.
	if err := src.entries.Delete("s02"); err != nil {
		t.Fatal(err)
	}
	streams := memStreams{}
	if _, err := src.ExportStores(streams.open, nil); err != nil {
		t.Fatal(err)
	}

	setFolderNaming(t, "")
	dir := t.TempDir()
	rep, err := ImportStores(dir, streams.reader)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OrphanItems != 1 || rep.OrphanRepairState != 1 || rep.EntriesWithoutItems != 0 {
		t.Errorf("import report = %+v, want the missing season's item and health dropped", rep)
	}
	setFolderNaming(t, config.WebdavUseHash)
	again, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	for _, name := range []string{"items", "repair_state"} {
		if got, want := storeKeys(t, again, name), []string{"s01"}; !reflect.DeepEqual(got, want) {
			t.Errorf("%s keys = %v, want %v", name, got, want)
		}
	}
}

// An entry with no protocol is in the current metadata format once it has
// been saved. Opening the store again, here under another folder naming as
// after a restore, must not re-save it: a re-save files it in the name index
// a second time, under the running naming.
func TestOpenDoesNotReindexEntryWithoutProtocol(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	setFolderNaming(t, config.WebdavUseHash)

	dir := t.TempDir()
	s, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AddOrUpdate(&Entry{InfoHash: "aaa", Name: "One", Category: "movies", Status: "downloaded",
		Files: map[string]*File{"one.mkv": {Name: "one.mkv", InfoHash: "aaa", Size: 1}}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	setFolderNaming(t, "")
	again, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	for _, name := range []string{"items", "repair_state"} {
		if got, want := storeKeys(t, again, name), []string{"aaa"}; !reflect.DeepEqual(got, want) {
			t.Errorf("%s keys after reopening = %v, want %v", name, got, want)
		}
	}
	if n, err := again.MigrateMetadata(); err != nil || n != 0 {
		t.Errorf("MigrateMetadata = %d, %v; want 0 (already in the current format)", n, err)
	}
}

// An entry stored in the old format (no metadata attributes) is still
// migrated, once.
func TestMigrateMetadataStillMigratesOldFormatEntry(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	s, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	data, err := proto.Marshal(EntryToProto(&Entry{InfoHash: "old", Name: "Old", Status: "downloaded"}))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.entries.Put("old", data, nil); err != nil {
		t.Fatal(err)
	}
	if n, err := s.MigrateMetadata(); err != nil || n != 1 {
		t.Fatalf("first MigrateMetadata = %d, %v; want 1", n, err)
	}
	if n, err := s.MigrateMetadata(); err != nil || n != 0 {
		t.Errorf("second MigrateMetadata = %d, %v; want 0", n, err)
	}
}
