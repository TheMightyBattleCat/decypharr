package storage

import (
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
)

// File IDs must be assigned once and survive entries being rebuilt from
// provider responses — the .strm URLs written from them live for years.
func TestFileIDsAreStableAcrossUpdates(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)

	s, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	infohash := "aabbccddeeff00112233445566778899aabbccdd"
	entry := &Entry{
		InfoHash: infohash,
		Name:     "Movie.2023",
		Files: map[string]*File{
			"Movie.2023.mkv": {Name: "Movie.2023.mkv", Size: 100, InfoHash: infohash},
		},
	}
	if err := s.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	id := entry.Files["Movie.2023.mkv"].ID
	if id == "" {
		t.Fatal("AddOrUpdate did not assign a file ID")
	}

	// Same entry rebuilt without IDs, plus a new file.
	rebuilt := &Entry{
		InfoHash: infohash,
		Name:     "Movie.2023",
		Files: map[string]*File{
			"Movie.2023.mkv": {Name: "Movie.2023.mkv", Size: 100, InfoHash: infohash},
			"Movie.2023.srt": {Name: "Movie.2023.srt", Size: 10, InfoHash: infohash},
		},
	}
	if err := s.AddOrUpdate(rebuilt); err != nil {
		t.Fatal(err)
	}
	if got := rebuilt.Files["Movie.2023.mkv"].ID; got != id {
		t.Fatalf("existing file ID changed: %q -> %q", id, got)
	}
	if rebuilt.Files["Movie.2023.srt"].ID == "" {
		t.Fatal("new file did not get an ID")
	}

	// IDs are persisted and resolvable.
	loaded, err := s.Get(infohash)
	if err != nil {
		t.Fatal(err)
	}
	if got := loaded.Files["Movie.2023.mkv"].ID; got != id {
		t.Fatalf("persisted ID = %q, want %q", got, id)
	}
	file, err := loaded.GetFileByID(id)
	if err != nil || file.Name != "Movie.2023.mkv" {
		t.Fatalf("GetFileByID(%q) = %v, %v", id, file, err)
	}
}

// A file ID must still be there after the store is closed and opened again:
// an ID that only lived in memory would give every .strm file a dead URL on
// the next restart.
func TestFileIDsSurviveReopen(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)

	dir := t.TempDir()
	s, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}

	infohash := "aabbccddeeff00112233445566778899aabbccdd"
	entry := &Entry{
		InfoHash: infohash,
		Name:     "Movie.2023",
		Files: map[string]*File{
			"Movie.2023.mkv": {Name: "Movie.2023.mkv", Size: 100, InfoHash: infohash},
		},
	}
	if err := s.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	id := entry.Files["Movie.2023.mkv"].ID
	if id == "" {
		t.Fatal("AddOrUpdate did not assign a file ID")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	loaded, err := reopened.Get(infohash)
	if err != nil {
		t.Fatal(err)
	}
	if got := loaded.Files["Movie.2023.mkv"].ID; got != id {
		t.Fatalf("ID after reopen = %q, want %q", got, id)
	}

	// The entry as a provider refresh rebuilds it: no IDs. It must get the
	// stored one back, not a new one.
	rebuilt := &Entry{
		InfoHash: infohash,
		Name:     "Movie.2023",
		Files: map[string]*File{
			"Movie.2023.mkv": {Name: "Movie.2023.mkv", Size: 100, InfoHash: infohash},
		},
	}
	if err := reopened.AddOrUpdate(rebuilt); err != nil {
		t.Fatal(err)
	}
	if got := rebuilt.Files["Movie.2023.mkv"].ID; got != id {
		t.Fatalf("ID after reopen and rebuild = %q, want %q", got, id)
	}
}
