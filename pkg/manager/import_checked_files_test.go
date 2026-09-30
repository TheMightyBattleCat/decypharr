package manager

import (
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

func TestImportChecksFile(t *testing.T) {
	const mib = 1024 * 1024
	entry := func(files ...*storage.File) *storage.Entry {
		e := &storage.Entry{Files: map[string]*storage.File{}}
		for _, f := range files {
			e.Files[f.Name] = f
		}
		return e
	}
	cases := []struct {
		name  string
		entry *storage.Entry
		file  string
		want  bool
	}{
		{"single SD episode under 100 MiB", entry(&storage.File{Name: "ep.mkv", Size: 80 * mib}), "ep.mkv", true},
		{"pack of small episodes", entry(&storage.File{Name: "e1.mkv", Size: 90 * mib}, &storage.File{Name: "e2.mkv", Size: 350 * mib}), "e1.mkv", true},
		{"clip bundled with a film", entry(&storage.File{Name: "film.mkv", Size: 4000 * mib}, &storage.File{Name: "featurette.mkv", Size: 60 * mib}), "featurette.mkv", false},
		{"large file always", entry(&storage.File{Name: "film.mkv", Size: 40000 * mib}, &storage.File{Name: "bonus.mkv", Size: 200 * mib}), "bonus.mkv", true},
		{"not a video", entry(&storage.File{Name: "ep.mkv", Size: 80 * mib}, &storage.File{Name: "ep.nfo", Size: 80 * mib}), "ep.nfo", false},
		{"empty file", entry(&storage.File{Name: "ep.mkv", Size: 0}), "ep.mkv", false},
		{"deleted large file does not set the bar", entry(&storage.File{Name: "old.mkv", Size: 4000 * mib, Deleted: true}, &storage.File{Name: "ep.mkv", Size: 80 * mib}), "ep.mkv", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := importChecksFile(tc.entry.Files[tc.file], largestVideoSize(tc.entry)); got != tc.want {
				t.Fatalf("importChecksFile = %v, want %v", got, tc.want)
			}
		})
	}
}
