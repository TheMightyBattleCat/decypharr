package usenet

import (
	"fmt"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// With two live records of one name, the sweep's availability probe must
// sample the record streaming serves (GetFileByName), not the first one: it
// used to STAT a record nobody reads, re-grabbing for its dead articles and
// passing the served record's.
func TestSampleFileMessageIDsFollowsStreamingRecord(t *testing.T) {
	record := func(prefix string, n int) storage.NZBFile {
		f := storage.NZBFile{Name: "e01.mkv", Size: int64(n) * 1000}
		for i := 0; i < n; i++ {
			start := int64(i) * 1000
			f.Segments = append(f.Segments, storage.NZBSegment{Number: i + 1, MessageID: fmt.Sprintf("%s%d@x", prefix, i), Bytes: 1000, StartOffset: start, EndOffset: start + 999})
		}
		return f
	}
	store := &NZBStorage{metaDir: t.TempDir(), logger: zerolog.Nop()}
	nzb := &storage.NZB{ID: "dup", Name: "Dup", Files: []storage.NZBFile{record("first", 10), record("last", 12)}}
	if err := store.AddNZB(nzb); err != nil {
		t.Fatal(err)
	}
	full, err := store.GetNZB("dup")
	if err != nil {
		t.Fatal(err)
	}
	served := full.GetFileByName("e01.mkv")
	sampled, err := store.SampleFileMessageIDs("dup", "e01.mkv", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(sampled) != len(served.Segments) || sampled[0] != served.Segments[0].MessageID {
		t.Fatalf("sampled %d ids starting %q, streaming serves %d starting %q", len(sampled), sampled[0], len(served.Segments), served.Segments[0].MessageID)
	}

	// A single record keeps the fast path and still samples it.
	single := &storage.NZB{ID: "one", Name: "One", Files: []storage.NZBFile{record("only", 8)}}
	if err := store.AddNZB(single); err != nil {
		t.Fatal(err)
	}
	ids, err := store.SampleFileMessageIDs("one", "e01.mkv", 100)
	if err != nil || len(ids) != 8 || ids[0] != "only0@x" {
		t.Fatalf("single record: ids=%v err=%v", ids, err)
	}
}
