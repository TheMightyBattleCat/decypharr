package storage

import "testing"

// contiguousFile builds a file of n articles of per bytes each, laid out from 0.
func contiguousFile(name string, n int, per int64) NZBFile {
	f := NZBFile{Name: name, Size: int64(n) * per}
	for i := 0; i < n; i++ {
		start := int64(i) * per
		f.Segments = append(f.Segments, NZBSegment{Number: i + 1, Bytes: per, StartOffset: start, EndOffset: start + per - 1})
	}
	return f
}

func TestGetFileByNameSameNameRecords(t *testing.T) {
	full := contiguousFile("e01.mkv", 85, 1000)
	oneVolume := contiguousFile("e01.mkv", 1, 1000)

	// Tide on Sark S02: whole, then again as its first RAR volume - and the
	// other order, so the choice does not hang on position.
	for name, files := range map[string][]NZBFile{
		"whole first": {full, oneVolume},
		"whole last":  {oneVolume, full},
	} {
		t.Run(name, func(t *testing.T) {
			n := &NZB{Files: files}
			if got := n.GetFileByName("e01.mkv"); got == nil || got.Size != full.Size {
				t.Fatalf("got %+v, want the whole record (%d B)", got, full.Size)
			}
			if c := n.FileNameCount("e01.mkv"); c != 2 {
				t.Fatalf("FileNameCount = %d, want 2", c)
			}
		})
	}

	// Fear Light & Clocks S04E02: the second record's articles run to twice
	// its size, so the one laid out to its size wins even though it is first.
	media := contiguousFile("ldr.mkv", 354, 1000)
	doubled := contiguousFile("ldr.mkv", 708, 1000)
	doubled.Size = media.Size
	if got := (&NZB{Files: []NZBFile{media, doubled}}).GetFileByName("ldr.mkv"); got == nil || len(got.Segments) != 354 {
		t.Fatalf("got %d segments, want the media record's 354", len(got.Segments))
	}

	deleted := full
	deleted.IsDeleted = true
	if got := (&NZB{Files: []NZBFile{deleted, oneVolume}}).GetFileByName("e01.mkv"); got == nil || got.Size != oneVolume.Size {
		t.Fatalf("a deleted record was chosen: %+v", got)
	}

	// Parish Man S02 Yatogam1: two records of one size a segment apart. The
	// one streaming has always served (the last) stays.
	a := contiguousFile("fg.mkv", 1331, 1000)
	b := contiguousFile("fg.mkv", 1331, 1000)
	b.Segments[0].MessageID = "other@x"
	if got := (&NZB{Files: []NZBFile{a, b}}).GetFileByName("fg.mkv"); got == nil || got.Segments[0].MessageID != "other@x" {
		t.Fatalf("equal records: got %+v, want the last", got)
	}
	notCovering := a
	notCovering.Size = a.Size + 7
	notCovering2 := notCovering
	notCovering2.Segments = append([]NZBSegment(nil), a.Segments[:10]...)
	if got := (&NZB{Files: []NZBFile{notCovering, notCovering2}}).GetFileByName("fg.mkv"); got == nil || len(got.Segments) != 10 {
		t.Fatalf("no record covers its size: got %d segments, want the last record's 10", len(got.Segments))
	}

	// A header decode has no segments: the last live record.
	headerA, headerB := NZBFile{Name: "e01.mkv", Size: 5}, NZBFile{Name: "e01.mkv", Size: 9}
	if got := (&NZB{Files: []NZBFile{headerA, headerB}}).GetFileByName("e01.mkv"); got == nil || got.Size != 9 {
		t.Fatalf("header-only lookup got %+v, want the last record", got)
	}
	if got := (&NZB{Files: []NZBFile{full}}).GetFileByName("other.mkv"); got != nil {
		t.Fatalf("missing name returned %+v", got)
	}
}
