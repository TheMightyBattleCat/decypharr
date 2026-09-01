package manager

import (
	"encoding/binary"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

type earlyFileSpec struct {
	name   string
	length uint64
}

// buildTestIndexN assembles a minimal, valid N-file PAR2 index (Main +
// FileDesc packets only) for the early-gate unit tests. FileIDs are 0x01,
// 0x02, ... in spec order, which ParseIndex's little-endian numeric sort
// keeps in that order.
func buildTestIndexN(t *testing.T, sliceSize uint64, specs []earlyFileSpec) (*par2.Index, map[string][16]byte) {
	t.Helper()
	var setID [16]byte
	setID[0] = 0x42

	mainBody := make([]byte, 12+16*len(specs))
	binary.LittleEndian.PutUint64(mainBody[0:8], sliceSize)
	binary.LittleEndian.PutUint32(mainBody[8:12], uint32(len(specs)))

	ids := make(map[string][16]byte, len(specs))
	for i, s := range specs {
		var id [16]byte
		id[0] = byte(i + 1)
		copy(mainBody[12+i*16:12+(i+1)*16], id[:])
		ids[s.name] = id
	}

	data := buildTestPar2Packet(t, setID, testMainType, mainBody)
	for _, s := range specs {
		id := ids[s.name]
		b := make([]byte, 56+len(s.name))
		copy(b[0:16], id[:])
		binary.LittleEndian.PutUint64(b[48:56], s.length)
		copy(b[56:], s.name)
		data = append(data, buildTestPar2Packet(t, setID, testFileDescType, b)...)
	}

	idx, err := par2.ParseIndex([]par2.Source{{Name: "test.par2", Data: data}})
	if err != nil {
		t.Fatalf("ParseIndex: %v", err)
	}
	return idx, ids
}

// postedFile builds a PostedFileRef with n segments of segBytes each and a
// real (non-estimated) final segment of tailBytes, so exactSegGeometry takes
// its exact-arithmetic path. Segment message IDs are "<name-0>", "<name-1>", ...
func postedFile(name string, segBytes, tailBytes int64, nFull int) storage.PostedFileRef {
	segs := make([]storage.Par2SegmentRef, 0, nFull+1)
	for i := 0; i < nFull; i++ {
		segs = append(segs, storage.Par2SegmentRef{
			MessageID: msgID(name, i),
			Bytes:     segBytes,
			Real:      true,
		})
	}
	segs = append(segs, storage.Par2SegmentRef{
		MessageID: msgID(name, nFull),
		Bytes:     tailBytes,
		Real:      true,
	})
	total := segBytes*int64(nFull) + tailBytes
	return storage.PostedFileRef{Name: name, Size: total, Segments: segs}
}

func msgID(name string, i int) string {
	return "<" + name + "-" + string(rune('0'+i)) + ">"
}

func deadAt(name string, idxs ...int) []overlay.DeadSegment {
	out := make([]overlay.DeadSegment, 0, len(idxs))
	for _, i := range idxs {
		out = append(out, overlay.DeadSegment{Index: i, MessageID: msgID(name, i)})
	}
	return out
}

func TestEarlyDamagedSliceCheck(t *testing.T) {
	// Every file below: segments 100/100/.../tail bytes, sliceSize 100, so a
	// full segment is exactly one slice and DamagedSlices maps 1:1.

	t.Run("unique length, three dead segments -> exact count", func(t *testing.T) {
		idx, _ := buildTestIndexN(t, 100, []earlyFileSpec{
			{"a.rar", 250}, // 100 + 100 + 50 -> slices 0,1,2
			{"b.rar", 400},
		})
		src := postedFile("a.rar", 100, 50, 2)
		pending := map[string][]overlay.DeadSegment{"a.rar": deadAt("a.rar", 0, 1, 2)}

		got := earlyDamagedSliceCheck(idx, pending, []storage.PostedFileRef{src, postedFile("b.rar", 100, 0, 4)}, nil, zerolog.Nop())
		if got != 3 {
			t.Fatalf("earlyDamagedSliceCheck = %d, want 3", got)
		}
	})

	t.Run("unique length, subset of dead segments -> exact lower count", func(t *testing.T) {
		idx, _ := buildTestIndexN(t, 100, []earlyFileSpec{{"a.rar", 250}, {"b.rar", 400}})
		src := postedFile("a.rar", 100, 50, 2)
		pending := map[string][]overlay.DeadSegment{"a.rar": deadAt("a.rar", 0, 2)}

		got := earlyDamagedSliceCheck(idx, pending, []storage.PostedFileRef{src}, nil, zerolog.Nop())
		if got != 2 {
			t.Fatalf("earlyDamagedSliceCheck = %d, want 2 (slices 0 and 2)", got)
		}
	})

	t.Run("length tie on the FileDesc side -> skipped, returns 0", func(t *testing.T) {
		idx, _ := buildTestIndexN(t, 100, []earlyFileSpec{
			{"a.rar", 250},
			{"b.rar", 250}, // same length as a.rar -> tie, needs MD5-16k
		})
		src := postedFile("a.rar", 100, 50, 2)
		pending := map[string][]overlay.DeadSegment{"a.rar": deadAt("a.rar", 0, 1, 2)}

		got := earlyDamagedSliceCheck(idx, pending, []storage.PostedFileRef{src, postedFile("b.rar", 100, 50, 2)}, nil, zerolog.Nop())
		if got != 0 {
			t.Fatalf("earlyDamagedSliceCheck = %d, want 0 (length tie is skipped)", got)
		}
	})

	t.Run("pending file absent from Par2Source -> skipped, returns 0", func(t *testing.T) {
		idx, _ := buildTestIndexN(t, 100, []earlyFileSpec{{"a.rar", 250}, {"b.rar", 400}})
		pending := map[string][]overlay.DeadSegment{"ghost.rar": deadAt("ghost.rar", 0)}

		got := earlyDamagedSliceCheck(idx, pending, []storage.PostedFileRef{postedFile("a.rar", 100, 50, 2)}, nil, zerolog.Nop())
		if got != 0 {
			t.Fatalf("earlyDamagedSliceCheck = %d, want 0 (not in Par2Source)", got)
		}
	})

	t.Run("two posted files share a length -> that file skipped even if FileDesc unique", func(t *testing.T) {
		idx, _ := buildTestIndexN(t, 100, []earlyFileSpec{{"a.rar", 250}, {"b.rar", 400}})
		// a.rar and c.rar both 250 bytes on the posted side.
		srcA := postedFile("a.rar", 100, 50, 2)
		srcC := postedFile("c.rar", 100, 50, 2)
		pending := map[string][]overlay.DeadSegment{"a.rar": deadAt("a.rar", 0, 1, 2)}

		got := earlyDamagedSliceCheck(idx, pending, []storage.PostedFileRef{srcA, srcC, postedFile("b.rar", 100, 0, 4)}, nil, zerolog.Nop())
		if got != 0 {
			t.Fatalf("earlyDamagedSliceCheck = %d, want 0 (posted-side length tie needs MD5-16k)", got)
		}
	})

	t.Run("multiple pending files, some unique some tied -> count from unique only", func(t *testing.T) {
		idx, _ := buildTestIndexN(t, 100, []earlyFileSpec{
			{"a.rar", 250}, // unique -> contributes
			{"b.rar", 400}, // tied with c.rar
			{"c.rar", 400}, // tied with b.rar
		})
		srcA := postedFile("a.rar", 100, 50, 2)  // 3 slices
		srcB := postedFile("b.rar", 100, 0, 4)   // 4 slices, but tied -> skipped
		pending := map[string][]overlay.DeadSegment{
			"a.rar": deadAt("a.rar", 0, 1, 2),
			"b.rar": deadAt("b.rar", 0, 1, 2, 3),
		}

		got := earlyDamagedSliceCheck(idx, pending, []storage.PostedFileRef{srcA, srcB, postedFile("c.rar", 100, 0, 4)}, nil, zerolog.Nop())
		if got != 3 {
			t.Fatalf("earlyDamagedSliceCheck = %d, want 3 (a.rar only; b.rar tied)", got)
		}
	})

	t.Run("empty pending -> 0", func(t *testing.T) {
		idx, _ := buildTestIndexN(t, 100, []earlyFileSpec{{"a.rar", 250}})
		got := earlyDamagedSliceCheck(idx, nil, []storage.PostedFileRef{postedFile("a.rar", 100, 50, 2)}, nil, zerolog.Nop())
		if got != 0 {
			t.Fatalf("earlyDamagedSliceCheck = %d, want 0", got)
		}
	})
}
