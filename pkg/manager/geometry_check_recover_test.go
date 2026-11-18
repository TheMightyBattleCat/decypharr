package manager

import (
	"encoding/hex"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/parser"
)

// The geometry check walks headers read off the posting. The sweep probe
// that runs it has no recover above it, so a panic in a header walk (a
// hostile or corrupt post) would end the process. It must instead come back
// as "could not check": the empty verdict, never a missing-volume one.
func TestPostedGeometryRecoversHeaderWalkPanic(t *testing.T) {
	// Tide on Sark S02E01's .r00 volume stored alone (see TestStartsMidArchive).
	prefix, err := hex.DecodeString("526172211a0700f1fb7301000d00000000000000564174c391440014f0fa027db835fe026f7fe593864a89401430170020000000000000000000000074656e6569676874792d746f737330326530312e6d6b7600b010257653868dc9")
	if err != nil {
		t.Fatal(err)
	}
	f := &storage.NZBFile{Name: "teneighty-toss02e01.mkv", FileType: storage.NZBFileTypeRar,
		Segments: []storage.NZBSegment{{Number: 1, SegmentDataStart: 88, Bytes: 767912}}}
	r := &Repair{logger: zerolog.Nop()}

	if v := r.postedGeometry("hash", f.Name, f, prefix); v.reason != reasonMissingVolume || !v.missingStart {
		t.Fatalf("control: got %+v, want missing volume at the start", v)
	}

	defer func() { readFileHeaderStart = parser.ReadFileHeaderStart }()
	readFileHeaderStart = func([]byte) (parser.FileHeaderStart, bool) {
		panic("slice bounds out of range")
	}
	var v geometryVerdict
	func() {
		defer func() {
			if p := recover(); p != nil {
				t.Fatalf("panic escaped the geometry check: %v", p)
			}
		}()
		v = r.postedGeometry("hash", f.Name, f, prefix)
	}()
	if v != (geometryVerdict{}) {
		t.Fatalf("after a header-walk panic: got %+v, want the empty verdict (could not check)", v)
	}
}
