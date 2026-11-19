package parser

import (
	"encoding/binary"
	"testing"

	"github.com/rs/zerolog"
)

func rar5TestVint(v uint64) []byte {
	var b []byte
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}

// rar5TestHeader builds a RAR5 header (CRC unchecked by the walks) with a
// data area of dataSize and the given type-specific fields.
func rar5TestHeader(typ uint64, dataSize uint64, fields []byte) []byte {
	var body []byte
	body = append(body, rar5TestVint(typ)...)
	body = append(body, rar5TestVint(RAR5HeaderFlagDataArea)...)
	body = append(body, rar5TestVint(dataSize)...)
	body = append(body, fields...)
	h := binary.LittleEndian.AppendUint32(nil, 0)
	h = append(h, rar5TestVint(uint64(len(body)))...)
	return append(h, body...)
}

// A RAR5 header whose data-size vint is above MaxInt64 turned negative in
// int64(dataSize): both walks then skipped nothing and read what follows as
// the next header (the snippet walk also moved its offset backwards), so the
// file header behind it came back as an entry at a wrong offset. Data that
// large is past any volume, so the walk must stop there, as it does for any
// data area running past the volume.
func TestRAR5HeaderDataSizeAboveMaxInt64StopsWalk(t *testing.T) {
	name := []byte("movie.mkv")
	var fileFields []byte
	fileFields = append(fileFields, rar5TestVint(0)...)  // file flags
	fileFields = append(fileFields, rar5TestVint(10)...) // unpacked size
	fileFields = append(fileFields, rar5TestVint(0)...)  // attributes
	fileFields = append(fileFields, rar5TestVint(0)...)  // compression info (stored)
	fileFields = append(fileFields, rar5TestVint(0)...)  // host OS
	fileFields = append(fileFields, rar5TestVint(uint64(len(name)))...)
	fileFields = append(fileFields, name...)

	build := func(serviceData uint64, serviceBytes int) ([]byte, int64) {
		a := append([]byte(nil), rar5Signature...)
		a = append(a, rar5TestHeader(RAR5HeaderTypeService, serviceData, nil)...)
		a = append(a, make([]byte, serviceBytes)...)
		a = append(a, rar5TestHeader(RAR5HeaderTypeFile, 10, fileFields)...)
		at := int64(len(a))
		a = append(a, make([]byte, 10)...)
		return a, at
	}
	walks := func(t *testing.T, data []byte) map[string][]*RARFileEntry {
		p := &RARParser{logger: zerolog.Nop()}
		snippet, _, _, err := p.parseRAR5Headers(data, 0, "a.rar", "")
		if err != nil {
			t.Fatalf("parseRAR5Headers: %v", err)
		}
		sp, stream, vol := fuzzRARStream(t, data, len(rar5Signature), 1000)
		res, err := sp.parseRAR5Stream(stream, 0, vol.Name, "")
		if err != nil {
			t.Fatalf("parseRAR5Stream: %v", err)
		}
		return map[string][]*RARFileEntry{"snippet": snippet, "stream": res.Files}
	}

	control, at := build(3, 3)
	for walk, files := range walks(t, control) {
		if len(files) != 1 || files[0].DataOffset != at {
			t.Fatalf("control, %s walk: got %d entries %+v, want one at %d", walk, len(files), files, at)
		}
	}

	for _, size := range []uint64{1<<64 - 1, 1<<63 + 5} {
		data, _ := build(size, 0)
		for walk, files := range walks(t, data) {
			if len(files) != 0 {
				t.Errorf("service data size %d, %s walk: got %d entries (first at %d), want none: the bytes after the header are inside its data area",
					size, walk, len(files), files[0].DataOffset)
			}
		}
	}
}
