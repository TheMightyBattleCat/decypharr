package parser

import (
	"encoding/binary"
	"testing"

	"github.com/rs/zerolog"
)

// A RAR4 HIGH_SIZE file header whose high size word has its top bit set
// combines into a negative int64. The parser must drop that entry instead of
// returning a file with a negative size.
func TestRAR4FileHeaderHighSizeTopBitDropped(t *testing.T) {
	name := []byte("movie.mkv")
	build := func(packedHigh, unpackedHigh uint32) *rar4Header {
		var d []byte
		d = binary.LittleEndian.AppendUint32(d, 4)    // packed size low
		d = binary.LittleEndian.AppendUint32(d, 4)    // unpacked size low
		d = append(d, 0)                              // host OS
		d = binary.LittleEndian.AppendUint32(d, 0)    // CRC
		d = binary.LittleEndian.AppendUint32(d, 0)    // file time
		d = append(d, 29, RAR4CompressionMethodStore) // version, method
		d = binary.LittleEndian.AppendUint16(d, uint16(len(name)))
		d = binary.LittleEndian.AppendUint32(d, 0) // attributes
		d = binary.LittleEndian.AppendUint32(d, packedHigh)
		d = binary.LittleEndian.AppendUint32(d, unpackedHigh)
		d = append(d, name...)
		return &rar4Header{Type: RAR4HeaderTypeFile, Flags: RAR4FileFlagHighSize, Data: d}
	}
	p := &RARParser{logger: zerolog.Nop()}

	if f := p.parseRAR4FileHeader(build(0, 1), 0, "a.rar", 100); f == nil || f.UncompressedSize != 1<<32|4 {
		t.Fatalf("control header: got %+v, want unpacked %d", f, int64(1<<32|4))
	}
	for _, tc := range []struct {
		label                    string
		packedHigh, unpackedHigh uint32
	}{
		{"unpacked high top bit", 0, 0x80000000},
		{"packed high top bit", 0x80000000, 0},
	} {
		if f := p.parseRAR4FileHeader(build(tc.packedHigh, tc.unpackedHigh), 0, "a.rar", 100); f != nil {
			t.Errorf("%s: got entry unpacked %d packed %d, want no entry", tc.label, f.UncompressedSize, f.PackedSize)
		}
	}
}

// The RAR5 unpacked size is a uint64 vint; a value above MaxInt64 must not
// come back as a negative UncompressedSize.
func TestRAR5FileHeaderUnpackedSizeAboveMaxInt64Dropped(t *testing.T) {
	vint := func(v uint64) []byte {
		var b []byte
		for v >= 0x80 {
			b = append(b, byte(v)|0x80)
			v >>= 7
		}
		return append(b, byte(v))
	}
	name := []byte("movie.mkv")
	build := func(unpacked uint64) []byte {
		var d []byte
		d = append(d, vint(0)...)        // file flags
		d = append(d, vint(unpacked)...) // unpacked size
		d = append(d, vint(0)...)        // attributes
		d = append(d, vint(0)...)        // compression info (stored)
		d = append(d, vint(0)...)        // host OS
		d = append(d, vint(uint64(len(name)))...)
		return append(d, name...)
	}
	p := &RARParser{logger: zerolog.Nop()}

	if f := p.parseRAR5FileHeader(build(1000), 0, 0, "a.rar", 100, 1000, ""); f == nil || f.UncompressedSize != 1000 {
		t.Fatalf("control header: got %+v, want unpacked 1000", f)
	}
	if f := p.parseRAR5FileHeader(build(1<<63|5), 0, 0, "a.rar", 100, 1000, ""); f != nil {
		t.Errorf("unpacked 2^63+5: got entry unpacked %d, want no entry", f.UncompressedSize)
	}
}
