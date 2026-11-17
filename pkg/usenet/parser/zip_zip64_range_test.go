package parser

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/rs/zerolog"
)

// zip64CentralEntry builds one central directory entry whose three 32-bit
// size/offset fields are 0xFFFFFFFF sentinels, with a ZIP64 extra record
// carrying the given 64-bit values in spec order.
func zip64CentralEntry(name string, unpacked, packed, offset uint64) []byte {
	var extra []byte
	extra = binary.LittleEndian.AppendUint16(extra, 0x0001)
	extra = binary.LittleEndian.AppendUint16(extra, 24)
	extra = binary.LittleEndian.AppendUint64(extra, unpacked)
	extra = binary.LittleEndian.AppendUint64(extra, packed)
	extra = binary.LittleEndian.AppendUint64(extra, offset)

	var b []byte
	b = binary.LittleEndian.AppendUint32(b, ZIPCentralDirectoryHeaderSig)
	b = binary.LittleEndian.AppendUint16(b, 45) // version made by
	b = binary.LittleEndian.AppendUint16(b, 45) // version needed
	b = binary.LittleEndian.AppendUint16(b, 0)  // flags
	b = binary.LittleEndian.AppendUint16(b, ZIPStoreMethod)
	b = binary.LittleEndian.AppendUint16(b, 0) // mod time
	b = binary.LittleEndian.AppendUint16(b, 0) // mod date
	b = binary.LittleEndian.AppendUint32(b, 0) // CRC32
	b = binary.LittleEndian.AppendUint32(b, 0xFFFFFFFF)
	b = binary.LittleEndian.AppendUint32(b, 0xFFFFFFFF)
	b = binary.LittleEndian.AppendUint16(b, uint16(len(name)))
	b = binary.LittleEndian.AppendUint16(b, uint16(len(extra)))
	b = binary.LittleEndian.AppendUint16(b, 0) // comment length
	b = binary.LittleEndian.AppendUint16(b, 0) // disk number start
	b = binary.LittleEndian.AppendUint16(b, 0) // internal attributes
	b = binary.LittleEndian.AppendUint32(b, 0) // external attributes
	b = binary.LittleEndian.AppendUint32(b, 0xFFFFFFFF)
	b = append(b, name...)
	return append(b, extra...)
}

// A ZIP64 extra value above MaxInt64 turns negative when read as int64. The
// entry carrying it must be dropped, and the entries after it still parsed.
func TestZIP64ExtraAboveMaxInt64Dropped(t *testing.T) {
	const big = 5 << 30 // a real ZIP64-sized file
	p := &ZIPParser{logger: zerolog.Nop()}

	ok, err := p.parseCentralDirEntry(bytes.NewReader(zip64CentralEntry("ok.mkv", big, big, big)))
	if err != nil || ok == nil || ok.UncompressedSize != big || ok.CompressedSize != big || ok.LocalHeaderOffset != big {
		t.Fatalf("control entry: got %+v, %v; want sizes and offset %d", ok, err, int64(big))
	}

	for _, tc := range []struct {
		label                    string
		unpacked, packed, offset uint64
	}{
		{"unpacked top bit", 1 << 63, big, big},
		{"packed top bit", big, 1 << 63, big},
		{"offset top bit", big, big, 0xEA30303030303030},
	} {
		dir := append(zip64CentralEntry("bad.mkv", tc.unpacked, tc.packed, tc.offset),
			zip64CentralEntry("next.mkv", big, big, 2*big)...)
		r := bytes.NewReader(dir)
		f, err := p.parseCentralDirEntry(r)
		if err != nil {
			t.Fatalf("%s: unexpected error %v", tc.label, err)
		}
		if f != nil {
			t.Fatalf("%s: entry kept: unpacked %d packed %d local header %d", tc.label, f.UncompressedSize, f.CompressedSize, f.LocalHeaderOffset)
		}
		next, err := p.parseCentralDirEntry(r)
		if err != nil || next == nil || next.Name != "next.mkv" || next.LocalHeaderOffset != 2*big {
			t.Fatalf("%s: following entry: got %+v, %v", tc.label, next, err)
		}
	}
}

// A ZIP64 end of central directory record whose directory size is above
// MaxInt64 is rejected instead of crashing the directory walk.
func TestZIP64EOCDSizeAboveMaxInt64(t *testing.T) {
	var rec []byte
	rec = binary.LittleEndian.AppendUint32(rec, ZIPZIP64EndOfCentralDirSig)
	rec = binary.LittleEndian.AppendUint64(rec, 44)         // record size
	rec = append(rec, make([]byte, 12)...)                  // versions, disk numbers
	rec = binary.LittleEndian.AppendUint64(rec, 1)          // entries this disk
	rec = binary.LittleEndian.AppendUint64(rec, 1)          // total entries
	rec = binary.LittleEndian.AppendUint64(rec, ^uint64(0)) // central dir size, -1 as int64
	rec = binary.LittleEndian.AppendUint64(rec, 0)          // central dir offset
	data := append(zip64CentralEntry("a.mkv", 1, 1, 0), rec...)
	data = append(data, make([]byte, 20)...) // ZIP64 EOCD locator
	eocdPos := len(data)
	data = append(data, make([]byte, 22)...)

	p := &ZIPParser{logger: zerolog.Nop()}
	eocd := &endOfCentralDirRecord{totalEntries: 0xFFFF, centralDirSize: 0xFFFFFFFF, centralDirOffset: 0xFFFFFFFF}
	files, err := p.parseCentralDirectory(data, eocd, eocdPos)
	if err == nil {
		t.Fatalf("ZIP64 record with directory size above MaxInt64 accepted: %d entries", len(files))
	}
}
