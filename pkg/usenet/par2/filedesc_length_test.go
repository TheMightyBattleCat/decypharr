package par2

import (
	"context"
	"encoding/binary"
	"strings"
	"testing"
)

// hostileLengthIndex builds a one-file recovery set whose FileDesc packet
// carries the given raw 64-bit length. The packet checksums are valid, which
// is what a poster crafting a set can always arrange.
func hostileLengthIndex(t *testing.T, rawLength uint64) Source {
	t.Helper()
	var set, file [16]byte
	set[0], file[0] = 0xA1, 0xA2
	mainBody := make([]byte, 12+16)
	binary.LittleEndian.PutUint64(mainBody[0:8], 8)
	binary.LittleEndian.PutUint32(mainBody[8:12], 1)
	copy(mainBody[12:], file[:])
	fd := make([]byte, 56+8)
	copy(fd[0:16], file[:])
	binary.LittleEndian.PutUint64(fd[48:56], rawLength)
	copy(fd[56:], "a.bin")
	ifsc := make([]byte, 16+20)
	copy(ifsc[0:16], file[:])
	data := append(append(buildPacket(t, set, typeMain, mainBody), buildPacket(t, set, typeFileDesc, fd)...), buildPacket(t, set, typeIFSC, ifsc)...)
	return Source{Name: "hostile.par2", Data: data}
}

// A FileDesc length with the top bit set became a negative Length, and one of
// 0x7fffffffffffffff made ceilDiv overflow so finalize computed a hugely
// negative slice count that slipped under the spec limit. Both sets were
// accepted as valid. They must be refused, and a length that needs more
// slices than the spec allows must be refused too.
func TestParseIndexRejectsHostileFileLengths(t *testing.T) {
	const sliceSize = 8
	cases := []struct {
		name    string
		length  uint64
		wantErr string
	}{
		{"top bit set (negative)", 0xffffffffffffff00, "length"},
		{"most negative", 0x8000000000000000, "length"},
		{"max int64 (ceilDiv overflow)", 0x7fffffffffffffff, "slices"},
		{"one slice over the spec limit", sliceSize*maxInputSlices + 1, "slices"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			idx, err := ParseIndex(context.Background(), []Source{hostileLengthIndex(t, tc.length)})
			if err == nil {
				t.Fatalf("hostile set accepted: NumSlices = %d, want an error", idx.NumSlices())
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not mention %q", err, tc.wantErr)
			}
		})
	}
}

// The boundaries around the limit: the largest legal file still parses with a
// non-negative count, and an empty file has no slices.
func TestParseIndexAcceptsLengthsInsideTheLimit(t *testing.T) {
	const sliceSize = 8
	for _, tc := range []struct {
		length uint64
		want   int64
	}{
		{0, 0},
		{1, 1},
		{sliceSize * maxInputSlices, maxInputSlices},
	} {
		idx, err := ParseIndex(context.Background(), []Source{hostileLengthIndex(t, tc.length)})
		if err != nil {
			t.Fatalf("length %d rejected: %v", tc.length, err)
		}
		if idx.NumSlices() != tc.want {
			t.Fatalf("length %d: NumSlices = %d, want %d", tc.length, idx.NumSlices(), tc.want)
		}
	}
}

// ceilDiv must not overflow when a+b-1 exceeds the int64 range.
func TestCeilDivDoesNotOverflow(t *testing.T) {
	if got := ceilDiv(0x7fffffffffffffff, 8); got != 0x1000000000000000 {
		t.Fatalf("ceilDiv(maxInt64, 8) = %d, want %d", got, int64(0x1000000000000000))
	}
	if got := ceilDiv(17, 8); got != 3 {
		t.Fatalf("ceilDiv(17, 8) = %d, want 3", got)
	}
}
