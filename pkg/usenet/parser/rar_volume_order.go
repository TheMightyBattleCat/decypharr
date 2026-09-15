package parser

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"

	"github.com/sourcegraph/conc/iter"

	"github.com/sirrobot01/decypharr/pkg/usenet/types"
)

// RAR4 header flags this file reads. Checked against archives written by RAR
// 6.23 (-ma4): a volume's main header carries 0x0001, the first volume's also
// 0x0100, and a single-volume archive's neither; every volume ends with a
// 20-byte end-of-archive block, flags 0x400F (0x400E on the last volume):
// data CRC (4 bytes), volume number (2 bytes, 0 for the first volume), 7
// reserved bytes.
const (
	rar4MainFlagVolume = 0x0001

	rar4EndFlagDataCRC   = 0x0002
	rar4EndFlagVolNumber = 0x0008

	// rar5ArchiveFlagVolume marks a RAR5 main header as one volume of a set.
	rar5ArchiveFlagVolume = 0x0001
	rar5HeaderFlagExtra   = 0x0001
	rar5HeaderFlagData    = 0x0002

	// rarVolumeProbeBytes is how much of an archive's first article
	// rarArchiveIsVolume needs: signature and main header.
	rarVolumeProbeBytes = 64
)

var (
	rar4Signature = []byte("Rar!\x1a\x07\x00")
	rar5Signature = []byte("Rar!\x1a\x07\x01\x00")
)

// rarArchiveIsVolume reads an archive's leading bytes and reports whether its
// main header marks it as one volume of a multi-volume set. known is false
// when prefix is not a RAR archive this can read (no signature, too short, or
// RAR5 headers encrypted before the main header).
func rarArchiveIsVolume(prefix []byte) (isVolume, known bool) {
	switch {
	case bytes.HasPrefix(prefix, rar5Signature):
		r := bytes.NewReader(prefix[len(rar5Signature):])
		if _, err := r.Seek(4, 1); err != nil { // header CRC32
			return false, false
		}
		if _, err := readVInt(r); err != nil { // header size
			return false, false
		}
		typ, err := readVInt(r)
		if err != nil || typ != 1 { // 1 = main archive header
			return false, false
		}
		flags, err := readVInt(r)
		if err != nil {
			return false, false
		}
		if flags&rar5HeaderFlagExtra != 0 {
			if _, err := readVInt(r); err != nil {
				return false, false
			}
		}
		if flags&rar5HeaderFlagData != 0 {
			if _, err := readVInt(r); err != nil {
				return false, false
			}
		}
		archiveFlags, err := readVInt(r)
		if err != nil {
			return false, false
		}
		return archiveFlags&rar5ArchiveFlagVolume != 0, true
	case bytes.HasPrefix(prefix, rar4Signature):
		h := prefix[len(rar4Signature):]
		if len(h) < 5 || h[2] != RAR4HeaderTypeArchive {
			return false, false
		}
		return binary.LittleEndian.Uint16(h[3:5])&rar4MainFlagVolume != 0, true
	}
	return false, false
}

// rar4EndVolumeNumber returns the volume number a RAR4 end-of-archive header
// stores, when its CRC checks out and it stores one.
func rar4EndVolumeNumber(h *rar4Header) (int, bool) {
	if h == nil || h.Type != RAR4HeaderTypeEnd {
		return 0, false
	}
	raw := make([]byte, 0, 5+len(h.Data))
	raw = append(raw, h.Type)
	raw = binary.LittleEndian.AppendUint16(raw, h.Flags)
	raw = binary.LittleEndian.AppendUint16(raw, h.HeadSize)
	raw = append(raw, h.Data...)
	if uint16(crc32.ChecksumIEEE(raw)) != h.CRC {
		return 0, false
	}
	return rar4EndBodyVolumeNumber(h.Flags, h.Data)
}

func rar4EndBodyVolumeNumber(flags uint16, body []byte) (int, bool) {
	if flags&rar4EndFlagVolNumber == 0 {
		return 0, false
	}
	off := 0
	if flags&rar4EndFlagDataCRC != 0 {
		off += 4
	}
	if off+2 > len(body) {
		return 0, false
	}
	return int(binary.LittleEndian.Uint16(body[off:])), true
}

// rar4TailVolumeNumber finds the last RAR4 end-of-archive header in tail - the
// decoded end of a volume - and returns the volume number it stores. Each
// candidate must pass its header CRC, so a 0x7B byte in file data is not
// mistaken for one.
func rar4TailVolumeNumber(tail []byte) (int, bool) {
	for i := len(tail) - 7; i >= 0; i-- {
		if tail[i+2] != RAR4HeaderTypeEnd {
			continue
		}
		size := int(binary.LittleEndian.Uint16(tail[i+5:]))
		if size < 7 || size > 64 || i+size > len(tail) {
			continue
		}
		h := &rar4Header{
			CRC:      binary.LittleEndian.Uint16(tail[i:]),
			Type:     tail[i+2],
			Flags:    binary.LittleEndian.Uint16(tail[i+3:]),
			HeadSize: uint16(size),
			Data:     tail[i+7 : i+size],
		}
		if n, ok := rar4EndVolumeNumber(h); ok {
			return n, true
		}
	}
	return 0, false
}

// articleBodyFunc fetches one article's decoded body.
type articleBodyFunc func(ctx context.Context, messageID string) ([]byte, error)

// rar4TailVolumeNumbers reads the volume number from the end of each volume in
// want: its last article, and the one before when the end block starts there.
// Volumes whose tail cannot be fetched or holds no numbered end block are left
// out of the result.
func rar4TailVolumeNumbers(ctx context.Context, volumes []*types.Volume, want []int, maxConcurrent int, fetch articleBodyFunc) map[int]int {
	type found struct {
		idx, num int
		ok       bool
	}
	mapper := iter.Mapper[int, found]{MaxGoroutines: max(1, maxConcurrent)}
	results := mapper.Map(want, func(ip *int) found {
		idx := *ip
		segs := volumes[idx].Segments
		var tail []byte
		// Two articles at most: the end block is 20 bytes, so it only
		// reaches back past a last article shorter than that.
		for k := len(segs) - 1; k >= 0 && k >= len(segs)-2; k-- {
			data, err := fetch(ctx, segs[k].MessageID)
			if err != nil {
				return found{idx: idx}
			}
			tail = append(data, tail...)
			if n, ok := rar4TailVolumeNumber(tail); ok {
				return found{idx: idx, num: n, ok: true}
			}
		}
		return found{idx: idx}
	})
	out := make(map[int]int, len(results))
	for _, r := range results {
		if r.ok {
			out[r.idx] = r.num
		}
	}
	return out
}

// orderEstablished reports whether entries give every one of total volumes a
// distinct number - after resolveVolumeOrder has placed any hole it could.
func orderEstablished(entries []volEntry, total int) bool {
	if len(entries) != total {
		return false
	}
	seen := make(map[int]bool, len(entries))
	for _, e := range entries {
		if !e.hasNum || seen[e.num] {
			return false
		}
		seen[e.num] = true
	}
	return true
}
