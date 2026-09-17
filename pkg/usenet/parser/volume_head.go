package parser

import (
	"bytes"
	"encoding/binary"
	"path/filepath"
	"strings"
)

// VolumeHeadBytes is how much of a RAR volume's first article ReadVolumeHead
// needs: the signature and the main archive header.
const VolumeHeadBytes = rarVolumeProbeBytes

// rar4MainFlagFirstVolume marks the first volume of a RAR4 set (RAR 3.0 and
// later write it).
const rar4MainFlagFirstVolume = 0x0100

// VolumeHead is what a RAR volume's leading bytes say about its place in its
// set, for checking a stored file's volume order after import.
type VolumeHead struct {
	// Version is 5 or 4, or 0 when the bytes do not start a RAR archive this
	// can read (no signature: the article starts mid-volume, or RAR5 headers
	// are encrypted before the main header).
	Version int
	// Volume reports the main header marks the archive as one volume of a set.
	Volume bool
	// Number is the RAR5 volume number (0 for the first volume) when
	// HasNumber. RAR4 main headers carry no number.
	Number    int
	HasNumber bool
	// First is the RAR4 first-volume flag.
	First bool
}

// ReadVolumeHead parses a RAR volume's first bytes. A RAR5 volume without the
// volume-number field is the first volume: RAR writes the field in every
// other volume.
func ReadVolumeHead(prefix []byte) VolumeHead {
	switch {
	case bytes.HasPrefix(prefix, rar5Signature):
		r := bytes.NewReader(prefix[len(rar5Signature):])
		if _, err := r.Seek(4, 1); err != nil { // header CRC32
			return VolumeHead{}
		}
		if _, err := readVInt(r); err != nil { // header size
			return VolumeHead{}
		}
		if typ, err := readVInt(r); err != nil || typ != 1 { // 1 = main archive header
			return VolumeHead{}
		}
		flags, err := readVInt(r)
		if err != nil {
			return VolumeHead{}
		}
		if flags&rar5HeaderFlagExtra != 0 {
			if _, err := readVInt(r); err != nil {
				return VolumeHead{}
			}
		}
		if flags&rar5HeaderFlagData != 0 {
			if _, err := readVInt(r); err != nil {
				return VolumeHead{}
			}
		}
		archiveFlags, err := readVInt(r)
		if err != nil {
			return VolumeHead{}
		}
		h := VolumeHead{Version: 5, Volume: archiveFlags&rar5ArchiveFlagVolume != 0}
		if !h.Volume {
			return h
		}
		if archiveFlags&RAR5MainFlagVolumeNumber == 0 {
			h.HasNumber = true
			return h
		}
		n, err := readVInt(r)
		if err != nil {
			return VolumeHead{Version: 5, Volume: true}
		}
		h.Number, h.HasNumber = int(n), true
		return h
	case bytes.HasPrefix(prefix, rar4Signature):
		m := prefix[len(rar4Signature):]
		if len(m) < 5 || m[2] != RAR4HeaderTypeArchive {
			return VolumeHead{}
		}
		flags := binary.LittleEndian.Uint16(m[3:5])
		return VolumeHead{Version: 4, Volume: flags&rar4MainFlagVolume != 0, First: flags&rar4MainFlagFirstVolume != 0}
	}
	return VolumeHead{}
}

// NameVolumeNumber returns the volume order a RAR volume's file name gives
// (name.partNN.rar, name.rar then name.rNN, or name.NNN), or false when the
// name gives none - an obfuscated name.
func NameVolumeNumber(name string) (int, bool) {
	ext := strings.ToLower(filepath.Ext(strings.TrimSpace(name)))
	if ext == "" {
		return 0, false
	}
	n := getRARVolumeOrder(strings.TrimSpace(name))
	if n == 999999 {
		return 0, false
	}
	return n, true
}

// FileHeaderStart is what a RAR volume's first file header says about the
// stored file whose data follows it.
type FileHeaderStart struct {
	// DataAt is the offset, from the start of prefix, where the file's data
	// in this volume begins.
	DataAt int64
	// SplitBefore reports the file's data continues from a previous volume:
	// this volume does not hold the file's first byte.
	SplitBefore bool
}

// rar4FileFlagSplitBefore and rar5HeaderFlagSplitBefore mark a file header
// whose data continues from the previous volume.
const (
	rar4FileFlagSplitBefore   = 0x0001
	rar4HeaderFlagLongBlock   = 0x8000
	rar5HeaderFlagSplitBefore = 0x0008
)

// ReadFileHeaderStart walks a RAR volume's leading bytes (from the start of
// its first article) to the first file header. ok is false when prefix does
// not start a RAR4/RAR5 volume or ends before that header does.
//
// Tide on Sark S02 on a production install stored each episode's .r00-.r83 volumes as a
// file of their own: its first volume's file header has split-before set, so
// the file served no Matroska header at all.
func ReadFileHeaderStart(prefix []byte) (FileHeaderStart, bool) {
	switch {
	case bytes.HasPrefix(prefix, rar5Signature):
		off := len(rar5Signature)
		for off < len(prefix) {
			r := bytes.NewReader(prefix[off:])
			if _, err := r.Seek(4, 1); err != nil { // header CRC32
				return FileHeaderStart{}, false
			}
			size, err := readVInt(r)
			if err != nil {
				return FileHeaderStart{}, false
			}
			bodyStart := off + (len(prefix[off:]) - r.Len())
			typ, err := readVInt(r)
			if err != nil {
				return FileHeaderStart{}, false
			}
			flags, err := readVInt(r)
			if err != nil {
				return FileHeaderStart{}, false
			}
			if flags&rar5HeaderFlagExtra != 0 {
				if _, err := readVInt(r); err != nil {
					return FileHeaderStart{}, false
				}
			}
			var dataSize uint64
			if flags&rar5HeaderFlagData != 0 {
				if dataSize, err = readVInt(r); err != nil {
					return FileHeaderStart{}, false
				}
			}
			end := bodyStart + int(size)
			if end > len(prefix) {
				return FileHeaderStart{}, false
			}
			if typ == 2 { // file header
				return FileHeaderStart{DataAt: int64(end), SplitBefore: flags&rar5HeaderFlagSplitBefore != 0}, true
			}
			if typ == 5 { // end of archive
				return FileHeaderStart{}, false
			}
			off = end + int(dataSize)
		}
		return FileHeaderStart{}, false
	case bytes.HasPrefix(prefix, rar4Signature):
		off := len(rar4Signature)
		for off+7 <= len(prefix) {
			typ := prefix[off+2]
			flags := binary.LittleEndian.Uint16(prefix[off+3:])
			size := int(binary.LittleEndian.Uint16(prefix[off+5:]))
			if size < 7 {
				return FileHeaderStart{}, false
			}
			if typ == 0x74 { // file header
				if off+size > len(prefix) {
					return FileHeaderStart{}, false
				}
				return FileHeaderStart{DataAt: int64(off + size), SplitBefore: flags&rar4FileFlagSplitBefore != 0}, true
			}
			if typ == RAR4HeaderTypeEnd {
				return FileHeaderStart{}, false
			}
			next := off + size
			if flags&rar4HeaderFlagLongBlock != 0 {
				if off+11 > len(prefix) {
					return FileHeaderStart{}, false
				}
				next += int(binary.LittleEndian.Uint32(prefix[off+7:]))
			}
			off = next
		}
		return FileHeaderStart{}, false
	}
	return FileHeaderStart{}, false
}

// TailVolumeNumber returns the volume number a RAR4 end-of-archive block in
// tail (a volume's decoded last bytes) stores.
func TailVolumeNumber(tail []byte) (int, bool) { return rar4TailVolumeNumber(tail) }
