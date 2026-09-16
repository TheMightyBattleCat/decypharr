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

// TailVolumeNumber returns the volume number a RAR4 end-of-archive block in
// tail (a volume's decoded last bytes) stores.
func TailVolumeNumber(tail []byte) (int, bool) { return rar4TailVolumeNumber(tail) }
