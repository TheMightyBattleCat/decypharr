// Package par2 implements the parts of the PAR2 v2.0 recovery format
// (https://parchive.github.io/doc/par2spec) needed to reconstruct damaged
// segments of a Usenet release from its posted recovery data: packet
// parsing, the posted-file/recovery-set slice mapping, GF(2^16) arithmetic,
// and the streaming repair/verify pass itself. It has no knowledge of NNTP,
// the overlay store, or any other decypharr package - callers assemble the
// raw bytes (however they fetched them) and hand them to this package.
package par2

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
)

// packetHeaderSize is the fixed size, in bytes, of every PAR2 packet header:
// magic(8) + length(8) + packet MD5(16) + Recovery Set ID(16) + type(16).
const packetHeaderSize = 64

// packetMagic is the fixed 8-byte value every PAR2 packet header starts with.
var packetMagic = [8]byte{'P', 'A', 'R', '2', 0, 'P', 'K', 'T'}

// packetType is a PAR2 packet's 16-byte type field.
type packetType [16]byte

func mustPacketType(s string) packetType {
	if len(s) != 16 {
		panic(fmt.Sprintf("par2: packet type literal %q is not 16 bytes", s))
	}
	var t packetType
	copy(t[:], s)
	return t
}

var (
	typeMain     = mustPacketType("PAR 2.0\x00Main\x00\x00\x00\x00")
	typeFileDesc = mustPacketType("PAR 2.0\x00FileDesc")
	typeIFSC     = mustPacketType("PAR 2.0\x00IFSC\x00\x00\x00\x00")
	typeRecvSlic = mustPacketType("PAR 2.0\x00RecvSlic")
)

// packetHeader is one parsed 64-byte PAR2 packet header.
type packetHeader struct {
	Length        int64 // total packet length (header + body), a multiple of 4
	MD5           [16]byte
	RecoverySetID [16]byte
	Type          packetType
}

// parsePacketHeader parses the 64-byte header at the start of b. It does not
// verify the packet MD5 (that covers the body too - see verifyPacketMD5) or
// bounds-check Length against the buffer holding the full packet; callers do
// both once they know where the packet body actually is.
func parsePacketHeader(b []byte) (packetHeader, error) {
	if len(b) < packetHeaderSize {
		return packetHeader{}, fmt.Errorf("par2: truncated packet header (%d bytes)", len(b))
	}
	if [8]byte(b[0:8]) != packetMagic {
		return packetHeader{}, fmt.Errorf("par2: bad packet magic")
	}
	length := int64(binary.LittleEndian.Uint64(b[8:16]))
	if length < packetHeaderSize || length%4 != 0 {
		return packetHeader{}, fmt.Errorf("par2: invalid packet length %d", length)
	}
	var h packetHeader
	h.Length = length
	copy(h.MD5[:], b[16:32])
	copy(h.RecoverySetID[:], b[32:48])
	copy(h.Type[:], b[48:64])
	return h, nil
}

// verifyPacketMD5 reports whether packet (the full packetHeader.Length bytes,
// header included) has a valid packet MD5 - the hash of everything from the
// Recovery Set ID field (offset 32) to the end of the packet.
func verifyPacketMD5(h packetHeader, packet []byte) bool {
	if int64(len(packet)) != h.Length {
		return false
	}
	return md5.Sum(packet[32:]) == h.MD5
}

// walkPackets calls fn once for every well-formed, MD5-verified packet found
// in data, back-to-back starting at offset 0 (the layout PAR2 files use - no
// padding between packets). It stops (without error) at the first byte range
// that isn't a valid, complete, MD5-correct packet header - typically the
// natural end of file, but also a defensible way to stop cleanly on trailing
// garbage rather than hard-failing the whole file over it.
func walkPackets(data []byte, fn func(h packetHeader, packet []byte, offset int64) error) error {
	pos := int64(0)
	for pos+packetHeaderSize <= int64(len(data)) {
		h, err := parsePacketHeader(data[pos:])
		if err != nil {
			break
		}
		if pos+h.Length > int64(len(data)) {
			break
		}
		packet := data[pos : pos+h.Length]
		if !verifyPacketMD5(h, packet) {
			return fmt.Errorf("par2: packet MD5 mismatch at offset %d (type %q)", pos, packetTypeName(h.Type))
		}
		if err := fn(h, packet, pos); err != nil {
			return err
		}
		pos += h.Length
	}
	return nil
}

func packetTypeName(t packetType) string {
	switch t {
	case typeMain:
		return "Main"
	case typeFileDesc:
		return "FileDesc"
	case typeIFSC:
		return "IFSC"
	case typeRecvSlic:
		return "RecvSlic"
	default:
		return fmt.Sprintf("%x", t[:])
	}
}
