package par2

import (
	"crypto/md5"
	"encoding/binary"
	"testing"
)

// buildPacket assembles a complete, MD5-correct PAR2 packet: header + body.
func buildPacket(t *testing.T, setID [16]byte, typ packetType, body []byte) []byte {
	t.Helper()
	for len(body)%4 != 0 {
		body = append(body, 0)
	}
	length := int64(packetHeaderSize + len(body))
	pkt := make([]byte, length)
	copy(pkt[0:8], packetMagic[:])
	binary.LittleEndian.PutUint64(pkt[8:16], uint64(length))
	copy(pkt[32:48], setID[:])
	copy(pkt[48:64], typ[:])
	copy(pkt[64:], body)
	sum := md5.Sum(pkt[32:])
	copy(pkt[16:32], sum[:])
	return pkt
}

func TestParsePacketHeaderRejectsBadMagic(t *testing.T) {
	b := make([]byte, packetHeaderSize)
	copy(b, []byte("NOTPAR2!"))
	if _, err := parsePacketHeader(b); err == nil {
		t.Fatalf("expected an error for a bad magic, got none")
	}
}

// A garbage header declaring a length near MaxInt64 used to wrap pos+Length
// negative, pass the bounds check and panic the slice. Lenient mode resyncs
// on magic inside damaged volumes, so it must resume past it instead.
func TestWalkPacketsSurvivesOverflowingLength(t *testing.T) {
	var setID [16]byte
	good := buildPacket(t, setID, typeMain, []byte("body"))
	bogus := make([]byte, packetHeaderSize)
	copy(bogus[0:8], packetMagic[:])
	binary.LittleEndian.PutUint64(bogus[8:16], uint64(1<<63-4)) // multiple of 4, near MaxInt64
	data := append(append(append([]byte{}, good...), bogus...), good...)

	seen := 0
	err := walkPackets(data, func(packetHeader, int64) {}, func(packetHeader, []byte, int64) error {
		seen++
		return nil
	})
	if err != nil {
		t.Fatalf("walkPackets: %v", err)
	}
	if seen != 2 {
		t.Fatalf("saw %d packets, want both good ones around the bogus header", seen)
	}
}

func TestParsePacketHeaderRejectsShortBuffer(t *testing.T) {
	if _, err := parsePacketHeader(make([]byte, 10)); err == nil {
		t.Fatalf("expected an error for a too-short buffer, got none")
	}
}

func TestWalkPacketsVerifiesMD5(t *testing.T) {
	var setID [16]byte
	setID[0] = 0xAB

	body := make([]byte, 8) // enough for a minimal fake body
	pkt := buildPacket(t, setID, typeMain, body)

	var seen int
	err := walkPackets(pkt, nil, func(h packetHeader, packet []byte, offset int64) error {
		seen++
		if h.Type != typeMain {
			t.Errorf("packet type = %q, want Main", packetTypeName(h.Type))
		}
		if h.RecoverySetID != setID {
			t.Errorf("RecoverySetID = %x, want %x", h.RecoverySetID, setID)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walkPackets: %v", err)
	}
	if seen != 1 {
		t.Fatalf("walkPackets visited %d packets, want 1", seen)
	}

	// Corrupt one byte of the body - MD5 verification must catch it. With a
	// nil onChecksumError callback walkPackets fails strict.
	corrupt := append([]byte(nil), pkt...)
	corrupt[70] ^= 0xFF
	err = walkPackets(corrupt, nil, func(h packetHeader, packet []byte, offset int64) error {
		return nil
	})
	if err == nil {
		t.Fatalf("expected a packet MD5 mismatch error on corrupted data, got none")
	}
}

func TestWalkPacketsSkipsBadPacketWithRecorder(t *testing.T) {
	var setID [16]byte
	setID[0] = 0x02

	p1 := buildPacket(t, setID, typeMain, make([]byte, 12))
	p2 := buildPacket(t, setID, typeFileDesc, make([]byte, 56))
	data := append(append([]byte{}, p1...), p2...)
	data[70] ^= 0xFF // corrupt p1's body; p2 stays intact

	var skippedOffsets []int64
	var visited []string
	err := walkPackets(data, func(h packetHeader, offset int64) {
		skippedOffsets = append(skippedOffsets, offset)
	}, func(h packetHeader, packet []byte, offset int64) error {
		visited = append(visited, packetTypeName(h.Type))
		return nil
	})
	if err != nil {
		t.Fatalf("walkPackets with a recorder should not error on a bad packet: %v", err)
	}
	if len(skippedOffsets) != 1 || skippedOffsets[0] != 0 {
		t.Fatalf("skipped offsets = %v, want [0]", skippedOffsets)
	}
	if len(visited) != 1 || visited[0] != "FileDesc" {
		t.Fatalf("visited packet types = %v, want [FileDesc]", visited)
	}
}

func TestWalkPacketsMultiplePackets(t *testing.T) {
	var setID [16]byte
	setID[0] = 0x01

	p1 := buildPacket(t, setID, typeMain, make([]byte, 12))
	p2 := buildPacket(t, setID, typeFileDesc, make([]byte, 56))
	data := append(append([]byte{}, p1...), p2...)

	var types []string
	err := walkPackets(data, nil, func(h packetHeader, packet []byte, offset int64) error {
		types = append(types, packetTypeName(h.Type))
		return nil
	})
	if err != nil {
		t.Fatalf("walkPackets: %v", err)
	}
	if len(types) != 2 || types[0] != "Main" || types[1] != "FileDesc" {
		t.Fatalf("walked types = %v, want [Main FileDesc]", types)
	}
}

func TestWalkPacketsStopsOnTrailingGarbage(t *testing.T) {
	var setID [16]byte
	p1 := buildPacket(t, setID, typeMain, make([]byte, 12))
	data := append(append([]byte{}, p1...), []byte{1, 2, 3}...) // not a valid packet header

	var seen int
	err := walkPackets(data, nil, func(h packetHeader, packet []byte, offset int64) error {
		seen++
		return nil
	})
	if err != nil {
		t.Fatalf("walkPackets should stop cleanly on trailing garbage, got error: %v", err)
	}
	if seen != 1 {
		t.Fatalf("walkPackets visited %d packets, want 1", seen)
	}
}
