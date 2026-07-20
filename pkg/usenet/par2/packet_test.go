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
	err := walkPackets(pkt, func(h packetHeader, packet []byte, offset int64) error {
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

	// Corrupt one byte of the body - MD5 verification must catch it.
	corrupt := append([]byte(nil), pkt...)
	corrupt[70] ^= 0xFF
	err = walkPackets(corrupt, func(h packetHeader, packet []byte, offset int64) error {
		return nil
	})
	if err == nil {
		t.Fatalf("expected a packet MD5 mismatch error on corrupted data, got none")
	}
}

func TestWalkPacketsMultiplePackets(t *testing.T) {
	var setID [16]byte
	setID[0] = 0x01

	p1 := buildPacket(t, setID, typeMain, make([]byte, 12))
	p2 := buildPacket(t, setID, typeFileDesc, make([]byte, 56))
	data := append(append([]byte{}, p1...), p2...)

	var types []string
	err := walkPackets(data, func(h packetHeader, packet []byte, offset int64) error {
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
	err := walkPackets(data, func(h packetHeader, packet []byte, offset int64) error {
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
