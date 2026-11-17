package par2

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"
)

// hostileHeaders is a .par2 file made only of packet headers that each
// declare "the rest of the file" as their length. Every header parses and
// every MD5 fails.
func hostileHeaders(size int) []byte {
	data := make([]byte, size)
	for pos := 0; pos+packetHeaderSize <= size; pos += packetHeaderSize {
		copy(data[pos:], packetMagic[:])
		rem := (size - pos) &^ 3
		binary.LittleEndian.PutUint64(data[pos+8:], uint64(rem))
	}
	return data
}

// A lenient walk of the hostile file used to hash the remainder once per
// header (16k MD5 passes over a 1 MiB file, size²/128 bytes). It must stop
// after a fixed number of failed packets that re-hash the same bytes.
func TestWalkPacketsHostileHeadersBounded(t *testing.T) {
	data := hostileHeaders(1 << 20)
	failed := 0
	start := time.Now()
	err := walkPackets(context.Background(), data, func(packetHeader, int64) { failed++ },
		func(packetHeader, []byte, int64) error { return nil })
	if err != nil {
		t.Fatalf("walkPackets: %v", err)
	}
	if failed > maxRehashedFailures+1 {
		t.Fatalf("hashed %d failed packets over the same region, want at most %d", failed, maxRehashedFailures+1)
	}
	if el := time.Since(start); el > 5*time.Second {
		t.Fatalf("walk took %v", el)
	}
}

// A cancelled job context ends the parse at once with the context's error,
// so the caller sees a cancel, not a corrupt PAR2.
func TestParseIndexSetReturnsPromptlyOnCancel(t *testing.T) {
	data := hostileHeaders(4 << 20)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	_, err := ParseIndexSet(ctx, []Source{{Name: "x.par2", Data: data}}, [16]byte{1})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ParseIndexSet on a cancelled ctx: err = %v, want context.Canceled", err)
	}
	if err := walkPackets(ctx, data, func(packetHeader, int64) {}, func(packetHeader, []byte, int64) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("walkPackets on a cancelled ctx: err = %v, want context.Canceled", err)
	}
	if el := time.Since(start); el > time.Second {
		t.Fatalf("cancelled parse took %v", el)
	}
}

// Damage at separate places costs nothing toward the limit: a volume with
// far more than maxRehashedFailures damaged packets still yields every intact
// packet after them. Half the damaged packets are corrupted in place, half
// are shortened (a dead article's bytes missing), so their declared length
// runs into the intact packet that follows.
func TestWalkPacketsSalvagesPastManySeparateDamagedPackets(t *testing.T) {
	var setID [16]byte
	setID[0] = 0x07
	const pairs = 3 * maxRehashedFailures
	var data []byte
	for i := range pairs {
		bad := buildPacket(t, setID, typeRecvSlic, make([]byte, 400))
		if i%2 == 0 {
			bad[100] ^= 0xFF
		} else {
			bad = append(bad[:200:200], bad[300:]...)
		}
		data = append(data, bad...)
		body := make([]byte, 200)
		binary.LittleEndian.PutUint32(body, uint32(i))
		data = append(data, buildPacket(t, setID, typeFileDesc, body)...)
	}
	failed, intact := 0, 0
	err := walkPackets(context.Background(), data, func(packetHeader, int64) { failed++ },
		func(h packetHeader, _ []byte, _ int64) error {
			intact++
			return nil
		})
	if err != nil {
		t.Fatalf("walkPackets: %v", err)
	}
	if intact != pairs {
		t.Fatalf("salvaged %d intact packets, want %d (failed=%d)", intact, pairs, failed)
	}
	if failed != pairs {
		t.Fatalf("reported %d damaged packets, want %d", failed, pairs)
	}
}
