package par2

import (
	"encoding/binary"
	"testing"
)

// secondSetIndex builds a minimal index file of another recovery set: one
// 4096-byte file named name.
func secondSetIndex(t *testing.T, setByte byte, name string) (Source, [16]byte) {
	t.Helper()
	var set, file [16]byte
	set[0], file[0] = setByte, setByte+1
	mainBody := make([]byte, 12+16)
	binary.LittleEndian.PutUint64(mainBody[0:8], 4096)
	binary.LittleEndian.PutUint32(mainBody[8:12], 1)
	copy(mainBody[12:], file[:])
	fd := make([]byte, 56+8)
	copy(fd[0:16], file[:])
	binary.LittleEndian.PutUint64(fd[48:56], 4096)
	copy(fd[56:], name)
	ifsc := make([]byte, 16+20)
	copy(ifsc[0:16], file[:])
	data := append(append(buildPacket(t, set, typeMain, mainBody), buildPacket(t, set, typeFileDesc, fd)...), buildPacket(t, set, typeIFSC, ifsc)...)
	return Source{Name: "Show.S01E02.par2", Data: data}, set
}

// A season pack carries one PAR2 set per episode. ParseIndex parses the set
// of the first Main packet and skips the rest; ParseIndexSet parses just the
// set it is asked for.
func TestParseIndexSetPicksOneSetOfSeveral(t *testing.T) {
	episode1 := loadFixtureSources(t)
	ep2, set2 := secondSetIndex(t, 0xE2, "e02.rar")
	sources := append([]Source{ep2}, episode1...)

	mixed, err := ParseIndex(sources)
	if err != nil {
		t.Fatalf("ParseIndex over two sets: %v, want the first Main packet's set", err)
	}
	if mixed.SetID != set2 || len(mixed.Files) != 1 || mixed.OtherSetPackets == 0 || len(mixed.SkippedPackets) != 0 {
		t.Fatalf("ParseIndex over two sets: set=%x files=%d other=%d skipped=%d, want set %x, 1 file, the other set counted and nothing skipped as bad",
			mixed.SetID, len(mixed.Files), mixed.OtherSetPackets, len(mixed.SkippedPackets), set2)
	}

	ids := SourceSetIDs(sources)
	if ids[0] != set2 {
		t.Fatalf("SourceSetIDs[0] = %x, want %x", ids[0], set2)
	}
	set1 := ids[1]
	if set1 == set2 || set1 == ([16]byte{}) {
		t.Fatalf("fixture set id %x", set1)
	}

	want, err := ParseIndex(episode1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseIndexSet(sources, set1)
	if err != nil {
		t.Fatalf("ParseIndexSet(episode 1): %v", err)
	}
	if got.SetID != set1 || got.NumSlices() != want.NumSlices() || len(got.Recovery) != len(want.Recovery) || len(got.Files) != len(want.Files) {
		t.Fatalf("episode 1 index: slices=%d/%d recovery=%d/%d files=%d/%d", got.NumSlices(), want.NumSlices(), len(got.Recovery), len(want.Recovery), len(got.Files), len(want.Files))
	}

	got2, err := ParseIndexSet(sources, set2)
	if err != nil {
		t.Fatalf("ParseIndexSet(episode 2): %v", err)
	}
	if got2.SetID != set2 || len(got2.Files) != 1 {
		t.Fatalf("episode 2 index: set=%x files=%d", got2.SetID, len(got2.Files))
	}

	// A zero set id is plain ParseIndex.
	if got0, err := ParseIndexSet(sources, [16]byte{}); err != nil || got0.SetID != set2 {
		t.Fatalf("ParseIndexSet with a zero set id: %v, want ParseIndex's choice %x", err, set2)
	}
}

// The first packet of the list belongs to a set whose Main packet is not in
// it (a recovery volume of another episode, with that episode's index not
// fetched). The parse adopts the set that has a Main packet instead of
// failing on the first packet's set.
func TestParseIndexAdoptsTheFirstMainPacketsSet(t *testing.T) {
	episode1 := loadFixtureSources(t)
	var set2 [16]byte
	set2[0] = 0xE2
	strayRecovery := Source{Name: "Show.S01E02.vol00+01.par2", Data: buildPacket(t, set2, typeRecvSlic, make([]byte, 4+4096))}
	sources := append([]Source{strayRecovery}, episode1...)

	want, err := ParseIndex(episode1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseIndex(sources)
	if err != nil {
		t.Fatalf("ParseIndex with another set's recovery packet first: %v", err)
	}
	if got.SetID != want.SetID || len(got.Recovery) != len(want.Recovery) || got.OtherSetPackets != 1 {
		t.Fatalf("set=%x recovery=%d other=%d, want set %x, recovery %d, 1 other-set packet",
			got.SetID, len(got.Recovery), got.OtherSetPackets, want.SetID, len(want.Recovery))
	}
}
