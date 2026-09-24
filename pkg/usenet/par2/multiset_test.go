package par2

import (
	"encoding/binary"
	"strings"
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

// A season pack carries one PAR2 set per episode. ParseIndex still refuses
// the mix; ParseIndexSet parses just the set it is asked for.
func TestParseIndexSetPicksOneSetOfSeveral(t *testing.T) {
	episode1 := loadFixtureSources(t)
	ep2, set2 := secondSetIndex(t, 0xE2, "e02.rar")
	sources := append([]Source{ep2}, episode1...)

	if _, err := ParseIndex(sources); err == nil || !strings.Contains(err.Error(), "different recovery set") {
		t.Fatalf("ParseIndex over two sets: err=%v, want the different-set error", err)
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
	if _, err := ParseIndexSet(sources, [16]byte{}); err == nil {
		t.Fatal("ParseIndexSet with a zero set id should behave like ParseIndex")
	}
}
