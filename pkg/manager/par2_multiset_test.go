package manager

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

// par2Packet builds one PAR2 packet (header + body, valid packet MD5).
func par2Packet(set [16]byte, typ string, body []byte) []byte {
	pkt := make([]byte, 64+len(body))
	copy(pkt[0:8], "PAR2\x00PKT")
	binary.LittleEndian.PutUint64(pkt[8:16], uint64(len(pkt)))
	copy(pkt[32:48], set[:])
	copy(pkt[48:64], typ)
	copy(pkt[64:], body)
	sum := md5.Sum(pkt[32:])
	copy(pkt[16:32], sum[:])
	return pkt
}

// otherSetIndex is a second recovery set's index file protecting one
// 4096-byte file called fileName.
func otherSetIndex(name, fileName string) (par2.Source, [16]byte) {
	var set, file [16]byte
	set[0], file[0] = 0xE2, 0xF2
	mainBody := make([]byte, 12+16)
	binary.LittleEndian.PutUint64(mainBody[0:8], 4096)
	binary.LittleEndian.PutUint32(mainBody[8:12], 1)
	copy(mainBody[12:], file[:])
	fd := make([]byte, 56+len(fileName)+(4-len(fileName)%4)%4)
	copy(fd[0:16], file[:])
	binary.LittleEndian.PutUint64(fd[48:56], 4096)
	copy(fd[56:], fileName)
	ifsc := make([]byte, 16+20)
	copy(ifsc[0:16], file[:])
	data := append(append(par2Packet(set, "PAR 2.0\x00Main\x00\x00\x00\x00", mainBody),
		par2Packet(set, "PAR 2.0\x00FileDesc", fd)...),
		par2Packet(set, "PAR 2.0\x00IFSC\x00\x00\x00\x00", ifsc)...)
	return par2.Source{Name: name, Data: data}, set
}

func multisetFixture(t *testing.T) (sources []par2.Source, set1, set2 [16]byte, vols []par2Volume, posted []storage.PostedFileRef) {
	t.Helper()
	ep1 := par2.Source{Name: "fixture.par2", Data: par2FixtureBytes(t, "fixture.par2")}
	idx1, err := par2.ParseIndex([]par2.Source{ep1})
	if err != nil {
		t.Fatal(err)
	}
	ep2, set2 := otherSetIndex("Show.S01E02.par2", "e02.rar")
	sources = []par2.Source{ep2, ep1}
	vols = []par2Volume{
		makeTestVol("fixture.vol0+1.par2", 1, "f0@news"),
		makeTestVol("Show.S01E02.vol00+01.par2", 1, "s0@news"),
		makeTestVol("fixture.vol1+2.par2", 2, "f1@news"),
	}
	for _, fd := range idx1.Files {
		posted = append(posted, storage.PostedFileRef{Name: fd.Name, Size: fd.Length,
			Segments: []storage.Par2SegmentRef{{MessageID: "<" + fd.Name + "-1>"}, {MessageID: "<" + fd.Name + "-2>"}}})
	}
	posted = append(posted, storage.PostedFileRef{Name: "e02.rar", Size: 4096,
		Segments: []storage.Par2SegmentRef{{MessageID: "<e02-1>"}, {MessageID: "<e02-2>"}}})
	return sources, idx1.SetID, set2, vols, posted
}

func TestChoosePar2SetPicksTheDamagedFilesSet(t *testing.T) {
	sources, set1, set2, vols, posted := multisetFixture(t)
	if _, err := par2.ParseIndex(sources); err == nil {
		t.Fatal("precondition: plain ParseIndex over two sets should fail")
	}

	// Episode 2 damaged only.
	c := choosePar2Set(sources, vols, posted, map[string][]overlay.DeadSegment{"e02.mkv": {{MessageID: "<e02-1>"}}})
	if c.setID != set2 {
		t.Fatalf("chose %x, want episode 2's set %x", c.setID, set2)
	}
	if len(c.sources) != 1 || c.sources[0].Name != "Show.S01E02.par2" {
		t.Fatalf("sources = %v", c.sources)
	}
	if len(c.vols) != 1 || c.vols[0].ref.Name != "Show.S01E02.vol00+01.par2" {
		t.Fatalf("vols = %v, want only episode 2's volume", c.vols)
	}
	if _, err := par2.ParseIndexSet(append(c.sources, sources...), c.setID); err != nil {
		t.Fatalf("ParseIndexSet on the chosen set: %v", err)
	}

	// Two files of set 1 damaged plus one of set 2: set 1 wins, set 2's
	// damage is deferred.
	f1, f2 := posted[0], posted[1]
	pending := map[string][]overlay.DeadSegment{
		"a.mkv": {{MessageID: f1.Segments[0].MessageID}, {MessageID: f2.Segments[1].MessageID}},
		"b.mkv": {{MessageID: "<e02-2>"}},
	}
	c = choosePar2Set(sources, vols, posted, pending)
	if c.setID != set1 {
		t.Fatalf("chose %x, want set 1 %x", c.setID, set1)
	}
	if c.deferred != 1 || len(c.pending["a.mkv"]) != 2 || len(c.pending["b.mkv"]) != 0 {
		t.Fatalf("deferred=%d pending=%v", c.deferred, c.pending)
	}
	if len(c.vols) != 2 {
		t.Fatalf("vols = %v, want set 1's two", c.vols)
	}
}

func TestChoosePar2SetLeavesAmbiguousOrSingleSetAlone(t *testing.T) {
	sources, _, _, vols, posted := multisetFixture(t)

	// One damaged file in each set: a tie, nothing changes.
	tie := map[string][]overlay.DeadSegment{"x": {{MessageID: posted[0].Segments[0].MessageID}, {MessageID: "<e02-1>"}}}
	if c := choosePar2Set(sources, vols, posted, tie); c.setID != ([16]byte{}) || len(c.sources) != 2 || len(c.vols) != 3 {
		t.Fatalf("tie changed the choice: %+v", c)
	}

	// A single set: unchanged.
	single := sources[1:]
	c := choosePar2Set(single, vols, posted, map[string][]overlay.DeadSegment{"x": {{MessageID: posted[0].Segments[0].MessageID}}})
	if c.setID != ([16]byte{}) || len(c.sources) != 1 || len(c.vols) != 3 {
		t.Fatalf("single set changed: %+v", c)
	}
}

// The errors runRepair returns for damage left to another set back off.
func TestMultisetDeferredErrorsAreTransient(t *testing.T) {
	for _, msg := range []string{
		"3 dead segment(s) in another PAR2 recovery set left for the next pass (transient: several recovery sets)",
		"every dead segment is in another PAR2 set (transient: chosen set has none)",
	} {
		if class := classifyPar2Failure(errors.New(msg)); class.terminal || class.suspect {
			t.Errorf("%q classified %+v, want transient", msg, class)
		}
	}
}

func TestPar2BaseName(t *testing.T) {
	for in, want := range map[string]string{
		"Show.S01E02.par2":          "show.s01e02",
		"Show.S01E02.vol03+04.PAR2": "show.s01e02",
		"Show.S01E02.vol-03.par2":   "show.s01e02",
		"Show.S01E02.vol07-12.par2": "show.s01e02",
	} {
		if got := par2BaseName(in); got != want {
			t.Errorf("par2BaseName(%q) = %q, want %q", in, got, want)
		}
	}
}
