package manager

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

// unwritablePatchFixture is one posted file "a.mkv" of 250 bytes in three
// articles, [0,80) [80,160) [160,250), all recorded dead, with every slice
// of the file rebuilt (slice size 100). The reader slots match the articles
// unless a test changes them; recordBytes overrides the slot length the
// overlay records for a segment.
type unwritablePatchFixture struct {
	p        *Par2Repair
	store    *overlay.Store
	idx      *par2.Index
	nzb      *storage.NZB
	refs     []par2DeadRef
	repaired map[int64][]byte
}

const unwritableNZBID = "nzb-p18"

func newUnwritablePatchFixture(t *testing.T, recordBytes map[int]int64) *unwritablePatchFixture {
	t.Helper()
	idx, fileA, _ := buildTestIndex(t, 100, 250, 300)
	store, err := overlay.NewStore(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("overlay.NewStore: %v", err)
	}
	u := usenet.NewWithOverlayForTest(store)
	bounds := [][2]int64{{0, 80}, {80, 160}, {160, 250}}
	file := storage.NZBFile{Name: "a.mkv", Size: 250}
	var refs []par2DeadRef
	for i, b := range bounds {
		mid := "<a" + string(rune('0'+i)) + "@x>"
		seg := storage.NZBSegment{Number: i + 1, MessageID: mid, Bytes: b[1] - b[0], StartOffset: b[0], EndOffset: b[1]}
		file.Segments = append(file.Segments, seg)
		recorded := seg.Bytes
		if n, ok := recordBytes[i]; ok {
			recorded = n
		}
		if err := store.RecordDead(unwritableNZBID, "a.mkv", i, mid, recorded); err != nil {
			t.Fatalf("RecordDead: %v", err)
		}
		refs = append(refs, par2DeadRef{
			file: "a.mkv",
			seg:  overlay.DeadSegment{Index: i, MessageID: mid, Bytes: seg.Bytes, Status: overlay.StatusDead},
			rng:  postedRange{fileID: fileA, start: b[0], end: b[1]},
		})
	}
	// Pass them in reverse: the write order must not depend on it.
	refs[0], refs[2] = refs[2], refs[0]
	return &unwritablePatchFixture{
		p:     &Par2Repair{manager: &Manager{usenet: u}, logger: zerolog.Nop()},
		store: store,
		idx:   idx,
		nzb:   &storage.NZB{ID: unwritableNZBID, Files: []storage.NZBFile{file}},
		refs:  refs,
		repaired: map[int64][]byte{
			0: repeatByte(0x10, 100),
			1: repeatByte(0x11, 100),
			2: repeatByte(0x12, 100),
		},
	}
}

func (f *unwritablePatchFixture) write(t *testing.T) ([]par2DeadRef, error, error) {
	t.Helper()
	return f.p.writeRecordedPatches(unwritableNZBID, "Entry", f.nzb, f.idx, f.repaired, f.refs)
}

func (f *unwritablePatchFixture) status(t *testing.T, seg int) overlay.SegmentStatus {
	t.Helper()
	m, err := f.store.GetManifest(unwritableNZBID)
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	fe := m.Files["a.mkv"]
	if fe == nil {
		t.Fatalf("no overlay record for a.mkv")
	}
	for _, d := range fe.DeadSegments {
		if d.Index == seg {
			return d.Status
		}
	}
	t.Fatalf("no overlay record for segment %d", seg)
	return ""
}

// P-18: one segment whose patch cannot be placed used to end the write loop
// at that segment, leaving the segments after it unpatched although the solve
// had rebuilt them, and the error was transient, so every retry re-read the
// whole release to fail at the same place. Now the others are written and
// the pass ends terminal.
func TestWriteRecordedPatchesSkipsAnUnwritableSegment(t *testing.T) {
	cases := []struct {
		name        string
		recordBytes map[int]int64
		nzbSlot     int64
		want        string
	}{{
		// readerPatchWindow: the reader's slot runs one byte past the article.
		name:    "slot past the article",
		nzbSlot: 81,
		want:    "does not fit the article",
	}, {
		// overlay.Store.WritePatch: the recorded slot is one byte longer than
		// the patch cut for it.
		name:        "overlay refuses the length",
		recordBytes: map[int]int64{1: 81},
		want:        "geometry disagree",
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newUnwritablePatchFixture(t, tc.recordBytes)
			if tc.nzbSlot > 0 {
				f.nzb.Files[0].Segments[1].Bytes = tc.nzbSlot
			}
			written, unwritable, err := f.write(t)
			if err != nil {
				t.Fatalf("fatal err = %v, want nil (an unwritable segment is skipped)", err)
			}
			if len(written) != 2 || written[0].seg.Index != 0 || written[1].seg.Index != 2 {
				t.Fatalf("written = %v, want segments 0 and 2 in order", written)
			}
			for seg, want := range map[int]overlay.SegmentStatus{0: overlay.StatusPatched, 1: overlay.StatusDead, 2: overlay.StatusPatched} {
				if got := f.status(t, seg); got != want {
					t.Errorf("segment %d status = %s, want %s", seg, got, want)
				}
			}
			if data, ok := f.store.PatchBytes(unwritableNZBID, "a.mkv", 2); !ok || len(data) != 90 ||
				!bytes.Equal(data[:40], repeatByte(0x11, 40)) || !bytes.Equal(data[40:], repeatByte(0x12, 50)) {
				t.Errorf("segment 2 patch = %d bytes (ok=%v), want [160,250) of the rebuilt slices", len(data), ok)
			}
			if unwritable == nil {
				t.Fatal("unwritable = nil, want the skipped segment reported")
			}
			if !strings.Contains(unwritable.Error(), "1 dead segment(s) cannot be patched") || !strings.Contains(unwritable.Error(), tc.want) {
				t.Errorf("unwritable = %q, want the count and %q", unwritable, tc.want)
			}
			if class := par2OutcomeClass(unwritable, 1); !class.terminal {
				t.Errorf("class of %q = %+v, want terminal on the first attempt", unwritable, class)
			}
		})
	}
}

// Errors that are not about where the patch goes still stop the loop at once:
// a deleted entry must not get its overlay directory re-created segment by
// segment.
func TestWriteRecordedPatchesStopsOnARejectedEntry(t *testing.T) {
	f := newUnwritablePatchFixture(t, nil)
	f.store.MarkRejected(unwritableNZBID)
	written, unwritable, err := f.write(t)
	if !errors.Is(err, overlay.ErrEntryRejected) {
		t.Fatalf("err = %v, want overlay.ErrEntryRejected", err)
	}
	if len(written) != 0 || unwritable != nil {
		t.Fatalf("written = %v, unwritable = %v; want nothing after the refusal", written, unwritable)
	}
}

// With every slot fitting, all three are written and nothing is reported.
func TestWriteRecordedPatchesWritesAll(t *testing.T) {
	f := newUnwritablePatchFixture(t, nil)
	written, unwritable, err := f.write(t)
	if err != nil || unwritable != nil || len(written) != 3 {
		t.Fatalf("written = %d, unwritable = %v, err = %v; want 3, nil, nil", len(written), unwritable, err)
	}
}
