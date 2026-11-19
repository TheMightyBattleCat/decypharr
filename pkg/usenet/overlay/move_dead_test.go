package overlay

import (
	"bytes"
	"reflect"
	"testing"
)

type deadAt struct {
	index int
	msgID string
	bytes int64
}

func deadRecords(t *testing.T, s *Store, nzbID, file string) []deadAt {
	t.Helper()
	m, err := s.GetManifest(nzbID)
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	fe := m.Files[file]
	if fe == nil {
		return nil
	}
	var out []deadAt
	for _, d := range fe.DeadSegments {
		out = append(out, deadAt{d.Index, d.MessageID, d.Bytes})
	}
	return out
}

// A volume reorder moved the file's articles after the damage was recorded:
// the record follows its article to the new index and takes that slot's
// length, which WritePatch then checks the repaired bytes against.
func TestMoveDeadSegmentsFollowsTheArticle(t *testing.T) {
	s := newTestStore(t)
	const nzbID, file = "nzb-move", "a.mkv"
	if err := s.RecordDead(nzbID, file, 4, "<x>", 100); err != nil {
		t.Fatal(err)
	}
	applied, err := s.MoveDeadSegments(nzbID, file, []DeadMove{{From: 4, To: 6, MessageID: "<x>", Bytes: 120}})
	if err != nil || len(applied) != 1 || applied[0].Dropped {
		t.Fatalf("applied=%+v err=%v, want the one move", applied, err)
	}
	if got, want := deadRecords(t, s, nzbID, file), []deadAt{{6, "<x>", 120}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("records %+v, want %+v", got, want)
	}
	if err := s.WritePatch(nzbID, file, 6, bytes.Repeat([]byte{1}, 120)); err != nil {
		t.Fatalf("WritePatch at the new index: %v", err)
	}
}

// Two dead articles that swapped places both move: one at a time, each
// would find the other's record in its way.
func TestMoveDeadSegmentsSwapsTwoRecords(t *testing.T) {
	s := newTestStore(t)
	const nzbID, file = "nzb-swap", "a.mkv"
	_ = s.RecordDead(nzbID, file, 4, "<x>", 100)
	_ = s.RecordDead(nzbID, file, 6, "<y>", 100)
	applied, err := s.MoveDeadSegments(nzbID, file, []DeadMove{
		{From: 4, To: 6, MessageID: "<x>"},
		{From: 6, To: 4, MessageID: "<y>"},
	})
	if err != nil || len(applied) != 2 {
		t.Fatalf("applied=%+v err=%v, want both moves", applied, err)
	}
	if got, want := deadRecords(t, s, nzbID, file), []deadAt{{4, "<y>", 100}, {6, "<x>", 100}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("records %+v, want %+v", got, want)
	}
}

// Moves that would put two records on one index, or that name a record not
// at From (another article, or already patched), change nothing.
func TestMoveDeadSegmentsRefusesConflicts(t *testing.T) {
	s := newTestStore(t)
	const nzbID, file = "nzb-refuse", "a.mkv"
	_ = s.RecordDead(nzbID, file, 1, "<p>", 100)
	_ = s.RecordDead(nzbID, file, 4, "<x>", 100)
	_ = s.RecordDead(nzbID, file, 6, "<other>", 100)
	_ = s.RecordDead(nzbID, file, 8, "<z>", 100)
	if err := s.WritePatch(nzbID, file, 1, bytes.Repeat([]byte{1}, 100)); err != nil {
		t.Fatal(err)
	}
	before := deadRecords(t, s, nzbID, file)
	applied, err := s.MoveDeadSegments(nzbID, file, []DeadMove{
		{From: 4, To: 6, MessageID: "<x>"},     // 6 holds another article that is staying
		{From: 8, To: 9, MessageID: "<wrong>"}, // the record at 8 is another article
		{From: 1, To: 2, MessageID: "<p>"},     // patched
		{From: 5, To: 7, MessageID: "<q>"},     // no record at 5
	})
	if err != nil || len(applied) != 0 {
		t.Fatalf("applied=%+v err=%v, want nothing", applied, err)
	}
	if got := deadRecords(t, s, nzbID, file); !reflect.DeepEqual(got, before) {
		t.Fatalf("records %+v, want them unchanged %+v", got, before)
	}

	// Two records asking for the same index: neither moves.
	_ = s.RecordDead(nzbID, file, 10, "<m>", 100)
	_ = s.RecordDead(nzbID, file, 11, "<n>", 100)
	applied, err = s.MoveDeadSegments(nzbID, file, []DeadMove{
		{From: 10, To: 12, MessageID: "<m>"},
		{From: 11, To: 12, MessageID: "<n>"},
	})
	if err != nil || len(applied) != 0 {
		t.Fatalf("applied=%+v err=%v, want neither of two moves to one index", applied, err)
	}
}

// The reader already recorded the article at its new index: the record at
// the old index is a stale copy and goes.
func TestMoveDeadSegmentsDropsAStaleDuplicate(t *testing.T) {
	s := newTestStore(t)
	const nzbID, file = "nzb-dup", "a.mkv"
	_ = s.RecordDead(nzbID, file, 4, "<x>", 100)
	_ = s.RecordDead(nzbID, file, 6, "<x>", 120)
	applied, err := s.MoveDeadSegments(nzbID, file, []DeadMove{{From: 4, To: 6, MessageID: "<x>", Bytes: 120}})
	if err != nil || len(applied) != 1 || !applied[0].Dropped || applied[0].From != 4 {
		t.Fatalf("applied=%+v err=%v, want the record at 4 dropped", applied, err)
	}
	if got, want := deadRecords(t, s, nzbID, file), []deadAt{{6, "<x>", 120}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("records %+v, want %+v", got, want)
	}
}
