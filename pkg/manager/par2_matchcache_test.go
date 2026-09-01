package manager

import (
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

func TestPar2MatchToCache(t *testing.T) {
	src := []storage.PostedFileRef{{Name: "a.rar"}, {Name: "b.rar"}}
	idA := [16]byte{1}
	idB := [16]byte{2}

	t.Run("full success -> cache", func(t *testing.T) {
		got := par2MatchToCache(src, []par2.Match{
			{PostedIndex: 0, FileID: idA},
			{PostedIndex: 1, FileID: idB, NameMismatch: true},
		}, nil)
		want := []storage.Par2MatchRef{
			{PostedName: "a.rar", FileID: idA},
			{PostedName: "b.rar", FileID: idB, NameMismatch: true},
		}
		if len(got) != len(want) {
			t.Fatalf("len = %d, want %d", len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("[%d] = %+v, want %+v", i, got[i], want[i])
			}
		}
	})

	t.Run("a skip -> not cached", func(t *testing.T) {
		if got := par2MatchToCache(src, []par2.Match{{PostedIndex: 0, FileID: idA}}, []par2.MatchSkip{{PostedIndex: 1}}); got != nil {
			t.Errorf("got %+v, want nil (skip present)", got)
		}
	})

	t.Run("not every posted file matched -> not cached", func(t *testing.T) {
		if got := par2MatchToCache(src, []par2.Match{{PostedIndex: 0, FileID: idA}}, nil); got != nil {
			t.Errorf("got %+v, want nil (1 of 2 matched)", got)
		}
	})

	t.Run("duplicate posted index -> not cached", func(t *testing.T) {
		if got := par2MatchToCache(src, []par2.Match{
			{PostedIndex: 0, FileID: idA},
			{PostedIndex: 0, FileID: idB},
		}, nil); got != nil {
			t.Errorf("got %+v, want nil (dup index)", got)
		}
	})
}

func TestPar2MatchFromCache(t *testing.T) {
	idx, ids := buildTestIndexN(t, 100, []earlyFileSpec{{"a.rar", 250}, {"b.rar", 250}})
	src := []storage.PostedFileRef{{Name: "a.rar"}, {Name: "b.rar"}}
	full := []storage.Par2MatchRef{
		{PostedName: "a.rar", FileID: ids["a.rar"]},
		{PostedName: "b.rar", FileID: ids["b.rar"], NameMismatch: true},
	}

	t.Run("full cache -> matches", func(t *testing.T) {
		got, ok := par2MatchFromCache(idx, src, full)
		if !ok || len(got) != 2 {
			t.Fatalf("ok=%v len=%d, want ok=true len=2", ok, len(got))
		}
		byIdx := map[int]par2.Match{}
		for _, m := range got {
			byIdx[m.PostedIndex] = m
		}
		if byIdx[0].FileID != ids["a.rar"] || byIdx[1].FileID != ids["b.rar"] || !byIdx[1].NameMismatch {
			t.Errorf("reconstructed matches wrong: %+v", got)
		}
	})

	t.Run("cache shorter than par2Source -> not ok", func(t *testing.T) {
		if _, ok := par2MatchFromCache(idx, src, full[:1]); ok {
			t.Errorf("ok=true, want false (partial cache)")
		}
	})

	t.Run("cache names a file not in par2Source -> not ok", func(t *testing.T) {
		bad := []storage.Par2MatchRef{full[0], {PostedName: "c.rar", FileID: ids["b.rar"]}}
		if _, ok := par2MatchFromCache(idx, src, bad); ok {
			t.Errorf("ok=true, want false (unknown posted name)")
		}
	})

	t.Run("cache names a FileID not in the index -> not ok", func(t *testing.T) {
		bad := []storage.Par2MatchRef{full[0], {PostedName: "b.rar", FileID: [16]byte{9, 9, 9}}}
		if _, ok := par2MatchFromCache(idx, src, bad); ok {
			t.Errorf("ok=true, want false (unknown FileID)")
		}
	})

	t.Run("empty cache -> not ok", func(t *testing.T) {
		if _, ok := par2MatchFromCache(idx, src, nil); ok {
			t.Errorf("ok=true, want false (no cache)")
		}
	})
}

// With two posted files of the SAME length, the network-free unique-length
// heuristic can't match either - but a full match cache resolves both, so the
// early gate sees the real damaged-slice count instead of 0.
func TestEarlyDamagedSliceCheckUsesMatchCache(t *testing.T) {
	idx, ids := buildTestIndexN(t, 100, []earlyFileSpec{
		{"a.rar", 250}, // 100 + 100 + 50 -> 3 slices
		{"b.rar", 250},
	})
	srcA := postedFile("a.rar", 100, 50, 2)
	srcB := postedFile("b.rar", 100, 50, 2)
	par2Source := []storage.PostedFileRef{srcA, srcB}
	pending := map[string][]overlay.DeadSegment{"a.rar": deadAt("a.rar", 0, 1, 2)}

	if got := earlyDamagedSliceCheck(idx, pending, par2Source, nil, zerolog.Nop()); got != 0 {
		t.Fatalf("without cache: got %d, want 0 (tied length, can't match)", got)
	}

	cache := []storage.Par2MatchRef{
		{PostedName: "a.rar", FileID: ids["a.rar"]},
		{PostedName: "b.rar", FileID: ids["b.rar"]},
	}
	if got := earlyDamagedSliceCheck(idx, pending, par2Source, cache, zerolog.Nop()); got != 3 {
		t.Fatalf("with cache: got %d, want 3", got)
	}
}
