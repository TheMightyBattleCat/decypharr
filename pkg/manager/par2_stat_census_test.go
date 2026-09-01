package manager

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

// mockStat returns a stat func: any message ID in notFound reports a
// definitive 430, any ID in transport reports a non-definitive error, every
// other ID is available.
func mockStat(notFound, transport map[string]bool) func(context.Context, []string) ([]nntp.StatResult, error) {
	return func(_ context.Context, ids []string) ([]nntp.StatResult, error) {
		out := make([]nntp.StatResult, len(ids))
		for i, id := range ids {
			switch {
			case notFound[id]:
				out[i] = nntp.StatResult{MessageID: id, Available: false, Error: par2NotFoundErr()}
			case transport[id]:
				out[i] = nntp.StatResult{MessageID: id, Available: false, Error: par2TransportErr()}
			default:
				out[i] = nntp.StatResult{MessageID: id, Available: true}
			}
		}
		return out, nil
	}
}

func volNames(vols []par2Volume) []string {
	out := make([]string, len(vols))
	for i, v := range vols {
		out[i] = v.ref.Name
	}
	return out
}

func availOf(vols []par2Volume) uint32 {
	var n uint32
	for _, v := range vols {
		n += v.count
	}
	return n
}

func TestStatRecoveryVolumes(t *testing.T) {
	ctx := context.Background()

	t.Run("all alive -> unchanged", func(t *testing.T) {
		vols := []par2Volume{
			makeTestVol("v0.par2", 1, "a@n"),
			makeTestVol("v1.par2", 1, "b@n"),
		}
		got := statRecoveryVolumes(ctx, parallelFetchNopLogger, mockStat(nil, nil), vols, "e")
		if availOf(got) != 2 || len(got) != 2 {
			t.Fatalf("got %v (avail %d), want 2 vols avail 2", volNames(got), availOf(got))
		}
	})

	t.Run("a 430 volume is dropped and available drops", func(t *testing.T) {
		vols := []par2Volume{
			makeTestVol("v0.par2", 3, "a@n"),
			makeTestVol("v1.par2", 3, "b@n"),
			makeTestVol("v2.par2", 3, "c@n"),
		}
		got := statRecoveryVolumes(ctx, parallelFetchNopLogger, mockStat(map[string]bool{"b@n": true}, nil), vols, "e")
		if availOf(got) != 6 {
			t.Fatalf("available = %d, want 6 (v1 dropped)", availOf(got))
		}
		for _, n := range volNames(got) {
			if n == "v1.par2" {
				t.Fatalf("v1.par2 should have been dropped, got %v", volNames(got))
			}
		}
	})

	t.Run("a transport error keeps the volume", func(t *testing.T) {
		vols := []par2Volume{
			makeTestVol("v0.par2", 1, "a@n"),
			makeTestVol("v1.par2", 1, "b@n"),
		}
		got := statRecoveryVolumes(ctx, parallelFetchNopLogger, mockStat(nil, map[string]bool{"b@n": true}), vols, "e")
		if availOf(got) != 2 || len(got) != 2 {
			t.Fatalf("got avail %d len %d, want 2/2 (ambiguous error is not a drop)", availOf(got), len(got))
		}
	})

	t.Run("one dead segment condemns a multi-segment volume", func(t *testing.T) {
		vols := []par2Volume{
			makeTestVol("v0.par2", 5, "a0@n", "a1@n", "a2@n"),
			makeTestVol("v1.par2", 5, "b0@n", "b1@n"),
		}
		got := statRecoveryVolumes(ctx, parallelFetchNopLogger, mockStat(map[string]bool{"a1@n": true}, nil), vols, "e")
		if availOf(got) != 5 || len(got) != 1 || got[0].ref.Name != "v1.par2" {
			t.Fatalf("got %v avail %d, want [v1.par2] avail 5", volNames(got), availOf(got))
		}
	})

	t.Run("whole-batch STAT error leaves vols untouched", func(t *testing.T) {
		vols := []par2Volume{makeTestVol("v0.par2", 4, "a@n")}
		failing := func(context.Context, []string) ([]nntp.StatResult, error) {
			return nil, fmt.Errorf("nntp client is closed")
		}
		got := statRecoveryVolumes(ctx, parallelFetchNopLogger, failing, vols, "e")
		if availOf(got) != 4 || len(got) != 1 {
			t.Fatalf("got avail %d len %d, want 4/1", availOf(got), len(got))
		}
	})

	t.Run("only the cap+slack prefix is probed", func(t *testing.T) {
		// MaxRepairSlices count-1 volumes: prefix is the first
		// MaxRepairSlices + 1 slack. A 430 on a volume past that prefix must
		// NOT be dropped because it was never STAT-ed.
		n := par2.MaxRepairSlices + 20
		vols := make([]par2Volume, n)
		var probed []string
		for i := range vols {
			id := fmt.Sprintf("v%d@n", i)
			vols[i] = makeTestVol(fmt.Sprintf("v%d.par2", i), 1, id)
		}
		// A stat func that records which IDs it was actually asked about, and
		// 430s an ID well past the cap.
		beyondCap := fmt.Sprintf("v%d@n", par2.MaxRepairSlices+10)
		stat := func(_ context.Context, ids []string) ([]nntp.StatResult, error) {
			probed = append(probed, ids...)
			out := make([]nntp.StatResult, len(ids))
			for i, id := range ids {
				out[i] = nntp.StatResult{MessageID: id, Available: id != beyondCap}
				if id == beyondCap {
					out[i].Error = par2NotFoundErr()
				}
			}
			return out, nil
		}
		got := statRecoveryVolumes(ctx, parallelFetchNopLogger, stat, vols, "e")
		if len(got) != n {
			t.Fatalf("len(got) = %d, want %d (nothing should be dropped - the 430 was past the probed prefix)", len(got), n)
		}
		if len(probed) != par2.MaxRepairSlices+1 {
			t.Fatalf("probed %d IDs, want %d (cap + 1 slack)", len(probed), par2.MaxRepairSlices+1)
		}
	})

	t.Run("empty vols -> empty", func(t *testing.T) {
		if got := statRecoveryVolumes(ctx, parallelFetchNopLogger, mockStat(nil, nil), nil, "e"); len(got) != 0 {
			t.Fatalf("got %d vols, want 0", len(got))
		}
	})
}

func TestStatPostedFileDamage(t *testing.T) {
	ctx := context.Background()
	src := []storage.PostedFileRef{
		{Name: "a.rar", Segments: []storage.Par2SegmentRef{{MessageID: "a0"}, {MessageID: "a1"}, {MessageID: "a2"}}},
		{Name: "b.rar", Segments: []storage.Par2SegmentRef{{MessageID: "b0"}, {MessageID: "b1"}}},
		{Name: "c.rar", Segments: []storage.Par2SegmentRef{{MessageID: "c0"}}},
	}
	// Only a.rar and b.rar are matched.
	matches := []par2.Match{{PostedIndex: 0}, {PostedIndex: 1}}

	t.Run("reports confirmed-missing across matched files only", func(t *testing.T) {
		got, ok := statPostedFileDamage(ctx, parallelFetchNopLogger,
			mockStat(map[string]bool{"a1": true, "b0": true, "c0": true}, nil), matches, src, "e")
		if !ok {
			t.Fatalf("completed = false, want true")
		}
		sort.Strings(got)
		want := []string{"a1", "b0"} // c0 not probed (c.rar unmatched)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("missing = %v, want %v", got, want)
		}
	})

	t.Run("ambiguous per-segment error is not reported missing", func(t *testing.T) {
		got, ok := statPostedFileDamage(ctx, parallelFetchNopLogger,
			mockStat(nil, map[string]bool{"a1": true}), matches, src, "e")
		if !ok || len(got) != 0 {
			t.Fatalf("got ok=%v missing=%v, want ok=true missing=[]", ok, got)
		}
	})

	t.Run("whole-batch error -> completed false, nil", func(t *testing.T) {
		failing := func(context.Context, []string) ([]nntp.StatResult, error) {
			return nil, fmt.Errorf("nntp client is closed")
		}
		got, ok := statPostedFileDamage(ctx, parallelFetchNopLogger, failing, matches, src, "e")
		if ok || got != nil {
			t.Fatalf("got ok=%v missing=%v, want ok=false missing=nil", ok, got)
		}
	})

	t.Run("no matched files -> completed true, nil", func(t *testing.T) {
		got, ok := statPostedFileDamage(ctx, parallelFetchNopLogger, mockStat(nil, nil), nil, src, "e")
		if !ok || got != nil {
			t.Fatalf("got ok=%v missing=%v, want ok=true missing=nil", ok, got)
		}
	})

	t.Run("out-of-range PostedIndex is skipped", func(t *testing.T) {
		got, ok := statPostedFileDamage(ctx, parallelFetchNopLogger, mockStat(map[string]bool{"a0": true}, nil),
			[]par2.Match{{PostedIndex: 0}, {PostedIndex: 99}, {PostedIndex: -1}}, src, "e")
		if !ok || len(got) != 1 || got[0] != "a0" {
			t.Fatalf("got ok=%v missing=%v, want ok=true missing=[a0]", ok, got)
		}
	})
}
