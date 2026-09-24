package manager

import (
	"context"
	"errors"
	"testing"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func aliveTestVol(name string, count uint32, ids ...string) par2Volume {
	v := par2Volume{ref: storage.Par2FileRef{Name: name}, count: count}
	for _, id := range ids {
		v.ref.Segments = append(v.ref.Segments, storage.Par2SegmentRef{MessageID: id})
	}
	return v
}

// The warm pass needs enough live recovery for its damage, not every PAR2
// article alive: one expired article in a volume it would not need used to
// disable it and send the file to a re-grab.
func TestAliveRecoverySlices(t *testing.T) {
	vols := []par2Volume{
		aliveTestVol("r.vol00+01.par2", 1, "<a1>"),
		aliveTestVol("r.vol01+02.par2", 2, "<b1>", "<b2>"),
		aliveTestVol("r.vol03+04.par2", 4, "<c1>", "<c2>"), // one article expired
		aliveTestVol("r.vol07+08.par2", 8, "<d1>"),         // unanswered
	}
	stat := func(_ context.Context, ids []string) ([]nntp.StatResult, error) {
		var out []nntp.StatResult
		for _, id := range ids {
			switch id {
			case "<c2>":
				out = append(out, nntp.StatResult{MessageID: id, Error: &nntp.Error{Type: nntp.ErrorTypeArticleNotFound}})
			case "<d1>":
				out = append(out, nntp.StatResult{MessageID: id, Error: errors.New("connection reset")})
			default:
				out = append(out, nntp.StatResult{MessageID: id, Available: true})
			}
		}
		return out, nil
	}
	got, err := aliveRecoverySlices(context.Background(), stat, vols)
	if err != nil {
		t.Fatal(err)
	}
	if got != 3 {
		t.Fatalf("alive slices = %d, want 3 (the two whole volumes)", got)
	}

	failing := func(context.Context, []string) ([]nntp.StatResult, error) { return nil, errors.New("down") }
	if _, err := aliveRecoverySlices(context.Background(), failing, vols); err == nil {
		t.Fatal("a failed STAT must not count as alive")
	}
	if n, err := aliveRecoverySlices(context.Background(), stat, nil); n != 0 || err != nil {
		t.Fatalf("no volumes: %d %v", n, err)
	}
}
