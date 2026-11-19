package parser

import (
	"io"
	"testing"

	"github.com/rs/zerolog"
)

// TestResolveVolumeOrderKnown_RAR4InferredNumberNotEstablished covers RAR4
// sets where one volume's tail could not be read, so its number is inferred.
// The inference is kept, but a set with a volume missing from the NZB looks
// the same, so the order must not be reported established: the sweep's
// volume-order check then reads those volumes.
func TestResolveVolumeOrderKnown_RAR4InferredNumberNotEstablished(t *testing.T) {
	p := &RARParser{logger: zerolog.New(io.Discard)}

	numOf := func(entries []volEntry, idx int) int {
		for _, e := range entries {
			if e.idx == idx {
				return e.num
			}
		}
		t.Fatalf("idx %d not in entries", idx)
		return -1
	}

	t.Run("one unread tail, nothing missing", func(t *testing.T) {
		entries := []volEntry{
			{idx: 0, num: 0, hasNum: true},
			{idx: 1, num: 1, hasNum: true},
			{idx: 2, hasNum: false}, // really 2
			{idx: 3, num: 3, hasNum: true},
			{idx: 4, num: 4, hasNum: true},
		}
		order, established := p.resolveVolumeOrderKnown(entries, 5, false)
		if got := numOf(entries, 2); got != 2 {
			t.Errorf("unread volume given number %d, want 2 (hole still inferred)", got)
		}
		if order != nil {
			t.Errorf("volumeOrder = %v, want nil (posting order unchanged)", order)
		}
		if established {
			t.Errorf("order established with an inferred volume number, want not established")
		}
	})

	t.Run("volume 2 missing, unread tail really 5", func(t *testing.T) {
		entries := []volEntry{
			{idx: 0, num: 0, hasNum: true},
			{idx: 1, num: 1, hasNum: true},
			{idx: 2, num: 3, hasNum: true},
			{idx: 3, num: 4, hasNum: true},
			{idx: 4, hasNum: false}, // really 5; inferred as 2
		}
		if _, established := p.resolveVolumeOrderKnown(entries, 5, false); established {
			t.Errorf("order established for a guessed placement (volume 2 missing), want not established")
		}
	})

	t.Run("volume 0 missing, unread tail really 5", func(t *testing.T) {
		entries := []volEntry{
			{idx: 0, num: 1, hasNum: true},
			{idx: 1, num: 2, hasNum: true},
			{idx: 2, num: 3, hasNum: true},
			{idx: 3, num: 4, hasNum: true},
			{idx: 4, hasNum: false}, // really 5; inferred as 0
		}
		if _, established := p.resolveVolumeOrderKnown(entries, 5, false); established {
			t.Errorf("order established for a guessed placement (volume 0 missing), want not established")
		}
	})

	t.Run("fully numbered contiguous set", func(t *testing.T) {
		entries := []volEntry{
			{idx: 0, num: 0, hasNum: true},
			{idx: 1, num: 1, hasNum: true},
			{idx: 2, num: 2, hasNum: true},
			{idx: 3, num: 3, hasNum: true},
			{idx: 4, num: 4, hasNum: true},
		}
		if _, established := p.resolveVolumeOrderKnown(entries, 5, false); !established {
			t.Errorf("fully numbered RAR4 set not established")
		}
	})

	t.Run("RAR5 unnumbered first volume is not a guess", func(t *testing.T) {
		entries := []volEntry{
			{idx: 0, hasNum: false},
			{idx: 1, num: 1, hasNum: true},
			{idx: 2, num: 2, hasNum: true},
			{idx: 3, num: 3, hasNum: true},
		}
		if _, established := p.resolveVolumeOrderKnown(entries, 4, true); !established {
			t.Errorf("complete RAR5 run not established")
		}
	})
}
