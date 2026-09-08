package manager

import (
	"encoding/json"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

func TestExactSegGeometry_RealProbe(t *testing.T) {
	segs := []storage.Par2SegmentRef{
		{MessageID: "a", Bytes: 1000, Real: true},
		{MessageID: "b", Bytes: 1000, Real: true},
		{MessageID: "c", Bytes: 500, Real: true},
	}
	bases, sizes := exactSegGeometry(segs, 2500, zerolog.Nop())

	wantBases := []int64{0, 1000, 2000}
	wantSizes := []int64{1000, 1000, 500}
	for i := range segs {
		if bases[i] != wantBases[i] {
			t.Errorf("bases[%d] = %d, want %d", i, bases[i], wantBases[i])
		}
		if sizes[i] != wantSizes[i] {
			t.Errorf("sizes[%d] = %d, want %d", i, sizes[i], wantSizes[i])
		}
	}
}

func TestExactSegGeometry_EstimatedScaled(t *testing.T) {
	segs := []storage.Par2SegmentRef{
		{MessageID: "a", Bytes: 999, Real: false},
		{MessageID: "b", Bytes: 999, Real: false},
		{MessageID: "c", Bytes: 999, Real: false},
	}
	trueLen := int64(2800)
	bases, sizes := exactSegGeometry(segs, trueLen, zerolog.Nop())

	if bases[0] != 0 {
		t.Errorf("bases[0] = %d, want 0", bases[0])
	}
	for i := 1; i < len(bases); i++ {
		if bases[i] <= bases[i-1] {
			t.Errorf("bases not monotonically increasing: bases[%d]=%d <= bases[%d]=%d", i, bases[i], i-1, bases[i-1])
		}
	}
	var sum int64
	for _, s := range sizes {
		sum += s
	}
	if sum != trueLen {
		t.Errorf("sum(sizes) = %d, want %d", sum, trueLen)
	}
}

func TestExactSegGeometry_NoTrueLen(t *testing.T) {
	segs := []storage.Par2SegmentRef{
		{MessageID: "a", Bytes: 1000},
		{MessageID: "b", Bytes: 1000},
		{MessageID: "c", Bytes: 500},
	}
	bases, sizes := exactSegGeometry(segs, 0, zerolog.Nop())

	wantBases := []int64{0, 1000, 2000}
	wantSizes := []int64{1000, 1000, 500}
	for i := range segs {
		if bases[i] != wantBases[i] {
			t.Errorf("bases[%d] = %d, want %d", i, bases[i], wantBases[i])
		}
		if sizes[i] != wantSizes[i] {
			t.Errorf("sizes[%d] = %d, want %d", i, sizes[i], wantSizes[i])
		}
	}
}

func TestExactSegGeometry_Empty(t *testing.T) {
	bases, sizes := exactSegGeometry(nil, 1000, zerolog.Nop())
	if len(bases) != 0 {
		t.Errorf("len(bases) = %d, want 0", len(bases))
	}
	if len(sizes) != 0 {
		t.Errorf("len(sizes) = %d, want 0", len(sizes))
	}
}

func TestPar2SegmentRef_JSONRoundTrip(t *testing.T) {
	t.Run("Real survives round trip", func(t *testing.T) {
		ref := storage.Par2SegmentRef{MessageID: "x", Bytes: 512, Real: true}
		data, err := json.Marshal(ref)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		var got storage.Par2SegmentRef
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		if !got.Real {
			t.Errorf("Real = false after round trip, want true")
		}
	})

	t.Run("old JSON without real key defaults false", func(t *testing.T) {
		var got storage.Par2SegmentRef
		if err := json.Unmarshal([]byte(`{"message_id":"x","bytes":512}`), &got); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		if got.Real {
			t.Errorf("Real = true for old JSON without real key, want false")
		}
	})

	t.Run("Real false omitted from JSON output", func(t *testing.T) {
		ref := storage.Par2SegmentRef{MessageID: "x", Bytes: 512, Real: false}
		data, err := json.Marshal(ref)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(data, &raw); err != nil {
			t.Fatalf("Unmarshal to map: %v", err)
		}
		if _, present := raw["real"]; present {
			t.Errorf("\"real\" key present in JSON output for Real=false, want absent (omitempty)")
		}
	})
}

// A yEnc file's final article is never larger than a full one, so an implied
// final segment bigger than the seed proves the persisted seed size and
// FileDesc.Length disagree. The exact branch must not commit to a uniform
// interior on that evidence: an overstated tail makes readRange demand bytes
// the article does not hold, which surfaces as ErrSegmentShort and gets folded
// into the damaged set as if the posting were truncated.
//
// Reachable via realPar2SegmentRefs, which marks every ref Real while
// tolerating a declared total within +/-1.5 x segmentSize - permitting a final
// segment up to 2.5x the seed.
func TestExactSegGeometry_RejectsFinalSegmentLargerThanSeed(t *testing.T) {
	segs := []storage.Par2SegmentRef{
		{MessageID: "a", Bytes: 700000, Real: true},
		{MessageID: "b", Bytes: 700000, Real: true},
		{MessageID: "c", Bytes: 300000, Real: true},
	}
	// 700000*2 + 1300000: the exact branch would claim a 1.3MB final segment
	// from a 700KB article.
	const trueLen = 2700000

	bases, sizes := exactSegGeometry(segs, trueLen, zerolog.Nop())

	if sizes[2] > segs[0].Bytes {
		t.Errorf("final segment = %d bytes, which exceeds the %d-byte seed article - the exact branch accepted impossible geometry",
			sizes[2], segs[0].Bytes)
	}
	// The scaled fallback still anchors the total on trueLen.
	var total int64
	for _, s := range sizes {
		total += s
	}
	if total != trueLen {
		t.Errorf("sizes sum to %d, want %d (the fallback must still anchor on FileDesc.Length)", total, trueLen)
	}
	if bases[0] != 0 {
		t.Errorf("bases[0] = %d, want 0", bases[0])
	}
	for i := 1; i < len(bases); i++ {
		if bases[i] != bases[i-1]+sizes[i-1] {
			t.Errorf("bases[%d] = %d, want %d (contiguous)", i, bases[i], bases[i-1]+sizes[i-1])
		}
	}
}

// The guard must not disturb the case it was carved out of: a final segment
// smaller than the seed is normal and still takes the exact path.
func TestExactSegGeometry_AllowsNormalShorterFinalSegment(t *testing.T) {
	segs := []storage.Par2SegmentRef{
		{MessageID: "a", Bytes: 700000, Real: true},
		{MessageID: "b", Bytes: 700000, Real: true},
		{MessageID: "c", Bytes: 300000, Real: true},
	}
	const trueLen = 700000*2 + 250000

	_, sizes := exactSegGeometry(segs, trueLen, zerolog.Nop())

	if sizes[0] != 700000 || sizes[1] != 700000 {
		t.Errorf("interior sizes = %v, want the exact seed size for both", sizes[:2])
	}
	if sizes[2] != 250000 {
		t.Errorf("final size = %d, want 250000 (trueLen - 2*seed)", sizes[2])
	}
}

// A single-segment file: lastSeg == trueLen, which can legitimately exceed the
// seed estimate. Both paths must produce the same answer, so the guard is
// harmless here.
func TestExactSegGeometry_SingleSegment(t *testing.T) {
	segs := []storage.Par2SegmentRef{{MessageID: "a", Bytes: 744960, Real: true}}
	const trueLen = 750000

	bases, sizes := exactSegGeometry(segs, trueLen, zerolog.Nop())
	if len(sizes) != 1 || sizes[0] != trueLen {
		t.Errorf("sizes = %v, want [%d] - a lone segment is exactly the file", sizes, int64(trueLen))
	}
	if bases[0] != 0 {
		t.Errorf("bases = %v, want [0]", bases)
	}
}
