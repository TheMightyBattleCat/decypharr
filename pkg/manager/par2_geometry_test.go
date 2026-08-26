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
