package par2

import (
	"strings"
	"testing"
)

// syntheticRepairIndex builds a minimal *Index with one file, for the
// error-path tests below that don't need a full recovery-set structure.
func syntheticRepairIndex(t *testing.T, sliceSize int64, numSlices int64) *Index {
	t.Helper()
	id := fid(9)
	idx := &Index{
		SliceSize: sliceSize,
		FileOrder: [][16]byte{id},
		Files:     map[[16]byte]*FileDesc{id: {FileID: id, Length: numSlices * sliceSize}},
		Slices:    make(map[[16]byte][]SliceChecksum),
	}
	if err := idx.finalize(); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	return idx
}

type nullSliceSource struct{}

func (nullSliceSource) ReadSlice(int64) ([]byte, error) { return nil, nil }

func TestRepairRejectsTooManyDamagedSlices(t *testing.T) {
	idx := syntheticRepairIndex(t, 4096, maxRepairSlices+10)
	damaged := make([]int64, maxRepairSlices+1)
	recovery := make([]RecoverySlice, maxRepairSlices+1)
	for i := range damaged {
		damaged[i] = int64(i)
		recovery[i] = RecoverySlice{Exponent: uint32(i), Data: make([]byte, 4096)}
	}
	_, err := Repair(idx, damaged, recovery, nullSliceSource{})
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("Repair with %d damaged slices: err = %v, want an 'exceeds' cap error", len(damaged), err)
	}
}

func TestRepairRejectsWrongRecoveryCount(t *testing.T) {
	idx := syntheticRepairIndex(t, 4096, 10)
	damaged := []int64{1, 2, 3}
	recovery := []RecoverySlice{{Exponent: 0, Data: make([]byte, 4096)}} // only 1, need 3
	_, err := Repair(idx, damaged, recovery, nullSliceSource{})
	if err == nil {
		t.Fatalf("expected an error when len(recovery) != len(damaged), got none")
	}
}

func TestRepairRejectsWrongSliceDataLength(t *testing.T) {
	idx := syntheticRepairIndex(t, 4096, 10)
	damaged := []int64{1}
	recovery := []RecoverySlice{{Exponent: 0, Data: make([]byte, 100)}} // wrong length
	_, err := Repair(idx, damaged, recovery, nullSliceSource{})
	if err == nil {
		t.Fatalf("expected an error for a recovery slice of the wrong length, got none")
	}
}

func TestRepairRejectsDuplicateDamagedIndex(t *testing.T) {
	idx := syntheticRepairIndex(t, 4096, 10)
	damaged := []int64{1, 1}
	recovery := []RecoverySlice{
		{Exponent: 0, Data: make([]byte, 4096)},
		{Exponent: 1, Data: make([]byte, 4096)},
	}
	_, err := Repair(idx, damaged, recovery, nullSliceSource{})
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("Repair with a duplicate damaged index: err = %v, want a 'duplicate' error", err)
	}
}

func TestRepairEmptyDamagedIsNoop(t *testing.T) {
	idx := syntheticRepairIndex(t, 4096, 10)
	out, err := Repair(idx, nil, nil, nullSliceSource{})
	if err != nil {
		t.Fatalf("Repair with no damaged slices: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("Repair with no damaged slices returned %d results, want 0", len(out))
	}
}

func TestRepairRejectsAccumulatorMemoryOverCap(t *testing.T) {
	// One damaged slice at an enormous slice size blows the accumulator cap
	// even for k=1.
	const hugeSliceSize = maxAccumulatorMemory + 4
	idx := syntheticRepairIndex(t, hugeSliceSize, 1)
	damaged := []int64{0}
	recovery := []RecoverySlice{{Exponent: 0, Data: make([]byte, hugeSliceSize)}}
	_, err := Repair(idx, damaged, recovery, nullSliceSource{})
	if err == nil || !strings.Contains(err.Error(), "accumulator memory") {
		t.Fatalf("Repair over the accumulator memory cap: err = %v, want an accumulator-memory error", err)
	}
}
