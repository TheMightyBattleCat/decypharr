package par2

import (
	"encoding/binary"
	"reflect"
	"testing"
)

// The cached constants must be exactly the spec's C_i = 2^{e_i}.
func TestInputConstantTableMatchesSpec(t *testing.T) {
	for _, i := range []int64{0, 1, 2, 3, 100, 1000, 12345, maxInputSlices - 1} {
		want := gfPow(2, uint32(nthValidExponent(int(i)+1)))
		if got := inputConstant(i); got != want {
			t.Fatalf("inputConstant(%d) = %d, want %d", i, got, want)
		}
	}
}

// A Main packet slice size near 2^58 passed the old checks; k*SliceSize then
// overflowed past repair's memory cap and make([]byte, sliceSize) panicked.
func TestParseMainRejectsAbsurdSliceSize(t *testing.T) {
	body := make([]byte, 12)
	binary.LittleEndian.PutUint64(body[0:8], 1<<58)
	if err := (&Index{}).parseMain(body); err == nil {
		t.Fatalf("parseMain accepted a 2^58-byte slice size")
	}
	binary.LittleEndian.PutUint64(body[0:8], 768000)
	if err := (&Index{}).parseMain(body); err != nil {
		t.Fatalf("parseMain rejected a normal slice size: %v", err)
	}
}

// A byte range past the file's end used to return the next file's slices.
func TestDamagedSlicesClampsToFileLength(t *testing.T) {
	fA, fB := fid(1), fid(2)
	idx := syntheticIndex(t, 4096, map[[16]byte]*FileDesc{
		fA: {FileID: fA, Length: 8192}, // slices 0-1
		fB: {FileID: fB, Length: 8192}, // slices 2-3
	}, [][16]byte{fA, fB})
	got, err := idx.DamagedSlices(fA, 4096, 20000)
	if err != nil {
		t.Fatalf("DamagedSlices: %v", err)
	}
	if want := []int64{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("DamagedSlices(A, 4096, 20000) = %v, want %v (must not reach B's slices)", got, want)
	}
	if got, _ := idx.DamagedSlices(fA, 9000, 20000); len(got) != 0 {
		t.Fatalf("range wholly past A's end = %v, want none", got)
	}
}

// A FileDesc outside the recovery set has no slices; matching to it ended in
// a terminal "map dead segment". It must not be matched at all.
func TestMatchFilesIgnoresFileDescsOutsideRecoverySet(t *testing.T) {
	inSet, outside := fid(1), fid(9)
	idx := syntheticIndex(t, 4096, map[[16]byte]*FileDesc{
		inSet: {FileID: inSet, Length: 5000, Name: "a.rar"},
	}, [][16]byte{inSet})
	idx.Files[outside] = &FileDesc{FileID: outside, Length: 7000, Name: "b.rar"}

	matches, _, err := MatchFiles(idx, []PostedFile{{Name: "b.rar", Length: 7000}, {Name: "a.rar", Length: 5000}})
	if err != nil {
		t.Fatalf("MatchFiles: %v", err)
	}
	for _, m := range matches {
		if m.FileID == outside {
			t.Fatalf("matched a FileDesc outside the recovery set: %+v", m)
		}
	}
	if len(matches) != 1 || matches[0].FileID != inSet {
		t.Fatalf("matches = %+v, want only a.rar -> the recovery-set file", matches)
	}
}

// A panic on one of accumulateSlice's worker goroutines comes back to the
// caller's goroutine, where the repair job's recover can catch it. Raised on
// the worker itself it could not be recovered and stopped the process.
func TestAccumulateSliceRaisesWorkerPanicOnCaller(t *testing.T) {
	recovery := make([]RecoverySlice, accumParallelMinK*2)
	accum := make([][]byte, len(recovery))
	for j := range accum {
		accum[j] = make([]byte, 8)
	}
	accum[len(accum)-1] = make([]byte, 4) // length mismatch: regionMulXOR panics
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected the worker's panic to reach the caller")
		}
	}()
	accumulateSlice(accum, make([]byte, 8), 2, recovery, 4)
}
