package par2

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
)

// --- synthetic recovery-set fixture -----------------------------------------

// benchFixture is a fully self-consistent recovery set: random input slices,
// their IFSC checksums, and GF(2^16)-correct recovery slices (exponents
// 0..k-1) generated with the same primitives Repair consumes, so Repair's
// own per-slice IFSC verification passes on the reconstructed output.
type benchFixture struct {
	idx      *Index
	data     [][]byte // input slice bytes, one per global slice index
	recovery []RecoverySlice
	damaged  []int64
}

type benchKey struct {
	sliceSize int64
	numSlices int
	k         int
}

var (
	benchFixtures   = map[benchKey]*benchFixture{}
	benchFixturesMu sync.Mutex
)

func getBenchFixture(tb testing.TB, sliceSize int64, numSlices, k int) *benchFixture {
	tb.Helper()
	key := benchKey{sliceSize, numSlices, k}
	benchFixturesMu.Lock()
	defer benchFixturesMu.Unlock()
	if f, ok := benchFixtures[key]; ok {
		return f
	}
	f := buildBenchFixture(tb, sliceSize, numSlices, k)
	benchFixtures[key] = f
	return f
}

func buildBenchFixture(tb testing.TB, sliceSize int64, numSlices, k int) *benchFixture {
	tb.Helper()
	if k > numSlices {
		tb.Fatalf("k=%d > numSlices=%d", k, numSlices)
	}
	rng := rand.New(rand.NewSource(1))

	id := fid(1)
	fileLen := int64(numSlices) * sliceSize
	idx := &Index{
		SliceSize: sliceSize,
		FileOrder: [][16]byte{id},
		Files:     map[[16]byte]*FileDesc{id: {FileID: id, Length: fileLen}},
		Slices:    map[[16]byte][]SliceChecksum{},
	}

	data := make([][]byte, numSlices)
	checks := make([]SliceChecksum, numSlices)
	for i := 0; i < numSlices; i++ {
		buf := make([]byte, sliceSize)
		rng.Read(buf)
		data[i] = buf
		checks[i] = md5AndCRC32(buf)
	}
	idx.Slices[id] = checks
	if err := idx.finalize(); err != nil {
		tb.Fatalf("finalize: %v", err)
	}

	// Recovery slice j = XOR over every input slice i of C_i^j * D_i, with
	// C_i = inputConstant(i). Repair seeds accum_j with this, cancels the
	// intact contributions, and is left with the damaged ones - see Repair.
	recovery := make([]RecoverySlice, k)
	for j := 0; j < k; j++ {
		acc := make([]byte, sliceSize)
		for i := 0; i < numSlices; i++ {
			regionMulXOR(acc, data[i], gfPow(inputConstant(int64(i)), uint32(j)))
		}
		recovery[j] = RecoverySlice{Exponent: uint32(j), Data: acc}
	}

	// Damage k slices spread across the file.
	damaged := make([]int64, k)
	step := numSlices / k
	for i := 0; i < k; i++ {
		damaged[i] = int64(i * step)
	}

	return &benchFixture{idx: idx, data: data, recovery: recovery, damaged: damaged}
}

// benchSliceSource serves intact slices from the fixture. Repair never asks
// for a damaged index (its own loop skips them), so no guard is needed.
type benchSliceSource struct{ data [][]byte }

func (s benchSliceSource) ReadSlice(i int64) ([]byte, error) { return s.data[i], nil }

// --- benchmarks ------------------------------------------------------------

const (
	benchSliceSize = 1 << 20 // 1 MiB, a realistic large-REMUX PAR2 slice
	benchNumSlices = 512     // 512 MiB of input data
)

// BenchmarkRepair measures a full end-to-end Repair (streaming accumulation
// + matrix solve + IFSC verification) with in-memory sources, so the number
// is pure CPU with no network. SetBytes is the total input data streamed.
func BenchmarkRepair(b *testing.B) {
	for _, k := range []int{32, 64} {
		b.Run(fmt.Sprintf("k=%d", k), func(b *testing.B) {
			f := getBenchFixture(b, benchSliceSize, benchNumSlices, k)
			src := benchSliceSource{data: f.data}
			b.SetBytes(int64(benchNumSlices) * benchSliceSize)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				out, err := Repair(f.idx, f.damaged, f.recovery, src)
				if err != nil {
					b.Fatalf("Repair: %v", err)
				}
				if len(out) != k {
					b.Fatalf("got %d repaired, want %d", len(out), k)
				}
			}
		})
	}
}

// BenchmarkGFAccumulate isolates the streaming-accumulation phase - for each
// of k recovery slices, XOR-accumulate every input slice scaled by its
// GF(2^16) factor - comparing the serial loop against accumulateSlice's
// per-slice fan-out. This is the part whose cost grows with
// total-recovery-set bytes AND with k. SetBytes is the input data streamed
// once (k is the multiplier on top).
func BenchmarkGFAccumulate(b *testing.B) {
	for _, k := range []int{32, 64} {
		f := getBenchFixture(b, benchSliceSize, benchNumSlices, k)
		accum := make([][]byte, k)
		for j := range accum {
			accum[j] = make([]byte, benchSliceSize)
		}

		b.Run(fmt.Sprintf("serial/k=%d", k), func(b *testing.B) {
			b.SetBytes(int64(benchNumSlices) * benchSliceSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for s := 0; s < benchNumSlices; s++ {
					accumulateSlice(accum, f.data[s], inputConstant(int64(s)), f.recovery, 1)
				}
			}
		})

		b.Run(fmt.Sprintf("parallel/k=%d", k), func(b *testing.B) {
			workers := runtime.GOMAXPROCS(0)
			b.SetBytes(int64(benchNumSlices) * benchSliceSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for s := 0; s < benchNumSlices; s++ {
					accumulateSlice(accum, f.data[s], inputConstant(int64(s)), f.recovery, workers)
				}
			}
		})
	}
}

// BenchmarkRegionMulXOR is the raw throughput of the core GF primitive on a
// single 1 MiB slice. Effective input throughput of the accumulation phase
// is roughly this divided by k (each input byte is processed once per
// recovery slice).
func BenchmarkRegionMulXOR(b *testing.B) {
	dst := make([]byte, benchSliceSize)
	src := make([]byte, benchSliceSize)
	rand.New(rand.NewSource(2)).Read(src)
	const factor = 0x8ac3 // an arbitrary non-trivial GF constant
	b.SetBytes(benchSliceSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		regionMulXOR(dst, src, factor)
	}
}

// BenchmarkGFSolve isolates the matrix phase: invert the k*k Vandermonde
// system and apply it across every 16-bit word position of a slice.
func BenchmarkGFSolve(b *testing.B) {
	for _, k := range []int{32, 64} {
		b.Run(fmt.Sprintf("k=%d", k), func(b *testing.B) {
			f := getBenchFixture(b, benchSliceSize, benchNumSlices, k)
			// Build M[j][d] = C_damaged[d]^{e_j}, same as Repair.
			m := make(gfMatrix, k)
			for j := 0; j < k; j++ {
				row := make([]uint16, k)
				for d := 0; d < k; d++ {
					row[d] = gfPow(inputConstant(f.damaged[d]), uint32(j))
				}
				m[j] = row
			}
			// Fake post-cancellation accumulators.
			accum := make([][]byte, k)
			for j := range accum {
				accum[j] = make([]byte, benchSliceSize)
				rand.New(rand.NewSource(int64(100 + j))).Read(accum[j])
			}
			out := make([][]byte, k)
			for d := range out {
				out[d] = make([]byte, benchSliceSize)
			}
			words := int64(benchSliceSize) / 2
			a := make([]uint16, k)
			b.SetBytes(benchSliceSize) // one slice's worth of solve output per op is k slices; report per-slice
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				inv, err := invertMatrix(m)
				if err != nil {
					b.Fatalf("invertMatrix: %v", err)
				}
				for w := int64(0); w < words; w++ {
					off := w * 2
					for j := 0; j < k; j++ {
						a[j] = binary.LittleEndian.Uint16(accum[j][off:])
					}
					for d := 0; d < k; d++ {
						var sv uint16
						row := inv[d]
						for j := 0; j < k; j++ {
							if row[j] == 0 || a[j] == 0 {
								continue
							}
							sv ^= gfMul(row[j], a[j])
						}
						binary.LittleEndian.PutUint16(out[d][off:], sv)
					}
				}
			}
		})
	}
}
