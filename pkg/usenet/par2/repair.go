package par2

import (
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sync"
)

// ErrChecksumMismatch wraps every error Repair returns because a slice - an
// intact one that failed its own IFSC check past maxIntactChecksumMismatches,
// or a reconstructed one that failed IFSC verification before being returned
// - didn't match its recorded MD5+CRC32. Callers can match it with
// errors.Is to distinguish this from an ordinary "not enough data" failure:
// it means the repair pass produced (or trusted) bytes it could prove were
// wrong, not merely that recovery data was unavailable - the important
// failure mode to surface distinctly (a "CRC canary").
var ErrChecksumMismatch = errors.New("par2: checksum verification failed")

// errIntactChecksumAbort marks the ErrChecksumMismatch Repair returns when
// intact INPUT slices fail their checksums (as opposed to a reconstructed
// output slice): a claim about how the caller read them, not about the
// recovery data.
var errIntactChecksumAbort = errors.New("intact slices failed their own IFSC checksum")

// ErrSliceUnavailable marks an intact slice whose bytes cannot be read at all
// (its backing article is gone, or decodes short). A SliceSource wraps its
// ReadSlice error with it to let Repair finish the pass instead of stopping
// there: the slice is skipped, every other unavailable slice is found in the
// same pass, and Repair returns them all together (wrapping this sentinel)
// once the pass ends, so the caller can add them to the damaged set in one
// retry rather than one slice per full re-read.
var ErrSliceUnavailable = errors.New("par2: intact slice unavailable")

// IsIntactChecksumAbort reports whether err is Repair's intact-slice checksum
// abort.
func IsIntactChecksumAbort(err error) bool {
	return errors.Is(err, errIntactChecksumAbort)
}

// MaxRepairSlices is the exported form of maxRepairSlices, for callers that
// want to fail before fetching any recovery data when they already know a
// damaged set is too large (e.g. against a vol-filename-derived recovery
// census).
const MaxRepairSlices = maxRepairSlices

// MaxAccumulatorMemory is the exported form of maxAccumulatorMemory.
const MaxAccumulatorMemory = maxAccumulatorMemory

// MaxRepairSlicesFor is how many damaged slices one Repair call accepts at
// this slice size: MaxRepairSlices, or fewer where k x sliceSize would pass
// MaxAccumulatorMemory. Large-slice sets (10 MiB is a common REMUX posting)
// are bound by memory, not by the slice cap. Callers use it to fail before
// fetching recovery data that Repair would refuse anyway.
func MaxRepairSlicesFor(sliceSize int64) int {
	if sliceSize <= 0 {
		return maxRepairSlices
	}
	return int(min(int64(maxRepairSlices), maxAccumulatorMemory/sliceSize))
}

const (
	// maxRepairSlices caps how many damaged slices a single Repair call will
	// attempt to reconstruct. Cost grows with k: accumulation is linear in k,
	// and applying the solved k×k matrix to every word of the output is k²
	// per word, so doubling k from 64 to 128 made BenchmarkRepair about 3x
	// slower. A repair under the cap costs the same whatever the cap is, so
	// the cap only decides whether a larger one runs slowly or is re-grabbed.
	maxRepairSlices = 128

	// maxAccumulatorMemory caps total accumulator memory (k recovery slices
	// x SliceSize bytes each, held for the whole streaming pass). At 1GiB it
	// binds ahead of the 128-slice cap once SliceSize exceeds 8MiB, which
	// covers most REMUX postings (10MiB slices allow 102). A repair's real
	// peak is about three to four times this: the fetched recovery volumes,
	// Repair's working copy of the chosen slices, and the rebuilt output.
	maxAccumulatorMemory = 1 << 30 // 1GiB

	// maxIntactChecksumMismatches is the "small threshold" of confirmed-
	// intact slices allowed to fail their own IFSC checksum during the
	// streaming pass before the whole repair aborts. Any mismatch at all
	// already means either a slice we trusted as intact silently isn't, or
	// (more likely, and worse) our posted-file/slice offset mapping has
	// drifted for this region - so this stays deliberately tiny rather than
	// merely nonzero: a real mapping bug reliably produces far more than a
	// couple of mismatches, while an isolated bad slice is skipped and
	// reported as unavailable (see RepairOptions.OnChecksumMismatch) rather
	// than aborting the pass - never accumulated. It exists
	// purely as an early, cheap warning; the reconstructed slices' own
	// CRC32+MD5 verification below is the actual, zero-tolerance
	// correctness gate - nothing is ever accepted on the strength of this
	// check alone.
	maxIntactChecksumMismatches = 2
)

// SliceSource provides the raw bytes of every INTACT input slice in a
// recovery set, resolved from wherever the caller actually stores/fetches
// posted-file bytes. Repair calls ReadSlice exactly once per intact slice,
// in ascending global-slice-index order, and never holds more than one
// slice's bytes from it at a time - the "streaming" in this package's
// design.
type SliceSource interface {
	// ReadSlice returns exactly Index.SliceSize bytes for global slice index
	// idx. The final slice of a file (whose real length may end mid-slice)
	// must be zero-padded out to SliceSize by the implementation.
	ReadSlice(idx int64) ([]byte, error)
}

// RecoverySlice is one RecvSlic packet's exponent and recovery data (the
// packet body with its 4-byte exponent prefix already stripped).
type RecoverySlice struct {
	Exponent uint32
	Data     []byte // exactly Index.SliceSize bytes
}

// RepairedSlice is one reconstructed slice's bytes, already verified against
// its IFSC MD5+CRC32.
type RepairedSlice struct {
	Index int64
	Data  []byte // exactly Index.SliceSize bytes; trim via Index.TrimSlice for a file's final slice
}

// Repair reconstructs the bytes of every slice in damaged (global slice
// indices, any order, no duplicates), given exactly len(damaged) recovery
// slices and a SliceSource covering every OTHER (intact) slice in the whole
// recovery set (idx.NumSlices() of them).
//
// This is the streaming pass described in the package's design: recovery
// slices seed k accumulators, every intact slice is read once and its
// contribution to each accumulator is cancelled out via regionMulXOR (with a
// free IFSC CRC32 check along the way - see maxIntactChecksumMismatches),
// then the resulting k equations in k unknowns are solved via a single
// GF(2^16) matrix inversion applied across every word position. Every
// reconstructed slice is verified against its IFSC MD5+CRC32 before being
// returned - a slice that fails is an error, never a returned (possibly
// wrong) result.
func Repair(idx *Index, damaged []int64, recovery []RecoverySlice, intact SliceSource) ([]RepairedSlice, error) {
	return RepairWith(idx, damaged, recovery, intact, RepairOptions{MaxUnavailable: -1})
}

// RepairOptions tunes RepairWith.
type RepairOptions struct {
	// MaxUnavailable stops a pass once more than this many intact slices
	// have come back ErrSliceUnavailable: the caller cannot cover that many
	// more damaged slices from its recovery data, so reading the rest of
	// the release only confirms a verdict already reached. Negative means
	// no limit.
	MaxUnavailable int
	// OnChecksumMismatch, when set, is called with the global index of each
	// intact slice that failed its IFSC checksum (up to the abort threshold).
	// Such a slice is skipped and counted as unavailable, so the caller can
	// add it to the damaged set. Called from Repair's own goroutine.
	OnChecksumMismatch func(idx int64)
}

// RepairWith is Repair with options.
func RepairWith(idx *Index, damaged []int64, recovery []RecoverySlice, intact SliceSource, opts RepairOptions) ([]RepairedSlice, error) {
	k := len(damaged)
	if k == 0 {
		return nil, nil
	}
	if k > maxRepairSlices {
		return nil, fmt.Errorf("par2: %d damaged slices exceeds the %d cap", k, maxRepairSlices)
	}
	if len(recovery) != k {
		return nil, fmt.Errorf("par2: need exactly %d recovery slices (one per damaged slice), got %d", k, len(recovery))
	}
	sliceSize := idx.SliceSize
	if sliceSize <= 0 || sliceSize%2 != 0 {
		return nil, fmt.Errorf("par2: invalid slice size %d", sliceSize)
	}
	if mem := int64(k) * sliceSize; mem > maxAccumulatorMemory {
		return nil, fmt.Errorf("par2: accumulator memory %d bytes exceeds the %d byte cap", mem, maxAccumulatorMemory)
	}

	damagedPos := make(map[int64]int, k) // global slice index -> position in damaged/recovery
	for i, d := range damaged {
		if d < 0 || d >= idx.numSlices {
			return nil, fmt.Errorf("par2: damaged slice index %d out of range [0, %d)", d, idx.numSlices)
		}
		if _, dup := damagedPos[d]; dup {
			return nil, fmt.Errorf("par2: duplicate damaged slice index %d", d)
		}
		damagedPos[d] = i
	}

	// Seed accumulators: A_j := R_{E_j}.
	accum := make([][]byte, k)
	for j, rs := range recovery {
		if int64(len(rs.Data)) != sliceSize {
			return nil, fmt.Errorf("par2: recovery slice %d has %d bytes, want %d", j, len(rs.Data), sliceSize)
		}
		buf := make([]byte, sliceSize)
		copy(buf, rs.Data)
		accum[j] = buf
	}

	// The per-slice fan-out below cancels each intact slice's contribution
	// out of all k accumulators in parallel - they are independent (disjoint
	// accum[j], shared read-only data). Accumulation is 85-90% of Repair's
	// CPU and scales with total-recovery-set-bytes x k, so on a large release
	// this is the difference between minutes and tens of minutes. Workers are
	// capped at k (no point spawning more) and at GOMAXPROCS.
	workers := runtime.GOMAXPROCS(0)
	if workers > k {
		workers = k
	}

	mismatches := 0
	unavailable := 0
	var firstUnavailable error
	for s := int64(0); s < idx.numSlices; s++ {
		if _, isDamaged := damagedPos[s]; isDamaged {
			continue
		}
		data, err := intact.ReadSlice(s)
		if err != nil && errors.Is(err, ErrSliceUnavailable) {
			if unavailable == 0 {
				firstUnavailable = fmt.Errorf("par2: read intact slice %d: %w", s, err)
			}
			unavailable++
			if opts.MaxUnavailable >= 0 && unavailable > opts.MaxUnavailable {
				return nil, fmt.Errorf("%d intact slice(s) unavailable, more than the %d spare recovery slices can cover - pass stopped, first: %w", unavailable, opts.MaxUnavailable, firstUnavailable)
			}
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("par2: read intact slice %d: %w", s, err)
		}
		if int64(len(data)) != sliceSize {
			return nil, fmt.Errorf("par2: intact slice %d has %d bytes, want %d", s, len(data), sliceSize)
		}

		ok, err := idx.verifySliceChecksum(s, data)
		if err != nil {
			return nil, fmt.Errorf("par2: verify intact slice %d: %w", s, err)
		}
		if !ok {
			mismatches++
			if mismatches > maxIntactChecksumMismatches {
				return nil, fmt.Errorf("%w: %d %w - aborting rather than risk a fabricated repair from a drifted offset mapping", ErrChecksumMismatch, mismatches, errIntactChecksumAbort)
			}
			// Never accumulate it: one wrong intact slice shifts every
			// accumulator, so the solve could only fail its own IFSC check
			// - after a full read, as a terminal "checksum verification
			// failed". Its true bytes are unknown here, exactly like an
			// unreadable slice, so it is reported the same way and the
			// caller can reconstruct it from parity next round.
			if opts.OnChecksumMismatch != nil {
				opts.OnChecksumMismatch(s)
			}
			if unavailable == 0 {
				firstUnavailable = fmt.Errorf("par2: intact slice %d failed its IFSC checksum: %w", s, ErrSliceUnavailable)
			}
			unavailable++
			if opts.MaxUnavailable >= 0 && unavailable > opts.MaxUnavailable {
				return nil, fmt.Errorf("%d intact slice(s) unavailable, more than the %d spare recovery slices can cover - pass stopped, first: %w", unavailable, opts.MaxUnavailable, firstUnavailable)
			}
			continue
		}

		accumulateSlice(accum, data, inputConstant(s), recovery, workers)
	}
	if unavailable > 0 {
		return nil, fmt.Errorf("%d intact slice(s) unavailable, first: %w", unavailable, firstUnavailable)
	}

	// Build and invert M[j][d] = C_{damaged[d]}^{E_j}.
	m := make(gfMatrix, k)
	for j, rs := range recovery {
		row := make([]uint16, k)
		for d, globalIdx := range damaged {
			row[d] = gfPow(inputConstant(globalIdx), rs.Exponent)
		}
		m[j] = row
	}
	inv, err := invertMatrix(m)
	if err != nil {
		return nil, err
	}

	out := make([]RepairedSlice, k)
	for d, globalIdx := range damaged {
		out[d] = RepairedSlice{Index: globalIdx, Data: make([]byte, sliceSize)}
	}

	words := sliceSize / 2
	a := make([]uint16, k)
	for w := int64(0); w < words; w++ {
		off := w * 2
		for j := range accum {
			a[j] = binary.LittleEndian.Uint16(accum[j][off:])
		}
		for d := 0; d < k; d++ {
			var s uint16
			row := inv[d]
			for j := 0; j < k; j++ {
				if row[j] == 0 || a[j] == 0 {
					continue
				}
				s ^= gfMul(row[j], a[j])
			}
			binary.LittleEndian.PutUint16(out[d].Data[off:], s)
		}
	}

	// Never fabricate: every reconstructed slice must match its IFSC
	// MD5+CRC32 before it's returned.
	for i := range out {
		ok, err := idx.verifySliceChecksum(out[i].Index, out[i].Data)
		if err != nil {
			return nil, fmt.Errorf("par2: verify reconstructed slice %d: %w", out[i].Index, err)
		}
		if !ok {
			return nil, fmt.Errorf("%w: reconstructed slice %d failed IFSC verification", ErrChecksumMismatch, out[i].Index)
		}
	}
	return out, nil
}

// accumParallelMinK is the smallest k for which accumulateSlice fans the
// per-slice work out across goroutines. Below it the goroutine hand-off costs
// more than the k serial regionMulXOR calls save.
const accumParallelMinK = 4

// accumulateSlice folds one intact slice's contribution into every
// accumulator: accum[j] ^= gfMul(data, C^Exp_j) for each recovery slice j.
// The k calls are independent - disjoint accum[j], shared read-only data -
// so for a large enough k they run on `workers` goroutines, each taking a
// contiguous block of the j range (the calls are uniform cost, so a static
// split balances). Falls back to the plain serial loop for small k or a
// single worker.
func accumulateSlice(accum [][]byte, data []byte, ci uint16, recovery []RecoverySlice, workers int) {
	k := len(recovery)
	if workers <= 1 || k < accumParallelMinK {
		for j, rs := range recovery {
			regionMulXOR(accum[j], data, gfPow(ci, rs.Exponent))
		}
		return
	}
	if workers > k {
		workers = k
	}
	per := (k + workers - 1) / workers
	var wg sync.WaitGroup
	// A panic on a worker goroutine cannot be recovered by the caller, so it
	// would stop the process. Each worker hands its panic back instead, and
	// the first one is raised again here, on the caller's goroutine, where
	// the repair job's own recover handles it.
	var panicOnce sync.Once
	var workerPanic any
	for w := 0; w < workers; w++ {
		lo := w * per
		if lo >= k {
			break
		}
		hi := lo + per
		if hi > k {
			hi = k
		}
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicOnce.Do(func() { workerPanic = r })
				}
			}()
			for j := lo; j < hi; j++ {
				regionMulXOR(accum[j], data, gfPow(ci, recovery[j].Exponent))
			}
		}(lo, hi)
	}
	wg.Wait()
	if workerPanic != nil {
		panic(workerPanic)
	}
}
