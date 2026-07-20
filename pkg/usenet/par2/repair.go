package par2

import (
	"encoding/binary"
	"fmt"
)

const (
	// maxRepairSlices caps how many damaged slices a single Repair call will
	// attempt to reconstruct: k damaged slices need a k×k GF(2^16) matrix
	// inversion (O(k^3)) and k recovery slices held fully in memory.
	maxRepairSlices = 64

	// maxAccumulatorMemory caps total accumulator memory (k recovery slices
	// x SliceSize bytes each, held for the whole streaming pass).
	maxAccumulatorMemory = 256 << 20 // 256MB

	// maxIntactChecksumMismatches is the "small threshold" of confirmed-
	// intact slices allowed to fail their own IFSC checksum during the
	// streaming pass before the whole repair aborts. Any mismatch at all
	// already means either a slice we trusted as intact silently isn't, or
	// (more likely, and worse) our posted-file/slice offset mapping has
	// drifted for this region - so this stays deliberately tiny rather than
	// merely nonzero: a real mapping bug reliably produces far more than a
	// couple of mismatches, while this still tolerates an isolated glitch
	// rather than aborting a would-be-correct repair over it. It exists
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

	mismatches := 0
	for s := int64(0); s < idx.numSlices; s++ {
		if _, isDamaged := damagedPos[s]; isDamaged {
			continue
		}
		data, err := intact.ReadSlice(s)
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
				return nil, fmt.Errorf("par2: %d intact slices failed their own IFSC checksum - aborting rather than risk a fabricated repair from a drifted offset mapping", mismatches)
			}
		}

		ci := inputConstant(s)
		for j, rs := range recovery {
			factor := gfPow(ci, rs.Exponent)
			regionMulXOR(accum[j], data, factor)
		}
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
			return nil, fmt.Errorf("par2: reconstructed slice %d failed IFSC verification", out[i].Index)
		}
	}
	return out, nil
}
