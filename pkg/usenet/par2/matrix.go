package par2

import "fmt"

// maxInputSlices is the PAR2 v2.0 spec's limit on the number of input
// slices a single recovery set may protect, a consequence of how
// inputConstant selects exponents (see its doc comment).
const maxInputSlices = 32768

// isValidExponentCandidate reports whether n is usable as one of the
// exponents PAR2 assigns to input slices: not a multiple of 3, 5, 17, or 257
// (each is a divisor of gfOrd = 65535 = 3*5*17*257, so multiples of them
// generate a strict subgroup rather than help fill out a large enough set of
// independent Vandermonde-style constants).
func isValidExponentCandidate(n int) bool {
	return n%3 != 0 && n%5 != 0 && n%17 != 0 && n%257 != 0
}

// inputConstant returns C_i for input slice i (0-indexed, global across the
// whole recovery set): C_i = 2^{e_i}, where e_i is the (i+1)-th positive
// integer not divisible by 3, 5, 17, or 257. This is the PAR2 v2.0 input-slice
// base value used both by the recovery-slice generator (which this package
// never runs - recovery slices are read pre-computed) and by repair's matrix
// construction.
func inputConstant(i int64) uint16 {
	if i < 0 || i >= maxInputSlices {
		// Not reachable through Index-derived slice indices (callers should
		// reject a recovery set this large before ever calling this), but
		// guard anyway rather than silently returning a bogus constant.
		panic(fmt.Sprintf("par2: input slice index %d out of range [0, %d)", i, maxInputSlices))
	}
	e := nthValidExponent(int(i) + 1)
	return gfPow(2, uint32(e))
}

// nthValidExponent returns the n-th (1-indexed) positive integer not
// divisible by 3, 5, 17, or 257.
func nthValidExponent(n int) int {
	count := 0
	for v := 1; ; v++ {
		if isValidExponentCandidate(v) {
			count++
			if count == n {
				return v
			}
		}
	}
}

// gfMatrix is a k×k (or k×2k for the augmented form used by invertMatrix)
// matrix of GF(2^16) elements, row-major.
type gfMatrix [][]uint16

// invertMatrix inverts m (a k×k matrix) over GF(2^16) via Gauss-Jordan
// elimination with row pivoting on any nonzero entry (there's no ordering
// notion in GF(2^16), unlike floating point, so "partial pivoting" here just
// means "any nonzero pivot works"). m is not modified.
func invertMatrix(m gfMatrix) (gfMatrix, error) {
	k := len(m)
	for _, row := range m {
		if len(row) != k {
			return nil, fmt.Errorf("par2: invertMatrix: matrix is not square (%d rows)", k)
		}
	}

	aug := make(gfMatrix, k)
	for i := range aug {
		aug[i] = make([]uint16, 2*k)
		copy(aug[i], m[i])
		aug[i][k+i] = 1
	}

	for col := 0; col < k; col++ {
		pivot := -1
		for r := col; r < k; r++ {
			if aug[r][col] != 0 {
				pivot = r
				break
			}
		}
		if pivot == -1 {
			return nil, fmt.Errorf("par2: singular matrix at column %d - damaged slice set is not independently recoverable from the chosen recovery slices", col)
		}
		aug[col], aug[pivot] = aug[pivot], aug[col]

		inv := gfInv(aug[col][col])
		if inv != 1 {
			row := aug[col]
			for c := col; c < 2*k; c++ {
				row[c] = gfMul(row[c], inv)
			}
		}

		for r := 0; r < k; r++ {
			if r == col {
				continue
			}
			factor := aug[r][col]
			if factor == 0 {
				continue
			}
			pivotRow := aug[col]
			row := aug[r]
			for c := col; c < 2*k; c++ {
				row[c] ^= gfMul(pivotRow[c], factor)
			}
		}
	}

	result := make(gfMatrix, k)
	for i := range result {
		result[i] = aug[i][k:]
	}
	return result, nil
}

// PickRecovery chooses k of the candidate recovery exponents (by index into
// exponents, in order of preference) whose rows M[j][d] = C_{damaged[d]}^{E_j}
// form an invertible matrix for this damaged set, greedily keeping each row
// that raises the rank. Repair used to take the first k and learn the matrix
// was singular only after streaming the whole release; with gapped exponents
// (skipped or 430'd volumes) that can happen, and a spare slice would have
// solved it. An error means no k of the candidates are independent.
func PickRecovery(damaged []int64, exponents []uint32, k int) ([]int, error) {
	if k == 0 {
		return nil, nil
	}
	if k != len(damaged) {
		return nil, fmt.Errorf("par2: PickRecovery: k=%d but %d damaged slices", k, len(damaged))
	}
	for _, d := range damaged {
		if d < 0 || d >= maxInputSlices {
			return nil, fmt.Errorf("par2: damaged slice index %d out of range", d)
		}
	}
	consts := make([]uint16, k)
	for d, g := range damaged {
		consts[d] = inputConstant(g)
	}
	// basis holds reduced rows; pivots[i] is basis[i]'s leading column.
	var basis [][]uint16
	var pivots []int
	var chosen []int
	for j, e := range exponents {
		row := make([]uint16, k)
		for d := range consts {
			row[d] = gfPow(consts[d], e)
		}
		for i, b := range basis {
			if f := row[pivots[i]]; f != 0 {
				for c := range row {
					row[c] ^= gfMul(b[c], f)
				}
			}
		}
		pivot := -1
		for c, v := range row {
			if v != 0 {
				pivot = c
				break
			}
		}
		if pivot < 0 {
			continue // dependent on the rows already chosen
		}
		inv := gfInv(row[pivot])
		for c := range row {
			row[c] = gfMul(row[c], inv)
		}
		basis = append(basis, row)
		pivots = append(pivots, pivot)
		chosen = append(chosen, j)
		if len(chosen) == k {
			return chosen, nil
		}
	}
	return nil, fmt.Errorf("par2: no %d of the %d recovery slices are independent for this damage", k, len(exponents))
}
