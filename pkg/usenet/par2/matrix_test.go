package par2

import (
	"math/rand"
	"testing"
)

// matMulVec computes m * v over GF(2^16).
func matMulVec(m gfMatrix, v []uint16) []uint16 {
	out := make([]uint16, len(m))
	for i, row := range m {
		var s uint16
		for j, e := range row {
			s ^= gfMul(e, v[j])
		}
		out[i] = s
	}
	return out
}

func identity(k int) gfMatrix {
	m := make(gfMatrix, k)
	for i := range m {
		m[i] = make([]uint16, k)
		m[i][i] = 1
	}
	return m
}

func matricesEqual(a, b gfMatrix) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				return false
			}
		}
	}
	return true
}

func TestInvertMatrixIdentity(t *testing.T) {
	for k := 1; k <= 4; k++ {
		id := identity(k)
		inv, err := invertMatrix(id)
		if err != nil {
			t.Fatalf("k=%d: invertMatrix(identity): %v", k, err)
		}
		if !matricesEqual(inv, id) {
			t.Fatalf("k=%d: inverse of identity should be identity, got %v", k, inv)
		}
	}
}

func TestInvertMatrixSingular(t *testing.T) {
	m := gfMatrix{
		{1, 2},
		{2, 4}, // row 2 = 2 * row 1 in GF(2^16) too (2*1=2, 2*2=4) -> singular
	}
	if _, err := invertMatrix(m); err == nil {
		t.Fatalf("expected an error inverting a singular matrix, got none")
	}
}

// TestSolverKFrom1To4 builds a Vandermonde-style matrix M[j][d] = C_d^{E_j}
// (exactly the shape Repair constructs), picks a random solution vector x
// (the "damaged slice words"), computes b = M*x (what the accumulators would
// hold), inverts M, and checks that M^-1 * b recovers x - for k = 1..4, as
// required.
func TestSolverKFrom1To4(t *testing.T) {
	rng := rand.New(rand.NewSource(1))

	for k := 1; k <= 4; k++ {
		// Distinct input-slice indices (columns) and distinct recovery
		// exponents (rows), exactly like Repair's construction.
		damaged := make([]int64, k)
		for i := range damaged {
			damaged[i] = int64(i * 7) // arbitrary distinct global slice indices
		}
		exponents := make([]uint32, k)
		for i := range exponents {
			exponents[i] = uint32(i + 1) // distinct exponents
		}

		m := make(gfMatrix, k)
		for j := range m {
			row := make([]uint16, k)
			for d := range row {
				row[d] = gfPow(inputConstant(damaged[d]), exponents[j])
			}
			m[j] = row
		}

		x := make([]uint16, k)
		for i := range x {
			x[i] = uint16(rng.Intn(1 << 16))
		}

		b := matMulVec(m, x)

		inv, err := invertMatrix(m)
		if err != nil {
			t.Fatalf("k=%d: invertMatrix: %v", k, err)
		}
		got := matMulVec(inv, b)
		for i := range x {
			if got[i] != x[i] {
				t.Fatalf("k=%d: solved x[%d] = %d, want %d", k, i, got[i], x[i])
			}
		}
	}
}
