package par2

import "testing"

func TestGFTablesGeneratorOrder(t *testing.T) {
	// 2 must be a primitive element: 2^65535 == 1, and no smaller positive
	// power of 2 equals 1 (i.e. the multiplicative order of 2 is exactly
	// gfOrd, not a proper divisor of it).
	if got := gfPow(2, gfOrd); got != 1 {
		t.Fatalf("2^%d = %d, want 1", gfOrd, got)
	}

	seen := make(map[uint16]int, gfOrd)
	x := uint16(1)
	for i := 0; i < gfOrd; i++ {
		if prev, dup := seen[x]; dup {
			t.Fatalf("2^%d repeats a value first seen at exponent %d (%d) - order is smaller than %d", i, prev, x, gfOrd)
		}
		seen[x] = i
		x = gfMul(x, 2)
	}
	if x != 1 {
		t.Fatalf("2^%d = %d, want 1 (full cycle should return to the identity)", gfOrd, x)
	}
	if len(seen) != gfOrd {
		t.Fatalf("2 generated only %d distinct nonzero elements, want %d", len(seen), gfOrd)
	}
}

func TestGFExpLogRoundTrip(t *testing.T) {
	for i := 0; i < gfOrd; i++ {
		v := gfExp[i]
		if v == 0 {
			t.Fatalf("gfExp[%d] = 0, want nonzero", i)
		}
		if int(gfLog[v]) != i {
			t.Fatalf("gfLog[gfExp[%d]] = %d, want %d", i, gfLog[v], i)
		}
	}
	if gfExp[gfOrd] != gfExp[0] {
		t.Fatalf("gfExp[gfOrd] = %d, want gfExp[0] = %d (2^65535 == 2^0)", gfExp[gfOrd], gfExp[0])
	}
}

func TestGFMulBasic(t *testing.T) {
	if got := gfMul(0, 12345); got != 0 {
		t.Errorf("gfMul(0, 12345) = %d, want 0", got)
	}
	if got := gfMul(12345, 0); got != 0 {
		t.Errorf("gfMul(12345, 0) = %d, want 0", got)
	}
	if got := gfMul(1, 42); got != 42 {
		t.Errorf("gfMul(1, 42) = %d, want 42", got)
	}
	if got := gfMul(2, 1); got != 2 {
		t.Errorf("gfMul(2, 1) = %d, want 2", got)
	}
}

func TestGFMulCommutativeAndConsistentWithLogTables(t *testing.T) {
	cases := []struct{ a, b uint16 }{
		{3, 7}, {12345, 54321}, {65535, 65535}, {2, 2}, {1, 1}, {40000, 3},
	}
	for _, c := range cases {
		got := gfMul(c.a, c.b)
		want := gfMul(c.b, c.a)
		if got != want {
			t.Errorf("gfMul(%d,%d)=%d != gfMul(%d,%d)=%d", c.a, c.b, got, c.b, c.a, want)
		}
		// Cross-check against a direct log-table computation.
		if c.a != 0 && c.b != 0 {
			sum := (int(gfLog[c.a]) + int(gfLog[c.b])) % gfOrd
			if want2 := gfExp[sum]; got != want2 {
				t.Errorf("gfMul(%d,%d) = %d, want %d (from raw log/exp)", c.a, c.b, got, want2)
			}
		}
	}
}

func TestGFInv(t *testing.T) {
	for _, a := range []uint16{1, 2, 3, 12345, 65535, 40000} {
		inv := gfInv(a)
		if got := gfMul(a, inv); got != 1 {
			t.Errorf("gfMul(%d, gfInv(%d)=%d) = %d, want 1", a, a, inv, got)
		}
	}
}

func TestGFPow(t *testing.T) {
	if got := gfPow(5, 0); got != 1 {
		t.Errorf("gfPow(5,0) = %d, want 1", got)
	}
	if got := gfPow(0, 5); got != 0 {
		t.Errorf("gfPow(0,5) = %d, want 0", got)
	}
	// gfPow(base, n) should equal repeated gfMul.
	base := uint16(12345)
	want := uint16(1)
	for n := uint32(0); n < 20; n++ {
		if got := gfPow(base, n); got != want {
			t.Errorf("gfPow(%d,%d) = %d, want %d", base, n, got, want)
		}
		want = gfMul(want, base)
	}
}

func TestRegionMulXOR(t *testing.T) {
	src := []byte{0x01, 0x00, 0xFF, 0xFF, 0x34, 0x12}
	dst := make([]byte, len(src))

	c := uint16(1)
	regionMulXOR(dst, src, c)
	for i := range dst {
		if dst[i] != src[i] {
			t.Fatalf("regionMulXOR with c=1 into a zeroed dst should equal src: dst=%x, src=%x", dst, src)
		}
	}

	// Multiplying by 0 must not change dst at all.
	before := append([]byte(nil), dst...)
	regionMulXOR(dst, src, 0)
	for i := range dst {
		if dst[i] != before[i] {
			t.Fatalf("regionMulXOR with c=0 modified dst: got %x, want unchanged %x", dst, before)
		}
	}

	// XOR-accumulating the same region twice with the same constant must
	// cancel back to the pre-accumulation value (GF(2) addition is its own
	// inverse).
	c = 4242
	dst2 := make([]byte, len(src))
	copy(dst2, before)
	regionMulXOR(dst2, src, c)
	regionMulXOR(dst2, src, c)
	for i := range dst2 {
		if dst2[i] != before[i] {
			t.Fatalf("double regionMulXOR with the same constant should cancel out: got %x, want %x", dst2, before)
		}
	}

	// Word-level cross-check against gfMul directly.
	dst3 := make([]byte, len(src))
	regionMulXOR(dst3, src, c)
	for i := 0; i+1 < len(src); i += 2 {
		w := uint16(src[i]) | uint16(src[i+1])<<8
		want := gfMul(w, c)
		got := uint16(dst3[i]) | uint16(dst3[i+1])<<8
		if got != want {
			t.Errorf("word %d: regionMulXOR produced %d, want gfMul=%d", i/2, got, want)
		}
	}
}

func TestInputConstantsAreDistinctAndNonzero(t *testing.T) {
	seen := make(map[uint16]int64)
	for i := int64(0); i < 500; i++ {
		c := inputConstant(i)
		if c == 0 {
			t.Fatalf("inputConstant(%d) = 0, want nonzero", i)
		}
		if prev, dup := seen[c]; dup {
			t.Fatalf("inputConstant(%d) collides with inputConstant(%d): both %d", i, prev, c)
		}
		seen[c] = i
	}
}

func TestNthValidExponentSkipsForbiddenMultiples(t *testing.T) {
	e := nthValidExponent(1)
	if e != 1 {
		t.Fatalf("nthValidExponent(1) = %d, want 1", e)
	}
	// 2 is the 2nd valid exponent (1 is valid, 2 is valid, 3 is skipped).
	if e := nthValidExponent(2); e != 2 {
		t.Fatalf("nthValidExponent(2) = %d, want 2", e)
	}
	for n := 1; n <= 1000; n++ {
		e := nthValidExponent(n)
		if !isValidExponentCandidate(e) {
			t.Fatalf("nthValidExponent(%d) = %d is not a valid candidate", n, e)
		}
	}
}
