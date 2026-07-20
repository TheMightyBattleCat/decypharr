package par2

// GF(2^16) arithmetic, per the PAR2 v2.0 specification. Every input/recovery
// slice is treated as a sequence of 16-bit little-endian words, each an
// element of this field.
const (
	gfBits = 16
	gfSize = 1 << gfBits // 65536 elements (including zero)
	gfOrd  = gfSize - 1  // 65535: order of the multiplicative group

	// gfPoly is the field's reduction polynomial, x^16 + x^12 + x^3 + x + 1,
	// as specified by PAR2 v2.0.
	gfPoly = 0x1100B
)

// gfExp[i] = 2^i for i in [0, gfOrd], and gfLog[gfExp[i]] = i for i in
// [0, gfOrd). 2 is a primitive element (generator) of the field, so gfExp
// enumerates every nonzero element exactly once as i ranges over
// [0, gfOrd). gfExp[gfOrd] duplicates gfExp[0] (=1, since 2^65535 == 1) so a
// summed pair of log values never needs more than one conditional
// subtraction to land back in range - see gfMul.
var (
	gfExp [gfSize]uint16
	gfLog [gfSize]uint16
)

func init() {
	initGFTables()
}

func initGFTables() {
	x := uint32(1)
	for i := 0; i < gfOrd; i++ {
		gfExp[i] = uint16(x)
		gfLog[x] = uint16(i)
		x <<= 1
		if x&gfSize != 0 {
			x ^= gfPoly
		}
	}
	gfExp[gfOrd] = gfExp[0]
}

// gfMul multiplies two field elements.
func gfMul(a, b uint16) uint16 {
	if a == 0 || b == 0 {
		return 0
	}
	sum := int(gfLog[a]) + int(gfLog[b])
	if sum >= gfOrd {
		sum -= gfOrd
	}
	return gfExp[sum]
}

// gfInv returns the multiplicative inverse of a (a must be nonzero).
func gfInv(a uint16) uint16 {
	if a == 0 {
		return 0
	}
	return gfExp[gfOrd-int(gfLog[a])]
}

// gfPow returns base^exp. exp is taken modulo gfOrd (the group's order),
// matching the field's cyclic structure - PAR2 exponents (segment index e_i,
// recovery slice exponent) can exceed gfOrd for large recovery sets.
func gfPow(base uint16, exp uint32) uint16 {
	if exp == 0 {
		return 1
	}
	if base == 0 {
		return 0
	}
	e := (uint64(gfLog[base]) * uint64(exp)) % uint64(gfOrd)
	return gfExp[e]
}

// regionMulXOR computes dst[i] ^= mul(src[i], c) for every 16-bit
// little-endian word in dst/src (both must be the same even length). This is
// the core streaming primitive: accumulating one input or recovery slice's
// contribution, scaled by a GF(2^16) constant, into an accumulator.
func regionMulXOR(dst, src []byte, c uint16) {
	if len(dst) != len(src) {
		panic("par2: regionMulXOR: length mismatch")
	}
	if c == 0 {
		return
	}
	if c == 1 {
		for i := 0; i+1 < len(dst); i += 2 {
			dst[i] ^= src[i]
			dst[i+1] ^= src[i+1]
		}
		return
	}
	logC := int(gfLog[c])
	for i := 0; i+1 < len(dst); i += 2 {
		w := uint16(src[i]) | uint16(src[i+1])<<8
		if w == 0 {
			continue
		}
		sum := int(gfLog[w]) + logC
		if sum >= gfOrd {
			sum -= gfOrd
		}
		p := gfExp[sum]
		dst[i] ^= byte(p)
		dst[i+1] ^= byte(p >> 8)
	}
}
