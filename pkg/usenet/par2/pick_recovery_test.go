package par2

import "testing"

// PickRecovery returns k exponents whose matrix inverts, skipping a
// candidate dependent on those already chosen.
func TestPickRecovery(t *testing.T) {
	damaged := []int64{3, 17, 40}

	picked, err := PickRecovery(damaged, []uint32{0, 1, 2, 5}, 3)
	if err != nil || len(picked) != 3 {
		t.Fatalf("picked=%v err=%v", picked, err)
	}
	checkInvertible(t, damaged, []uint32{0, 1, 2}, picked)

	// A repeated exponent is a duplicate row: it must be skipped for a spare.
	picked, err = PickRecovery(damaged, []uint32{0, 0, 1, 7}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if picked[0] != 0 || picked[1] != 2 || picked[2] != 3 {
		t.Fatalf("picked %v, want [0 2 3] (the duplicate skipped)", picked)
	}

	if _, err := PickRecovery(damaged, []uint32{4, 4, 4}, 3); err == nil {
		t.Fatal("dependent candidates only: want an error")
	}
	if got, err := PickRecovery(nil, nil, 0); got != nil || err != nil {
		t.Fatal("k=0")
	}
}

func checkInvertible(t *testing.T, damaged []int64, exps []uint32, picked []int) {
	t.Helper()
	m := make(gfMatrix, len(picked))
	for r, j := range picked {
		row := make([]uint16, len(damaged))
		for d, g := range damaged {
			row[d] = gfPow(inputConstant(g), exps[j])
		}
		m[r] = row
	}
	if _, err := invertMatrix(m); err != nil {
		t.Fatalf("picked matrix is singular: %v", err)
	}
}
