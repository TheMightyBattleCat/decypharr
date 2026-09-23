package vfs

import "testing"

func TestObserveDue(t *testing.T) {
	for _, tc := range []struct {
		last, pct int32
		want      bool
	}{
		{0, 0, true},  // first read of the handle
		{1, 3, false}, // within the step
		{1, 6, true},
		{40, 44, false},
		{40, 45, true},
		{60, 20, false}, // a seek back reports nothing new
	} {
		if got := observeDue(tc.last, tc.pct); got != tc.want {
			t.Errorf("observeDue(%d, %d) = %v, want %v", tc.last, tc.pct, got, tc.want)
		}
	}
}
