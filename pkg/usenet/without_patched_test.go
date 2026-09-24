package usenet

import (
	"reflect"
	"testing"
)

// The sweep's availability sample must not STAT a patched segment: its
// article is dead (that's why PAR2 rebuilt it), and counting it missing sent
// a repaired file to a re-grab.
func TestWithoutPatched(t *testing.T) {
	all := sampleIndices(10, 100)
	if got := withoutPatched(all, nil); !reflect.DeepEqual(got, all) {
		t.Fatalf("no patches: got %v, want %v", got, all)
	}
	patched := map[int]struct{}{0: {}, 4: {}, 9: {}}
	if got, want := withoutPatched(all, patched), []int{1, 2, 3, 5, 6, 7, 8}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	every := map[int]struct{}{}
	for _, i := range all {
		every[i] = struct{}{}
	}
	if got := withoutPatched(all, every); len(got) != 0 {
		t.Fatalf("every segment patched: got %v, want none", got)
	}
}
