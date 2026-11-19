package vfs

import (
	"testing"
)

// PeekHasCachedRange answers exactly what PeekCachedRange would, without
// reading: true for a fully cached range of the live item, false for a
// partly cached or uncached one, and false for a closed item whose sidecar
// still lists the range (HasCachedRange says true there, but
// PeekCachedRange cannot read from it). The warm sweep preflight relies on
// this agreement: a "yes" the pass's read then misses would send the whole
// release over NNTP.
func TestPeekHasCachedRangeAgreesWithPeekCachedRange(t *testing.T) {
	c := newWritableTestCache(t)
	m := &Manager{cache: c}

	const entryName, filename = "Some.Release", "episode.mkv"
	const fileSize = 1024
	item := newWritableTestItem(t, c, entryName, filename, fileSize)
	if err := m.WriteCachedRange(entryName, filename, fileSize, make([]byte, 100), 200); err != nil {
		t.Fatalf("WriteCachedRange: %v", err)
	}

	cases := []struct {
		name      string
		off, size int64
		want      bool
	}{
		{"whole cached range", 200, 100, true},
		{"inside the cached range", 250, 10, true},
		{"runs past the cached range", 250, 100, false},
		{"before the cached range", 0, 50, false},
		{"zero length", 200, 0, false},
	}
	for _, tc := range cases {
		has := m.PeekHasCachedRange(entryName, filename, tc.off, tc.size)
		read := tc.size > 0 && m.PeekCachedRange(entryName, filename, make([]byte, tc.size), tc.off)
		if has != tc.want || read != tc.want {
			t.Errorf("%s: PeekHasCachedRange = %v, PeekCachedRange = %v, want both %v", tc.name, has, read, tc.want)
		}
	}
	if m.PeekHasCachedRange(entryName, "other.mkv", 200, 100) {
		t.Errorf("PeekHasCachedRange = true for a file with no cache item")
	}

	// Closed item: its sidecar still lists the range, the live map doesn't.
	item.flushMetadata(true)
	c.items.Delete(item.key)
	if !m.HasCachedRange(entryName, filename, 200, 100) {
		t.Fatalf("precondition: HasCachedRange should find the range in the sidecar")
	}
	if m.PeekCachedRange(entryName, filename, make([]byte, 100), 200) {
		t.Fatalf("precondition: PeekCachedRange should miss once the item is closed")
	}
	if m.PeekHasCachedRange(entryName, filename, 200, 100) {
		t.Errorf("PeekHasCachedRange = true for a closed item, want false (PeekCachedRange cannot read it)")
	}
}
