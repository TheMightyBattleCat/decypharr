package webdav

import "testing"

// WebDAV and the /stream route share resolveRange. A file with a byte range
// is a slice of its backing download, so a requested range has to be moved by
// the slice's start on both routes.
func TestResolveRange(t *testing.T) {
	slice := &[2]int64{1000, 1999}
	tests := []struct {
		name       string
		header     string
		size       int64
		byteRange  *[2]int64
		start, end int64
	}{
		{"no header, whole file", "", 500, nil, 0, -1},
		{"no header, slice", "", 1000, slice, 1000, 1999},
		{"range, whole file", "bytes=100-199", 500, nil, 100, 199},
		{"range, slice", "bytes=100-199", 1000, slice, 1100, 1199},
		{"open-ended range, slice", "bytes=900-", 1000, slice, 1900, 1999},
		{"more than one range", "bytes=0-1,5-6", 500, nil, 0, 0},
		{"not a byte range", "items=0-1", 500, nil, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end := resolveRange(tt.header, tt.size, tt.byteRange)
			if start != tt.start || end != tt.end {
				t.Fatalf("resolveRange(%q) = %d-%d, want %d-%d", tt.header, start, end, tt.start, tt.end)
			}
		})
	}
}
