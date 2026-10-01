package webdav

import "testing"

// WebDAV and the /stream route share resolveRange. It returns offsets inside
// the file, whether or not the file is a slice of its backing download:
// Manager.Stream moves them by the slice's start.
func TestResolveRange(t *testing.T) {
	tests := []struct {
		name       string
		header     string
		size       int64
		start, end int64
	}{
		{"no header", "", 500, 0, -1},
		{"range", "bytes=100-199", 500, 100, 199},
		{"open-ended range", "bytes=900-", 1000, 900, 999},
		{"suffix range", "bytes=-50", 1000, 950, 999},
		{"more than one range", "bytes=0-1,5-6", 500, 0, 0},
		{"not a byte range", "items=0-1", 500, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end := resolveRange(tt.header, tt.size)
			if start != tt.start || end != tt.end {
				t.Fatalf("resolveRange(%q) = %d-%d, want %d-%d", tt.header, start, end, tt.start, tt.end)
			}
		})
	}
}
