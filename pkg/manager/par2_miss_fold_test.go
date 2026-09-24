package manager

import (
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func TestClassifyPar2Miss(t *testing.T) {
	const sliceSize = 64
	content := postedContent(256)
	var slices [][]byte
	for off := 0; off < len(content); off += sliceSize {
		slices = append(slices, content[off:off+sliceSize])
	}
	fileID := [16]byte{0x21}
	idx := buildSingleFileIndexWithIFSC(t, sliceSize, fileID, "real.r05", slices) // FileDesc length 256

	// An obfuscated posted file whose estimated size (250) misses 256 by less
	// than an article (32).
	near := storage.PostedFileRef{Name: "a1b2c3", Size: 250, Segments: make([]storage.Par2SegmentRef, 8)}
	for i := range near.Segments {
		near.Segments[i] = storage.Par2SegmentRef{MessageID: "<n>", Bytes: 32}
	}
	timeout := &nntp.Error{Type: nntp.ErrorTypeTimeout, Message: "timeout"}
	gone := &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Message: "430"}

	cases := []struct {
		name     string
		source   []storage.PostedFileRef
		skipped  map[string]error
		terminal bool
	}{
		{"never posted", nil, nil, true},
		{"estimated size, skipped for a timeout", []storage.PostedFileRef{near}, map[string]error{"a1b2c3": timeout}, false},
		{"estimated size, not skipped (another volume)", []storage.PostedFileRef{near}, nil, true},
		{"estimated size, head confirmed gone", []storage.PostedFileRef{near}, map[string]error{"a1b2c3": gone}, true},
		{"exact length, unmatched", []storage.PostedFileRef{{Name: "x", Size: 256}}, nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			reason, terminal := classifyPar2Miss(idx, fileID, c.source, c.skipped)
			if terminal != c.terminal {
				t.Errorf("terminal = %v (%s), want %v", terminal, reason, c.terminal)
			}
		})
	}
}

func TestFoldUncoveredFiles(t *testing.T) {
	const sliceSize = 64
	content := postedContent(128) // 2 slices
	var slices [][]byte
	for off := 0; off < len(content); off += sliceSize {
		slices = append(slices, content[off:off+sliceSize])
	}
	fileID := [16]byte{0x22}
	idx := buildSingleFileIndexWithIFSC(t, sliceSize, fileID, "release.nfo", slices)

	structural := func([16]byte) (string, bool) { return "no retained posted file", true }
	transient := func([16]byte) (string, bool) { return "posted file failed to fetch/hash during matching", false }

	t.Run("structural miss that fits is folded", func(t *testing.T) {
		damaged := map[int64]struct{}{}
		n, err := foldUncoveredFiles(zerolog.Nop(), "e", idx, [][16]byte{fileID}, damaged, 5, structural)
		if err != nil || n != 2 || len(damaged) != 2 {
			t.Fatalf("folded=%d damaged=%d err=%v, want the file's 2 slices folded", n, len(damaged), err)
		}
	})
	t.Run("structural miss over budget stays terminal", func(t *testing.T) {
		_, err := foldUncoveredFiles(zerolog.Nop(), "e", idx, [][16]byte{fileID}, map[int64]struct{}{}, 1, structural)
		if err == nil || !classifyPar2Failure(err).terminal {
			t.Fatalf("err = %v, want a terminal failure", err)
		}
	})
	t.Run("transient miss over budget is not terminal", func(t *testing.T) {
		_, err := foldUncoveredFiles(zerolog.Nop(), "e", idx, [][16]byte{fileID}, map[int64]struct{}{}, 1, transient)
		if err == nil {
			t.Fatal("expected an error")
		}
		if class := classifyPar2Failure(err); class.terminal {
			t.Fatalf("a transient miss over budget went terminal: %q", class.reason)
		}
	})
}
