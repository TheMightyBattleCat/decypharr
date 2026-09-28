package usenet

import (
	"encoding/binary"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// FuzzDecodeNZB: any byte string decodes to a record or an error, never a
// panic or an unbounded allocation.
func FuzzDecodeNZB(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{codecMagicV2})
	f.Add([]byte("\xb1\xd1\xd1\xd1\xd1\xd1\xd1\xd1\xed\xff\x01"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = decodeNZB(data)
		_, _ = decodeNZBV2Header(data)
	})
}

// A corrupt .meta must be an error, never a panic: the sweep decodes records
// with no recover above it, so a panic ended the process on every sweep that
// reached the file.
func TestCorruptMetaIsAnErrorNotAPanic(t *testing.T) {
	noPanic := func(t *testing.T, name string, fn func() error) {
		t.Helper()
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("%s panicked: %v", name, r)
			}
		}()
		if err := fn(); err == nil {
			t.Fatalf("%s accepted corrupt input", name)
		}
	}

	t.Run("region length >= 2^63", func(t *testing.T) {
		data := binary.AppendUvarint([]byte{codecMagicV2}, 1<<63)
		data = append(data, 0, 0, 0)
		noPanic(t, "decodeNZB", func() error { _, err := decodeNZB(data); return err })
	})
	t.Run("span length >= 2^63", func(t *testing.T) {
		w := &byteWriter{}
		w.uvarint(1 << 63)
		noPanic(t, "decodeHeader", func() error { _, _, err := decodeHeader(w.buf); return err })
	})
	for name, fn := range map[string]func(*byteReader) error{
		"readPar2FileRefs": func(r *byteReader) error { _, err := readPar2FileRefs(r); return err },
		"readPar2Segments": func(r *byteReader) error { _, err := readPar2Segments(r); return err },
		"readStrings":      func(r *byteReader) error { _, err := readStrings(r); return err },
	} {
		t.Run(name+" count 2^62", func(t *testing.T) {
			w := &byteWriter{}
			w.uvarint(1 << 62)
			noPanic(t, name, func() error { return fn(&byteReader{buf: w.buf}) })
		})
	}
	t.Run("empty meta", func(t *testing.T) {
		noPanic(t, "decodeNZB", func() error { _, err := decodeNZB(nil); return err })
	})
}

// A file's segment count near 2^63 in an otherwise valid record sized the
// segment slice and panicked makeslice, both when the whole record was
// decoded and when the sweep sampled one file's message IDs.
func TestCorruptSegmentCountIsAnErrorNotAPanic(t *testing.T) {
	one := &storage.NZB{ID: "x", Files: []storage.NZBFile{{Name: "a.mkv", Segments: []storage.NZBSegment{{Number: 1, Bytes: 10, MessageID: "a@b"}}}}}
	none := &storage.NZB{ID: "x", Files: []storage.NZBFile{{Name: "a.mkv"}}}
	h1, h0 := encodeHeader(one), encodeHeader(none)
	if len(h1) != len(h0) {
		t.Fatalf("precondition: headers differ in length (%d, %d)", len(h1), len(h0))
	}
	at := -1
	for i := range h1 {
		if h1[i] != h0[i] {
			if at != -1 {
				t.Fatal("precondition: headers differ in more than the segment count")
			}
			at = i
		}
	}
	if at == -1 {
		t.Fatal("precondition: segment count not found in the header")
	}
	bad := append(binary.AppendUvarint(append([]byte{}, h1[:at]...), 1<<63), h1[at+1:]...)
	segMeta, msgIDs := encodeSegments(one)
	hc, sc, mc := zstdEnc.EncodeAll(bad, nil), zstdEnc.EncodeAll(segMeta, nil), zstdEnc.EncodeAll(msgIDs, nil)
	blob := binary.AppendUvarint([]byte{codecMagicV2}, uint64(len(hc)))
	blob = append(blob, hc...)
	blob = binary.AppendUvarint(blob, uint64(len(sc)))
	blob = append(append(blob, sc...), mc...)

	for name, fn := range map[string]func() error{
		"decodeNZB":                   func() error { _, err := decodeNZB(blob); return err },
		"decodeFileMessageIDsSampled": func() error { _, _, err := decodeFileMessageIDsSampled(blob, "a.mkv", 100); return err },
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panicked: %v", r)
				}
			}()
			if err := fn(); err == nil {
				t.Fatal("accepted a corrupt segment count")
			}
		})
	}
}
