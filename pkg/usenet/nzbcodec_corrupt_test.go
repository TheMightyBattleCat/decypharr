package usenet

import (
	"encoding/binary"
	"testing"
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
