package parser

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"unicode/utf16"

	"github.com/javi11/sevenzip"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// aes7zFixture is github.com/javi11/sevenzip's testdata/aes7z.7z (BSD-3,
// Matt Dainty): ten stored entries, each AES-256 encrypted in its own folder
// under the password "password" - the layout of the encrypted 7z postings
// (a season pack on a production install, 2026-10-15) this parser used to
// serve as ciphertext.
const aes7zFixture = "testdata/sevenzip/aes7z.7z"

// TestEncrypted7zEntriesDecryptToTheLibrarysPlaintext decrypts each entry's
// packed bytes as the reader will - one AES-CBC chain from the entry's
// offset, key from sevenZipKey, IV from the folder - and compares the result,
// cut to the entry's size, with the library's own extraction.
func TestEncrypted7zEntriesDecryptToTheLibrarysPlaintext(t *testing.T) {
	archive, err := os.ReadFile(aes7zFixture)
	if err != nil {
		t.Fatal(err)
	}
	r, err := sevenzip.OpenReaderWithPassword(aes7zFixture, "password")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	entries, err := r.ListFilesWithOffsets()
	if err != nil {
		t.Fatal(err)
	}
	folderFiles := make(map[int]int)
	for _, e := range entries {
		folderFiles[e.FolderIndex]++
	}
	baseSegments := []storage.NZBSegment{{MessageID: "whole", Bytes: int64(len(archive))}}

	want := make(map[string][]byte)
	for _, f := range r.File {
		rc, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		want[f.Name], err = io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatal(err)
		}
	}

	if len(entries) != 10 {
		t.Fatalf("fixture lists %d entries, want 10", len(entries))
	}
	for _, e := range entries {
		if !e.Encrypted || e.Compressed {
			t.Fatalf("%s: encrypted=%v compressed=%v; the fixture changed", e.Name, e.Encrypted, e.Compressed)
		}
		file, err := encrypted7zFile(e, folderFiles[e.FolderIndex], "password", baseSegments)
		if err != nil {
			t.Fatalf("%s: %v", e.Name, err)
		}
		if !file.IsEncrypted || file.Size != int64(e.Size) || len(file.EncryptionKey) != 32 || len(file.EncryptionIV) != 16 {
			t.Fatalf("%s: file %+v", e.Name, file)
		}
		if len(file.Segments) != 1 || file.Segments[0].SegmentDataStart != e.Offset || file.Segments[0].Bytes != int64(e.PackedSize) {
			t.Fatalf("%s: segments %+v, want offset %d for the %d packed bytes", e.Name, file.Segments, e.Offset, e.PackedSize)
		}

		packed := bytes.Clone(archive[e.Offset : e.Offset+int64(e.PackedSize)])
		block, err := aes.NewCipher(file.EncryptionKey)
		if err != nil {
			t.Fatal(err)
		}
		cipher.NewCBCDecrypter(block, file.EncryptionIV).CryptBlocks(packed, packed)
		if !bytes.Equal(packed[:e.Size], want[e.Name]) {
			t.Fatalf("%s: decrypted bytes differ from the library's extraction", e.Name)
		}
	}
}

func TestSevenZipKey(t *testing.T) {
	if _, err := sevenZipKey("password", nil, max7zKDFIterations+1); err == nil {
		t.Fatal("no error for a key derivation over the round limit")
	}
	if _, err := sevenZipKey("password", nil, -1); err == nil {
		t.Fatal("no error for negative rounds")
	}
	// Zero rounds is 7-Zip's cycles=0x3F: salt and UTF-16LE password, padded.
	key, err := sevenZipKey("ab", []byte{9}, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := append([]byte{9, 'a', 0, 'b', 0}, make([]byte, 27)...)
	if !bytes.Equal(key, want) {
		t.Fatalf("direct key = %x, want %x", key, want)
	}
	// Non-ASCII passwords are UTF-16 code units, surrogate pairs included.
	units := utf16.Encode([]rune("é😀"))
	if len(units) != 3 {
		t.Fatalf("utf16 units = %d", len(units))
	}
	key, err = sevenZipKey("é😀", nil, 0)
	if err != nil || key[0] != 0xe9 || key[1] != 0 || key[2] != byte(units[1]) || key[3] != byte(units[1]>>8) {
		t.Fatalf("key = %x err=%v", key, err)
	}
}

// encryptedHead returns name's 16-byte opening encrypted as 7-Zip would
// under password, and the entry describing it.
func encryptedHead(t *testing.T, name string, plain []byte, password string) (sevenzip.FileInfo, []byte) {
	t.Helper()
	entry := sevenzip.FileInfo{Name: name, Encrypted: true, AESIV: bytes.Repeat([]byte{3}, 16), KDFIterations: 16}
	key, err := sevenZipKey(password, nil, entry.KDFIterations)
	if err != nil {
		t.Fatal(err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	head := make([]byte, 16)
	cipher.NewCBCEncrypter(block, entry.AESIV).CryptBlocks(head, plain[:16])
	return entry, head
}

func TestChoose7zPassword(t *testing.T) {
	ebml := append([]byte{0x1a, 0x45, 0xdf, 0xa3}, bytes.Repeat([]byte{0x42}, 12)...)
	mkv, mkvHead := encryptedHead(t, "Show.S01E01.mkv", ebml, "right")
	entries := []sevenzip.FileInfo{mkv, {Name: "Show.S01E01.nfo", Encrypted: true}}
	heads := map[int][]byte{0: mkvHead}

	for _, test := range []struct {
		name       string
		heads      map[int][]byte
		candidates []string
		want       string
		verified   bool
		err        bool
	}{
		{"meta password first", heads, []string{"right", "other"}, "right", true, false},
		{"later candidate", heads, []string{"wrong", "also wrong", "right"}, "right", true, false},
		{"none opens it", heads, []string{"wrong"}, "", false, true},
		{"no candidates", heads, nil, "", false, true},
		{"nothing to verify against", nil, []string{"first", "second"}, "first", false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, verified, err := choose7zPassword(entries, test.heads, test.candidates)
			if (err != nil) != test.err || got != test.want || verified != test.verified {
				t.Fatalf("got %q verified=%v err=%v", got, verified, err)
			}
			if err != nil && !errors.Is(err, ErrArchivePassword) {
				t.Fatalf("error %v is not ErrArchivePassword", err)
			}
		})
	}
}

func TestMediaHeadMatches(t *testing.T) {
	pad := func(b []byte) []byte { return append(b, make([]byte, 16-len(b))...) }
	for _, test := range []struct {
		name      string
		head      []byte
		known, ok bool
	}{
		{"a.mkv", pad([]byte{0x1a, 0x45, 0xdf, 0xa3}), true, true},
		{"a.MKV", pad([]byte{0x1a, 0x45, 0xdf}), true, false},
		{"a.mp4", pad([]byte{0, 0, 0, 0x20, 'f', 't', 'y', 'p'}), true, true},
		{"a.mp4", pad([]byte{0, 0, 0, 0x20, 'j', 'u', 'n', 'k'}), true, false},
		{"a.avi", pad([]byte("RIFF\x00\x00\x00\x00AVI ")), true, true},
		{"a.ts", pad([]byte{0x47}), true, true},
		{"a.m2ts", pad([]byte{0, 0, 0, 0, 0x47}), true, true},
		{"a.nfo", pad([]byte("anything")), false, false},
		{"a.mkv", []byte{0x1a, 0x45, 0xdf, 0xa3}, false, false}, // short read
	} {
		known, ok := mediaHeadMatches(test.name, test.head)
		if known != test.known || ok != test.ok {
			t.Errorf("%s % x: known=%v ok=%v", test.name, test.head[:min(len(test.head), 8)], known, ok)
		}
	}
}

func TestCheck7zStreamable(t *testing.T) {
	good := sevenzip.FileInfo{Name: filepath.Join("d", "a.mkv"), Encrypted: true, Size: 100, PackedSize: 112}
	if err := check7zStreamable(good, 1); err != nil {
		t.Fatalf("stored single-file folder refused: %v", err)
	}
	compressed := good
	compressed.Compressed = true
	short := good
	short.PackedSize = 96
	unaligned := good
	unaligned.PackedSize = 108
	padded := good
	padded.PackedSize = 128
	for name, test := range map[string]struct {
		file    sevenzip.FileInfo
		folders int
	}{
		"compressed":               {compressed, 1},
		"solid":                    {good, 3},
		"short packing":            {short, 1},
		"unaligned":                {unaligned, 1},
		"a whole block of padding": {padded, 1},
	} {
		if err := check7zStreamable(test.file, test.folders); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
