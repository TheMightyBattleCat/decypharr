package parser

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/usenet/types"
)

// fuzzDeadline bounds one input's parse. Go's fuzzer has no per-input hang
// detector, so a walk that never ends must fail the input itself.
const fuzzDeadline = 10 * time.Second

// mustEnd runs fn on the test goroutine. When fn has not returned by the
// deadline, a watchdog panics, which crashes the fuzz worker and makes the
// fuzzer record the input as failing.
func mustEnd(t *testing.T, fn func()) {
	t.Helper()
	watchdog := time.AfterFunc(fuzzDeadline, func() {
		panic(fmt.Sprintf("%s: parse did not end within %v", t.Name(), fuzzDeadline))
	})
	defer watchdog.Stop()
	fn()
}

// rarFuzzSeeds returns the committed RAR volume fixtures that start with sig.
func rarFuzzSeeds(f *testing.F, sig []byte) [][]byte {
	ents, err := os.ReadDir(filepath.Join("testdata", "rarvol"))
	if err != nil {
		f.Fatal(err)
	}
	var out [][]byte
	for _, e := range ents {
		b, err := os.ReadFile(filepath.Join("testdata", "rarvol", e.Name()))
		if err != nil {
			f.Fatal(err)
		}
		if bytes.HasPrefix(b, sig) {
			out = append(out, b)
		}
	}
	if len(out) == 0 {
		f.Fatalf("no RAR fixtures start with %q", sig)
	}
	return out
}

// fuzzArticleBytes turns a fuzz value into an article size that puts the
// segment boundaries at different places in the headers.
func fuzzArticleBytes(v uint16) int { return 16 + int(v%4096) }

// fuzzRARStream posts data as one volume and returns a stream positioned
// after the signature, as parseArchive leaves it.
func fuzzRARStream(t *testing.T, data []byte, sigSize int, article uint16) (*RARParser, *rarReader, *types.Volume) {
	t.Helper()
	posted := newPostedArticles()
	vol := posted.volume("fuzz.rar", data, fuzzArticleBytes(article))
	p := &RARParser{logger: zerolog.Nop(), maxConcurrent: 1, fetchBody: posted.fetch}
	stream := newRarReader(context.Background(), posted.fetch, []*types.Volume{vol})
	if err := stream.Skip(int64(sigSize)); err != nil {
		t.Fatalf("skip signature: %v", err)
	}
	return p, stream, vol
}

// checkRAREntries fails on a negative size, offset or part count.
func checkRAREntries(t *testing.T, files []*RARFileEntry) {
	t.Helper()
	for _, f := range files {
		if f == nil {
			t.Fatal("nil file entry")
		}
		if f.UncompressedSize < 0 || f.PackedSize < 0 || f.DataOffset < 0 || f.VolumeIndex < 0 {
			t.Fatalf("entry %q: unpacked %d packed %d offset %d volume %d", f.Name, f.UncompressedSize, f.PackedSize, f.DataOffset, f.VolumeIndex)
		}
		for _, part := range f.VolumeParts {
			if part.DataOffset < 0 || part.PackedSize < 0 || part.UnpackedSize < 0 || part.TrimmedBytes < 0 || part.PartNumber < 0 {
				t.Fatalf("entry %q part: %+v", f.Name, *part)
			}
		}
	}
}

func FuzzParseRAR4Stream(f *testing.F) {
	for _, s := range rarFuzzSeeds(f, rar4Signature) {
		f.Add(s, uint16(1000))
		f.Add(s, uint16(7))
	}
	f.Fuzz(func(t *testing.T, data []byte, article uint16) {
		if !bytes.HasPrefix(data, rar4Signature) {
			return
		}
		p, stream, vol := fuzzRARStream(t, data, len(rar4Signature), article)
		mustEnd(t, func() {
			files, volNum, hasVolNum, _ := p.parseRAR4Stream(stream, 0, vol.Name, vol.Size)
			checkRAREntries(t, files)
			if hasVolNum && volNum < 0 {
				t.Fatalf("volume number %d", volNum)
			}
		})
	})
}

func FuzzParseRAR5Stream(f *testing.F) {
	for _, s := range rarFuzzSeeds(f, rar5Signature) {
		f.Add(s, uint16(1000))
		f.Add(s, uint16(7))
	}
	f.Fuzz(func(t *testing.T, data []byte, article uint16) {
		if !bytes.HasPrefix(data, rar5Signature) {
			return
		}
		p, stream, vol := fuzzRARStream(t, data, len(rar5Signature), article)
		mustEnd(t, func() {
			res, err := p.parseRAR5Stream(stream, 0, vol.Name, "")
			if err != nil || res == nil {
				return
			}
			checkRAREntries(t, res.Files)
			if res.HasVolumeNumber && res.VolumeNumber < 0 {
				t.Fatalf("volume number %d", res.VolumeNumber)
			}
		})
	})
}

func FuzzReadFileHeaderStart(f *testing.F) {
	for _, sig := range [][]byte{rar4Signature, rar5Signature} {
		for _, s := range rarFuzzSeeds(f, sig) {
			f.Add(s)
			f.Add(s[:min(len(s), 128)])
		}
	}
	f.Fuzz(func(t *testing.T, prefix []byte) {
		mustEnd(t, func() {
			h, ok := ReadFileHeaderStart(prefix)
			if !ok {
				return
			}
			if h.DataAt < 0 || h.DataAt > int64(len(prefix)) {
				t.Fatalf("DataAt %d outside the %d-byte prefix", h.DataAt, len(prefix))
			}
		})
	})
}

// zipFuzzSeed builds a stored ZIP with n files and the given comment.
func zipFuzzSeed(f *testing.F, n int, comment string) []byte {
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	for i := range n {
		fw, err := w.CreateHeader(&zip.FileHeader{Name: fmt.Sprintf("dir/file%d.mkv", i), Method: zip.Store})
		if err != nil {
			f.Fatal(err)
		}
		if _, err := fw.Write([]byte(strings.Repeat("x", 100*(i+1)))); err != nil {
			f.Fatal(err)
		}
	}
	if err := w.SetComment(comment); err != nil {
		f.Fatal(err)
	}
	if err := w.Close(); err != nil {
		f.Fatal(err)
	}
	return buf.Bytes()
}

func FuzzParseCentralDirectory(f *testing.F) {
	f.Add(zipFuzzSeed(f, 1, ""))
	f.Add(zipFuzzSeed(f, 3, "a trailing comment"))
	whole := zipFuzzSeed(f, 3, "")
	f.Add(whole[len(whole)-120:]) // a tail snippet that starts mid-directory
	f.Fuzz(func(t *testing.T, data []byte) {
		p := &ZIPParser{logger: zerolog.Nop()}
		mustEnd(t, func() {
			eocd, pos, err := p.findEndOfCentralDirectory(data)
			if err != nil {
				return
			}
			if pos < 0 || pos > len(data) {
				t.Fatalf("EOCD position %d outside %d bytes", pos, len(data))
			}
			files, err := p.parseCentralDirectory(data, eocd, pos)
			if err != nil {
				return
			}
			if len(files) > len(data)/46 {
				t.Fatalf("%d entries from %d bytes", len(files), len(data))
			}
			for _, e := range files {
				if e.UncompressedSize < 0 || e.CompressedSize < 0 || e.LocalHeaderOffset < 0 {
					t.Fatalf("entry %q: unpacked %d packed %d local header %d", e.Name, e.UncompressedSize, e.CompressedSize, e.LocalHeaderOffset)
				}
			}
		})
	})
}
