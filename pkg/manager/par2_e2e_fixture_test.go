package manager

// P2E harness, part 2: the release fixture. Three directly-posted .mkv
// files (two 600 KiB, one 150 KiB tail) of seeded random bytes, a PAR2 set
// made by par2cmdline at test time, every file cut into 100 KiB yEnc
// articles, and the storage.NZB record laid out the way the parser builds it
// for directly-posted media (parser/misc.go:258-327 for Files,
// parser/parser.go:791-813 for Par2Source with Real refs; PAR2 files never
// Real, parser/parser.go:672).

import (
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

const (
	p2eArticleSize = 100 * 1024
	p2eSliceSize   = 16384
	p2eRelease     = "P2E.Rel"
)

type p2eFile struct {
	name     string
	data     []byte
	isPar2   bool
	ids      []string
	payloads [][]byte
}

type p2eFixture struct {
	dir    string
	posted []*p2eFile
	par2   []*p2eFile // index first, then volumes sorted by name
	all    map[string]*p2eArticle
}

// p2eYenc encodes one multipart yEnc article with full escaping (NUL, LF, CR,
// '=' always; TAB/space at line start/end; '.' at line start so no NNTP
// dot-stuffing is ever needed).
func p2eYenc(name string, fileSize int64, part, total int, offset int64, data []byte) string {
	var b strings.Builder
	b.Grow(len(data)*102/100 + 512)
	fmt.Fprintf(&b, "=ybegin part=%d total=%d line=128 size=%d name=%s\r\n", part, total, fileSize, name)
	fmt.Fprintf(&b, "=ypart begin=%d end=%d\r\n", offset+1, offset+int64(len(data)))
	col := 0
	for i, c := range data {
		e := c + 42
		esc := false
		switch e {
		case 0x00, 0x0A, 0x0D, '=':
			esc = true
		case '\t', ' ':
			esc = col == 0 || col >= 126 || i == len(data)-1
		case '.':
			esc = col == 0
		}
		if esc {
			b.WriteByte('=')
			b.WriteByte(e + 64)
			col += 2
		} else {
			b.WriteByte(e)
			col++
		}
		if col >= 128 {
			b.WriteString("\r\n")
			col = 0
		}
	}
	if col > 0 {
		b.WriteString("\r\n")
	}
	fmt.Fprintf(&b, "=yend size=%d part=%d pcrc32=%08x\r\n", len(data), part, crc32.ChecksumIEEE(data))
	return b.String()
}

func p2eMsgID(name string, part int) string {
	tag := strings.NewReplacer(".", "-", "+", "p").Replace(name)
	return fmt.Sprintf("p2e-%s-%03d@p2e.test", tag, part)
}

func (f *p2eFile) cut(all map[string]*p2eArticle) { f.cutSized(all, p2eArticleSize) }

// cutSized is cut with articles of size bytes.
func (f *p2eFile) cutSized(all map[string]*p2eArticle, size int) {
	n := (len(f.data) + size - 1) / size
	for i := 0; i < n; i++ {
		off := i * size
		end := min(off+size, len(f.data))
		p := f.data[off:end]
		id := p2eMsgID(f.name, i+1)
		half := p[:len(p)/2]
		a := &p2eArticle{
			id:        id,
			data:      p,
			body:      p2eYenc(f.name, int64(len(f.data)), i+1, n, int64(off), p),
			shortBody: p2eYenc(f.name, int64(len(f.data)), i+1, n, int64(off), half),
		}
		all[id] = a
		f.ids = append(f.ids, id)
		f.payloads = append(f.payloads, p)
	}
}

// p2eSet is one PAR2 recovery set of a fixture: the set's base name, the
// indexes into p2eFixture.posted of the files it protects, and its
// redundancy in percent (0 means 20).
type p2eSet struct {
	base       string
	files      []int
	redundancy int
}

// p2eTestdata holds the default set (p2eDefaultSet) as par2cmdline 1.2.0
// made it for this fixture, so tests that use it need no par2 binary.
// TestP2eTestdataMatchesPar2cmdline checks it against a fresh run.
const p2eTestdata = "testdata/p2e"

var p2eDefaultSet = p2eSet{base: p2eRelease, files: []int{0, 1, 2}}

// buildP2eFixture writes the release into dir and runs par2cmdline on it,
// once per set. With no sets, one set named p2eRelease protects every file;
// it is copied from p2eTestdata when par2Bin is "".
func buildP2eFixture(dir, par2Bin string, sets ...p2eSet) (*p2eFixture, error) {
	rng := rand.New(rand.NewSource(20261116))
	mk := func(name string, size int) (*p2eFile, error) {
		d := make([]byte, size)
		rng.Read(d)
		if err := os.WriteFile(filepath.Join(dir, name), d, 0o644); err != nil {
			return nil, err
		}
		return &p2eFile{name: name, data: d}, nil
	}
	fx := &p2eFixture{dir: dir, all: map[string]*p2eArticle{}}
	for _, spec := range []struct {
		name string
		size int
	}{
		{p2eRelease + ".part1.mkv", 600 * 1024},
		{p2eRelease + ".part2.mkv", 600 * 1024},
		{p2eRelease + ".part3.mkv", 150 * 1024},
	} {
		f, err := mk(spec.name, spec.size)
		if err != nil {
			return nil, err
		}
		fx.posted = append(fx.posted, f)
	}
	if len(sets) == 0 && par2Bin == "" {
		if err := p2eCopyTestdata(dir); err != nil {
			return nil, err
		}
	} else if len(sets) == 0 {
		sets = []p2eSet{p2eDefaultSet}
	}
	for _, set := range sets {
		redundancy := set.redundancy
		if redundancy == 0 {
			redundancy = 20
		}
		args := []string{"create", "-q", "-s16384", fmt.Sprintf("-r%d", redundancy), "-n3", set.base + ".par2"}
		for _, i := range set.files {
			args = append(args, fx.posted[i].name)
		}
		cmd := exec.Command(par2Bin, args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			return nil, fmt.Errorf("par2 create %s: %v: %s", set.base, err, out)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".par2") {
			names = append(names, e.Name())
		}
	}
	sort.Slice(names, func(i, j int) bool {
		// index (no ".vol") first
		vi, vj := strings.Contains(names[i], ".vol"), strings.Contains(names[j], ".vol")
		if vi != vj {
			return !vi
		}
		return names[i] < names[j]
	})
	for _, n := range names {
		d, err := os.ReadFile(filepath.Join(dir, n))
		if err != nil {
			return nil, err
		}
		fx.par2 = append(fx.par2, &p2eFile{name: n, data: d, isPar2: true})
	}
	for _, f := range fx.posted {
		f.cut(fx.all)
	}
	for _, f := range fx.par2 {
		f.cut(fx.all)
	}
	return fx, nil
}

func p2eCopyTestdata(dir string) error {
	entries, err := os.ReadDir(p2eTestdata)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".par2") {
			continue
		}
		d, err := os.ReadFile(filepath.Join(p2eTestdata, e.Name()))
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, e.Name()), d, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func (fx *p2eFixture) file(name string) *p2eFile {
	for _, f := range append(append([]*p2eFile{}, fx.posted...), fx.par2...) {
		if f.name == name || strings.Contains(f.name, name) {
			return f
		}
	}
	return nil
}

// nzb builds the stored NZB record for nzbID.
func (fx *p2eFixture) nzb(nzbID string) *storage.NZB {
	nzb := &storage.NZB{
		ID:          nzbID,
		Name:        p2eRelease,
		Category:    "p2e",
		Groups:      []string{"alt.binaries.p2e"},
		Status:      "completed",
		Downloaded:  true,
		AddedOn:     time.Now(),
		CompletedOn: time.Now(),
	}
	var total int64
	for _, f := range fx.posted {
		nf := storage.NZBFile{
			NzbID:    nzbID,
			Name:     f.name,
			Size:     int64(len(f.data)),
			Groups:   []string{"alt.binaries.p2e"},
			FileType: storage.NZBFileTypeMedia,
		}
		ref := storage.PostedFileRef{Name: f.name, Size: int64(len(f.data))}
		var off int64
		for i, id := range f.ids {
			b := int64(len(f.payloads[i]))
			nf.Segments = append(nf.Segments, storage.NZBSegment{
				Number:      i + 1,
				MessageID:   id,
				Bytes:       b,
				StartOffset: off,
				EndOffset:   off + b - 1,
				Group:       p2eRelease,
			})
			ref.Segments = append(ref.Segments, storage.Par2SegmentRef{MessageID: id, Bytes: b, Real: true})
			off += b
		}
		total += nf.Size
		nzb.Files = append(nzb.Files, nf)
		nzb.Par2Source = append(nzb.Par2Source, ref)
	}
	for _, f := range fx.par2 {
		ref := storage.Par2FileRef{Name: f.name, Size: int64(len(f.data))}
		for i, id := range f.ids {
			ref.Segments = append(ref.Segments, storage.Par2SegmentRef{MessageID: id, Bytes: int64(len(f.payloads[i]))})
		}
		nzb.Par2Files = append(nzb.Par2Files, ref)
	}
	nzb.TotalSize = total
	return nzb
}
