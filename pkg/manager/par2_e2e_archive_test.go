package manager

// P2E on archive layouts: a RAR4 and a RAR5 volume set (written by RAR 6.23,
// parser testdata) posted in 1,000-byte articles with a PAR2 set over the
// volumes. The stored record comes from the real parser (ParseWithID, then
// Process) against the fake server, so every member segment carries the
// parser's own SegmentDataStart and Bytes. Dead articles in the middle volume
// are recorded on the member, as the reader's padding path records them; the
// pass must patch each one with the member's bytes, and a read of the member
// must return the whole file.

import (
	"bytes"
	"context"
	"fmt"
	"html"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

const (
	p2eArchiveArticle = 1000
	p2eArchiveMember  = "movie.mkv" // stored under the release's name
	p2eArchiveSize    = 12000       // movie.mkv in both sets (rar_volume_order_test.go)
)

// buildP2eArchiveFixture copies the volumes from the parser's testdata into
// dir, makes a PAR2 set over them with par2cmdline and cuts every file into
// p2eArchiveArticle-byte articles.
func buildP2eArchiveFixture(dir, par2Bin, base string, vols []string) (*p2eFixture, error) {
	fx := &p2eFixture{dir: dir, all: map[string]*p2eArticle{}}
	for _, v := range vols {
		d, err := os.ReadFile(filepath.Join("..", "usenet", "parser", "testdata", "rarvol", v))
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(dir, v), d, 0o644); err != nil {
			return nil, err
		}
		fx.posted = append(fx.posted, &p2eFile{name: v, data: d})
	}
	// 512-byte slices, so one dead article costs two or three slices, and
	// enough redundancy that three dead articles are well within reach.
	args := append([]string{"create", "-q", "-s512", "-r60", "-n3", base + ".par2"}, vols...)
	cmd := exec.Command(par2Bin, args...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("par2 create: %v: %s", err, out)
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
	for _, f := range append(append([]*p2eFile{}, fx.posted...), fx.par2...) {
		f.cutSized(fx.all, p2eArchiveArticle)
	}
	return fx, nil
}

// nzbXML is the release as an indexer's NZB: one <file> per posted file in
// upload order, subjects in the usual `"name" yEnc (i/n)` form.
func (fx *p2eFixture) nzbXML() []byte {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	b.WriteString(`<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">` + "\n")
	files := append(append([]*p2eFile{}, fx.posted...), fx.par2...)
	for i, f := range files {
		subject := fmt.Sprintf(`%s [%d/%d] - "%s" yEnc (1/%d)`, p2eRelease, i+1, len(files), f.name, len(f.ids))
		fmt.Fprintf(&b, `<file poster="p2e@p2e.test" date="1763294400" subject="%s">`+"\n", html.EscapeString(subject))
		b.WriteString("<groups><group>alt.binaries.p2e</group></groups>\n<segments>\n")
		for j, id := range f.ids {
			fmt.Fprintf(&b, `<segment bytes="%d" number="%d">%s</segment>`+"\n", len(fx.all[id].body), j+1, id)
		}
		b.WriteString("</segments>\n</file>\n")
	}
	b.WriteString("</nzb>\n")
	return []byte(b.String())
}

// parseThroughServer runs the import's parse and process steps on the NZB
// against the fake server, as an import does, and returns the stored record.
func parseThroughServer(t *testing.T, fx *p2eFixture, nzbID string) *storage.NZB {
	t.Helper()
	u, err := usenet.New()
	if err != nil {
		t.Fatalf("usenet.New: %v", err)
	}
	defer func() { _ = u.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	nzb, groups, err := u.ParseWithID(ctx, nzbID, p2eRelease, fx.nzbXML(), "p2e")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	nzb, err = u.Process(ctx, nzb, groups)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	return nzb
}

// adoptMember adds the record's member named by match to fx as a file whose
// payloads are the posted bytes of each reader segment, read out of the
// posted articles at the parser's [SegmentDataStart, +Bytes) slots, and whose
// data is those payloads joined. The caller checks the data against another
// source before trusting it.
func adoptMember(t *testing.T, fx *p2eFixture, nzb *storage.NZB, match func(name string) bool) (*p2eFile, *storage.NZBFile) {
	t.Helper()
	var member *storage.NZBFile
	var names []string
	for i := range nzb.Files {
		names = append(names, nzb.Files[i].Name)
		if match(nzb.Files[i].Name) && !nzb.Files[i].IsDeleted {
			member = &nzb.Files[i]
		}
	}
	if member == nil {
		t.Fatalf("member not in the parsed record (files %v)", names)
	}
	f := &p2eFile{name: member.Name}
	var off int64
	for i, seg := range member.Segments {
		a := fx.all[seg.MessageID]
		if a == nil {
			t.Fatalf("segment %d: %s is not a posted article", i, seg.MessageID)
		}
		if seg.StartOffset != off || seg.EndOffset != off+seg.Bytes-1 {
			t.Fatalf("segment %d covers [%d,%d], want it to start at %d and hold %d bytes", i, seg.StartOffset, seg.EndOffset, off, seg.Bytes)
		}
		if seg.SegmentDataStart < 0 || seg.SegmentDataStart+seg.Bytes > int64(len(a.data)) {
			t.Fatalf("segment %d: slot [%d,+%d) outside its %d-byte article", i, seg.SegmentDataStart, seg.Bytes, len(a.data))
		}
		p := a.data[seg.SegmentDataStart : seg.SegmentDataStart+seg.Bytes]
		f.ids = append(f.ids, seg.MessageID)
		f.payloads = append(f.payloads, p)
		f.data = append(f.data, p...)
		off += seg.Bytes
	}
	fx.posted = append(fx.posted, f)
	return f, member
}

// middleVolumeDead picks the member segments whose articles belong to vol:
// the first (it shares its article with the volume header), one interior
// segment, and the last (it shares its article with the end of the volume).
func middleVolumeDead(t *testing.T, fx *p2eFixture, member *p2eFile, rec *storage.NZBFile, vol string) []p2eSeg {
	t.Helper()
	v := fx.file(vol)
	inVol := map[string]bool{}
	for _, id := range v.ids {
		inVol[id] = true
	}
	var idx []int
	for i, seg := range rec.Segments {
		if inVol[seg.MessageID] {
			idx = append(idx, i)
		}
	}
	if len(idx) < 3 {
		t.Fatalf("%d member segments in %s, want at least 3", len(idx), vol)
	}
	first, mid, last := idx[0], idx[len(idx)/2], idx[len(idx)-1]
	if rec.Segments[first].SegmentDataStart == 0 {
		t.Fatalf("first member segment of %s starts its article; the volume header should come first", vol)
	}
	if lastArt := fx.all[rec.Segments[last].MessageID]; rec.Segments[last].Bytes >= int64(len(lastArt.data)) {
		t.Fatalf("last member segment of %s fills its %d-byte article; the end of the volume should follow it", vol, len(lastArt.data))
	}
	return []p2eSeg{{member, first}, {member, mid}, {member, last}}
}

// Dead articles in the middle volume of a RAR4 and a RAR5 set: the patches
// hold the member's bytes and a read of the member returns all of it. The
// two sets store the same 12,000-byte file, so their records must give the
// same bytes; that holds only if the parser put every segment of both at
// the right slot.
func TestP2EPar2RARLayouts(t *testing.T) {
	par2Bin, err := exec.LookPath("par2")
	if err != nil {
		t.Skip("par2 (par2cmdline) not installed")
	}
	sets := []struct {
		name, base, middle string
		vols               []string
	}{
		{"rar4", "rar4set", "rar4set.r00", []string{"rar4set.rar", "rar4set.r00", "rar4set.r01"}},
		{"rar5", "rar5set", "rar5set.part2.rar", []string{"rar5set.part1.rar", "rar5set.part2.rar", "rar5set.part3.rar"}},
	}
	members := map[string][]byte{}
	for _, set := range sets {
		t.Run(set.name, func(t *testing.T) {
			p2eStartClock(t)
			fx, err := buildP2eArchiveFixture(t.TempDir(), par2Bin, set.base, set.vols)
			if err != nil {
				t.Fatalf("fixture: %v", err)
			}
			servers := p2eServeFixture(t, 1, fx)
			nzb := parseThroughServer(t, fx, "p2e-archive-"+set.name)
			member, rec := adoptMember(t, fx, nzb, func(name string) bool { return strings.HasSuffix(name, ".mkv") })
			if len(member.data) != p2eArchiveSize || rec.Size != p2eArchiveSize {
				t.Fatalf("member holds %d bytes, record size %d, want %d", len(member.data), rec.Size, p2eArchiveSize)
			}
			members[set.name] = member.data

			h := newP2eHarnessNZB(t, servers[0], fx, nzb)
			h.recordDead(middleVolumeDead(t, fx, member, rec, set.middle)...)
			for _, s := range h.dead {
				h.srv.set(s.file.ids[s.idx], p2eNotFound)
			}
			o := h.run(nil)
			h.report(o)
			p2eCompleted(len(h.dead)).check(t, h, o)
			h.readBack(member)
		})
	}
	if a, b := members["rar4"], members["rar5"]; a != nil && b != nil && !bytes.Equal(a, b) {
		t.Fatalf("the RAR4 and RAR5 records give different bytes for %s (first difference at %d)", p2eArchiveMember, firstDiff(a, b))
	}
}
