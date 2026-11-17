package manager

import (
	"bytes"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

var p2eIFSCType = []byte("PAR 2.0\x00IFSC\x00\x00\x00\x00")

// p2eCorruptIFSC damages, in place, the body of every IFSC packet of f for
// the file whose ID starts the body (the first IFSC packet's file when
// fileID is nil), so the packet MD5 fails and the parser skips it. The
// file's size and article layout stay the same; its articles are rebuilt and
// served again. It returns the file ID it targeted.
func p2eCorruptIFSC(t *testing.T, servers []*p2eServer, fx *p2eFixture, f *p2eFile, fileID []byte) []byte {
	t.Helper()
	n := 0
	for off := 0; ; {
		i := bytes.Index(f.data[off:], p2eIFSCType)
		if i < 0 {
			break
		}
		typeAt := off + i
		body := typeAt + 16 // the type field is the header's last 16 bytes
		if fileID == nil {
			fileID = append([]byte(nil), f.data[body:body+16]...)
		}
		if bytes.Equal(f.data[body:body+16], fileID) {
			f.data[body+16] ^= 0xff // first slice checksum
			n++
		}
		off = body
	}
	if n == 0 {
		t.Fatalf("no IFSC packet for %x in %s", fileID, f.name)
	}
	f.ids, f.payloads = nil, nil
	f.cut(fx.all)
	for _, id := range f.ids {
		for _, srv := range servers {
			srv.addArticle(fx.all[id])
		}
	}
	return fileID
}

// Every copy of one file's slice checksums in the index file and the
// volumes the first fetch takes is unreadable; the volume fetched last holds
// a good copy. The pass fetches it before the stream and repairs the dead article.
// Before the fix the stream failed at that file's first intact slice
// ("no IFSC checksum"), a transient failure every later pass repeated.
func TestPar2MissingSliceChecksumsFetchesAnotherVolume(t *testing.T) {
	// At 25% the three volumes hold 8, 7 and 7 slices. runRepair fetches
	// volumes smallest first, and the first fetch takes the two 7-slice
	// volumes (one plus a slack volume): the largest is left for later.
	servers, fx := newP2eEnvSets(t, 1, p2eSet{base: p2eRelease, files: []int{0, 1, 2}, redundancy: 25})
	vols := fx.par2[1:]
	if len(vols) != 3 {
		t.Fatalf("fixture has %d volumes, want 3", len(vols))
	}
	good := vols[0]
	for _, v := range vols[1:] {
		if len(v.data) == len(good.data) {
			t.Fatalf("two volumes of %d bytes, want the largest unique", len(v.data))
		}
		if len(v.data) > len(good.data) {
			good = v
		}
	}
	var id []byte
	for _, f := range fx.par2 {
		if f != good {
			id = p2eCorruptIFSC(t, servers, fx, f, id)
		}
	}
	h := newP2eHarness(t, servers[0], fx, "p2e-missing-ifsc")
	f := fx.posted[0]
	h.recordDead(p2eSeg{f, 1})
	servers[0].set(f.ids[1], p2eNotFound)

	o := h.run(nil)
	h.report(o)

	if o.LogMsg != "par2 repair completed" {
		t.Fatalf("pass ended %q (class %q, err %q), want completed", o.LogMsg, o.Class, o.Err)
	}
	a, err := h.p.manager.storage.LatestPar2RepairAttemptForNzb(h.nzbID)
	if err != nil || a == nil {
		t.Fatalf("attempt record: %v", err)
	}
	if a.Outcome != storage.Par2RepairOutcomeCompleted || a.SegmentsPatched != 1 {
		t.Errorf("attempt outcome %s, %d segment(s) patched (reason %q), want completed with 1", a.Outcome, a.SegmentsPatched, a.FailReason)
	}
	if o.Mismatch != 0 || o.Patched != 1 {
		t.Errorf("overlay: %s, want 1 patch matching the original bytes", o.Overlay)
	}
}

// When no PAR2 file of the set holds a readable copy, the pass ends terminal
// with a reason that says so, instead of a transient stream failure retried
// for ever.
func TestPar2MissingSliceChecksumsEverywhereIsTerminal(t *testing.T) {
	servers, fx := newP2eEnv(t, 1)
	var id []byte
	for _, f := range fx.par2 {
		id = p2eCorruptIFSC(t, servers, fx, f, id)
	}
	h := newP2eHarness(t, servers[0], fx, "p2e-missing-ifsc-all")
	f := fx.posted[0]
	h.recordDead(p2eSeg{f, 1})
	servers[0].set(f.ids[1], p2eNotFound)

	o := h.run(nil)
	h.report(o)

	if o.Class != "terminal" || !strings.Contains(o.Err, "PAR2 set has no slice checksums for 1 file(s)") {
		t.Fatalf("pass ended %q class=%q err=%q, want terminal with the missing-checksums reason", o.LogMsg, o.Class, o.Err)
	}
}
