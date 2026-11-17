package manager

// A whole urgent pass on an obfuscated posting: the PAR2 files are posted
// under random names with no ".volNN+MM" marker, so nothing but their sizes
// tells the index from a recovery volume.

import (
	"fmt"
	"testing"
)

// The fixture's volumes are far under par2UnnamedVolumeFloor, so the census
// keeps every PAR2 file as an index file and their recovery slices arrive
// with the index. The pass must count those slices as recovery data in hand
// and repair, not stop at "no PAR2 recovery volumes retained".
func TestP2EPar2ObfuscatedPostingRepairs(t *testing.T) {
	servers, fx := newP2eEnv(t, 1)
	srv := servers[0]
	f1, f2 := fx.posted[0], fx.posted[1]

	nzb := fx.nzb("p2e-obfuscated")
	for i := range nzb.Par2Files {
		pf := fx.par2[i]
		if nzb.Par2Files[i].Name != pf.name {
			t.Fatalf("Par2Files[%d] is %s, want %s (fixture order changed)", i, nzb.Par2Files[i].Name, pf.name)
		}
		name := fmt.Sprintf("%08x%08x.par2", 0x5eed0000+i*7919, 0xc0ffee^(i*104729))
		nzb.Par2Files[i].Name = name
		// Re-post the articles under the obfuscated name, so the yEnc
		// headers agree with the NZB record.
		for part, id := range pf.ids {
			off := int64(part * p2eArticleSize)
			srv.addArticle(&p2eArticle{
				id:   id,
				data: pf.payloads[part],
				body: p2eYenc(name, int64(len(pf.data)), part+1, len(pf.ids), off, pf.payloads[part]),
			})
		}
	}
	if vols, idx := censusPar2Volumes(nzb.Par2Files); len(vols) != 0 || len(idx) != len(nzb.Par2Files) {
		t.Fatalf("census: %d volumes, %d index files; want every obfuscated file kept as an index file (fixture volumes are under the floor)", len(vols), len(idx))
	}

	h := newP2eHarnessNZB(t, srv, fx, nzb)
	h.recordDead(p2eSeg{f1, 1}, p2eSeg{f2, 3})
	for _, s := range h.dead {
		h.srv.set(s.file.ids[s.idx], p2eNotFound)
	}
	o := h.run(nil)
	h.report(o)
	p2eCompleted(2).check(t, h, o)
	if o.Server.Body == 0 {
		t.Errorf("no BODY served; the pass did not run against the server")
	}
}
