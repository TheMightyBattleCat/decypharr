package manager

// P2E memory measurement (FIXES row 71, REVIEW.md §11.4 Q14): whole URGENT
// passes over a release sized so the accumulator (k damaged slices x slice
// size) is tens of MiB, alone and as many at once as the lanes allow, with
// the live heap sampled throughout. K139 puts a pass's real peak at about
// 3-4x its accumulator (the fetched recovery volumes, Repair's working copy,
// the output); the test fails above 4x (plus the whole recovery set when a
// pass has to fetch every volume for missing slice checksums, row 41).
//
// It allocates hundreds of MiB and runs par2cmdline, so it only runs with
// P2E_MEMORY=1:
//
//	P2E_MEMORY=1 go test ./pkg/manager/ -run TestP2EPar2PeakMemory -v -timeout 30m

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

const (
	p2eMemSlice      = 1 << 20   // PAR2 slice size
	p2eMemArticle    = 256 << 10 // article payload; 4 per slice
	p2eMemFileMiB    = 40        // per posted file; 3 files
	p2eMemRedundancy = 80        // percent: 96 recovery slices for 120
	p2eMemVolumes    = 12        // uniform volumes of 8 slices
	p2eMemDeadStride = 3         // one dead article every 3rd slice from slice 1: 13 per file
	// p2eMemPasses is every lane that can run a pass at once: the batch
	// loop (1), the urgent workers (par2DefaultUrgentConcurrency), the cold
	// sweep's slot (1), the warm sweep (1) and RunNow (par2ManualSlots).
	p2eMemPasses = 1 + par2DefaultUrgentConcurrency + 1 + 1 + par2ManualSlots
	// p2eMemBound is K139's upper multiplier.
	p2eMemBound = 4.0
)

// p2eMemCut is p2eFile.cut at p2eMemArticle, with no short-body variant
// (the memory test never serves one, and it would sit in the heap).
func (f *p2eFile) p2eMemCut(all map[string]*p2eArticle) {
	n := (len(f.data) + p2eMemArticle - 1) / p2eMemArticle
	for i := 0; i < n; i++ {
		off := i * p2eMemArticle
		end := min(off+p2eMemArticle, len(f.data))
		p := f.data[off:end]
		id := p2eMsgID(f.name, i+1)
		all[id] = &p2eArticle{id: id, data: p, body: p2eYenc(f.name, int64(len(f.data)), i+1, n, int64(off), p)}
		f.ids = append(f.ids, id)
		f.payloads = append(f.payloads, p)
	}
}

// buildP2eMemFixture writes three p2eMemFileMiB files and a uniform PAR2 set
// over them into dir, and cuts everything into p2eMemArticle articles.
func buildP2eMemFixture(t *testing.T, dir, par2Bin string) *p2eFixture {
	t.Helper()
	rng := rand.New(rand.NewSource(20261118))
	fx := &p2eFixture{dir: dir, all: map[string]*p2eArticle{}}
	args := []string{"create", "-q", fmt.Sprintf("-s%d", p2eMemSlice), fmt.Sprintf("-r%d", p2eMemRedundancy),
		fmt.Sprintf("-n%d", p2eMemVolumes), "-u", p2eRelease + ".par2"}
	for i := 1; i <= 3; i++ {
		name := fmt.Sprintf("%s.part%d.mkv", p2eRelease, i)
		d := make([]byte, p2eMemFileMiB<<20)
		rng.Read(d)
		if err := os.WriteFile(filepath.Join(dir, name), d, 0o644); err != nil {
			t.Fatal(err)
		}
		fx.posted = append(fx.posted, &p2eFile{name: name, data: d})
		args = append(args, name)
	}
	cmd := exec.Command(par2Bin, args...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("par2 create: %v: %s", err, out)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
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
			t.Fatal(err)
		}
		fx.par2 = append(fx.par2, &p2eFile{name: n, data: d, isPar2: true})
	}
	for _, f := range append(append([]*p2eFile{}, fx.posted...), fx.par2...) {
		f.p2eMemCut(fx.all)
	}
	return fx
}

// p2eMemRelabel returns a copy of fx whose articles carry message IDs
// prefixed with tag (same payloads and bodies, shared in memory), so N
// NZBs of one release share nothing on the client side.
func p2eMemRelabel(fx *p2eFixture, tag string) *p2eFixture {
	out := &p2eFixture{dir: fx.dir, all: map[string]*p2eArticle{}}
	cp := func(f *p2eFile) *p2eFile {
		nf := &p2eFile{name: f.name, data: f.data, isPar2: f.isPar2, payloads: f.payloads}
		for _, id := range f.ids {
			a := *fx.all[id]
			a.id = tag + "-" + id
			out.all[a.id] = &a
			nf.ids = append(nf.ids, a.id)
		}
		return nf
	}
	for _, f := range fx.posted {
		out.posted = append(out.posted, cp(f))
	}
	for _, f := range fx.par2 {
		out.par2 = append(out.par2, cp(f))
	}
	return out
}

// p2eMemCorruptIFSC is p2eCorruptIFSC without the re-cut (the bodies are
// rebuilt by the caller): it damages every IFSC packet of fileID in f.data.
func p2eMemCorruptIFSC(t *testing.T, f *p2eFile, fileID []byte) []byte {
	t.Helper()
	n := 0
	for off := 0; ; {
		i := bytes.Index(f.data[off:], p2eIFSCType)
		if i < 0 {
			break
		}
		body := off + i + 16
		if fileID == nil {
			fileID = append([]byte(nil), f.data[body:body+16]...)
		}
		if bytes.Equal(f.data[body:body+16], fileID) {
			f.data[body+16] ^= 0xff
			n++
		}
		off = body
	}
	if n == 0 {
		t.Fatalf("no IFSC packet for %x in %s", fileID, f.name)
	}
	return fileID
}

// p2eMemSampler records the peak of the live heap (as of the last GC) and
// of the bytes in live-or-unswept heap objects, sampled every 2 ms.
type p2eMemSampler struct {
	stop     chan struct{}
	done     chan struct{}
	peakLive uint64
	peakObj  uint64
}

func p2eMemRead() (live, obj uint64) {
	s := []metrics.Sample{{Name: "/gc/heap/live:bytes"}, {Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(s)
	return s[0].Value.Uint64(), s[1].Value.Uint64()
}

func startP2eMemSampler() *p2eMemSampler {
	s := &p2eMemSampler{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(s.done)
		tk := time.NewTicker(2 * time.Millisecond)
		defer tk.Stop()
		for {
			live, obj := p2eMemRead()
			s.peakLive = max(s.peakLive, live)
			s.peakObj = max(s.peakObj, obj)
			select {
			case <-s.stop:
				return
			case <-tk.C:
			}
		}
	}()
	return s
}

func (s *p2eMemSampler) finish() { close(s.stop); <-s.done }

type p2eMemCase struct {
	name string
	// missingIFSC: every copy of one file's slice checksums is unreadable
	// except in the last volume, so the pass fetches every volume of the
	// set before the stream (row 41) and holds them all.
	missingIFSC bool
	passes      int
}

func TestP2EPar2PeakMemory(t *testing.T) {
	if os.Getenv("P2E_MEMORY") == "" {
		t.Skip("set P2E_MEMORY=1 to run (hundreds of MiB, par2cmdline)")
	}
	par2Bin, err := exec.LookPath("par2")
	if err != nil {
		t.Skip("par2 (par2cmdline) not installed")
	}
	for _, c := range []p2eMemCase{
		{name: "one_pass", passes: 1},
		{name: "every_lane", passes: p2eMemPasses},
		{name: "missing_checksums_one_pass", missingIFSC: true, passes: 1},
		{name: "missing_checksums_every_lane", missingIFSC: true, passes: p2eMemPasses},
	} {
		t.Run(c.name, func(t *testing.T) { runP2eMemCase(t, par2Bin, c) })
	}
}

func runP2eMemCase(t *testing.T, par2Bin string, c p2eMemCase) {
	servers, _ := newP2eEnv(t, 1)
	srv := servers[0]
	base := buildP2eMemFixture(t, t.TempDir(), par2Bin)
	recoverySet := int64(0)
	for _, f := range base.par2[1:] {
		recoverySet += int64(len(f.data))
	}
	if c.missingIFSC {
		var id []byte
		for _, f := range base.par2[:len(base.par2)-1] {
			id = p2eMemCorruptIFSC(t, f, id)
		}
		for _, f := range base.par2 {
			f.ids, f.payloads = nil, nil
			f.p2eMemCut(base.all)
		}
	}

	cfg := config.Get()
	oldRepair := cfg.Repair
	cfg.Repair.PadMaxRunSegments = 16
	cfg.Repair.PadMaxTotalSegments = 512
	cfg.Repair.PadMaxByteRatio = 0.10
	t.Cleanup(func() { cfg.Repair = oldRepair })

	fxs := make([]*p2eFixture, c.passes)
	ids := make([]string, c.passes)
	for i := range fxs {
		fxs[i] = p2eMemRelabel(base, fmt.Sprintf("n%d", i))
		ids[i] = fmt.Sprintf("p2e-mem-%d", i)
		for _, a := range fxs[i].all {
			srv.addArticle(a)
		}
	}
	h := newP2eHarnessNZB(t, srv, fxs[0], fxs[0].nzb(ids[0]))
	st, err := overlay.NewStore(filepath.Join(config.GetMainPath(), "usenet", "overlay"), zerolog.Nop())
	if err != nil {
		t.Fatalf("overlay.NewStore: %v", err)
	}
	rc := config.Get().Repair
	st.SetPolicy(overlay.Policy{MaxRunSegments: rc.PadMaxRunSegments, MaxTotalSegments: rc.PadMaxTotalSegments, MaxByteRatio: rc.PadMaxByteRatio})

	k := 0
	for i, fx := range fxs {
		if i > 0 {
			nzb := fx.nzb(ids[i])
			nzb.Name = fmt.Sprintf("%s.n%d", p2eRelease, i)
			if err := h.u.AddNZBForTest(nzb); err != nil {
				t.Fatalf("AddNZBForTest: %v", err)
			}
			if err := h.p.manager.storage.AddOrUpdate(&storage.Entry{
				InfoHash: ids[i], Name: nzb.Name, Protocol: config.ProtocolNZB, Category: "p2e",
			}); err != nil {
				t.Fatalf("store entry: %v", err)
			}
		}
		k = 0
		for _, f := range fx.posted {
			per := p2eMemSlice / p2eMemArticle
			for s := 1; s*per < len(f.ids); s += p2eMemDeadStride { // slice 0 is the header region
				a := s * per
				if _, verdict := st.Decide(ids[i], f.name, a, f.ids[a], int64(len(f.payloads[a])), int64(len(f.data)), len(f.ids)); verdict != overlay.VerdictDegraded {
					t.Fatalf("Decide(%s#%d) verdict %v, want degraded", f.name, a, verdict)
				}
				srv.set(f.ids[a], p2eNotFound)
				k++
			}
		}
	}
	acc := int64(k) * p2eMemSlice
	h.warmup()

	old := debug.SetGCPercent(10)
	defer debug.SetGCPercent(old)
	runtime.GC()
	runtime.GC()
	baseLive, baseObj := p2eMemRead()
	smp := startP2eMemSampler()
	start := time.Now()
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func() { defer wg.Done(); h.p.runJob(id, laneUrgent) }()
	}
	wg.Wait()
	wall := time.Since(start)
	smp.finish()

	for i, id := range ids {
		a, err := h.p.manager.storage.LatestPar2RepairAttemptForNzb(id)
		if err != nil || a == nil {
			t.Fatalf("pass %d: no attempt record: %v", i, err)
		}
		if a.Outcome != storage.Par2RepairOutcomeCompleted || a.SegmentsPatched != k {
			t.Fatalf("pass %d: outcome %s, %d patched (reason %q), want completed with %d", i, a.Outcome, a.SegmentsPatched, a.FailReason, k)
		}
	}
	for i, fx := range fxs {
		man, err := h.u.OverlayManifest(ids[i])
		if err != nil || man == nil {
			t.Fatalf("pass %d: manifest: %v", i, err)
		}
		for name, fe := range man.Files {
			f := fx.file(name)
			for _, d := range fe.DeadSegments {
				data, ok := h.u.OverlayPatchBytes(ids[i], name, d.Index)
				if d.Status != overlay.StatusPatched || !ok || !bytes.Equal(data, f.payloads[d.Index]) {
					t.Fatalf("pass %d: %s#%d status %v, patch readable %v, not equal to the posted bytes", i, short(name), d.Index, d.Status, ok)
				}
			}
		}
	}

	accAll := float64(int64(c.passes) * acc)
	liveDelta := float64(smp.peakLive) - float64(baseLive)
	objDelta := float64(smp.peakObj) - float64(baseObj)
	ratio := liveDelta / accAll
	t.Logf("P2EMEM|%s|passes=%d release=%dMiB slice=%dKiB k=%d acc=%dMiB/pass recovery_set=%dMiB GOGC=10 wall=%s base_live=%dMiB peak_live=%dMiB (+%.0fMiB) base_obj=%dMiB peak_obj=%dMiB (+%.0fMiB) ratio_live=%.2f ratio_obj=%.2f",
		c.name, c.passes, 3*p2eMemFileMiB, p2eMemSlice>>10, k, acc>>20, recoverySet>>20, wall.Round(100*time.Millisecond),
		baseLive>>20, smp.peakLive>>20, liveDelta/(1<<20), baseObj>>20, smp.peakObj>>20, objDelta/(1<<20), ratio, objDelta/accAll)
	// K139's bound per pass. A pass missing slice checksums may also hold
	// every recovery volume of the set (row 41's documented cost), which
	// K139's "fetched volumes" term (the volumes covering k, plus a spare)
	// does not cover: it is allowed the whole recovery set on top.
	bound := p2eMemBound * accAll
	what := fmt.Sprintf("K139's %.0fx accumulator", p2eMemBound)
	if c.missingIFSC {
		bound += float64(int64(c.passes) * recoverySet)
		what += " + the whole recovery set"
	}
	if liveDelta > bound {
		t.Errorf("peak live heap +%.0f MiB for %d pass(es) of %d MiB accumulator (%.2fx): over %s (%.0f MiB)", liveDelta/(1<<20), c.passes, acc>>20, ratio, what, bound/(1<<20))
	}
}
