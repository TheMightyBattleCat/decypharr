package manager

// P2E harness, part 3: builds the smallest honest Manager/Par2Repair around
// a real usenet.Usenet (usenet.New, pkg/usenet/usenet.go:431) whose only
// provider is the fake server, a real NZB record (AddNZBForTest,
// pkg/usenet/testsupport.go:37), a real storage.Storage, and the real overlay
// store usenet.New creates. Dead segments are recorded through
// overlay.Store.Decide - the call the reader's padding path makes
// (pkg/usenet/fs/reader/fetcher.go:653-658) - on a second Store rooted at the
// same directory (manifests are re-read from disk on every call,
// overlay.go:541), before the pass starts.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

const p2eWarmupID = "p2e-warmup@p2e.test"

type p2eLogBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *p2eLogBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *p2eLogBuf) lines() []map[string]any {
	l.mu.Lock()
	data := append([]byte(nil), l.buf.Bytes()...)
	l.mu.Unlock()
	var out []map[string]any
	for _, ln := range bytes.Split(data, []byte("\n")) {
		if len(ln) == 0 {
			continue
		}
		m := map[string]any{}
		if json.Unmarshal(ln, &m) == nil {
			out = append(out, m)
		}
	}
	return out
}

type p2eHarness struct {
	t      *testing.T
	srv    *p2eServer
	fx     *p2eFixture
	p      *Par2Repair
	repair *Repair
	u      *usenet.Usenet
	nzbID  string
	logs   *p2eLogBuf
	dead   []p2eSeg
	notes  []string
	// afterWarmup runs once the warm-up fetch has succeeded, just before the pass.
	afterWarmup func()
}

type p2eSeg struct {
	file *p2eFile
	idx  int
}

func newP2eHarness(t *testing.T, srv *p2eServer, fx *p2eFixture, nzbID string) *p2eHarness {
	t.Helper()
	srv.reset()
	// Sets a fresh config path (the per-scenario data dir) and a fresh
	// storage.Storage; builds Repair{handlers} + Par2Repair{queues, progress}.
	p, repair := newTestPar2Repair(t)
	// regrabOnTerminal (par2_repair.go:1861) dereferences m.arr on every
	// URGENT terminal verdict; a nil *arr.Storage panics there and runJob's
	// recover then records a second (transient) outcome over the real one.
	p.manager.arr = arr.NewStorage()
	logs := &p2eLogBuf{}
	p.logger = zerolog.New(logs).Level(zerolog.DebugLevel).With().Timestamp().Logger()

	u, err := usenet.New()
	if err != nil {
		t.Fatalf("usenet.New: %v", err)
	}
	t.Cleanup(func() { _ = u.Close() })
	p.manager.usenet = u

	if err := u.AddNZBForTest(fx.nzb(nzbID)); err != nil {
		t.Fatalf("AddNZBForTest: %v", err)
	}
	if err := p.manager.storage.AddOrUpdate(&storage.Entry{
		InfoHash: nzbID, Name: p2eRelease, Protocol: config.ProtocolNZB, Category: "p2e",
	}); err != nil {
		t.Fatalf("store entry: %v", err)
	}
	return &p2eHarness{t: t, srv: srv, fx: fx, p: p, repair: repair, u: u, nzbID: nzbID, logs: logs}
}

// recordDead records a segment exactly as the reader's padding path does:
// overlay.Store.Decide(nzbID, file, segIdx, msgID, SegmentDataSize, TotalSize,
// SegmentCount) (fetcher.go:653-658; SegmentDataSize = NZBSegment.Bytes for a
// directly-posted file, reader/cache.go:366-378).
func (h *p2eHarness) recordDead(segs ...p2eSeg) {
	h.t.Helper()
	st, err := overlay.NewStore(filepath.Join(config.GetMainPath(), "usenet", "overlay"), zerolog.Nop())
	if err != nil {
		h.t.Fatalf("overlay.NewStore: %v", err)
	}
	rc := config.Get().Repair
	st.SetPolicy(overlay.Policy{MaxRunSegments: rc.PadMaxRunSegments, MaxTotalSegments: rc.PadMaxTotalSegments, MaxByteRatio: rc.PadMaxByteRatio})
	for _, s := range segs {
		dec, verdict := st.Decide(h.nzbID, s.file.name, s.idx, s.file.ids[s.idx], int64(len(s.file.payloads[s.idx])), int64(len(s.file.data)), len(s.file.ids))
		h.notes = append(h.notes, fmt.Sprintf("Decide(%s#%d)=decision %v verdict %v", short(s.file.name), s.idx, dec, verdict))
		h.dead = append(h.dead, s)
	}
}

func short(name string) string { return strings.TrimPrefix(name, p2eRelease+".") }

func (h *p2eHarness) warmup() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := h.u.FetchArticle(ctx, p2eWarmupID); err != nil {
		h.t.Fatalf("warm-up fetch through the real client failed: %v", err)
	}
}

// waitPhase polls the job's progress until it reports phase, or done closes.
func (h *p2eHarness) waitPhase(done <-chan struct{}, phase Par2JobPhase) bool {
	for {
		select {
		case <-done:
			return false
		default:
		}
		if pr, ok := h.p.Progress(h.nzbID); ok && pr.Phase == phase {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func (h *p2eHarness) jobCancel() bool {
	h.p.runningMu.Lock()
	defer h.p.runningMu.Unlock()
	if j, ok := h.p.running[h.nzbID]; ok && j.cancel != nil {
		j.cancel()
		return true
	}
	return false
}

// ---- goroutine accounting ----

var p2eGoroutineHdr = regexp.MustCompile(`^goroutine (\d+) \[([^\]]*)\]:`)

func p2eGoroutines() map[string]string {
	buf := make([]byte, 1<<22)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	out := map[string]string{}
	for _, block := range strings.Split(string(buf), "\n\n") {
		m := p2eGoroutineHdr.FindStringSubmatch(block)
		if m == nil {
			continue
		}
		out[m[1]] = block
	}
	return out
}

func p2eIsServerG(stack string) bool {
	return strings.Contains(stack, "(*p2eServer)")
}

// p2eTopFrame names the first non-runtime frame of a goroutine.
func p2eTopFrame(stack string) string {
	lines := strings.Split(stack, "\n")
	for i := 1; i < len(lines); i += 2 {
		fn := strings.TrimSpace(lines[i])
		if strings.HasPrefix(fn, "runtime.") || strings.HasPrefix(fn, "internal/") || strings.HasPrefix(fn, "sync.") ||
			strings.HasPrefix(fn, "time.") || strings.HasPrefix(fn, "net.") || strings.HasPrefix(fn, "bufio.") ||
			strings.HasPrefix(fn, "io.") || strings.HasPrefix(fn, "os.") || strings.HasPrefix(fn, "crypto/") || strings.HasPrefix(fn, "syscall.") {
			continue
		}
		if idx := strings.LastIndex(fn, "("); idx > 0 {
			fn = fn[:idx]
		}
		return fn
	}
	return strings.TrimSpace(lines[0])
}

// ---- outcome ----

type p2eOutcome struct {
	Name        string
	Wall        time.Duration
	Err         string // verbatim error from the "par2 repair unavailable" line (or "" on completion)
	Class       string // from the same line's class field (runJob's par2OutcomeClass result)
	LogMsg      string // "par2 repair completed" / "par2 repair unavailable" / other last line
	State       string
	Attempt     string
	Handler     string
	Phase       string
	ProgLastErr string
	Overlay     string
	OverlayDiff []string
	Mismatch    int
	Patched     int
	StillDead   int
	GDeltaRaw   int
	GDeltaNoSrv int
	GNew        []string
	Server      p2eServerCounts
	Tally       string
}

func (h *p2eHarness) run(during func(done <-chan struct{})) p2eOutcome {
	h.t.Helper()
	h.warmup()
	if h.afterWarmup != nil {
		h.afterWarmup()
	}
	time.Sleep(50 * time.Millisecond)
	before := p2eGoroutines()
	nBefore := runtime.NumGoroutine()
	h.srv.resetCounters()

	done := make(chan struct{})
	var hookWG sync.WaitGroup
	if during != nil {
		hookWG.Add(1)
		go func() { defer hookWG.Done(); during(done) }()
	}
	start := time.Now()
	h.p.runJob(h.nzbID, laneUrgent)
	wall := time.Since(start)
	close(done)
	hookWG.Wait()
	srvCounts := h.srv.counts()

	time.Sleep(time.Second)
	after := p2eGoroutines()
	nAfter := runtime.NumGoroutine()

	o := p2eOutcome{Name: h.t.Name(), Wall: wall, Server: srvCounts, GDeltaRaw: nAfter - nBefore}
	srvBefore, srvAfter := 0, 0
	for _, s := range before {
		if p2eIsServerG(s) {
			srvBefore++
		}
	}
	newTop := map[string]int{}
	for id, s := range after {
		if p2eIsServerG(s) {
			srvAfter++
			continue
		}
		if _, ok := before[id]; !ok {
			newTop[p2eTopFrame(s)]++
		}
	}
	o.GDeltaNoSrv = o.GDeltaRaw - (srvAfter - srvBefore)
	for k, v := range newTop {
		o.GNew = append(o.GNew, fmt.Sprintf("%dx %s", v, k))
	}
	sort.Strings(o.GNew)

	// Log-derived outcome (runJob's own summary line carries class + tally).
	for _, ln := range h.logs.lines() {
		msg, _ := ln["message"].(string)
		switch msg {
		case "par2 repair unavailable", "par2 repair completed":
			o.LogMsg = msg
			if e, ok := ln["error"].(string); ok {
				o.Err = e
			}
			if c, ok := ln["class"].(string); ok {
				o.Class = c
			}
			var parts []string
			for _, k := range []string{"recorded", "patched_recorded", "healed", "patched_discovered", "rounds", "stat_sweep", "slices_rebuilt", "usenet_bytes"} {
				if v, ok := ln[k]; ok {
					parts = append(parts, fmt.Sprintf("%s=%v", k, v))
				}
			}
			o.Tally = strings.Join(parts, " ")
		}
	}
	if o.LogMsg == "" {
		// runJob returned before runRepair (or via a path with no summary line).
		for _, ln := range h.logs.lines() {
			if lvl, _ := ln["level"].(string); lvl == "warn" || lvl == "error" || lvl == "info" {
				msg, _ := ln["message"].(string)
				o.LogMsg = msg
			}
		}
	}

	if st, err := h.p.manager.storage.GetPar2RepairState(h.nzbID); err == nil && st != nil {
		next := "-"
		if !st.NextRetryAt.IsZero() {
			next = time.Until(st.NextRetryAt).Round(time.Second).String()
		}
		o.State = fmt.Sprintf("attempts=%d suspect=%d terminal=%v next_retry_in=%s discovered=%d", st.AttemptCount, st.SuspectCount, st.Terminal, next, st.DeadSegmentsDiscovered)
		if st.Terminal {
			o.State += " reason=" + st.TerminalReason
		}
	} else {
		o.State = fmt.Sprintf("none (err=%v)", err)
	}
	if a, err := h.p.manager.storage.LatestPar2RepairAttemptForNzb(h.nzbID); err == nil && a != nil {
		o.Attempt = fmt.Sprintf("outcome=%s read=%d slices=%d patched=%d crc_canary=%v", a.Outcome, a.ReadBytes, a.SlicesRepaired, a.SegmentsPatched, a.CRCCanary)
		if o.Err == "" && a.FailReason != "" {
			o.Err = a.FailReason
		}
	} else {
		o.Attempt = fmt.Sprintf("none (err=%v)", err)
	}
	if kind, term, exists := h.repair.handlers.State(h.nzbID); exists {
		o.Handler = fmt.Sprintf("%v terminal=%v", kind, term)
	} else {
		o.Handler = "released"
	}
	if pr, ok := h.p.Progress(h.nzbID); ok {
		o.Phase = string(pr.Phase)
		o.ProgLastErr = pr.LastError
	}
	h.collectOverlay(&o)
	return o
}

func (h *p2eHarness) collectOverlay(o *p2eOutcome) {
	dir := filepath.Join(config.GetMainPath(), "usenet", "overlay", h.nzbID)
	man, err := h.u.OverlayManifest(h.nzbID)
	if err != nil || man == nil {
		_, statErr := os.Stat(dir)
		o.Overlay = fmt.Sprintf("no manifest (err=%v, dir_exists=%v)", err, statErr == nil)
		return
	}
	if len(man.Files) == 0 {
		_, statErr := os.Stat(dir)
		o.Overlay = fmt.Sprintf("empty manifest (dir_exists=%v)", statErr == nil)
		return
	}
	names := make([]string, 0, len(man.Files))
	for n := range man.Files {
		names = append(names, n)
	}
	sort.Strings(names)
	var segs []string
	for _, n := range names {
		fe := man.Files[n]
		f := h.fx.file(n)
		for _, d := range fe.DeadSegments {
			desc := fmt.Sprintf("%s#%d:%s", short(n), d.Index, d.Status)
			if d.Status == overlay.StatusPatched {
				o.Patched++
				data, ok := h.u.OverlayPatchBytes(h.nzbID, n, d.Index)
				switch {
				case !ok:
					desc += ":UNREADABLE"
					o.Mismatch++
				case f == nil || d.Index >= len(f.payloads):
					desc += ":NOORIG"
					o.Mismatch++
				case bytes.Equal(data, f.payloads[d.Index]):
					desc += ":eq"
				default:
					desc += fmt.Sprintf(":MISMATCH(len %d vs %d, first diff @%d)", len(data), len(f.payloads[d.Index]), firstDiff(data, f.payloads[d.Index]))
					o.Mismatch++
				}
			} else {
				o.StillDead++
			}
			segs = append(segs, desc)
		}
		segs = append(segs, fmt.Sprintf("[%s verdict=%s]", short(n), fe.Verdict))
	}
	o.OverlayDiff = segs
	o.Overlay = fmt.Sprintf("patched=%d (mismatch=%d) still_dead=%d", o.Patched, o.Mismatch, o.StillDead)
}

func firstDiff(a, b []byte) int {
	for i := 0; i < min(len(a), len(b)); i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}

func (h *p2eHarness) report(o p2eOutcome) {
	t := h.t
	t.Logf("==== %s", o.Name)
	for _, n := range h.notes {
		t.Logf("  note: %s", n)
	}
	t.Logf("  wall=%s log=%q class=%q", o.Wall.Round(time.Millisecond), o.LogMsg, o.Class)
	t.Logf("  err=%q", o.Err)
	t.Logf("  tally: %s", o.Tally)
	t.Logf("  state: %s", o.State)
	t.Logf("  attempt: %s", o.Attempt)
	t.Logf("  handler: %s  progress: phase=%s last_error=%q", o.Handler, o.Phase, o.ProgLastErr)
	t.Logf("  overlay: %s  %v", o.Overlay, o.OverlayDiff)
	t.Logf("  goroutines: delta_raw=%d delta_excl_server=%d new_non_server=%v", o.GDeltaRaw, o.GDeltaNoSrv, o.GNew)
	s := o.Server
	t.Logf("  server: BODY=%d (ok=%d 430=%d quota=%d stalled=%d wrong=%d trunc=%d short=%d) STAT=%d (223=%d 430=%d) DATE=%d other=%d%v conns=%d refused=%d bytes=%d",
		s.Body, s.BodyOK, s.Body430, s.BodyQuota, s.BodyStalled, s.BodyWrong, s.BodyTrunc, s.BodyShort,
		s.Stat, s.Stat223, s.Stat430, s.Date, s.Other, s.OtherCmds, s.Conns, s.Refused, s.Bytes)
	// Every warn/error line plus info lines from the repair, compactly.
	for _, ln := range h.logs.lines() {
		lvl, _ := ln["level"].(string)
		if lvl == "debug" && !p2eInterestingDebug(ln) {
			continue
		}
		msg, _ := ln["message"].(string)
		var extra []string
		for _, k := range []string{"error", "class", "reason", "duration", "file", "segment", "slices", "healed", "remaining", "newly_damaged", "confirmed_missing", "short_read", "corrupt", "checksum_bad", "volumes_fetched", "volumes_not_found", "volumes_errored", "slices_fetched", "needed", "early_k", "available", "damaged", "providers", "round", "articles", "gate", "message_id"} {
			if v, ok := ln[k]; ok {
				vs := fmt.Sprintf("%v", v)
				if len(vs) > 300 {
					vs = vs[:300] + "..."
				}
				extra = append(extra, k+"="+vs)
			}
		}
		ts, _ := ln["time"].(string)
		if len(ts) >= 19 {
			ts = ts[11:19]
		}
		t.Logf("    %s [%s] %s %s", ts, lvl, msg, strings.Join(extra, " "))
	}
	t.Logf("P2EROW|%s|%s|%s|%s|%s|%s|%s|%s|%d|%d|BODY=%d STAT=%d bytes=%d",
		strings.TrimPrefix(o.Name, "TestP2EPar2EndToEnd/"), o.Wall.Round(100*time.Millisecond), oneLine(o.Err), o.Class, o.State, o.Attempt, o.Overlay, o.Phase,
		o.GDeltaRaw, o.GDeltaNoSrv, s.Body, s.Stat, s.Bytes)
}

func p2eInterestingDebug(ln map[string]any) bool {
	msg, _ := ln["message"].(string)
	return strings.Contains(msg, "panic") || strings.Contains(msg, "different upload") || strings.Contains(msg, "index file fetch failed")
}

func oneLine(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > 400 {
		s = s[:400] + "..."
	}
	return s
}

// newP2eEnv starts n fake servers holding every fixture article, builds the
// release fixture with par2cmdline and configures one provider per server,
// in priority order. Providers are told apart by host, so the first is
// 127.0.0.1 and the rest are "localhost" (n is at most 2). It skips the test
// when par2cmdline is not installed.
func newP2eEnv(t *testing.T, n int) ([]*p2eServer, *p2eFixture) {
	t.Helper()
	par2Bin, err := exec.LookPath("par2")
	if err != nil {
		t.Skip("par2 (par2cmdline) not installed")
	}
	// Production starts the cached clock at boot; nntp deadlines are built
	// from utils.Now(), which is frozen at package init without this. The
	// first refresh comes one tick after Start: wait for it, or in a full
	// package run the clock is still at init time and every connection
	// deadline is already past.
	utils.StartGlobalCachedTime()
	for deadline := time.Now().Add(5 * time.Second); time.Since(utils.Now()) > time.Second; {
		if time.Now().After(deadline) {
			t.Fatalf("cached clock did not catch up: utils.Now() is %s behind", time.Since(utils.Now()))
		}
		time.Sleep(50 * time.Millisecond)
	}

	fx, err := buildP2eFixture(t.TempDir(), par2Bin)
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	warm := []byte("p2e warm-up payload")
	warmArticle := &p2eArticle{id: p2eWarmupID, data: warm, body: p2eYenc("warmup.bin", int64(len(warm)), 1, 1, 0, warm)}

	if n < 1 || n > 2 {
		t.Fatalf("newP2eEnv: %d servers, want 1 or 2", n)
	}
	servers := make([]*p2eServer, n)
	providers := make([]config.UsenetProvider, n)
	for i := range servers {
		srv, err := newP2eServer()
		if err != nil {
			t.Fatalf("fake server: %v", err)
		}
		t.Cleanup(srv.close)
		for _, a := range fx.all {
			srv.addArticle(a)
		}
		srv.addArticle(warmArticle)
		servers[i] = srv
		host := "127.0.0.1"
		if i > 0 {
			host = "localhost"
		}
		providers[i] = config.UsenetProvider{Host: host, Port: srv.port(), MaxConnections: 8, Priority: i + 1}
	}

	config.SetConfigPath(t.TempDir())
	cfg := config.Get()
	oldUsenet := cfg.Usenet
	cfg.Usenet = config.Usenet{
		Providers:                providers,
		MaxConnections:           8,
		ProcessingMaxConnections: 8,
		DiskBufferPath:           t.TempDir(),
	}
	t.Cleanup(func() { cfg.Usenet = oldUsenet })
	return servers, fx
}
