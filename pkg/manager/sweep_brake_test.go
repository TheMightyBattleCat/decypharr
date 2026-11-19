package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/notifications"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// brakeCallbacks counts the repair notifications a callback URL receives.
type brakeCallbacks struct {
	mu   sync.Mutex
	msgs []notifications.CallbackPayload
}

func (b *brakeCallbacks) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.msgs)
}

// newTestRepairForBrake is newTestRepairForClaims plus notifications sent to a
// local callback URL, and one sweep worker so entries finish in names order.
func newTestRepairForBrake(t *testing.T) (*Repair, *brakeCallbacks) {
	t.Helper()
	repair := newTestRepairForClaims(t)

	saved := config.Get().Repair
	config.Get().Repair.Workers = 1
	t.Cleanup(func() { config.Get().Repair = saved })

	got := &brakeCallbacks{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p notifications.CallbackPayload
		_ = json.NewDecoder(r.Body).Decode(&p)
		got.mu.Lock()
		got.msgs = append(got.msgs, p)
		got.mu.Unlock()
	}))
	t.Cleanup(srv.Close)
	repair.manager.Notifications = notifications.New(&config.Notifications{Enabled: true, CallbackURL: srv.URL}, repair.logger)
	return repair, got
}

// brakeScript is one entry of a fake sweep: the result its single file probes to.
type brakeScript struct {
	name string
	res  fileResult
}

var (
	brakeReadFailed = fileResult{protocol: config.ProtocolNZB, healthy: true, readFailed: true, unverifiedReason: unverifiedInconclusive}
	brakeHealthy    = fileResult{protocol: config.ProtocolNZB, healthy: true, decodeConclusive: true}
	brake430        = fileResult{protocol: config.ProtocolNZB, broken: true, reason: "usenet_segment_missing"}
	brakeUnreadable = fileResult{protocol: config.ProtocolNZB, broken: true, reason: ffprobeReasonUnreadable + ": Invalid data found when processing input"}
	brakeDecodeBad  = fileResult{protocol: config.ProtocolNZB, broken: true, reason: "ffprobe_decode_errors: corrupt frame"}
)

// runBrakeSweep runs probeAndHealCandidates, auto-repair on, over script with
// a fake file probe, the way executeSweep does (brake on the context).
func runBrakeSweep(t *testing.T, repair *Repair, script []brakeScript) *storage.RepairRun {
	t.Helper()
	byFile := make(map[string]fileResult, len(script))
	candidates := make(map[string]*candidate, len(script))
	names := make([]string, 0, len(script))
	for i, s := range script {
		file := s.name + ".mkv"
		res := s.res
		res.name = file
		res.infoHash = fmt.Sprintf("nzb-%02d", i)
		byFile[file] = res
		candidates[s.name] = &candidate{name: s.name, item: &storage.EntryItem{
			Name:  s.name,
			Files: map[string]*storage.File{file: {Name: file, InfoHash: res.infoHash}},
		}}
		names = append(names, s.name)
	}
	repair.probeFileFn = func(_ context.Context, _ *candidate, name string, _ RepairRunOptions, _ bool) fileResult {
		return byFile[name]
	}

	run := &storage.RepairRun{ID: "brake-run"}
	ctx := repair.withSweepBrake(context.Background(), run)
	if err := repair.probeAndHealCandidates(ctx, run, candidates, names, newHealCache(), RepairRunOptions{}, true, &supersessionContext{}); err != nil {
		t.Fatalf("probeAndHealCandidates: %v", err)
	}
	return run
}

// waitCallbacks waits up to a few seconds for want notifications (Notify sends
// on its own goroutine), then a moment more for any extra one.
func waitCallbacks(got *brakeCallbacks, want int) int {
	deadline := time.Now().Add(5 * time.Second)
	for want > 0 && got.count() < want && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)
	return got.count()
}

// Five files in a row whose read failed stop the sweep's healing: a broken
// entry after the fifth is recorded broken but not deleted or re-searched,
// even after healthy files, and one repair notification says why. A broken
// entry before the fifth is healed as before.
func TestSweepBrakeStopsHealingAfterFiveFailedReads(t *testing.T) {
	repair, got := newTestRepairForBrake(t)

	script := []brakeScript{
		{"before-broken", brakeDecodeBad},
		{"read-1", brakeReadFailed},
		{"read-2", brakeReadFailed},
		{"unreadable-3", brakeUnreadable}, // counts; broken, so healed (brake not yet tripped)
		{"read-4", brakeReadFailed},
		{"unreadable-5", brakeUnreadable}, // the fifth: trips, and its own entry is not healed
		{"after-broken", brakeDecodeBad},
		{"after-healthy", brakeHealthy},
		{"after-430", brake430},
		{"after-broken-2", brakeDecodeBad},
	}
	run := runBrakeSweep(t, repair, script)

	wantHealed := map[string]bool{"before-broken": true, "unreadable-3": true}
	for _, s := range script {
		if !s.res.broken {
			continue
		}
		if run.WasHealed(s.name) != wantHealed[s.name] {
			t.Errorf("%s: healed=%v, want %v", s.name, run.WasHealed(s.name), wantHealed[s.name])
		}
		h, _ := repair.manager.storage.GetEntryHealth(s.name)
		if h == nil || h.Status != storage.HealthBroken {
			t.Errorf("%s: health %+v, want recorded broken", s.name, h)
		}
	}
	if run.Stats.Probed != len(script) {
		t.Errorf("probed %d entries, want %d: probing goes on after the brake", run.Stats.Probed, len(script))
	}

	if n := waitCallbacks(got, 1); n != 1 {
		t.Fatalf("got %d notifications, want 1", n)
	}
	msg := got.msgs[0]
	if msg.Event != string(config.EventRepairFailed) || msg.Status != "warning" {
		t.Errorf("notification event=%q status=%q, want %q warning", msg.Event, msg.Status, config.EventRepairFailed)
	}
	for _, want := range []string{"brake-run", "5 files in a row could not be read", "unreadable-5.mkv"} {
		if !strings.Contains(msg.Message, want) {
			t.Errorf("notification message %q does not name %q", msg.Message, want)
		}
	}
}

// Failed reads broken up by healthy files or articles a server reported
// missing never trip the brake, and torrent results are not counted: every
// broken entry is healed and nothing is sent.
func TestSweepBrakeMixedResultsNeverTrip(t *testing.T) {
	repair, got := newTestRepairForBrake(t)

	torrentHealthy := fileResult{protocol: config.ProtocolTorrent, healthy: true}
	var script []brakeScript
	add := func(prefix string, n int, res fileResult) {
		for i := range n {
			script = append(script, brakeScript{fmt.Sprintf("%s-%d-%d", prefix, len(script), i), res})
		}
	}
	for range 3 {
		add("read", 4, brakeReadFailed)
		add("healthy", 1, brakeHealthy)
		add("read", 3, brakeReadFailed)
		add("decode", 1, brakeDecodeBad) // neutral: neither counts nor resets
		add("430", 1, brake430)
		add("read", 2, brakeReadFailed)
		add("torrent", 1, torrentHealthy) // neutral: no reset
		add("unreadable", 1, brakeUnreadable)
		add("430", 1, brake430)
	}
	run := runBrakeSweep(t, repair, script)

	for _, s := range script {
		if s.res.broken && !run.WasHealed(s.name) {
			t.Errorf("%s: broken entry not healed; the brake tripped on mixed results", s.name)
		}
	}
	if n := waitCallbacks(got, 0); n != 0 {
		t.Fatalf("got %d notifications, want none", n)
	}
}

func TestSweepBrakeSignal(t *testing.T) {
	cases := []struct {
		name string
		res  fileResult
		want brakeSignal
	}{
		// The metadata read failed twice on a file decode-verified earlier:
		// healthy, no unverified reason, but it must count, not reset.
		{"verified earlier, read failed", fileResult{protocol: config.ProtocolNZB, healthy: true, readFailed: true}, brakeCount},
		{"decode check, read failed", brakeReadFailed, brakeCount},
		{"unreadable container", brakeUnreadable, brakeCount},
		{"healthy", brakeHealthy, brakeReset},
		{"healthy, decode timed out", fileResult{protocol: config.ProtocolNZB, healthy: true, unverifiedReason: unverifiedTimeout}, brakeReset},
		{"430 at STAT", brake430, brakeReset},
		{"430 deferred", fileResult{protocol: config.ProtocolNZB, reason: "usenet_segment_missing_deferred"}, brakeReset},
		{"430 during ffprobe", fileResult{protocol: config.ProtocolNZB, broken: true, reason: ffprobeReasonDeadSegment}, brakeReset},
		{"430 seen by sweep", fileResult{protocol: config.ProtocolNZB, broken: true, reason: "sweep_dead_segment_ffprobe_override"}, brakeReset},
		{"decode errors", brakeDecodeBad, brakeNeutral},
		{"torrent healthy", fileResult{protocol: config.ProtocolTorrent, healthy: true}, brakeNeutral},
		{"torrent broken", fileResult{protocol: config.ProtocolTorrent, broken: true, reason: ffprobeReasonUnreadable}, brakeNeutral},
		{"skipped", fileResult{reason: "entry_not_found"}, brakeNeutral},
	}
	for _, tc := range cases {
		if got := sweepBrakeSignal(tc.res); got != tc.want {
			t.Errorf("%s: signal %d, want %d", tc.name, got, tc.want)
		}
	}
}

// A sweep without a brake (RecheckEntry, the import gate) is unaffected, and
// a tripped brake turns autoRepairStillOn off for the rest of the sweep.
func TestSweepBrakeGatesAutoRepairStillOn(t *testing.T) {
	repair := newTestRepairForClaims(t)
	if !repair.autoRepairStillOn(context.Background(), true) {
		t.Fatal("no brake on the context: auto-repair should stay on")
	}
	ctx := repair.withSweepBrake(context.Background(), &storage.RepairRun{ID: "r"})
	b := sweepBrakeFrom(ctx)
	for range sweepBrakeLimit - 1 {
		b.observe([]fileResult{brakeReadFailed})
	}
	if !repair.autoRepairStillOn(ctx, true) {
		t.Fatal("four failed reads tripped the brake")
	}
	b.observe([]fileResult{brakeReadFailed})
	if repair.autoRepairStillOn(ctx, true) {
		t.Fatal("five failed reads in a row left auto-repair on")
	}
	b.observe([]fileResult{brakeHealthy, brakeHealthy})
	if repair.autoRepairStillOn(ctx, true) {
		t.Fatal("healthy files after the trip released the brake")
	}
}

// One pack whose every file ffprobe cannot open (a fake or wrong-password
// release) counts once: it does not trip the brake alone, and the pack is
// healed. Five different entries with one such file each still trip it, and
// failed reads within one entry still count per file.
func TestSweepBrakeOneUnreadablePackCountsOnce(t *testing.T) {
	pack := func(n int, res fileResult) []fileResult {
		out := make([]fileResult, n)
		for i := range out {
			out[i] = res
			out[i].name = fmt.Sprintf("E%02d.mkv", i+1)
		}
		return out
	}

	b := &sweepBrake{}
	b.observe(pack(sweepBrakeLimit, brakeUnreadable))
	if b.holding() || b.inARow != 1 {
		t.Fatalf("one pack of %d unreadable files: tripped=%v count=%d, want not tripped, count 1", sweepBrakeLimit, b.holding(), b.inARow)
	}

	b = &sweepBrake{}
	for range sweepBrakeLimit {
		b.observe(pack(1, brakeUnreadable))
	}
	if !b.holding() {
		t.Fatalf("%d entries with one unreadable file each did not trip the brake", sweepBrakeLimit)
	}

	b = &sweepBrake{}
	b.observe(pack(sweepBrakeLimit, brakeReadFailed))
	if !b.holding() {
		t.Fatalf("one entry with %d failed reads did not trip the brake", sweepBrakeLimit)
	}

	tagged := fileResult{protocol: config.ProtocolNZB, broken: true, reason: ffprobeReasonUnreadable + ffprobeReadFailedTag + ": short read"}
	b = &sweepBrake{}
	b.observe(pack(sweepBrakeLimit, tagged))
	if !b.holding() {
		t.Fatalf("one entry with %d read-failed unreadable files did not trip the brake", sweepBrakeLimit)
	}

	// Through the sweep: the pack is healed and no notification is sent.
	repair, got := newTestRepairForBrake(t)
	const entry = "garbage-pack"
	files := make(map[string]*storage.File)
	byFile := make(map[string]fileResult)
	for _, res := range pack(sweepBrakeLimit, brakeUnreadable) {
		res.infoHash = "nzb-pack"
		files[res.name] = &storage.File{Name: res.name, InfoHash: res.infoHash}
		byFile[res.name] = res
	}
	repair.probeFileFn = func(_ context.Context, _ *candidate, name string, _ RepairRunOptions, _ bool) fileResult {
		return byFile[name]
	}
	candidates := map[string]*candidate{entry: {name: entry, item: &storage.EntryItem{Name: entry, Files: files}}}
	run := &storage.RepairRun{ID: "brake-pack"}
	ctx := repair.withSweepBrake(context.Background(), run)
	if err := repair.probeAndHealCandidates(ctx, run, candidates, []string{entry}, newHealCache(), RepairRunOptions{}, true, &supersessionContext{}); err != nil {
		t.Fatalf("probeAndHealCandidates: %v", err)
	}
	if sweepBrakeFrom(ctx).holding() {
		t.Error("the sweep's brake tripped on one unreadable pack")
	}
	if !run.WasHealed(entry) {
		t.Error("the unreadable pack was not healed")
	}
	if n := waitCallbacks(got, 0); n != 0 {
		t.Errorf("got %d notifications, want none", n)
	}
}
