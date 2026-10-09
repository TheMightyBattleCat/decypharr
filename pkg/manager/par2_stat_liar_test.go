package manager

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

// statProvider is one stand-in provider's STAT view: the articles it reports
// missing. Everything else it reports present, true or not.
type statProvider struct {
	host    string
	missing map[string]bool
}

// providerStat answers a batch STAT the way the NNTP client does across
// providers: the first provider still asked that reports an article present
// settles it and is named; an article every asked provider reports missing
// is not found; with nobody left to ask it could not be verified. calls
// counts the IDs asked about.
func providerStat(providers []statProvider, calls *int) func(context.Context, []string) ([]nntp.StatResult, error) {
	return func(ctx context.Context, ids []string) ([]nntp.StatResult, error) {
		if calls != nil {
			*calls += len(ids)
		}
		out := make([]nntp.StatResult, len(ids))
		for i, id := range ids {
			out[i] = nntp.StatResult{MessageID: id}
			asked := 0
			for _, p := range providers {
				if nntp.StatHostExcluded(ctx, p.host) {
					continue
				}
				asked++
				if !p.missing[id] {
					out[i].Available, out[i].Host = true, p.host
					break
				}
			}
			switch {
			case out[i].Available:
			case asked == 0:
				out[i].Error = par2TransportErr()
			default:
				out[i].Error = par2NotFoundErr()
			}
		}
		return out, nil
	}
}

func statLiarFixture() ([]storage.PostedFileRef, []par2.Match) {
	src := []storage.PostedFileRef{
		{Name: "a.rar", Segments: []storage.Par2SegmentRef{{MessageID: "a0"}, {MessageID: "a1"}, {MessageID: "a2"}}},
		{Name: "b.rar", Segments: []storage.Par2SegmentRef{{MessageID: "b0"}, {MessageID: "b1"}}},
	}
	return src, []par2.Match{{PostedIndex: 0}, {PostedIndex: 1}}
}

// One provider reporting a known-dead article present no longer switches the
// damage sweep off: it is left out, and the sweep runs on the others.
func TestStatPostedFileDamageLeavesOutALyingProvider(t *testing.T) {
	src, matches := statLiarFixture()
	dead := map[string]bool{"a0": true, "b1": true}
	stat := providerStat([]statProvider{
		{host: "liar", missing: nil},
		{host: "honest-1", missing: dead},
		{host: "honest-2", missing: dead},
	}, nil)

	got, ok, excluded := statPostedFileDamage(context.Background(), parallelFetchNopLogger, stat, matches, src, []string{"a0"}, "e", nil)

	sort.Strings(got)
	if !ok || fmt.Sprint(got) != "[a0 b1]" {
		t.Fatalf("got ok=%v missing=%v, want the sweep to run and report [a0 b1]", ok, got)
	}
	if fmt.Sprint(excluded) != "[liar]" {
		t.Fatalf("excluded = %v, want [liar]", excluded)
	}
}

// Two providers lying are both left out, one probe at a time.
func TestStatPostedFileDamageLeavesOutEveryLyingProvider(t *testing.T) {
	src, matches := statLiarFixture()
	dead := map[string]bool{"a0": true}
	stat := providerStat([]statProvider{
		{host: "liar-1"}, {host: "liar-2"}, {host: "honest", missing: dead},
	}, nil)

	got, ok, excluded := statPostedFileDamage(context.Background(), parallelFetchNopLogger, stat, matches, src, []string{"a0"}, "e", nil)

	if !ok || fmt.Sprint(got) != "[a0]" {
		t.Fatalf("got ok=%v missing=%v, want [a0]", ok, got)
	}
	if fmt.Sprint(excluded) != "[liar-1 liar-2]" {
		t.Fatalf("excluded = %v, want both liars", excluded)
	}
}

// When every provider reports the known-dead article present there is no one
// left whose STAT can be believed: the sweep is skipped, as before, and only
// the controls were ever asked about.
func TestStatPostedFileDamageSkipsWhenEveryProviderLies(t *testing.T) {
	src, matches := statLiarFixture()
	calls := 0
	stat := providerStat([]statProvider{{host: "liar-1"}, {host: "liar-2"}}, &calls)

	got, ok, excluded := statPostedFileDamage(context.Background(), parallelFetchNopLogger, stat, matches, src, []string{"a0"}, "e", nil)

	if ok || got != nil {
		t.Fatalf("got ok=%v missing=%v, want the sweep skipped", ok, got)
	}
	if len(excluded) != 2 {
		t.Fatalf("excluded = %v, want both providers", excluded)
	}
	if calls != 3 {
		t.Fatalf("asked about %d IDs, want 3 (the one control, three times)", calls)
	}
}

// A control that could not be asked is unknown, not a lie: no provider is
// left out for it, and the sweep is skipped rather than run uncalibrated.
func TestStatPostedFileDamageConnectionErrorIsNotALie(t *testing.T) {
	src, matches := statLiarFixture()
	calls := 0
	counting := func(ctx context.Context, ids []string) ([]nntp.StatResult, error) {
		calls += len(ids)
		return mockStat(nil, map[string]bool{"a0": true})(ctx, ids)
	}

	got, ok, excluded := statPostedFileDamage(context.Background(), parallelFetchNopLogger, counting, matches, src, []string{"a0"}, "e", nil)

	if ok || got != nil {
		t.Fatalf("got ok=%v missing=%v, want the sweep skipped", ok, got)
	}
	if len(excluded) != 0 {
		t.Fatalf("excluded = %v, want nobody left out for a connection error", excluded)
	}
	if calls != 1 {
		t.Fatalf("asked about %d IDs, want only the control", calls)
	}
}

// A stand-in that reports present without naming a provider gives nothing to
// leave out, so the sweep is skipped as it always was.
func TestStatPostedFileDamageUnnamedLiarSkips(t *testing.T) {
	src, matches := statLiarFixture()
	got, ok, excluded := statPostedFileDamage(context.Background(), parallelFetchNopLogger,
		mockStat(map[string]bool{"a1": true}, nil), matches, src, []string{"a0"}, "e", nil)
	if ok || got != nil || len(excluded) != 0 {
		t.Fatalf("got ok=%v missing=%v excluded=%v, want skipped with nobody left out", ok, got, excluded)
	}
}

// confirmFixture is one posted file of n articles whose bodies fetch as the
// given errors say (nil = fetches fine).
func confirmFixture(t *testing.T, n int, errs map[string]error) (map[[16]byte]*postedFileFetcher, map[string]postedSegPos, *atomic.Int64) {
	t.Helper()
	fileID := [16]byte{1}
	ref := storage.PostedFileRef{Name: "a.rar", Size: int64(n) * 100}
	where := make(map[string]postedSegPos, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("a%d", i)
		ref.Segments = append(ref.Segments, storage.Par2SegmentRef{MessageID: id, Bytes: 100})
		where[id] = postedSegPos{fileID: fileID, index: i}
	}
	fetched := &atomic.Int64{}
	var mu sync.Mutex
	fetch := func(_ context.Context, id string, _ usenet.ArticleCheck) ([]byte, error) {
		fetched.Add(1)
		mu.Lock()
		defer mu.Unlock()
		if err := errs[id]; err != nil {
			return nil, err
		}
		return make([]byte, 100), nil
	}
	f := newPostedFileFetcher(context.Background(), fetch, ref, nil, int64(n)*100, zerolog.Nop())
	return map[[16]byte]*postedFileFetcher{fileID: f}, where, fetched
}

func badArticleIndexes(f *postedFileFetcher) []int {
	var out []int
	f.badArticles.Range(func(k, _ any) bool {
		out = append(out, k.(int))
		return true
	})
	sort.Ints(out)
	return out
}

// With a provider left out, a miss is kept only when its body cannot be
// fetched from anyone: the left-out provider may hold it after all. A
// fetch that fails some other way decides nothing.
func TestConfirmStatMissesKeepsOnlyUnfetchableArticles(t *testing.T) {
	fetchers, where, fetched := confirmFixture(t, 5, map[string]error{
		"a0": par2NotFoundErr(),  // gone everywhere
		"a2": par2TransportErr(), // could not be fetched just now
		"a4": par2NotFoundErr(),  // gone everywhere
	})
	candidates := []string{"a0", "a1", "a2", "a4", "not-in-any-file"}

	got := confirmStatMisses(context.Background(), parallelFetchNopLogger, "e", fetchers, where, candidates,
		[]string{"liar"}, 3, 64, nil)

	sort.Strings(got)
	if fmt.Sprint(got) != "[a0 a4]" {
		t.Fatalf("confirmed = %v, want [a0 a4]", got)
	}
	if n := fetched.Load(); n != 4 {
		t.Fatalf("fetched %d bodies, want 4 (every candidate that belongs to a file)", n)
	}
	// Confirmed articles are recorded as bad so the pass patches them once
	// their slices are rebuilt; the others are not.
	if bad := badArticleIndexes(fetchers[[16]byte{1}]); fmt.Sprint(bad) != "[0 4]" {
		t.Fatalf("bad articles = %v, want [0 4]", bad)
	}
}

// Far more misses than the recovery data could ever rebuild are not fetched
// one by one, and none is seeded.
func TestConfirmStatMissesOverLimitFetchesNothing(t *testing.T) {
	fetchers, where, fetched := confirmFixture(t, 6, map[string]error{
		"a0": par2NotFoundErr(), "a1": par2NotFoundErr(), "a2": par2NotFoundErr(),
	})
	got := confirmStatMisses(context.Background(), parallelFetchNopLogger, "e", fetchers, where,
		[]string{"a0", "a1", "a2"}, []string{"liar"}, 3, 2, nil)
	if got != nil {
		t.Fatalf("confirmed = %v, want none over the limit", got)
	}
	if n := fetched.Load(); n != 0 {
		t.Fatalf("fetched %d bodies over the limit, want 0", n)
	}
	if bad := badArticleIndexes(fetchers[[16]byte{1}]); len(bad) != 0 {
		t.Fatalf("bad articles = %v, want none", bad)
	}
}

// A sweep that asked every provider records its misses as bad without a
// fetch, so they are patched after the solve instead of rebuilt and thrown
// away.
func TestMarkStatMissingBadRecordsArticles(t *testing.T) {
	fetchers, where, fetched := confirmFixture(t, 4, nil)
	markStatMissingBad(fetchers, where, []string{"a1", "a3", "not-in-any-file"})
	if bad := badArticleIndexes(fetchers[[16]byte{1}]); fmt.Sprint(bad) != "[1 3]" {
		t.Fatalf("bad articles = %v, want [1 3]", bad)
	}
	if n := fetched.Load(); n != 0 {
		t.Fatalf("fetched %d bodies, want 0", n)
	}
}

// Misses from a sweep with a provider left out are seeded only while they
// fit the recovery budget; over it they are left for the round loop, whose
// verdict is not terminal by default. A full sweep always seeds.
func TestSeedStatSlicesBudgetRule(t *testing.T) {
	cases := []struct {
		name                    string
		reduced                 bool
		recorded, extra, budget int
		want                    bool
	}{
		{"full sweep within budget", false, 1, 3, 5, true},
		{"full sweep over budget still seeds", false, 1, 30, 5, true},
		{"reduced sweep within budget", true, 1, 3, 5, true},
		{"reduced sweep exactly at budget", true, 1, 4, 5, true},
		{"reduced sweep over budget seeds nothing", true, 1, 5, 5, false},
	}
	for _, tc := range cases {
		if got := seedStatSlices(tc.reduced, tc.recorded, tc.extra, tc.budget); got != tc.want {
			t.Errorf("%s: seedStatSlices = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestStatSweepState(t *testing.T) {
	cases := []struct {
		completed bool
		excluded  []string
		want      string
	}{
		{true, nil, "complete"},
		{false, nil, "incomplete"},
		{true, []string{"a.example"}, "complete, without a.example"},
		{false, []string{"a.example", "b.example"}, "incomplete, without a.example, b.example"},
	}
	for _, tc := range cases {
		if got := statSweepState(tc.completed, tc.excluded, false); got != tc.want {
			t.Errorf("statSweepState(%v, %v, false) = %q, want %q", tc.completed, tc.excluded, got, tc.want)
		}
	}
	if got, want := statSweepState(true, []string{"a.example"}, true), "complete, without a.example, misses left for the solve"; got != want {
		t.Errorf("statSweepState left for the solve = %q, want %q", got, want)
	}
}

func TestStatConfirmCap(t *testing.T) {
	for budget, want := range map[int]int{0: 64, 5: 64, 16: 128, 128: 1024, 1000: 1024} {
		if got := statConfirmCap(budget); got != want {
			t.Errorf("statConfirmCap(%d) = %d, want %d", budget, got, want)
		}
	}
}

// sweepScenario runs the damage check the way runRepair does - the sweep,
// then settling its misses, then folding their slices into the damaged set -
// over one posted file whose article i is slice i. recorded is the slice
// set the overlay's dead segments already cover.
func sweepScenario(t *testing.T, providers []statProvider, bodyErrs map[string]error, controls []string, recorded []int64, budget int) (settled []string, damaged []int64, state string, fetched int64, bad []int) {
	t.Helper()
	const n = 6
	fetchers, where, fetchCount := confirmFixture(t, n, bodyErrs)
	src := []storage.PostedFileRef{{Name: "a.rar"}}
	for i := 0; i < n; i++ {
		src[0].Segments = append(src[0].Segments, storage.Par2SegmentRef{MessageID: fmt.Sprintf("a%d", i)})
	}
	ctx := context.Background()

	missing, swept, excluded := statPostedFileDamage(ctx, parallelFetchNopLogger, providerStat(providers, nil),
		[]par2.Match{{PostedIndex: 0}}, src, controls, "e", nil)
	settled, left := settleStatMisses(ctx, parallelFetchNopLogger, "e", fetchers, where, missing, excluded, 3, budget, nil)

	damagedSet := make(map[int64]struct{})
	for _, s := range recorded {
		damagedSet[s] = struct{}{}
	}
	slicesOf := func(mid string) []int64 {
		pos, ok := where[mid]
		if !ok {
			return nil
		}
		return []int64{int64(pos.index)}
	}
	if _, seeded := foldStatSlices(damagedSet, settled, slicesOf, len(excluded) > 0, budget); !seeded {
		left = true
	}
	for s := range damagedSet {
		damaged = append(damaged, s)
	}
	sort.Slice(damaged, func(i, j int) bool { return damaged[i] < damaged[j] })
	sort.Strings(settled)
	return settled, damaged, statSweepState(swept, excluded, left), fetchCount.Load(), badArticleIndexes(fetchers[[16]byte{1}])
}

// Every provider asked: the misses are believed without a fetch, recorded
// so they get patched, and seeded even past the budget, as before.
func TestDamageCheckFullSweepSeedsEverything(t *testing.T) {
	dead := map[string]bool{"a0": true, "a3": true, "a4": true}
	settled, damaged, state, fetched, bad := sweepScenario(t,
		[]statProvider{{host: "h1", missing: dead}, {host: "h2", missing: dead}},
		nil, []string{"a0"}, []int64{0}, 1)

	if fmt.Sprint(settled) != "[a0 a3 a4]" {
		t.Fatalf("settled = %v, want [a0 a3 a4]", settled)
	}
	if fmt.Sprint(damaged) != "[0 3 4]" {
		t.Fatalf("damaged = %v, want [0 3 4] (seeded past the budget of 1)", damaged)
	}
	if state != "complete" {
		t.Fatalf("state = %q, want complete", state)
	}
	if fetched != 0 {
		t.Fatalf("fetched %d bodies, want 0", fetched)
	}
	if fmt.Sprint(bad) != "[0 3 4]" {
		t.Fatalf("bad articles = %v, want [0 3 4]", bad)
	}
}

// One provider left out, damage within budget: only the misses whose body
// cannot be fetched are seeded. The recorded-dead control turns out to be
// fetchable, so it is not among the settled misses and the heal step still
// tries it.
func TestDamageCheckReducedSweepSeedsOnlyConfirmedMisses(t *testing.T) {
	honest := map[string]bool{"a0": true, "a2": true, "a3": true, "a5": true}
	settled, damaged, state, fetched, bad := sweepScenario(t,
		[]statProvider{{host: "liar"}, {host: "honest", missing: honest}},
		map[string]error{"a2": par2NotFoundErr(), "a3": par2NotFoundErr()}, // a0 and a5 fetch fine
		[]string{"a0"}, []int64{0}, 5)

	if fmt.Sprint(settled) != "[a2 a3]" {
		t.Fatalf("settled = %v, want [a2 a3] (a0 and a5 are alive after all)", settled)
	}
	if fmt.Sprint(damaged) != "[0 2 3]" {
		t.Fatalf("damaged = %v, want [0 2 3]", damaged)
	}
	if state != "complete, without liar" {
		t.Fatalf("state = %q, want %q", state, "complete, without liar")
	}
	if fetched != 4 {
		t.Fatalf("fetched %d bodies, want 4 (one per miss)", fetched)
	}
	if fmt.Sprint(bad) != "[2 3]" {
		t.Fatalf("bad articles = %v, want [2 3]", bad)
	}
}

// One provider left out, damage over budget: nothing is seeded, so the
// opening capacity gate cannot fail the job on it, and the summary says the
// misses were left for the solve.
func TestDamageCheckReducedSweepOverBudgetSeedsNothing(t *testing.T) {
	honest := map[string]bool{"a0": true, "a2": true, "a3": true}
	notFound := map[string]error{"a0": par2NotFoundErr(), "a2": par2NotFoundErr(), "a3": par2NotFoundErr()}
	settled, damaged, state, _, _ := sweepScenario(t,
		[]statProvider{{host: "liar"}, {host: "honest", missing: honest}},
		notFound, []string{"a0"}, []int64{0}, 2)

	if fmt.Sprint(settled) != "[a0 a2 a3]" {
		t.Fatalf("settled = %v, want [a0 a2 a3]", settled)
	}
	if fmt.Sprint(damaged) != "[0]" {
		t.Fatalf("damaged = %v, want only the recorded slice [0]", damaged)
	}
	if want := "complete, without liar, misses left for the solve"; state != want {
		t.Fatalf("state = %q, want %q", state, want)
	}
}
