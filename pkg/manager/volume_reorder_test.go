package manager

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// reorderFile is voFile with offsets filled in, as a stored file has them.
func reorderFile(tags []string, articles []int) *storage.NZBFile {
	f := voFile(tags, articles)
	f.IsStored = true
	var off int64
	for i := range f.Segments {
		f.Segments[i].StartOffset = off
		off += f.Segments[i].Bytes
		f.Segments[i].EndOffset = off - 1
	}
	f.Size = off
	return f
}

func TestVolumeReorder(t *testing.T) {
	// 44 and 45 swapped, as the yEnc names gave them.
	order, err := volumeReorder([]int{43, 45, 44, 46}, volumeSourceYencName)
	if err != nil || !slices.Equal(order, []int{0, 2, 1, 3}) {
		t.Fatalf("got %v, %v", order, err)
	}
	// A RAR5 set's first volume carries no number and stays first.
	order, err = volumeReorder([]int{-1, 2, 1, 3}, volumeSourceRAR5Header)
	if err != nil || !slices.Equal(order, []int{0, 2, 1, 3}) {
		t.Fatalf("got %v, %v", order, err)
	}
	for name, tc := range map[string]struct {
		nums   []int
		source string
	}{
		"in order":            {[]int{1, 2, 3}, volumeSourceRAR5Header},
		"hole past the first": {[]int{1, -1, 0}, volumeSourceRAR5Header},
		"duplicate":           {[]int{2, 1, 2}, volumeSourceYencName},
		"first-volume flag":   {[]int{1, 0, 1}, volumeSourceRAR4First},
		"import":              {[]int{2, 1}, volumeSourceImport},
	} {
		if order, err := volumeReorder(tc.nums, tc.source); !errors.Is(err, errReorderRefused) {
			t.Errorf("%s: got %v, %v", name, order, err)
		}
	}
}

func TestApplyVolumeOrder(t *testing.T) {
	f := reorderFile([]string{"a", "c", "b", "d"}, []int{3, 3, 3, 2})
	vols := layoutVolumes(f.Segments)
	segs, err := applyVolumeOrder(f.Segments, vols, []int{0, 2, 1, 3})
	if err != nil {
		t.Fatal(err)
	}
	want := reorderFile([]string{"a", "b", "c", "d"}, []int{3, 3, 3, 2})
	if !slices.Equal(segs, want.Segments) {
		t.Fatalf("reordered layout differs from one stored in order:\n got %+v\nwant %+v", segs, want.Segments)
	}
	if f.Segments[3].MessageID != "c-1" {
		t.Fatal("the stored layout was changed in place")
	}

	// The short last volume cannot change places with a full one.
	if _, err := applyVolumeOrder(f.Segments, vols, []int{0, 1, 3, 2}); !errors.Is(err, errReorderRefused) {
		t.Fatalf("volumes of different sizes swapped: %v", err)
	}
	// Same article count, another data start.
	g := reorderFile([]string{"a", "c", "b", "d"}, []int{3, 3, 3, 2})
	g.Segments[6].SegmentDataStart, g.Segments[6].Bytes = 80, voArticle-80
	if _, err := applyVolumeOrder(g.Segments, layoutVolumes(g.Segments), []int{0, 2, 1, 3}); !errors.Is(err, errReorderRefused) {
		t.Fatalf("volumes laid out differently swapped: %v", err)
	}
}

func TestConfirmOrderByContent(t *testing.T) {
	ctx := context.Background()
	f := reorderFile([]string{"a", "c", "b", "d"}, []int{3, 3, 3, 2})
	vols := layoutVolumes(f.Segments)
	order := []int{0, 2, 1, 3}
	bodies := func(times map[string]int) volumeBodyFunc {
		return func(_ context.Context, id string) ([]byte, error) {
			if ts, ok := times[id]; ok {
				return mkvCluster(ts), nil
			}
			return make([]byte, 400), nil
		}
	}

	// The names said b comes before c, and b's video is the earlier.
	if err := confirmOrderByContent(ctx, f, vols, order, bodies(map[string]int{"a-1": 0, "b-1": 9000, "c-1": 18000, "d-1": 27000})); err != nil {
		t.Fatal(err)
	}
	// A Cluster that only opens in a volume's second article still counts.
	if err := confirmOrderByContent(ctx, f, vols, order, bodies(map[string]int{"b-2": 9000, "c-1": 18000})); err != nil {
		t.Fatal(err)
	}
	// The content says the stored order was right: the names lied.
	err := confirmOrderByContent(ctx, f, vols, order, bodies(map[string]int{"a-1": 0, "c-1": 9000, "b-1": 18000, "d-1": 27000}))
	if !errors.Is(err, errReorderRefused) {
		t.Fatalf("content against the new order accepted: %v", err)
	}
	// A volume that moves and shows no Cluster is not confirmed.
	if err := confirmOrderByContent(ctx, f, vols, order, bodies(map[string]int{"a-1": 0, "b-1": 9000, "d-1": 27000})); !errors.Is(err, errReorderRefused) {
		t.Fatalf("an unread volume accepted: %v", err)
	}
	// A fetch error is not a refusal: it is reported as it is.
	failing := func(context.Context, string) ([]byte, error) { return nil, errors.New("430 no such article") }
	if err := confirmOrderByContent(ctx, f, vols, order, failing); err == nil || errors.Is(err, errReorderRefused) {
		t.Fatalf("fetch error: got %v", err)
	}
	// Compressed data and other containers cannot be confirmed.
	mp4 := reorderFile([]string{"a", "c", "b", "d"}, []int{3, 3, 3, 2})
	mp4.Name = "f.mp4"
	if err := confirmOrderByContent(ctx, mp4, vols, order, bodies(nil)); !errors.Is(err, errReorderRefused) {
		t.Fatalf("mp4 accepted: %v", err)
	}
}

func TestReorderMisorderedLeavesOtherFilesAlone(t *testing.T) {
	// No usenet: every reorder is refused, and the record must come back
	// exactly as it went in so Replace can re-grab the file.
	r := &Repair{manager: &Manager{}}
	h := &storage.EntryHealth{EntryName: "e", DecodeVerifiedFingerprint: "fp", UnverifiedFiles: []storage.UnverifiedFile{
		{FileName: "a.mkv", InfoHash: "id", Reason: reasonVolumeOrder},
		{FileName: "b.mkv", InfoHash: "id", Reason: reasonTailTruncated},
	}}
	if n := r.reorderMisordered(context.Background(), h); n != 0 {
		t.Fatalf("fixed %d files with no usenet", n)
	}
	if len(h.UnverifiedFiles) != 2 || h.DecodeVerifiedFingerprint != "fp" || h.Dirty {
		t.Fatalf("record changed: %+v", h)
	}
}

func TestAutoReorderResultsRecordsARefusalOnce(t *testing.T) {
	// No usenet: the reorder is refused for a reason about the install, which
	// stands in for any refusal about the file.
	r := &Repair{manager: &Manager{}}
	c := &candidate{name: "e"}
	check := &storage.VolumeOrderCheck{FileName: "a.mkv", Layout: "k", Misordered: true, Detail: "stored volume 3 of 4 is archive volume 2"}
	results := []fileResult{
		{name: "a.mkv", infoHash: "id", healthy: true, unverifiedReason: reasonVolumeOrder, unverifiedDetail: check.Detail, volumeCheck: check},
		{name: "b.mkv", infoHash: "id", healthy: true, unverifiedReason: unverifiedTimeout},
		{name: "c.mkv", infoHash: "id", healthy: true},
	}
	if r.autoReorderResults(context.Background(), c, results) {
		t.Fatal("reported a reorder with no usenet")
	}
	// Still listed, never broken, and the reason is on the record.
	a := results[0]
	if !a.healthy || a.broken || a.unverifiedReason != reasonVolumeOrder || a.volumeReordered {
		t.Fatalf("file changed state: %+v", a)
	}
	if check.ReorderRefused != "usenet is not configured" || !strings.Contains(a.unverifiedDetail, "not reordered in place: usenet is not configured") {
		t.Fatalf("refusal not recorded: %q / %q", check.ReorderRefused, a.unverifiedDetail)
	}
	if results[1].unverifiedReason != unverifiedTimeout || results[1].unverifiedDetail != "" || results[2].volumeCheck != nil {
		t.Fatalf("other files touched: %+v", results[1:])
	}
	// The next sweep reads the recorded reason and does not try again.
	next := []fileResult{{name: "a.mkv", infoHash: "id", healthy: true, unverifiedReason: reasonVolumeOrder, unverifiedDetail: check.Detail,
		volumeCheck: &storage.VolumeOrderCheck{FileName: "a.mkv", Layout: "k", Misordered: true, Detail: check.Detail, ReorderRefused: "volumes are laid out differently"}}}
	if r.autoReorderResults(context.Background(), c, next) || next[0].volumeCheck.ReorderRefused != "volumes are laid out differently" ||
		!strings.Contains(next[0].unverifiedDetail, "volumes are laid out differently") {
		t.Fatalf("a recorded refusal was retried or lost: %+v", next[0])
	}
}

func TestReorderBusyIsARefusalThatIsNotRecorded(t *testing.T) {
	err := fmt.Errorf("%w: %w; try again when nothing is playing it", errReorderRefused, errReorderBusy)
	if !errors.Is(err, errReorderRefused) || !errors.Is(err, errReorderBusy) {
		t.Fatal("an open file should be both a refusal (Replace falls back) and busy (the sweep retries)")
	}
}

func TestMergeVolumeOrderChecksDropsAReorderedFilesVerdict(t *testing.T) {
	prior := []storage.VolumeOrderCheck{{FileName: "a.mkv", Layout: "old", Misordered: true}, {FileName: "b.mkv", Layout: "kept"}}
	// Reordered, and the read of the new layout reached no verdict.
	got := mergeVolumeOrderChecks(prior, []string{"a.mkv", "b.mkv"}, []fileResult{{name: "a.mkv", volumeReordered: true}, {name: "b.mkv"}})
	if len(got) != 1 || got[0].FileName != "b.mkv" {
		t.Fatalf("got %+v", got)
	}
	// Reordered and read again: the new verdict replaces the old.
	got = mergeVolumeOrderChecks(prior, []string{"a.mkv", "b.mkv"}, []fileResult{
		{name: "a.mkv", volumeReordered: true, volumeCheck: &storage.VolumeOrderCheck{FileName: "a.mkv", Layout: "new"}}})
	if len(got) != 2 || got[0].Layout != "new" || got[0].Misordered {
		t.Fatalf("got %+v", got)
	}
}
