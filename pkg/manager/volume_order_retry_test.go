package manager

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

// rar4Set builds a four-volume RAR4 file whose volumes carry the given
// end-of-archive numbers in stored order, the posted files it came from, and
// counting head and body fetchers. A tail listed in dead answers 430.
func rar4Set(order []int, dead map[string]bool) (*storage.NZBFile, []storage.PostedFileRef, volumeHeadFunc, volumeBodyFunc, *atomic.Int32) {
	tags := []string{"a", "b", "c", "d"}
	articles := []int{3, 3, 3, 3}
	f := voFile(tags, articles)
	heads := fakeHeads{}
	bodies := map[string][]byte{}
	var posted []storage.PostedFileRef
	for k, tag := range tags {
		heads[tag+"-1"] = usenet.ArticleHead{Prefix: rar4Head(order[k] == 0), Name: "obfuscated" + tag, Part: 1}
		p := storage.PostedFileRef{Name: "obfuscated" + tag}
		for n := 1; n <= articles[k]+1; n++ {
			p.Segments = append(p.Segments, storage.Par2SegmentRef{MessageID: fmt.Sprintf("%s-%d", tag, n)})
		}
		posted = append(posted, p)
		bodies[fmt.Sprintf("%s-%d", tag, articles[k]+1)] = rar4End(order[k])
	}
	var fetches atomic.Int32
	head := func(ctx context.Context, id string) (usenet.ArticleHead, error) {
		fetches.Add(1)
		return heads.head(ctx, id)
	}
	body := func(_ context.Context, id string) ([]byte, error) {
		fetches.Add(1)
		if dead[id] {
			return nil, errors.New("430 no such article")
		}
		if b, ok := bodies[id]; ok {
			return b, nil
		}
		return make([]byte, 64), nil
	}
	return f, posted, head, body, &fetches
}

func TestReadVolumeOrderRemembersNoVerdictUntilRetry(t *testing.T) {
	ctx := context.Background()
	key := t.Name()
	t.Cleanup(func() { volumeOrderCache.Delete(key) })

	// Volume b's tail article is gone: no end-of-archive number for it.
	f, posted, head, body, fetches := rar4Set([]int{0, 1, 2, 3}, map[string]bool{"b-4": true})

	if _, err := readVolumeOrder(ctx, key, f, posted, head, body); err == nil {
		t.Fatal("a dead tail article gave a verdict")
	}
	if fetches.Load() == 0 {
		t.Fatal("first sweep read nothing")
	}

	// Second sweep within the retry time: nothing is fetched, still no verdict.
	before := fetches.Load()
	if _, err := readVolumeOrder(ctx, key, f, posted, head, body); !errors.Is(err, errVolumeOrderRetryLater) {
		t.Fatalf("second sweep: err %v, want retry-later (no verdict)", err)
	}
	if got := fetches.Load(); got != before {
		t.Fatalf("second sweep within the retry time fetched %d articles", got-before)
	}
	if c, _ := volumeOrderCache.Load(key); func() bool { _, ok := c.(volumeOrderVerdict); return ok }() {
		t.Fatal("a no-verdict read was cached as a verdict")
	}

	// Retry time passed: the set is read again, and it is still no verdict.
	volumeOrderCache.Store(key, volumeOrderNoVerdict{retryAt: time.Now().Add(-time.Second)})
	before = fetches.Load()
	_, err := readVolumeOrder(ctx, key, f, posted, head, body)
	if err == nil || errors.Is(err, errVolumeOrderRetryLater) {
		t.Fatalf("after the retry time: err %v, want a fresh read with no verdict", err)
	}
	if fetches.Load() == before {
		t.Fatal("after the retry time the set was not read again")
	}
	if _, ok := mustLoad(t, key).(volumeOrderNoVerdict); !ok {
		t.Fatal("the fresh no-verdict read was not remembered")
	}

	// A layout change (another key) is read at once.
	other := key + "/relaid"
	t.Cleanup(func() { volumeOrderCache.Delete(other) })
	before = fetches.Load()
	if _, err := readVolumeOrder(ctx, other, f, posted, head, body); err == nil || errors.Is(err, errVolumeOrderRetryLater) {
		t.Fatalf("new layout: err %v, want a fresh read", err)
	}
	if fetches.Load() == before {
		t.Fatal("a new layout was not read")
	}
}

func TestReadVolumeOrderStoppedSweepIsNotRemembered(t *testing.T) {
	key := t.Name()
	t.Cleanup(func() { volumeOrderCache.Delete(key) })
	f, posted, _, body, _ := rar4Set([]int{0, 1, 2, 3}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stopped := func(ctx context.Context, id string) (usenet.ArticleHead, error) {
		return usenet.ArticleHead{}, ctx.Err()
	}
	if _, err := readVolumeOrder(ctx, key, f, posted, stopped, body); err == nil {
		t.Fatal("a stopped sweep gave a verdict")
	}
	if _, ok := volumeOrderCache.Load(key); ok {
		t.Fatal("a stopped sweep was remembered")
	}
}

func TestReadVolumeOrderVerdictsUnchanged(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name       string
		order      []int
		misordered bool
	}{
		{"in order", []int{0, 1, 2, 3}, false},
		{"misordered", []int{0, 3, 1, 2}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := t.Name()
			t.Cleanup(func() { volumeOrderCache.Delete(key) })
			f, posted, head, body, fetches := rar4Set(tc.order, nil)
			v, err := readVolumeOrder(ctx, key, f, posted, head, body)
			if err != nil || v.misordered != tc.misordered || v.source != volumeSourceRAR4End || v.volumes != 4 {
				t.Fatalf("got %+v, %v", v, err)
			}
			before := fetches.Load()
			again, err := readVolumeOrder(ctx, key, f, posted, head, body)
			if err != nil || again.misordered != v.misordered || again.source != v.source || again.volumes != v.volumes || fetches.Load() != before {
				t.Fatalf("cached verdict: got %+v, %v, %d fetches", again, err, fetches.Load()-before)
			}
		})
	}
}

func mustLoad(t *testing.T, key string) any {
	t.Helper()
	v, ok := volumeOrderCache.Load(key)
	if !ok {
		t.Fatalf("nothing cached for %s", key)
	}
	return v
}
