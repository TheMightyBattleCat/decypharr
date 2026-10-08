//go:build livebox

package manager

// Live check of burstChunks against real providers, run on the production install with a
// scratch data folder. Not part of any normal build (tag livebox).
//
//	LB_ROOT     scratch data folder (config.json + usenet/meta/<id>.meta)
//	LB_NZB      NZB id
//	LB_DURABLE  the live DFS cache data file for the same file (read directly
//	            from disk, never through the mount) - run C compares against it
//	LB_OUT      scratch file standing in for the durable cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

// fileStore is a durable cache backed by a sparse scratch file, with presence
// tracked per byte range written.
type fileStore struct {
	f      *os.File
	ranges [][2]int64
	writes int64
}

func (s *fileStore) WriteCachedRange(_, _ string, _ int64, p []byte, off int64) error {
	if _, err := s.f.WriteAt(p, off); err != nil {
		return err
	}
	s.ranges = append(s.ranges, [2]int64{off, off + int64(len(p))})
	s.writes += int64(len(p))
	return nil
}

func (s *fileStore) HasCachedRange(_, _ string, off, length int64) bool {
	// Simple: covered if a union of written ranges covers [off, off+length).
	pos := off
	for changed := true; changed && pos < off+length; {
		changed = false
		for _, r := range s.ranges {
			if r[0] <= pos && r[1] > pos {
				pos = r[1]
				changed = true
			}
		}
	}
	return pos >= off+length
}

func providerBytes(u *usenet.Usenet) map[string]int64 {
	out := map[string]int64{}
	st := u.Stats()
	if ps, ok := st["providers"].([]map[string]any); ok {
		for _, p := range ps {
			if h, ok := p["host"].(string); ok {
				if b, ok := p["bytes_used"].(int64); ok {
					out[h] = b
				}
			}
		}
	}
	return out
}

func totalDelta(a, b map[string]int64) (int64, map[string]int64) {
	d := map[string]int64{}
	var t int64
	for h, v := range b {
		if x := v - a[h]; x != 0 {
			d[h] = x
			t += x
		}
	}
	return t, d
}

func TestLiveBurst(t *testing.T) {
	root, id := os.Getenv("LB_ROOT"), os.Getenv("LB_NZB")
	if root == "" || id == "" {
		t.Skip("LB_ROOT/LB_NZB not set")
	}
	utils.StartGlobalCachedTime()
	config.SetConfigPath(root)
	cfg := config.Get()
	cfg.Usenet.DiskBufferPath = filepath.Join(root, "streams")
	u, err := usenet.New()
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	nzb, err := u.GetNZB(id)
	if err != nil {
		t.Fatal(err)
	}
	best := -1
	for i, f := range nzb.Files {
		if !f.IsDeleted && (best < 0 || f.Size > nzb.Files[best].Size) {
			best = i
		}
	}
	file := nzb.Files[best]
	size := file.Size
	t.Logf("file %s size %d segments %d", file.Name, size, len(file.Segments))

	out, err := os.OpenFile(os.Getenv("LB_OUT"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	if err := out.Truncate(size); err != nil {
		t.Fatal(err)
	}
	log := zerolog.New(os.Stderr).Level(zerolog.WarnLevel)
	const GB = int64(1e9)

	run := func(name string, fn func() error, fetched int64) {
		b0 := providerBytes(u)
		start := time.Now()
		err := fn()
		d := time.Since(start)
		tot, per := totalDelta(b0, providerBytes(u))
		t.Logf("%s: %.1f s, %.1f MiB/s of range, provider bytes %.3f GB = %.3fx range, per provider %v, err=%v",
			name, d.Seconds(), float64(fetched)/d.Seconds()/(1<<20), float64(tot)/1e9, float64(tot)/float64(fetched), per, err)
	}

	runs := os.Getenv("LB_RUNS")
	if runs == "" {
		runs = "ABC"
	}
	has := func(r string) bool { return strings.Contains(runs, r) }

	// A: chunked burst over [1 GB, 3 GB), none of it cached.
	storeA := &fileStore{f: out}
	if has("A") {
		var resA burstResult
		run("A chunked 1-3 GB", func() error {
			var err error
			resA, err = burstChunks(context.Background(), u, storeA, storeA, "live", id, file.Name, 1*GB, 3*GB, 12, durableBurstChunk, log, burstOpts{})
			return err
		}, 2*GB)
		t.Logf("A result: fetched %d skipped %d persist %+v", resA.fetched, resA.skipped, resA.persist)
		if !storeA.HasCachedRange("", "", 1*GB, 2*GB-1<<20) {
			t.Errorf("A: [1 GB, 3 GB) not fully persisted")
		}
	}

	// B: the old one-shot fetch over [3 GB, 5 GB), for throughput only.
	if has("B") {
		run("B one-shot 3-5 GB", func() error {
			return u.ReadAheadRange(context.Background(), id, file.Name, 3*GB, 2*GB, 12)
		}, 2*GB)
	}

	// C: persisted bytes must equal the live DFS cache's own copy.
	if dp := os.Getenv("LB_DURABLE"); dp != "" && has("C") {
		cOff, cLen := int64(20*GB), int64(300<<20)
		storeC := &fileStore{f: out}
		run("C chunked 300 MB inside live-cached region", func() error {
			_, err := burstChunks(context.Background(), u, storeC, storeC, "live", id, file.Name, cOff, cOff+cLen, 12, durableBurstChunk, log, burstOpts{})
			return err
		}, cLen)
		live, err := os.Open(dp)
		if err != nil {
			t.Fatal(err)
		}
		defer live.Close()
		a, b := make([]byte, 8<<20), make([]byte, 8<<20)
		ha, hb := sha256.New(), sha256.New()
		var zeros, mism int
		for off := cOff; off < cOff+cLen; off += int64(len(a)) {
			if _, err := out.ReadAt(a, off); err != nil {
				t.Fatal(err)
			}
			if _, err := live.ReadAt(b, off); err != nil {
				t.Fatal(err)
			}
			ha.Write(a)
			hb.Write(b)
			if !bytes.Equal(a, b) {
				mism++
				first, last := -1, -1
				zeroA, zeroB := 0, 0
				for i := range a {
					if a[i] != b[i] {
						if first < 0 {
							first = i
						}
						last = i
						if a[i] == 0 {
							zeroA++
						}
						if b[i] == 0 {
							zeroB++
						}
					}
				}
				segAt := func(o int64) string {
					for i, sg := range file.Segments {
						if sg.StartOffset <= o && o <= sg.EndOffset {
							return fmt.Sprintf("seg %d [%d,%d] bytes=%d", i, sg.StartOffset, sg.EndOffset, sg.EndOffset-sg.StartOffset+1)
						}
					}
					return "no segment"
				}
				fo, lo := off+int64(first), off+int64(last)
				t.Logf("C diff: bytes [%d, %d] (%d long); differing bytes zero in persisted=%d, in live=%d; first in %s; last in %s",
					fo, lo, lo-fo+1, zeroA, zeroB, segAt(fo), segAt(lo))
				t.Logf("C diff sample persisted=% x live=% x", a[first:min(first+16, len(a))], b[first:min(first+16, len(b))])
			}
			if bytes.Equal(a, make([]byte, len(a))) {
				zeros++
			}
		}
		t.Logf("C compare: 8 MiB blocks mismatched=%d all-zero=%d sha persisted=%x live=%x", mism, zeros, ha.Sum(nil)[:8], hb.Sum(nil)[:8])
		if mism > 0 || zeros > 0 {
			t.Errorf("C: persisted bytes differ from the live cache")
		}
	}
	fmt.Println("done")
}
