//go:build livebox

package manager

// Live check of postedFileFetcher.resolveGeometry and the dead-segment heal
// against real providers, run on the production install with a scratch data folder. Reads
// only: nothing is written to the overlay or any cache. Not part of any
// normal build (tag livebox).
//
//	LB_ROOT  scratch data folder (config.json + usenet/meta/<id>.meta)
//	LB_NZB   NZB id
//	LB_DEAD  comma-separated message IDs recorded dead (optional)

import (
	"context"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

func TestLivePar2Geometry(t *testing.T) {
	root, id := os.Getenv("LB_ROOT"), os.Getenv("LB_NZB")
	if root == "" || id == "" {
		t.Skip("LB_ROOT/LB_NZB not set")
	}
	utils.StartGlobalCachedTime()
	config.SetConfigPath(root)
	u, err := usenet.New()
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	nzb, err := u.GetNZB(id)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	vol := regexp.MustCompile(`(?i)\.vol\d+[+-]\d+\.par2$`)
	var sources []par2.Source
	for _, f := range nzb.Par2Files {
		if vol.MatchString(f.Name) {
			continue
		}
		data, err := fetchWholePar2File(ctx, u.FetchArticle, f)
		if err != nil {
			t.Fatalf("fetch %s: %v", f.Name, err)
		}
		sources = append(sources, par2.Source{Name: f.Name, Data: data})
	}
	idx, err := par2.ParseIndex(sources)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("index: %d files, slice size %d, %d slices", len(idx.Files), idx.SliceSize, idx.NumSlices())

	byName := map[string]*par2.FileDesc{}
	for _, fd := range idx.Files {
		byName[strings.ToLower(fd.Name)] = fd
	}
	dead := map[string]bool{}
	for _, m := range strings.Split(os.Getenv("LB_DEAD"), ",") {
		if m = strings.Trim(strings.TrimSpace(m), "<>"); m != "" {
			dead[m] = true
		}
	}

	// sliceOK reads one slice of pf fresh from Usenet and checks its IFSC.
	sliceOK := func(pf *postedFileFetcher, fd *par2.FileDesc, local int64) (bool, error) {
		base, err := idx.SliceBase(fd.FileID)
		if err != nil {
			return false, err
		}
		data, _, err := pf.readRange(local*idx.SliceSize, idx.SliceSize, false)
		if err != nil {
			return false, err
		}
		return idx.VerifySliceChecksum(base+local, data)
	}

	measured, lastBad, estimatedBad, measuredBad, healed, healFailed := 0, 0, 0, 0, 0, 0
	for i, src := range nzb.Par2Source {
		fd := byName[strings.ToLower(src.Name)]
		if fd == nil {
			t.Logf("%s: no FileDesc by name", src.Name)
			continue
		}
		if len(src.Segments) == 1 {
			// Small single-article files: MD5-16k against the index.
			sum, err := computeMD5_16k(ctx, u.FetchArticleChecked, src)
			t.Logf("%s: stored %d bytes, FileDesc %d; MD5-16k match=%v err=%v", src.Name, src.Size, fd.Length, err == nil && sum == fd.MD5_16k, err)
			continue
		}
		est := newPostedFileFetcher(ctx, u.FetchArticleChecked, src, nil, fd.Length, zerolog.Nop())
		pf := newPostedFileFetcher(ctx, u.FetchArticleChecked, src, nil, fd.Length, zerolog.Nop())
		pf.resolveGeometry()
		if pf.exact {
			measured++
			// The last article's size is derived, not measured: check it.
			last := len(src.Segments) - 1
			data, err := pf.segmentData(last)
			if err != nil || int64(len(data)) != pf.segSizes[last] {
				lastBad++
				t.Logf("%s: last article %d decoded %d bytes (err=%v), geometry says %d", src.Name, last+1, len(data), err, pf.segSizes[last])
			}
		}
		// A mid-file slice, far from the first article, through both geometries.
		if i < 4 {
			local := (fd.Length / idx.SliceSize) / 2
			eok, eerr := sliceOK(est, fd, local)
			mok, merr := sliceOK(pf, fd, local)
			if !eok {
				estimatedBad++
			}
			if !mok {
				measuredBad++
			}
			t.Logf("%s: exact=%v seed=%d last=%d | slice %d estimated ok=%v err=%v | measured ok=%v err=%v",
				src.Name, pf.exact, pf.segSizes[0], pf.segSizes[len(pf.segSizes)-1], local, eok, eerr, mok, merr)
		}
		for s, seg := range src.Segments {
			if !dead[strings.Trim(seg.MessageID, "<>")] {
				continue
			}
			rng := postedRange{fileID: fd.FileID, start: pf.base[s], end: pf.base[s] + pf.segSizes[s]}
			data, ok := readVerifiedRange(idx, pf, rng, rng.start, rng.end)
			if ok {
				healed++
			} else {
				healFailed++
			}
			t.Logf("dead %s (%s article %d): verified=%v bytes=%d", seg.MessageID, src.Name, s+1, ok, len(data))
		}
	}
	t.Logf("SUMMARY measured=%d/%d last_article_mismatches=%d estimated_slice_failures=%d measured_slice_failures=%d healed=%d heal_failed=%d",
		measured, len(nzb.Par2Source), lastBad, estimatedBad, measuredBad, healed, healFailed)
}
