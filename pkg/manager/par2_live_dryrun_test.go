//go:build livebox

package manager

// Live dry run of a PAR2 repair pass against real providers, run on the production install
// with a scratch data folder holding copies of the entry's meta and overlay
// manifest. runRepair writes its patches into that scratch overlay, never the
// service's. Not part of any normal build (tag livebox).
//
//	LB_ROOT  scratch data folder (config.json, usenet/meta/<id>.meta,
//	         usenet/overlay/<id>/)
//	LB_NZBS  comma-separated NZB ids
//	LB_MODE  "dump" prints message IDs for probing; "repair" runs runRepair

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

func TestLivePar2DryRun(t *testing.T) {
	root, ids := os.Getenv("LB_ROOT"), os.Getenv("LB_NZBS")
	if root == "" || ids == "" {
		t.Skip("LB_ROOT/LB_NZBS not set")
	}
	utils.StartGlobalCachedTime()
	config.SetConfigPath(root)
	u, err := usenet.New()
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	strg, err := storage.NewStorage(root + "/scratch-storage")
	if err != nil {
		t.Fatal(err)
	}
	defer strg.Close()
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, NoColor: true, TimeFormat: "15:04:05"}).
		Level(zerolog.DebugLevel).With().Timestamp().Logger()
	p := &Par2Repair{manager: &Manager{storage: strg, usenet: u}, logger: logger}

	for _, id := range strings.Split(ids, ",") {
		id = strings.TrimSpace(id)
		nzb, err := u.GetNZB(id)
		if err != nil {
			t.Errorf("%s: %v", id, err)
			continue
		}
		pending, err := u.OverlayPendingRepair(id)
		if err != nil {
			t.Errorf("%s: pending: %v", id, err)
			continue
		}
		fmt.Printf("\n===== %s (%s) par2files=%d par2source=%d files=%d\n", nzb.Name, id, len(nzb.Par2Files), len(nzb.Par2Source), len(nzb.Files))

		if os.Getenv("LB_MODE") == "dump" {
			for _, f := range nzb.Par2Files {
				fmt.Printf("PAR2 %s size=%d segs=%d first=%s\n", f.Name, f.Size, len(f.Segments), firstID(f.Segments))
				if os.Getenv("LB_ALLSEGS") != "" {
					for i, s := range f.Segments {
						fmt.Printf("PSEG %s %d %d %s\n", f.Name, i, s.Bytes, s.MessageID)
					}
				}
			}
			names := make([]string, 0, len(pending))
			for n := range pending {
				names = append(names, n)
			}
			sort.Strings(names)
			for _, n := range names {
				for _, d := range pending[n] {
					fmt.Printf("DEAD %s seg=%d bytes=%d msgid=%s\n", n, d.Index, d.Bytes, d.MessageID)
				}
			}
			continue
		}

		if os.Getenv("LB_MODE") == "fetchpar2" {
			for _, f := range nzb.Par2Files {
				bad := 0
				for i, seg := range f.Segments {
					fctx, fcancel := context.WithTimeout(context.Background(), 2*time.Minute)
					st := time.Now()
					data, err := u.FetchArticle(fctx, seg.MessageID)
					fcancel()
					if err != nil {
						bad++
						fmt.Printf("FETCHERR %s seg=%d took=%s err=%v\n", f.Name, i, time.Since(st).Round(time.Millisecond), err)
					} else if int64(len(data)) < seg.Bytes*9/10 {
						fmt.Printf("SHORT %s seg=%d len=%d want~%d\n", f.Name, i, len(data), seg.Bytes)
					}
				}
				fmt.Printf("PAR2FETCH %s segs=%d failed=%d\n", f.Name, len(f.Segments), bad)
			}
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Minute)
		var readBytes, cacheBytes int64
		var slicesRepaired, deadDiscovered int
		progress := newPar2JobProgressState(id, nzb.Name)
		start := time.Now()
		err = p.runRepair(ctx, id, nzb.Name, pending, &readBytes, &cacheBytes, &slicesRepaired, &deadDiscovered, progress)
		cancel()
		class := par2OutcomeClass(err, 1)
		fmt.Printf("RESULT %s err=%v terminal=%v suspect=%v read=%d slices_repaired=%d dead_discovered=%d took=%s\n",
			nzb.Name, err, class.terminal, class.suspect, readBytes, slicesRepaired, deadDiscovered, time.Since(start).Round(time.Second))
		for n, segs := range pending {
			patched := 0
			for _, d := range segs {
				if _, ok := u.OverlayPatchBytes(id, n, d.Index); ok {
					patched++
				}
			}
			fmt.Printf("PATCHED %s %d/%d\n", n, patched, len(segs))
		}
	}
}

func firstID(segs []storage.Par2SegmentRef) string {
	if len(segs) == 0 {
		return ""
	}
	return segs[0].MessageID
}
