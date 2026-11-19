package manager

import (
	"fmt"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// Sizes for a posting of one index and volumes of 280 slices of 16 KiB, as
// par2cmdline -n makes them (about 4.6 MB each).
const (
	estIndex = 2560
	estSlice = 16384
	estHeld  = 280
)

var estVolSize = int64(estIndex*8 + estHeld*(estSlice+par2RecoveryPacketOverhead))

// The checks before a pass count an obfuscated posting's equal-size volumes
// at one slice each. With more dead segments than volumes, Availability
// refused "only 2 recovery slices retained" and par2RecoveryCapacity gave 2,
// so the sticky-failed sweep re-grabbed when extrapolated damage was over 2:
// the posting never reached the pass, whose gates count the
// volumes from the parsed slice size. Equal-size volumes are now "unknown":
// coverage passes and the capacity is -1 (sticky_failed.go re-grabs only
// on a capacity >= 0). A doubling posting keeps its count and its refusal.
func TestPrePassChecksTakeEqualSizeEstimatedVolumesAsUnknown(t *testing.T) {
	p, _ := newTestPar2Repair(t)
	cfg := config.Get()
	prev := cfg.Repair.Par2Repair
	on := true
	cfg.Repair.Par2Repair = &on
	t.Cleanup(func() { cfg.Repair.Par2Repair = prev })

	store, err := overlay.NewStore(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("overlay.NewStore: %v", err)
	}
	u, err := usenet.NewWithNZBStorageForTest(store)
	if err != nil {
		t.Fatalf("NewWithNZBStorageForTest: %v", err)
	}
	p.manager.usenet = u

	const step = 1 << 20 // one doubling-posting slice plus its packet
	cases := []struct {
		id          string
		files       []storage.Par2FileRef
		dead        int
		wantOK      bool
		wantCap     int
		wantReasonH string
	}{
		{
			id: "obfuscated-equal",
			files: []storage.Par2FileRef{
				{Name: "5eed0000c0ffee.par2", Size: estIndex},
				{Name: "5eed1eefc1a5e7.par2", Size: estVolSize},
				{Name: "5eed3dde0d1c0ab.par2", Size: estVolSize},
			},
			dead: 5, wantOK: true, wantCap: -1,
		},
		{
			// Under par2UnnamedVolumeFloor the census keeps them as index
			// files, but the estimate still counts them one slice each.
			id: "obfuscated-equal-small",
			files: []storage.Par2FileRef{
				{Name: "a1.par2", Size: 40_000},
				{Name: "b2.par2", Size: 400_000},
				{Name: "c3.par2", Size: 400_000},
				{Name: "d4.par2", Size: 400_000},
			},
			dead: 5, wantOK: true, wantCap: -1,
		},
		{
			id: "numbered-equal",
			files: []storage.Par2FileRef{
				{Name: "rel.par2", Size: estIndex},
				{Name: "rel.vol-01.par2", Size: estVolSize},
				{Name: "rel.vol-02.par2", Size: estVolSize},
			},
			dead: 5, wantOK: true, wantCap: -1,
		},
		{
			// Control: 1, 2 and 4 steps give 7 slices, a count, and more
			// dead segments than that are still refused.
			id: "obfuscated-doubling",
			files: []storage.Par2FileRef{
				{Name: "x0.par2", Size: 40_000},
				{Name: "x1.par2", Size: 40_000 + step},
				{Name: "x2.par2", Size: 40_000 + 2*step},
				{Name: "x3.par2", Size: 40_000 + 4*step},
			},
			dead: 9, wantOK: false, wantCap: 7, wantReasonH: "only 7 recovery slices retained, need at least 9",
		},
		{
			// Control: North Glen's doubling numbered set counts 1, 3 and 5
			// (see TestCensusPar2VolumesNumberedWithoutRange).
			id: "numbered-doubling",
			files: []storage.Par2FileRef{
				{Name: "SP.vol-01.par2", Size: 37396},
				{Name: "SP.vol-02.par2", Size: 806026},
				{Name: "SP.vol-03.par2", Size: 1611406},
				{Name: "SP.vol-04.par2", Size: 3184808},
			},
			dead: 10, wantOK: false, wantCap: 9, wantReasonH: "only 9 recovery slices retained, need at least 10",
		},
		{
			// Control: a season pack's equal-size index files are not volumes.
			id: "season-pack-index-files",
			files: []storage.Par2FileRef{
				{Name: "e1.par2", Size: 40_000},
				{Name: "e2.par2", Size: 40_000},
				{Name: "e3.par2", Size: 40_000 + 2*step},
			},
			dead: 2, wantOK: false, wantCap: 1, wantReasonH: "only 1 recovery slices retained, need at least 2",
		},
	}
	for _, c := range cases {
		t.Run(c.id, func(t *testing.T) {
			if err := u.AddNZBForTest(&storage.NZB{
				ID: c.id, Name: c.id,
				Par2Source: []storage.PostedFileRef{{Name: "movie.mkv"}},
				Par2Files:  c.files,
			}); err != nil {
				t.Fatalf("AddNZBForTest: %v", err)
			}
			for i := 0; i < c.dead; i++ {
				if err := store.RecordDead(c.id, "movie.mkv", i*3, fmt.Sprintf("<%s-%d@test>", c.id, i), 700_000); err != nil {
					t.Fatalf("RecordDead: %v", err)
				}
			}
			ok, reason := p.Availability(c.id)
			if ok != c.wantOK {
				t.Errorf("Availability = %v (%q), want %v", ok, reason, c.wantOK)
			}
			if c.wantReasonH != "" && !strings.Contains(reason, c.wantReasonH) {
				t.Errorf("Availability reason %q, want it to contain %q", reason, c.wantReasonH)
			}
			if got := p.par2RecoveryCapacity(c.id); got != c.wantCap {
				t.Errorf("par2RecoveryCapacity = %d, want %d", got, c.wantCap)
			}
		})
	}
}

// Numbered volumes of one size ("rel.vol-01.par2", "rel.vol-02.par2") are
// one size step each, so the census counts one slice per volume, and
// recountSizedVolumes skipped them (they were not marked sized): the early
// gate after the index parse declined on the census. Marked sized, they are
// recounted from the slice size. A doubling numbered set (North Glen's, see
// TestCensusPar2VolumesNumberedWithoutRange) is not marked.
func TestCensusNumberedEqualSizeVolumesAreRecounted(t *testing.T) {
	files := []storage.Par2FileRef{
		{Name: "rel.par2", Size: estIndex},
		{Name: "rel.vol-01.par2", Size: estVolSize},
		{Name: "rel.vol-02.par2", Size: estVolSize},
	}
	vols, idx := censusPar2Volumes(files)
	if len(vols) != 2 || len(idx) != 1 || vols[0].count != 1 || vols[1].count != 1 {
		t.Fatalf("census: vols %+v, index %+v; want two volumes of 1 slice and rel.par2", vols, idx)
	}
	out := recountSizedVolumes(vols, files, estSlice)
	for _, v := range out {
		if v.count < estHeld || v.count > estHeld+1 {
			t.Errorf("%s recounted at %d slices, want %d (at most one over)", v.ref.Name, v.count, estHeld)
		}
	}

	doubling := []storage.Par2FileRef{
		{Name: "SP.vol-01.par2", Size: 37396},
		{Name: "SP.vol-02.par2", Size: 806026},
		{Name: "SP.vol-03.par2", Size: 1611406},
		{Name: "SP.vol-04.par2", Size: 3184808},
	}
	dvols, _ := censusPar2Volumes(doubling)
	for _, v := range dvols {
		if v.sized {
			t.Errorf("%s marked sized in a doubling numbered set", v.ref.Name)
		}
	}
}
