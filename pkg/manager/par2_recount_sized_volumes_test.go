package manager

import (
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// Equal-size volumes of an obfuscated posting are counted at one slice each
// by the census. Recounted from the slice size, each is at least what it
// holds, never below what the census said; named volumes and the input
// slice are left alone.
func TestRecountSizedVolumes(t *testing.T) {
	const (
		index = 2560
		slice = 16384
		held  = 280
	)
	// Each volume repeats the index's metadata more than once, as
	// par2cmdline's do.
	volSize := int64(index*8 + held*(slice+par2RecoveryPacketOverhead))
	files := []storage.Par2FileRef{
		{Name: "a.par2", Size: index},
		{Name: "b.par2", Size: volSize},
		{Name: "c.par2", Size: volSize},
	}
	vols, _ := censusPar2Volumes(files)
	if len(vols) != 2 || vols[0].count != 1 || vols[1].count != 1 {
		t.Fatalf("census: %+v; want two sized volumes of 1 slice", vols)
	}
	named := par2Volume{ref: storage.Par2FileRef{Name: "x.vol0+1.par2", Size: volSize}, count: 1}
	in := append(vols, named)
	out := recountSizedVolumes(in, files, slice)
	for _, v := range out[:2] {
		if v.count < held || v.count > held+1 {
			t.Errorf("%s recounted at %d slices, want %d (at most one over)", v.ref.Name, v.count, held)
		}
	}
	if out[2].count != 1 {
		t.Errorf("named volume recounted to %d, want its name's 1", out[2].count)
	}
	if in[0].count != 1 {
		t.Errorf("input changed: %d slices, want the census's 1 (the recount copies)", in[0].count)
	}
	if got := par2VolumeSlices(out); got != out[0].count+out[1].count+1 {
		t.Errorf("par2VolumeSlices = %d, want the sum %d", got, out[0].count+out[1].count+1)
	}

	// A count the census already has above the recount is kept.
	high := []par2Volume{{ref: files[1], count: 1000, sized: true}}
	if got := recountSizedVolumes(high, files, slice); got[0].count != 1000 {
		t.Errorf("recount lowered a census count to %d, want 1000 kept", got[0].count)
	}
}
