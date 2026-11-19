package par2

import (
	"context"
	"encoding/binary"
	"testing"
)

// A RecvSlic packet whose data is not exactly one slice is not a recovery
// slice: the parse skips it, reports it, and the repair uses the real ones.
func TestParseIndexDropsWrongLengthRecoveryPacket(t *testing.T) {
	gf := loadGoldenFixture(t)
	real := len(gf.idx.Recovery)

	body := make([]byte, 4+8) // 8 bytes of data, not a slice
	binary.LittleEndian.PutUint32(body, 0)
	crafted := Source{Name: "crafted.vol.par2", Data: buildPacket(t, gf.idx.SetID, typeRecvSlic, body)}
	srcs := append([]Source{crafted}, gf.sources...)

	idx, err := ParseIndex(context.Background(), srcs)
	if err != nil {
		t.Fatalf("ParseIndex: %v", err)
	}
	if len(idx.Recovery) != real {
		t.Fatalf("recovery refs = %d, want the real count %d", len(idx.Recovery), real)
	}
	for _, r := range idx.Recovery {
		if r.Length != idx.SliceSize {
			t.Fatalf("kept a recovery ref of %d bytes, slice size %d", r.Length, idx.SliceSize)
		}
	}
	var found bool
	for _, sp := range idx.SkippedPackets {
		if sp.Type == "RecvSlic" && sp.Source == 0 && sp.Offset == 0 {
			found = true
		}
	}
	if !found {
		t.Fatalf("crafted packet not reported in SkippedPackets: %+v", idx.SkippedPackets)
	}

	damaged := []int64{0, 1}
	exps := make([]uint32, len(idx.Recovery))
	for i, r := range idx.Recovery {
		exps[i] = r.Exponent
	}
	picked, err := PickRecovery(damaged, exps, len(damaged))
	if err != nil {
		t.Fatalf("PickRecovery: %v", err)
	}
	recovery := make([]RecoverySlice, len(picked))
	for i, j := range picked {
		ref := idx.Recovery[j]
		recovery[i] = RecoverySlice{Exponent: ref.Exponent, Data: srcs[ref.Source].Data[ref.Offset : ref.Offset+ref.Length]}
	}
	dmg := map[int64]struct{}{0: {}, 1: {}}
	if _, err := Repair(idx, damaged, recovery, &fixtureSliceSource{idx: idx, damaged: dmg, byFile: gf.byFile}); err != nil {
		t.Fatalf("Repair with the picked refs: %v", err)
	}
}
