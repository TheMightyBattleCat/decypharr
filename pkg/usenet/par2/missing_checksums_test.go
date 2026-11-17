package par2

import (
	"context"
	"testing"
)

// dropIFSC returns sources with every IFSC packet for fileID removed.
func dropIFSC(t *testing.T, sources []Source, fileID [16]byte) []Source {
	t.Helper()
	out := make([]Source, 0, len(sources))
	for _, src := range sources {
		var data []byte
		err := walkPackets(context.Background(), src.Data, nil, func(h packetHeader, packet []byte, _ int64) error {
			if h.Type == typeIFSC && [16]byte(packet[packetHeaderSize:packetHeaderSize+16]) == fileID {
				return nil
			}
			data = append(data, packet...)
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", src.Name, err)
		}
		out = append(out, Source{Name: src.Name, Data: data})
	}
	return out
}

// A set whose sources hold no IFSC packet for one file parses, and
// MissingChecksums names that file and no other.
func TestMissingChecksumsNamesFileWithoutIFSC(t *testing.T) {
	sources := loadFixtureSources(t)
	full, err := ParseIndex(context.Background(), sources)
	if err != nil {
		t.Fatalf("ParseIndex: %v", err)
	}
	if got := full.MissingChecksums(); len(got) != 0 {
		t.Fatalf("complete set: MissingChecksums = %x, want none", got)
	}

	victim := full.FileOrder[len(full.FileOrder)-1]
	idx, err := ParseIndex(context.Background(), dropIFSC(t, sources, victim))
	if err != nil {
		t.Fatalf("ParseIndex without the victim's IFSC: %v", err)
	}
	got := idx.MissingChecksums()
	if len(got) != 1 || got[0] != victim {
		t.Fatalf("MissingChecksums = %x, want only %x", got, victim)
	}
}

// A file with fewer checksums than its length needs is listed too: a
// truncated IFSC packet would fail the stream as surely as a missing one.
func TestMissingChecksumsShortIFSC(t *testing.T) {
	idx, err := ParseIndex(context.Background(), loadFixtureSources(t))
	if err != nil {
		t.Fatalf("ParseIndex: %v", err)
	}
	victim := idx.FileOrder[0]
	idx.Slices[victim] = idx.Slices[victim][:len(idx.Slices[victim])-1]
	got := idx.MissingChecksums()
	if len(got) != 1 || got[0] != victim {
		t.Fatalf("MissingChecksums = %x, want only %x", got, victim)
	}
}
