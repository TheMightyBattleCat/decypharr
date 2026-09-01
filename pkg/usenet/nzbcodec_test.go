package usenet

import (
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

func sampleNZBWithPar2() *storage.NZB {
	return &storage.NZB{
		ID:         "nzb-1",
		Name:       "Some.Release.2024",
		TotalSize:  100,
		DatePosted: time.Unix(1000, 0),
		AddedOn:    time.Unix(1000, 0),
		Files: []storage.NZBFile{
			{
				Name: "Some.Release.2024.mkv",
				Size: 100,
				Segments: []storage.NZBSegment{
					{Number: 1, MessageID: "<seg1@example>", Bytes: 100, EndOffset: 99},
				},
			},
		},
		Par2Files: []storage.Par2FileRef{
			{
				Name: "Some.Release.2024.par2",
				Size: 5000,
				Segments: []storage.Par2SegmentRef{
					{MessageID: "<par2idx@example>", Bytes: 5000},
				},
			},
			{
				Name: "Some.Release.2024.vol000+01.par2",
				Size: 8000,
				Segments: []storage.Par2SegmentRef{
					{MessageID: "<par2vol@example>", Bytes: 8000},
				},
			},
		},
		Par2Source: []storage.PostedFileRef{
			{
				Name: "Some.Release.2024.rar",
				Size: 15_000_000,
				Segments: []storage.Par2SegmentRef{
					{MessageID: "<rar1@example>", Bytes: 750_000},
					{MessageID: "<rar2@example>", Bytes: 750_000},
				},
			},
		},
	}
}

func TestNZBCodecV2RoundTripsPar2Fields(t *testing.T) {
	nzb := sampleNZBWithPar2()

	data, err := encodeNZBV2(nzb)
	if err != nil {
		t.Fatalf("encodeNZBV2: %v", err)
	}
	if !isCodecV2(data) {
		t.Fatalf("encoded data is not recognized as v2")
	}

	got, err := decodeNZBV2(data)
	if err != nil {
		t.Fatalf("decodeNZBV2: %v", err)
	}

	if len(got.Par2Files) != len(nzb.Par2Files) {
		t.Fatalf("Par2Files len = %d, want %d", len(got.Par2Files), len(nzb.Par2Files))
	}
	for i, want := range nzb.Par2Files {
		if got.Par2Files[i].Name != want.Name || got.Par2Files[i].Size != want.Size {
			t.Errorf("Par2Files[%d] = %+v, want %+v", i, got.Par2Files[i], want)
		}
		if len(got.Par2Files[i].Segments) != len(want.Segments) {
			t.Fatalf("Par2Files[%d].Segments len = %d, want %d", i, len(got.Par2Files[i].Segments), len(want.Segments))
		}
		for j, wantSeg := range want.Segments {
			if got.Par2Files[i].Segments[j] != wantSeg {
				t.Errorf("Par2Files[%d].Segments[%d] = %+v, want %+v", i, j, got.Par2Files[i].Segments[j], wantSeg)
			}
		}
	}

	if len(got.Par2Source) != len(nzb.Par2Source) {
		t.Fatalf("Par2Source len = %d, want %d", len(got.Par2Source), len(nzb.Par2Source))
	}
	for i, want := range nzb.Par2Source {
		if got.Par2Source[i].Name != want.Name || got.Par2Source[i].Size != want.Size {
			t.Errorf("Par2Source[%d] = %+v, want %+v", i, got.Par2Source[i], want)
		}
		for j, wantSeg := range want.Segments {
			if got.Par2Source[i].Segments[j] != wantSeg {
				t.Errorf("Par2Source[%d].Segments[%d] = %+v, want %+v", i, j, got.Par2Source[i].Segments[j], wantSeg)
			}
		}
	}

	// Header-only decode must also see the (small) PAR2 lists, since they
	// live in the header region alongside the per-file metadata.
	headerOnly, err := decodeNZBV2Header(data)
	if err != nil {
		t.Fatalf("decodeNZBV2Header: %v", err)
	}
	if len(headerOnly.Par2Files) != len(nzb.Par2Files) || len(headerOnly.Par2Source) != len(nzb.Par2Source) {
		t.Fatalf("header-only decode missing PAR2 fields: got %d/%d, want %d/%d",
			len(headerOnly.Par2Files), len(headerOnly.Par2Source), len(nzb.Par2Files), len(nzb.Par2Source))
	}
}

func TestNZBCodecV2WithoutPar2FieldsRoundTrips(t *testing.T) {
	nzb := sampleNZBWithPar2()
	nzb.Par2Files = nil
	nzb.Par2Source = nil

	data, err := encodeNZBV2(nzb)
	if err != nil {
		t.Fatalf("encodeNZBV2: %v", err)
	}
	got, err := decodeNZBV2(data)
	if err != nil {
		t.Fatalf("decodeNZBV2: %v", err)
	}
	if len(got.Par2Files) != 0 || len(got.Par2Source) != 0 {
		t.Fatalf("expected empty Par2Files/Par2Source, got %d/%d", len(got.Par2Files), len(got.Par2Source))
	}
}

// TestNZBCodecV2DecodesPreExistingBlobsWithoutPar2Trailer reproduces a header
// blob byte-for-byte as encodeHeader produced it BEFORE Par2Files/Par2Source
// existed (no trailing PAR2 section at all, not even empty-count markers), to
// prove decodeHeader's r.pos < len(buf) backward-compat check actually
// distinguishes "field predates this record" from "field is empty" rather
// than merely happening to work when both encode and decode are current.
func TestNZBCodecV2DecodesPreExistingBlobsWithoutPar2Trailer(t *testing.T) {
	nzb := sampleNZBWithPar2()

	w := &byteWriter{}
	w.str(nzb.ID)
	w.str(nzb.Name)
	w.str(nzb.Title)
	w.str(nzb.Path)
	w.varint(nzb.TotalSize)
	w.varint(nzb.DatePosted.Unix())
	w.str(nzb.Category)
	w.uvarint(uint64(len(nzb.Groups)))
	for _, g := range nzb.Groups {
		w.str(g)
	}
	w.boolean(nzb.Downloaded)
	w.varint(nzb.AddedOn.Unix())
	w.varint(nzb.LastActivity.Unix())
	w.str(nzb.Status)
	w.f64(nzb.Progress)
	w.f64(nzb.Percentage)
	w.varint(nzb.SizeDownloaded)
	w.varint(nzb.ETA)
	w.varint(nzb.Speed)
	w.varint(nzb.CompletedOn.Unix())
	w.boolean(nzb.IsBad)
	w.str(nzb.Storage)
	w.str(nzb.FailMessage)
	w.str(nzb.Password)
	w.uvarint(uint64(len(nzb.Files)))
	for i := range nzb.Files {
		f := &nzb.Files[i]
		w.str(f.Name)
		w.str(f.InternalPath)
		w.varint(f.Size)
		w.varint(f.StartOffset)
		w.uvarint(uint64(len(f.Groups)))
		for _, g := range f.Groups {
			w.str(g)
		}
		w.str(string(f.FileType))
		w.str(f.Password)
		w.boolean(f.IsDeleted)
		w.boolean(f.IsStored)
		w.varint(f.SegmentSize)
		w.raw(f.EncryptionKey)
		w.raw(f.EncryptionIV)
		w.boolean(f.IsEncrypted)
		w.uvarint(uint64(len(f.Segments)))
	}
	// Deliberately NO trailing PAR2 section here - this is exactly what
	// encodeHeader produced before Par2Files/Par2Source existed.

	got, counts, err := decodeHeader(w.buf)
	if err != nil {
		t.Fatalf("decodeHeader on a pre-existing-format blob: %v", err)
	}
	if len(counts) != len(nzb.Files) {
		t.Fatalf("counts len = %d, want %d", len(counts), len(nzb.Files))
	}
	if got.Par2Files != nil || got.Par2Source != nil {
		t.Fatalf("expected nil Par2Files/Par2Source decoding a pre-existing blob, got %v / %v", got.Par2Files, got.Par2Source)
	}
	if got.ID != nzb.ID || got.Name != nzb.Name {
		t.Fatalf("basic header fields did not round-trip: got ID=%q Name=%q", got.ID, got.Name)
	}
}

// TestNZBCodecV2RoundTripsPar2SegmentReal checks the trailing Real-provenance
// bitset: a mix of Real true/false across both Par2Files and Par2Source
// segments must survive a full encode/decode in the same flattened order.
func TestNZBCodecV2RoundTripsPar2SegmentReal(t *testing.T) {
	nzb := sampleNZBWithPar2()
	nzb.Par2Files[0].Segments[0].Real = true   // idx 0
	nzb.Par2Files[1].Segments[0].Real = false  // idx 1
	nzb.Par2Source[0].Segments[0].Real = false // idx 2
	nzb.Par2Source[0].Segments[1].Real = true  // idx 3

	data, err := encodeNZBV2(nzb)
	if err != nil {
		t.Fatalf("encodeNZBV2: %v", err)
	}
	got, err := decodeNZBV2(data)
	if err != nil {
		t.Fatalf("decodeNZBV2: %v", err)
	}

	if !got.Par2Files[0].Segments[0].Real || got.Par2Files[1].Segments[0].Real {
		t.Errorf("Par2Files Real = [%v %v], want [true false]",
			got.Par2Files[0].Segments[0].Real, got.Par2Files[1].Segments[0].Real)
	}
	if got.Par2Source[0].Segments[0].Real || !got.Par2Source[0].Segments[1].Real {
		t.Errorf("Par2Source[0] Real = [%v %v], want [false true]",
			got.Par2Source[0].Segments[0].Real, got.Par2Source[0].Segments[1].Real)
	}

	// The header-only path must decode the same trailer without consuming the
	// segment map.
	headerOnly, err := decodeNZBV2Header(data)
	if err != nil {
		t.Fatalf("decodeNZBV2Header: %v", err)
	}
	if !headerOnly.Par2Files[0].Segments[0].Real || !headerOnly.Par2Source[0].Segments[1].Real {
		t.Errorf("header-only Real flags did not round-trip: %+v / %+v",
			headerOnly.Par2Files[0].Segments[0], headerOnly.Par2Source[0].Segments[1])
	}
}

// TestNZBCodecV2DecodesPar2BlobWithoutRealBitset reproduces a blob written
// AFTER Par2Files/Par2Source landed but BEFORE the Real bitset trailer: the
// PAR2 refs are present, no bitset follows. decodeHeader must accept it and
// leave every segment's Real=false.
func TestNZBCodecV2DecodesPar2BlobWithoutRealBitset(t *testing.T) {
	nzb := sampleNZBWithPar2()
	nzb.Par2Files[0].Segments[0].Real = true // ignored: not serialized here

	w := &byteWriter{}
	w.str(nzb.ID)
	w.str(nzb.Name)
	w.str(nzb.Title)
	w.str(nzb.Path)
	w.varint(nzb.TotalSize)
	w.varint(nzb.DatePosted.Unix())
	w.str(nzb.Category)
	w.uvarint(uint64(len(nzb.Groups)))
	for _, g := range nzb.Groups {
		w.str(g)
	}
	w.boolean(nzb.Downloaded)
	w.varint(nzb.AddedOn.Unix())
	w.varint(nzb.LastActivity.Unix())
	w.str(nzb.Status)
	w.f64(nzb.Progress)
	w.f64(nzb.Percentage)
	w.varint(nzb.SizeDownloaded)
	w.varint(nzb.ETA)
	w.varint(nzb.Speed)
	w.varint(nzb.CompletedOn.Unix())
	w.boolean(nzb.IsBad)
	w.str(nzb.Storage)
	w.str(nzb.FailMessage)
	w.str(nzb.Password)
	w.uvarint(uint64(len(nzb.Files)))
	for i := range nzb.Files {
		f := &nzb.Files[i]
		w.str(f.Name)
		w.str(f.InternalPath)
		w.varint(f.Size)
		w.varint(f.StartOffset)
		w.uvarint(uint64(len(f.Groups)))
		for _, g := range f.Groups {
			w.str(g)
		}
		w.str(string(f.FileType))
		w.str(f.Password)
		w.boolean(f.IsDeleted)
		w.boolean(f.IsStored)
		w.varint(f.SegmentSize)
		w.raw(f.EncryptionKey)
		w.raw(f.EncryptionIV)
		w.boolean(f.IsEncrypted)
		w.uvarint(uint64(len(f.Segments)))
	}
	// PAR2 refs, but deliberately NO Real bitset trailer.
	writePar2FileRefs(w, nzb.Par2Files)
	writePar2FileRefs(w, nzb.Par2Source)

	got, _, err := decodeHeader(w.buf)
	if err != nil {
		t.Fatalf("decodeHeader on a par2-without-bitset blob: %v", err)
	}
	if len(got.Par2Files) != 2 || len(got.Par2Source) != 1 {
		t.Fatalf("PAR2 refs did not round-trip: %d / %d", len(got.Par2Files), len(got.Par2Source))
	}
	for _, f := range got.Par2Files {
		for _, s := range f.Segments {
			if s.Real {
				t.Errorf("Par2Files segment Real = true, want false (no bitset present)")
			}
		}
	}
	for _, f := range got.Par2Source {
		for _, s := range f.Segments {
			if s.Real {
				t.Errorf("Par2Source segment Real = true, want false (no bitset present)")
			}
		}
	}
	if got.Par2Match != nil {
		t.Errorf("Par2Match = %v, want nil (no trailer present)", got.Par2Match)
	}
}

// TestNZBCodecV2RoundTripsPar2Match checks the posted-file match cache
// trailer round-trips through a full encode/decode and a header-only decode.
func TestNZBCodecV2RoundTripsPar2Match(t *testing.T) {
	nzb := sampleNZBWithPar2()
	nzb.Par2Match = []storage.Par2MatchRef{
		{PostedName: "Some.Release.2024.rar", FileID: [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, NameMismatch: false},
		{PostedName: "Some.Release.2024.r00", FileID: [16]byte{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}, NameMismatch: true},
	}

	data, err := encodeNZBV2(nzb)
	if err != nil {
		t.Fatalf("encodeNZBV2: %v", err)
	}
	got, err := decodeNZBV2(data)
	if err != nil {
		t.Fatalf("decodeNZBV2: %v", err)
	}
	if len(got.Par2Match) != len(nzb.Par2Match) {
		t.Fatalf("Par2Match len = %d, want %d", len(got.Par2Match), len(nzb.Par2Match))
	}
	for i, want := range nzb.Par2Match {
		if got.Par2Match[i] != want {
			t.Errorf("Par2Match[%d] = %+v, want %+v", i, got.Par2Match[i], want)
		}
	}

	headerOnly, err := decodeNZBV2Header(data)
	if err != nil {
		t.Fatalf("decodeNZBV2Header: %v", err)
	}
	if len(headerOnly.Par2Match) != len(nzb.Par2Match) {
		t.Fatalf("header-only Par2Match len = %d, want %d", len(headerOnly.Par2Match), len(nzb.Par2Match))
	}
	for i, want := range nzb.Par2Match {
		if headerOnly.Par2Match[i] != want {
			t.Errorf("header-only Par2Match[%d] = %+v, want %+v", i, headerOnly.Par2Match[i], want)
		}
	}
}

// TestNZBCodecV2RoundTripsWithoutPar2Match confirms the common case - PAR2
// refs and Real bitset present, no match cache yet - encodes and decodes
// cleanly with Par2Match nil.
func TestNZBCodecV2RoundTripsWithoutPar2Match(t *testing.T) {
	nzb := sampleNZBWithPar2()
	nzb.Par2Files[0].Segments[0].Real = true

	data, err := encodeNZBV2(nzb)
	if err != nil {
		t.Fatalf("encodeNZBV2: %v", err)
	}
	got, err := decodeNZBV2(data)
	if err != nil {
		t.Fatalf("decodeNZBV2: %v", err)
	}
	if got.Par2Match != nil {
		t.Fatalf("Par2Match = %v, want nil", got.Par2Match)
	}
	if !got.Par2Files[0].Segments[0].Real {
		t.Errorf("Real bitset did not round-trip alongside an empty match cache")
	}
}

// TestNZBCodecV2DecodesPar2BlobWithoutMatchTrailer reproduces a blob written
// AFTER the Real bitset landed but BEFORE the match-cache trailer: refs and
// bitset present, nothing after. decodeHeader must accept it, Par2Match nil.
func TestNZBCodecV2DecodesPar2BlobWithoutMatchTrailer(t *testing.T) {
	nzb := sampleNZBWithPar2()
	nzb.Par2Source[0].Segments[1].Real = true

	w := &byteWriter{}
	w.str(nzb.ID)
	w.str(nzb.Name)
	w.str(nzb.Title)
	w.str(nzb.Path)
	w.varint(nzb.TotalSize)
	w.varint(nzb.DatePosted.Unix())
	w.str(nzb.Category)
	w.uvarint(uint64(len(nzb.Groups)))
	for _, g := range nzb.Groups {
		w.str(g)
	}
	w.boolean(nzb.Downloaded)
	w.varint(nzb.AddedOn.Unix())
	w.varint(nzb.LastActivity.Unix())
	w.str(nzb.Status)
	w.f64(nzb.Progress)
	w.f64(nzb.Percentage)
	w.varint(nzb.SizeDownloaded)
	w.varint(nzb.ETA)
	w.varint(nzb.Speed)
	w.varint(nzb.CompletedOn.Unix())
	w.boolean(nzb.IsBad)
	w.str(nzb.Storage)
	w.str(nzb.FailMessage)
	w.str(nzb.Password)
	w.uvarint(uint64(len(nzb.Files)))
	for i := range nzb.Files {
		f := &nzb.Files[i]
		w.str(f.Name)
		w.str(f.InternalPath)
		w.varint(f.Size)
		w.varint(f.StartOffset)
		w.uvarint(uint64(len(f.Groups)))
		for _, g := range f.Groups {
			w.str(g)
		}
		w.str(string(f.FileType))
		w.str(f.Password)
		w.boolean(f.IsDeleted)
		w.boolean(f.IsStored)
		w.varint(f.SegmentSize)
		w.raw(f.EncryptionKey)
		w.raw(f.EncryptionIV)
		w.boolean(f.IsEncrypted)
		w.uvarint(uint64(len(f.Segments)))
	}
	// PAR2 refs + Real bitset, but deliberately NO match-cache trailer.
	writePar2FileRefs(w, nzb.Par2Files)
	writePar2FileRefs(w, nzb.Par2Source)
	writeRealBitset(w, par2RealTargets(nzb.Par2Files, nzb.Par2Source)...)

	got, _, err := decodeHeader(w.buf)
	if err != nil {
		t.Fatalf("decodeHeader on a par2-without-match-trailer blob: %v", err)
	}
	if got.Par2Match != nil {
		t.Fatalf("Par2Match = %v, want nil (no trailer present)", got.Par2Match)
	}
	if !got.Par2Source[0].Segments[1].Real {
		t.Errorf("Real bitset did not round-trip without a match trailer")
	}
}
