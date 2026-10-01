package manager

import "testing"

// A byte-ranged file is one file inside a larger download (a stored RAR on
// Real-Debrid that the provider did not unpack): its bytes sit at
// [ByteRange[0], ByteRange[1]] of the link, and File.Size is its own length.
//
// Manager.Stream takes offsets and checks them against File.Size with
// normalizeStreamRange, then asks the link for exactly that range. These
// cases put the offsets each caller sends today through that check. want*
// is what the link request has to be for the client to get the right bytes.
//
// This is arithmetic on the range check alone: it does not drive
// Manager.Stream (which needs a live link) or the callers. The WebDAV
// offsets are the ones webdav.TestResolveRange pins; the mount's and the
// pipe's were read from the code (downloaders.go and share_stream.go pass
// offsets inside the file). A fix that moves the offset inside Stream
// leaves normalizeStreamRange as it is, so it needs its own test of the
// range sent to the link, and TestResolveRange is the one that changes.
func TestFollowupByteRangedFileStreamRange(t *testing.T) {
	const (
		size   = int64(1000) // the file's own length
		offset = int64(4000) // where it starts inside the link
	)
	br := [2]int64{offset, offset + size - 1}

	cases := []struct {
		name       string
		start, end int64 // what the caller hands Manager.Stream
		wantStart  int64 // correct first byte of the link request
		wantEnd    int64 // correct last byte of the link request
		gotErr     bool
		gotStart   int64
		gotEnd     int64
		defect     string
	}{
		{
			// webdav.resolveRange with no Range header: the slice itself.
			name: "webdav whole file", start: br[0], end: br[1],
			wantStart: 4000, wantEnd: 4999,
			gotErr: true,
			defect: "the slice start is checked against the file's own size and refused",
		},
		{
			// webdav.resolveRange with "bytes=0-99": moved by the slice start.
			name: "webdav first 100 bytes", start: offset, end: offset + 99,
			wantStart: 4000, wantEnd: 4099,
			gotErr: true,
			defect: "same refusal",
		},
		{
			// The DFS mount and the share / .strm pipe hand over offsets
			// inside the file and never add the slice start.
			name: "mount first 100 bytes", start: 0, end: 99,
			wantStart: 4000, wantEnd: 4099,
			gotStart: 0, gotEnd: 99,
			defect: "the link is read from its own start: the client gets the archive header, not the file",
		},
		{
			name: "mount last 100 bytes", start: 900, end: 999,
			wantStart: 4900, wantEnd: 4999,
			gotStart: 900, gotEnd: 999,
			defect: "same shift",
		},
	}

	for _, c := range cases {
		start, end, err := normalizeStreamRange(size, c.start, c.end)
		if (err != nil) != c.gotErr {
			t.Errorf("%s: err = %v, recorded gotErr = %v", c.name, err, c.gotErr)
			continue
		}
		if err == nil && (start != c.gotStart || end != c.gotEnd) {
			t.Errorf("%s: link request %d-%d, recorded %d-%d", c.name, start, end, c.gotStart, c.gotEnd)
		}
		right := err == nil && start == c.wantStart && end == c.wantEnd
		if right {
			t.Errorf("%s: now correct (%d-%d); drop the defect note", c.name, start, end)
		}
		t.Logf("DEFECT (recorded): %s: sent %d-%d, link needs %d-%d: %s", c.name, c.start, c.end, c.wantStart, c.wantEnd, c.defect)
	}
}

// With a small slice start (one file stored at the front of an archive, the
// usual case) the WebDAV path is not refused; it loses the file's tail
// instead.
func TestFollowupByteRangedFileSmallOffsetLosesTail(t *testing.T) {
	const (
		size   = int64(1000)
		offset = int64(64) // an archive header's worth
	)
	// webdav.resolveRange, no Range header: (offset, offset+size-1).
	start, end, err := normalizeStreamRange(size, offset, offset+size-1)
	if err != nil {
		t.Fatal(err)
	}
	if start != 64 || end != 999 {
		t.Fatalf("link request %d-%d, recorded 64-999", start, end)
	}
	t.Logf("DEFECT (recorded): whole file asked, link request %d-%d: %d bytes served, the last %d of the file are missing", start, end, end-start+1, offset)

	// A tail read ("bytes=950-999", what a player does for an MKV index).
	if _, _, err := normalizeStreamRange(size, offset+950, offset+999); err == nil {
		t.Fatal("tail read now succeeds; drop the defect note")
	}
	t.Log("DEFECT (recorded): a read of the file's last 50 bytes is refused as beyond the file size")
}
