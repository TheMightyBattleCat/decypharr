package parser

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/usenet/types"
)

// A RAR 3.x/4.x archive comment is a 0x7A "CMT" sub-block right after the main
// header, with its data after its header. The stream walker must skip that
// data and still reach the file header.
func TestRAR4CommentBlockBeforeFileHeader(t *testing.T) {
	orig := rarFixture(t, "proof4.rar")
	mainSize := int(binary.LittleEndian.Uint16(orig[7+5:]))
	cut := 7 + mainSize

	comment := []byte("posted by someone, enjoy the release")
	hdr := make([]byte, 0, 35)
	hdr = append(hdr, 0, 0) // CRC (not checked by the parser)
	hdr = append(hdr, 0x7A) // sub-block
	hdr = binary.LittleEndian.AppendUint16(hdr, 0x8000)
	hdr = binary.LittleEndian.AppendUint16(hdr, 35)
	hdr = binary.LittleEndian.AppendUint32(hdr, uint32(len(comment))) // PACK_SIZE
	hdr = binary.LittleEndian.AppendUint32(hdr, uint32(len(comment))) // UNP_SIZE
	hdr = append(hdr, 2)                                              // host OS
	hdr = append(hdr, 0, 0, 0, 0)                                     // CRC
	hdr = append(hdr, 0, 0, 0, 0)                                     // time
	hdr = append(hdr, 29, 0x30)                                       // version, method
	hdr = binary.LittleEndian.AppendUint16(hdr, 3)                    // name size
	hdr = append(hdr, 0, 0, 0, 0)                                     // attr
	hdr = append(hdr, 'C', 'M', 'T')
	if len(hdr) != 35 {
		t.Fatalf("hdr %d", len(hdr))
	}
	withComment := append(append(append([]byte{}, orig[:cut]...), append(hdr, comment...)...), orig[cut:]...)

	var wantName string
	var wantSize int64
	for i, data := range [][]byte{orig, withComment} {
		posted := newPostedArticles()
		vols := []*types.Volume{posted.volume("proof.rar", data, 1000)}
		p := &RARParser{logger: zerolog.Nop(), maxConcurrent: 2, fetchBody: posted.fetch}
		info, err := p.parseArchive(context.Background(), vols, "", true)
		if err != nil {
			t.Fatalf("variant %d: parse error: %v", i, err)
		}
		if len(info.Files) != 1 {
			t.Fatalf("variant %d: files = %d, want 1", i, len(info.Files))
		}
		f := info.Files[0]
		if i == 0 {
			wantName, wantSize = f.Name, f.UncompressedSize
			continue
		}
		if f.Name != wantName || f.UncompressedSize != wantSize {
			t.Fatalf("with comment: got %q (%d), want %q (%d)", f.Name, f.UncompressedSize, wantName, wantSize)
		}
	}
}
