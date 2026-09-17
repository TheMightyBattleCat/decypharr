package manager

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

const voArticle = 700_000

// voFile builds a stored RAR file whose volumes hold the given article counts,
// in stored order. Each volume's first segment has a data start (its headers)
// and its segments are numbered from 1; message IDs are "<tag>-<n>" where tag
// names the volume.
func voFile(tags []string, articles []int) *storage.NZBFile {
	f := &storage.NZBFile{Name: "f.mkv", FileType: storage.NZBFileTypeRar}
	for k, tag := range tags {
		for n := 1; n <= articles[k]; n++ {
			s := storage.NZBSegment{Number: n, MessageID: fmt.Sprintf("%s-%d", tag, n), Bytes: voArticle}
			if n == 1 {
				s.SegmentDataStart = 100
				s.Bytes = voArticle - 100
			}
			f.Segments = append(f.Segments, s)
		}
	}
	return f
}

func rar5Head(num int) []byte {
	b := []byte("Rar!\x1a\x07\x01\x00")
	b = append(b, 0, 0, 0, 0, 0x08, 0x01, 0x00) // crc, size, type 1 (main), flags
	if num == 0 {
		return append(b, 0x01) // volume, no number field: the first volume
	}
	return append(b, 0x03, byte(num))
}

func rar4Head(first bool) []byte {
	flags := uint16(0x0001)
	if first {
		flags |= 0x0100
	}
	b := []byte("Rar!\x1a\x07\x00")
	b = append(b, 0, 0, 0x73)
	b = binary.LittleEndian.AppendUint16(b, flags)
	return binary.LittleEndian.AppendUint16(b, 13)
}

// rar4End is a volume's decoded tail ending in an end-of-archive block that
// stores num, with a valid header CRC.
func rar4End(num int) []byte {
	body := []byte{0x7B}
	body = binary.LittleEndian.AppendUint16(body, 0x400F)
	body = binary.LittleEndian.AppendUint16(body, 20)
	body = append(body, 1, 2, 3, 4) // data CRC
	body = binary.LittleEndian.AppendUint16(body, uint16(num))
	body = append(body, make([]byte, 7)...)
	crc := uint16(crc32.ChecksumIEEE(body))
	out := append([]byte("file data..."), byte(crc), byte(crc>>8))
	return append(out, body...)
}

type fakeHeads map[string]usenet.ArticleHead

func (h fakeHeads) head(_ context.Context, id string) (usenet.ArticleHead, error) {
	a, ok := h[id]
	if !ok {
		return usenet.ArticleHead{}, errors.New("430 no such article")
	}
	return a, nil
}

func TestShortVolumeBeforeFull(t *testing.T) {
	cases := []struct {
		name     string
		articles []int
		want     bool
	}{
		{"in order, final volume last", []int{10, 10, 10, 4}, false},
		// Lamplight: the short final volume stored before a full one.
		{"final volume before a full one", []int{10, 10, 4, 10}, true},
		// Kivi: the final volume far from the end.
		{"final volume in the middle", []int{10, 4, 10, 10, 10}, true},
		// A season-wide set's episode starts and ends mid-volume.
		{"partial first and last volume", []int{3, 10, 10, 10, 6}, false},
		{"two volumes", []int{4, 10}, false},
		{"no full size repeats", []int{10, 8, 6, 4}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tags := make([]string, len(c.articles))
			for k := range tags {
				tags[k] = fmt.Sprint("v", k)
			}
			f := voFile(tags, c.articles)
			if got := shortVolumeBeforeFull(layoutVolumes(f.Segments)); got != c.want {
				t.Fatalf("got %v, want %v (volumes %+v)", got, c.want, layoutVolumes(f.Segments))
			}
			if got := classifyGeometry(f, nil).shortVolume; got != c.want {
				t.Fatalf("classifyGeometry shortVolume %v, want %v", got, c.want)
			}
		})
	}
}

// Tide on Sark S02E01's .r00 volume stored as a file of its own starts
// mid-archive; its .rar volume, and a file whose header is not the one its
// data follows, do not.
func TestStartsMidArchive(t *testing.T) {
	mustHex := func(s string) []byte {
		b, err := hex.DecodeString(s)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	const tosRar = "526172211a0700b2ef7301010d00000000000000f8d974c291440014f0fa027db835fe02ee993c46864a89401430170020000000000000000000000074656e6569676874792d746f737330326530312e6d6b7600b01025761a45dfa3"
	const tosR00 = "526172211a0700f1fb7301000d00000000000000564174c391440014f0fa027db835fe026f7fe593864a89401430170020000000000000000000000074656e6569676874792d746f737330326530312e6d6b7600b010257653868dc9"
	file := func(sds int64) *storage.NZBFile {
		return &storage.NZBFile{Name: "teneighty-toss02e01.mkv", FileType: storage.NZBFileTypeRar,
			Segments: []storage.NZBSegment{{Number: 1, SegmentDataStart: sds, Bytes: 767912}}}
	}
	cases := []struct {
		name   string
		f      *storage.NZBFile
		prefix string
		want   bool
	}{
		{".r00 stored alone", file(88), tosR00, true},
		{".rar volume", file(88), tosRar, false},
		// A season-set episode starting mid-article after another file's
		// continued data: that header's data is not where this file starts.
		{"another file's header", file(500_000), tosR00, false},
		{"no data start", file(0), tosR00, false},
		{"not RAR", &storage.NZBFile{FileType: storage.NZBFileTypeMedia, Segments: []storage.NZBSegment{{SegmentDataStart: 88}}}, tosR00, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := startsMidArchive(c.f, mustHex(c.prefix)); got != c.want {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}

func TestLayoutVolumes(t *testing.T) {
	f := voFile([]string{"a", "b"}, []int{3, 2})
	// A volume stored without its header article still splits on the number.
	f.Segments = append(f.Segments, storage.NZBSegment{Number: 1, MessageID: "c-1", Bytes: 5}, storage.NZBSegment{Number: 2, MessageID: "c-2", Bytes: 5})
	vols := layoutVolumes(f.Segments)
	want := []layoutVolume{{first: 0, segs: 3, bytes: 3*voArticle - 100, header: true}, {first: 3, segs: 2, bytes: 2*voArticle - 100, header: true}, {first: 5, segs: 2, bytes: 10}}
	if len(vols) != len(want) {
		t.Fatalf("got %+v", vols)
	}
	for k := range want {
		if vols[k] != want[k] {
			t.Fatalf("volume %d: got %+v, want %+v", k, vols[k], want[k])
		}
	}
}

func TestCheckFileVolumeOrder(t *testing.T) {
	ctx := context.Background()
	tags := []string{"a", "b", "c", "d"}
	articles := []int{3, 3, 3, 2}

	t.Run("rar5 in order", func(t *testing.T) {
		heads := fakeHeads{}
		for k, tag := range tags {
			heads[tag+"-1"] = usenet.ArticleHead{Prefix: rar5Head(k), Name: "obf" + tag, Part: 1}
		}
		v, err := checkFileVolumeOrder(ctx, voFile(tags, articles), nil, heads.head, nil)
		if err != nil || v.misordered || v.numbered != 4 || v.source != volumeSourceRAR5Header {
			t.Fatalf("got %+v, %v", v, err)
		}
	})

	// Long Crossing: adjacent swaps.
	t.Run("rar5 swapped", func(t *testing.T) {
		heads := fakeHeads{}
		for k, tag := range tags {
			heads[tag+"-1"] = usenet.ArticleHead{Prefix: rar5Head([]int{0, 1, 3, 2}[k]), Part: 1}
		}
		v, err := checkFileVolumeOrder(ctx, voFile(tags, articles), nil, heads.head, nil)
		if err != nil || !v.misordered || v.position != 4 || v.number != 2 || v.prev != 3 || v.outOfPlace != 1 {
			t.Fatalf("got %+v, %v", v, err)
		}
		if v.detail() != "stored volume 4 of 4 is archive volume 2, after 3; 1 of 4 numbered volumes out of place (rar5_header)" {
			t.Fatalf("detail %q", v.detail())
		}
	})

	// A reused message ID serving another upload's article says nothing.
	t.Run("foreign article ignored", func(t *testing.T) {
		heads := fakeHeads{}
		for k, tag := range tags {
			heads[tag+"-1"] = usenet.ArticleHead{Prefix: rar5Head(k), Part: 1}
		}
		heads["b-1"] = usenet.ArticleHead{Prefix: rar5Head(0), Name: "9ae7a116d98fab6b", Part: 3435}
		v, err := checkFileVolumeOrder(ctx, voFile(tags, articles), nil, heads.head, nil)
		if err != nil || v.misordered || v.numbered != 3 {
			t.Fatalf("got %+v, %v", v, err)
		}
	})

	// Lamplight: RAR4 headers carry no number; the yEnc names do.
	t.Run("rar4 by yenc names", func(t *testing.T) {
		heads := fakeHeads{}
		for k, tag := range tags {
			heads[tag+"-1"] = usenet.ArticleHead{Prefix: rar4Head(k == 0), Name: fmt.Sprintf("bpnq7qsg.part%02d.rar", []int{1, 2, 4, 3}[k]), Part: 1}
		}
		v, err := checkFileVolumeOrder(ctx, voFile(tags, articles), nil, heads.head, nil)
		if err != nil || !v.misordered || v.source != volumeSourceYencName || v.position != 4 {
			t.Fatalf("got %+v, %v", v, err)
		}
	})

	t.Run("names from two sets are not trusted", func(t *testing.T) {
		heads := fakeHeads{}
		for k, tag := range tags {
			heads[tag+"-1"] = usenet.ArticleHead{Prefix: rar4Head(k == 0), Name: fmt.Sprintf("set%d.part%02d.rar", k%2, k+1), Part: 1}
		}
		v, err := checkFileVolumeOrder(ctx, voFile(tags, articles), nil, heads.head, nil)
		if err != nil || v.source != volumeSourceRAR4First || v.misordered {
			t.Fatalf("got %+v, %v", v, err)
		}
	})

	// Kivi: obfuscated names, RAR4; the end-of-archive numbers via Par2Source.
	t.Run("rar4 by end-of-archive numbers", func(t *testing.T) {
		f := voFile(tags, articles)
		heads := fakeHeads{}
		var posted []storage.PostedFileRef
		bodies := map[string][]byte{}
		for k, tag := range tags {
			heads[tag+"-1"] = usenet.ArticleHead{Prefix: rar4Head(k == 0), Name: "obfuscated" + tag, Part: 1}
			p := storage.PostedFileRef{Name: "obfuscated" + tag}
			for n := 1; n <= articles[k]+1; n++ { // one more article: the recovery record
				p.Segments = append(p.Segments, storage.Par2SegmentRef{MessageID: fmt.Sprintf("%s-%d", tag, n)})
			}
			posted = append(posted, p)
			bodies[fmt.Sprintf("%s-%d", tag, articles[k]+1)] = rar4End([]int{0, 3, 1, 2}[k])
		}
		body := func(_ context.Context, id string) ([]byte, error) {
			if b, ok := bodies[id]; ok {
				return b, nil
			}
			return make([]byte, 64), nil
		}
		v, err := checkFileVolumeOrder(ctx, f, posted, heads.head, body)
		if err != nil || !v.misordered || v.source != volumeSourceRAR4End || v.position != 3 || v.number != 1 || v.prev != 3 {
			t.Fatalf("got %+v, %v", v, err)
		}
	})

	t.Run("rar4 first volume stored later", func(t *testing.T) {
		heads := fakeHeads{}
		for k, tag := range tags {
			heads[tag+"-1"] = usenet.ArticleHead{Prefix: rar4Head(k == 2), Name: "obfuscated" + tag, Part: 1}
		}
		v, err := checkFileVolumeOrder(ctx, voFile(tags, articles), nil, heads.head, nil)
		if err != nil || !v.misordered || v.source != volumeSourceRAR4First || v.position != 3 {
			t.Fatalf("got %+v, %v", v, err)
		}
	})

	t.Run("rar4 with nothing to order by", func(t *testing.T) {
		heads := fakeHeads{}
		for _, tag := range tags {
			heads[tag+"-1"] = usenet.ArticleHead{Prefix: rar4Head(false), Name: "obfuscated" + tag, Part: 1}
		}
		v, err := checkFileVolumeOrder(ctx, voFile(tags, articles), nil, heads.head, nil)
		if err != nil || v.misordered || v.source != "" {
			t.Fatalf("got %+v, %v", v, err)
		}
	})

	// Finding Ada: an episode starts inside a volume of a season-wide set; its
	// first stored article is not a volume's first.
	t.Run("file starting mid-volume", func(t *testing.T) {
		heads := fakeHeads{}
		for k, tag := range tags {
			heads[tag+"-1"] = usenet.ArticleHead{Prefix: rar5Head(65 + k), Part: 1}
		}
		heads["a-1"] = usenet.ArticleHead{Prefix: []byte("mid-volume data, a file header of the next episode"), Part: 40}
		v, err := checkFileVolumeOrder(ctx, voFile(tags, articles), nil, heads.head, nil)
		if err != nil || v.misordered || v.numbered != 3 {
			t.Fatalf("got %+v, %v", v, err)
		}
	})

	t.Run("fetch error, no verdict", func(t *testing.T) {
		heads := fakeHeads{"a-1": {Prefix: rar5Head(0), Part: 1}}
		if _, err := checkFileVolumeOrder(ctx, voFile(tags, articles), nil, heads.head, nil); err == nil {
			t.Fatal("a failed fetch gave a verdict")
		}
	})

	t.Run("single volume", func(t *testing.T) {
		v, err := checkFileVolumeOrder(ctx, voFile([]string{"a"}, []int{3}), nil, fakeHeads{}.head, nil)
		if err != nil || v.misordered || v.volumes != 1 {
			t.Fatalf("got %+v, %v", v, err)
		}
	})
}
