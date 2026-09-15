package parser

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/types"
)

// testdata/rarvol holds archives written by RAR 6.23 on a production install:
//
//	rar a -ma4 -m0 -v5k -vn rar4set.rar movie.mkv   (12,000 random bytes)
//	rar a -ma5 -m0 -v5k rar5set.rar movie.mkv
//	rar a -ma4 -m0 proof4.rar proof.jpg             (2,000 random bytes)
//	rar a -ma5 -m0 proof5.rar proof.jpg
func rarFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "rarvol", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// postedArticles splits data into articles of articleBytes and serves them.
type postedArticles struct {
	mu      sync.Mutex
	bodies  map[string][]byte
	fetched map[string]int
	fail    map[string]bool
}

func newPostedArticles() *postedArticles {
	return &postedArticles{bodies: map[string][]byte{}, fetched: map[string]int{}, fail: map[string]bool{}}
}

func (a *postedArticles) volume(name string, data []byte, articleBytes int) *types.Volume {
	v := &types.Volume{Name: name, Size: int64(len(data))}
	for i, off := 0, 0; off < len(data); i, off = i+1, off+articleBytes {
		end := min(off+articleBytes, len(data))
		id := fmt.Sprintf("<%s-%d>", name, i+1)
		a.bodies[id] = data[off:end]
		v.Segments = append(v.Segments, storage.NZBSegment{Number: i + 1, MessageID: id, Bytes: int64(end - off), StartOffset: int64(off), EndOffset: int64(end - 1)})
	}
	return v
}

func (a *postedArticles) fetch(_ context.Context, id string) ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.fetched[id]++
	if a.fail[id] {
		return nil, errors.New("simulated dead article")
	}
	b, ok := a.bodies[id]
	if !ok {
		return nil, errors.New("no such article " + id)
	}
	return append([]byte(nil), b...), nil
}

func TestRARArchiveIsVolume_RealHeaders(t *testing.T) {
	cases := []struct {
		file          string
		volume, known bool
	}{
		{"rar4set.rar", true, true},
		{"rar4set.r00", true, true},
		{"rar4set.r01", true, true},
		{"rar5set.part1.rar", true, true},
		{"rar5set.part3.rar", true, true},
		{"proof4.rar", false, true},
		{"proof5.rar", false, true},
	}
	for _, tc := range cases {
		data := rarFixture(t, tc.file)
		volume, known := rarArchiveIsVolume(data[:rarVolumeProbeBytes])
		if volume != tc.volume || known != tc.known {
			t.Errorf("%s: volume=%v known=%v, want %v %v", tc.file, volume, known, tc.volume, tc.known)
		}
	}
	if _, known := rarArchiveIsVolume([]byte("\x1aE\xdf\xa3 matroska, not an archive at all..")); known {
		t.Error("a Matroska header read as a RAR archive")
	}
}

func TestRAR4TailVolumeNumber_RealTails(t *testing.T) {
	for want, file := range []string{"rar4set.rar", "rar4set.r00", "rar4set.r01"} {
		data := rarFixture(t, file)
		got, ok := rar4TailVolumeNumber(data[len(data)-100:])
		if !ok || got != want {
			t.Errorf("%s: volume %d ok=%v, want %d", file, got, ok, want)
		}
	}
	if _, ok := rar4TailVolumeNumber(rarFixture(t, "proof4.rar")); ok {
		t.Error("proof4.rar is a single-volume archive; its end header stores no volume number")
	}
	data := rarFixture(t, "rar4set.r00")
	tail := append([]byte(nil), data[len(data)-100:]...)
	tail[len(tail)-14] ^= 0xff // inside the end header's volume number
	if n, ok := rar4TailVolumeNumber(tail); ok {
		t.Errorf("a corrupted end header gave volume %d; its CRC must reject it", n)
	}
}

// An obfuscated RAR4 set - names that give no order, posted out of order -
// is put back in volume order from each volume's end-of-archive header.
// Articles of 1,000 bytes leave the last volume a final article of 18 bytes,
// shorter than its 20-byte end header, so its tail read takes two articles.
func TestParseArchive_OrdersUnnamedRAR4VolumesByEndHeader(t *testing.T) {
	posted := newPostedArticles()
	volumes := []*types.Volume{
		posted.volume("9f2c07c9.rar", rarFixture(t, "rar4set.r01"), 1000),
		posted.volume("5efda6b0.rar", rarFixture(t, "rar4set.rar"), 1000),
		posted.volume("c094d114.rar", rarFixture(t, "rar4set.r00"), 1000),
	}
	p := &RARParser{logger: zerolog.Nop(), maxConcurrent: 2, fetchBody: posted.fetch}
	info, err := p.parseArchive(context.Background(), volumes, "")
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{1, 2, 0}; fmt.Sprint(info.VolumeOrder) != fmt.Sprint(want) || !info.VolumeOrderKnown {
		t.Fatalf("VolumeOrder = %v known=%v, want %v known", info.VolumeOrder, info.VolumeOrderKnown, want)
	}
	if len(info.Files) != 1 || info.Files[0].Name != "movie.mkv" || info.Files[0].UncompressedSize != 12000 {
		t.Fatalf("files = %+v, want movie.mkv of 12000 bytes", info.Files)
	}
}

// With estimated article sizes - every article sized 3% short, as the 0.97
// estimate does - the header walk skips a file's data to the wrong byte and
// the end header it reads there fails its CRC, so the order must come from
// reading the volumes' tails.
func TestParseArchive_UnnamedRAR4OrderFromTailsWhenSizesAreEstimates(t *testing.T) {
	posted := newPostedArticles()
	volumes := []*types.Volume{
		posted.volume("9f2c07c9.rar", rarFixture(t, "rar4set.r01"), 1000),
		posted.volume("5efda6b0.rar", rarFixture(t, "rar4set.rar"), 1000),
		posted.volume("c094d114.rar", rarFixture(t, "rar4set.r00"), 1000),
	}
	for _, v := range volumes {
		v.Size = 0
		for i := range v.Segments {
			v.Segments[i].Bytes = v.Segments[i].Bytes * 97 / 100
			v.Size += v.Segments[i].Bytes
		}
	}
	p := &RARParser{logger: zerolog.Nop(), maxConcurrent: 1, fetchBody: posted.fetch}
	for _, v := range volumes {
		stream := newRarReader(context.Background(), posted.fetch, []*types.Volume{v})
		if err := stream.Skip(7); err != nil {
			t.Fatal(err)
		}
		if _, _, walked, _ := p.parseRAR4Stream(stream, 0, v.Name, v.Size); walked {
			t.Fatalf("%s: the header walk reached the end header; the test no longer covers the tail read", v.Name)
		}
	}
	info, err := p.parseArchive(context.Background(), volumes, "")
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{1, 2, 0}; fmt.Sprint(info.VolumeOrder) != fmt.Sprint(want) || !info.VolumeOrderKnown {
		t.Fatalf("VolumeOrder = %v known=%v, want %v known", info.VolumeOrder, info.VolumeOrderKnown, want)
	}
}

// Volumes already in order are verified, not reordered.
func TestParseArchive_UnnamedRAR4VolumesInOrder(t *testing.T) {
	posted := newPostedArticles()
	volumes := []*types.Volume{
		posted.volume("a.rar", rarFixture(t, "rar4set.rar"), 1000),
		posted.volume("b.rar", rarFixture(t, "rar4set.r00"), 1000),
		posted.volume("c.rar", rarFixture(t, "rar4set.r01"), 1000),
	}
	p := &RARParser{logger: zerolog.Nop(), maxConcurrent: 2, fetchBody: posted.fetch}
	info, err := p.parseArchive(context.Background(), volumes, "")
	if err != nil {
		t.Fatal(err)
	}
	if info.VolumeOrder != nil || !info.VolumeOrderKnown {
		t.Fatalf("VolumeOrder = %v known=%v, want nil and known", info.VolumeOrder, info.VolumeOrderKnown)
	}
}

// A volume whose tail will not fetch leaves the order unverified: nothing is
// reordered on a guess.
func TestParseArchive_UnnamedRAR4TailFetchFails(t *testing.T) {
	posted := newPostedArticles()
	volumes := []*types.Volume{
		posted.volume("9f2c07c9.rar", rarFixture(t, "rar4set.r01"), 1000),
		posted.volume("5efda6b0.rar", rarFixture(t, "rar4set.rar"), 1000),
		posted.volume("c094d114.rar", rarFixture(t, "rar4set.r00"), 1000),
	}
	for _, v := range volumes[1:] {
		posted.fail[v.Segments[len(v.Segments)-1].MessageID] = true
	}
	p := &RARParser{logger: zerolog.Nop(), maxConcurrent: 2, fetchBody: posted.fetch}
	info, err := p.parseArchive(context.Background(), volumes, "")
	if err != nil {
		t.Fatal(err)
	}
	if info.VolumeOrder != nil || info.VolumeOrderKnown {
		t.Fatalf("VolumeOrder = %v known=%v, want nil and unknown", info.VolumeOrder, info.VolumeOrderKnown)
	}
}

// Named RAR4 volumes keep their name order; their tails are not read.
func TestParseArchive_NamedRAR4VolumesReadNoTails(t *testing.T) {
	posted := newPostedArticles()
	volumes := []*types.Volume{
		posted.volume("x.rar", rarFixture(t, "rar4set.rar"), 1000),
		posted.volume("x.r00", rarFixture(t, "rar4set.r00"), 1000),
		posted.volume("x.r01", rarFixture(t, "rar4set.r01"), 1000),
	}
	p := &RARParser{logger: zerolog.Nop(), maxConcurrent: 2, fetchBody: posted.fetch}
	info, err := p.parseArchive(context.Background(), volumes, "")
	if err != nil {
		t.Fatal(err)
	}
	if info.VolumeOrder != nil {
		t.Fatalf("VolumeOrder = %v, want nil", info.VolumeOrder)
	}
	for _, v := range volumes {
		last := v.Segments[len(v.Segments)-1].MessageID
		if posted.fetched[last] > 0 && len(v.Segments) > 2 {
			t.Logf("%s: last article read by the header walk", v.Name)
		}
	}
}
