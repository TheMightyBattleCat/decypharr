package parser

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/usenet/types"
)

var duplicateNameSets = []struct {
	name string
	vols []string
}{
	{"rar5", []string{"rar5set.part1.rar", "rar5set.part2.rar", "rar5set.part3.rar"}},
	{"rar4", []string{"rar4set.rar", "rar4set.r00", "rar4set.r01"}},
}

// The first volume's header starts movie.mkv; the later volumes' headers
// continue it.
func TestParseArchive_SplitBeforeFlagFromRealHeaders(t *testing.T) {
	for _, set := range duplicateNameSets {
		posted := newPostedArticles()
		var volumes []*types.Volume
		for _, v := range set.vols {
			volumes = append(volumes, posted.volume(v, rarFixture(t, v), 1000))
		}
		p := &RARParser{logger: zerolog.Nop(), maxConcurrent: 1, fetchBody: posted.fetch}
		for i, v := range volumes {
			data := rarFixture(t, set.vols[i])
			var files []*RARFileEntry
			var err error
			if set.name == "rar5" {
				files, _, _, err = p.parseRAR5Headers(data, i, v.Name, "")
			} else {
				files, _, _, err = p.parseRAR4Headers(data, i, v.Name)
			}
			if err != nil || len(files) != 1 {
				t.Fatalf("%s: files=%d err=%v", v.Name, len(files), err)
			}
			if want := i > 0; files[0].SplitBefore != want {
				t.Errorf("%s (bytes path): SplitBefore=%v, want %v", v.Name, files[0].SplitBefore, want)
			}
			stream := newRarReader(context.Background(), posted.fetch, []*types.Volume{v})
			if set.name == "rar5" {
				if err := stream.Skip(8); err != nil {
					t.Fatal(err)
				}
				res, err := p.parseRAR5Stream(stream, i, v.Name, "")
				if err != nil || len(res.Files) != 1 {
					t.Fatalf("%s: stream err=%v", v.Name, err)
				}
				files = res.Files
			} else {
				if err := stream.Skip(7); err != nil {
					t.Fatal(err)
				}
				files, _, _, err = p.parseRAR4Stream(stream, i, v.Name, v.Size)
				if err != nil || len(files) != 1 {
					t.Fatalf("%s: stream err=%v", v.Name, err)
				}
			}
			if want := i > 0; files[0].SplitBefore != want {
				t.Errorf("%s (stream path): SplitBefore=%v, want %v", v.Name, files[0].SplitBefore, want)
			}
		}
	}
}

// Two copies of one archive, each holding movie.mkv, merged into one set by
// the obfuscated merge. The file must not be served as the parts of both
// copies run together: one copy is kept.
func TestParseArchive_SameNameInTwoArchivesNotJoined(t *testing.T) {
	for _, set := range duplicateNameSets {
		for _, layout := range []string{"sequential", "interleaved"} {
			t.Run(set.name+"/"+layout, func(t *testing.T) {
				posted := newPostedArticles()
				var volumes []*types.Volume
				add := func(copyName string, i int) {
					volumes = append(volumes, posted.volume(fmt.Sprintf("%s%d.rar", copyName, i), rarFixture(t, set.vols[i]), 1000))
				}
				if layout == "sequential" {
					for _, c := range []string{"a", "b"} {
						for i := range set.vols {
							add(c, i)
						}
					}
				} else {
					for i := range set.vols {
						add("a", i)
						add("b", i)
					}
				}
				p := &RARParser{logger: zerolog.Nop(), maxConcurrent: 2, fetchBody: posted.fetch}
				info, err := p.parseArchive(context.Background(), volumes, "", false)
				if err != nil {
					t.Fatal(err)
				}
				if len(info.Files) != 1 || info.Files[0].Name != "movie.mkv" {
					t.Fatalf("files = %+v, want one movie.mkv", info.Files)
				}
				f := info.Files[0]
				copies := map[byte]bool{}
				var parts []int
				var sum int64
				for _, vp := range f.VolumeParts {
					parts = append(parts, vp.PartNumber)
					sum += vp.UnpackedSize
					copies[volumes[vp.PartNumber].Name[0]] = true
				}
				if len(copies) != 1 {
					t.Fatalf("movie.mkv mixes parts of both copies: parts %v", parts)
				}
				if sum > f.UncompressedSize {
					t.Fatalf("parts hold %d bytes for a %d-byte file", sum, f.UncompressedSize)
				}
				if layout == "sequential" {
					// Copy a is complete: it is served whole, from its own volumes.
					if fmt.Sprint(parts) != "[0 1 2]" || sum != 12000 {
						t.Fatalf("parts %v holding %d bytes, want copy a's [0 1 2] holding 12000", parts, sum)
					}
				} else if !strings.HasPrefix(volumes[parts[0]].Name, "a") || sum == f.UncompressedSize {
					// Interleaved, no copy chains cleanly: the first copy's
					// start is kept and comes out short, so the file is left
					// out later rather than served spliced.
					t.Fatalf("parts %v holding %d bytes, want copy a's start only, short", parts, sum)
				}
			})
		}
	}
}
