package parser

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/Tensai75/nzbparser"
	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

type fakeVolumeProbe struct {
	mu     sync.Mutex
	volume map[string]bool // filename -> is a volume; absent: header unreadable
	probed []string
}

func (f *fakeVolumeProbe) probe(_ context.Context, file nzbparser.NzbFile) (bool, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.probed = append(f.probed, file.Filename)
	v, ok := f.volume[file.Filename]
	return v, ok
}

func singleFileRarGroups(files ...nzbparser.NzbFile) (map[string]*FileGroup, nzbparser.NzbFiles) {
	groups := map[string]*FileGroup{}
	for _, f := range files {
		groups[f.Filename] = &FileGroup{BaseName: f.Filename, Type: storage.NZBFileTypeRar, Files: []nzbparser.NzbFile{f}, Groups: map[string]struct{}{}}
	}
	return groups, files
}

// Der Schwaebische Nachmittagstee UNTAVC posts name.proof.rar (1 article) and
// name.subs.rar (8 articles) beside its .rar/.rNN set. Both are single-volume
// archives; merging them made one bogus two-volume archive.
func TestMergeObfuscatedRarGroups_KeepsSingleVolumeArchivesApart(t *testing.T) {
	const base = "der.schwaebische.nachmittagstee.2025.german.dl.1080p.bluray.avc-untavc"
	groups, raw := singleFileRarGroups(
		postedVolume(63, base+".proof.rar", 1, 800),
		postedVolume(74, base+".subs.rar", 8, 900),
	)
	probe := &fakeVolumeProbe{volume: map[string]bool{base + ".proof.rar": false, base + ".subs.rar": false}}
	p := &NZBParser{logger: zerolog.Nop(), maxConcurrent: 2, volumeProbe: probe.probe}

	got := p.mergeObfuscatedRarGroups(context.Background(), groups, raw)
	if len(got) != 2 || got[base+".proof.rar"] == nil || got[base+".subs.rar"] == nil {
		t.Fatalf("groups = %v, want proof and subs left as their own groups", keysOf(got))
	}
}

// An obfuscated set of plain .rar volumes is still merged, with one standalone
// archive left out. Only the files not sharing the inner volumes' article
// count have their header read: the last volume and the proof.
func TestMergeObfuscatedRarGroups_ProbesOnlyOddSizedFiles(t *testing.T) {
	groups, raw := singleFileRarGroups(
		postedVolume(1, "k3j9.rar", 5, 100),
		postedVolume(2, "a9xq.rar", 5, 100),
		postedVolume(3, "zz1p.rar", 5, 100),
		postedVolume(4, "q0w2.rar", 2, 40),
		postedVolume(5, "release.proof.rar", 1, 30),
	)
	probe := &fakeVolumeProbe{volume: map[string]bool{"q0w2.rar": true, "release.proof.rar": false}}
	p := &NZBParser{logger: zerolog.Nop(), maxConcurrent: 2, volumeProbe: probe.probe}

	got := p.mergeObfuscatedRarGroups(context.Background(), groups, raw)
	sort.Strings(probe.probed)
	if want := []string{"q0w2.rar", "release.proof.rar"}; len(probe.probed) != 2 || probe.probed[0] != want[0] || probe.probed[1] != want[1] {
		t.Fatalf("probed %v, want %v", probe.probed, want)
	}
	if len(got) != 2 || got["release.proof.rar"] == nil {
		t.Fatalf("groups = %v, want the merged set and the proof", keysOf(got))
	}
	for k, g := range got {
		if k != "release.proof.rar" && len(g.Files) != 4 {
			t.Fatalf("merged group %s has %d volumes, want 4", k, len(g.Files))
		}
	}
}

// A header that cannot be read leaves the file in the merge, as before.
func TestMergeObfuscatedRarGroups_UnreadableHeaderStillMerges(t *testing.T) {
	groups, raw := singleFileRarGroups(
		postedVolume(1, "k3j9.rar", 5, 100),
		postedVolume(2, "a9xq.rar", 2, 100),
	)
	probe := &fakeVolumeProbe{volume: map[string]bool{}}
	p := &NZBParser{logger: zerolog.Nop(), maxConcurrent: 2, volumeProbe: probe.probe}

	if got := p.mergeObfuscatedRarGroups(context.Background(), groups, raw); len(got) != 1 {
		t.Fatalf("groups = %v, want one merged group", keysOf(got))
	}
}

func keysOf(m map[string]*FileGroup) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
