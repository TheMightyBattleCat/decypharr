package manager

import (
	"context"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// cacheFolderRecorder is a DFS MountManager that records the cache folder
// every PeekCachedRange and ForgetCachedRange call names.
type cacheFolderRecorder struct {
	mu     sync.Mutex
	peeks  []string
	forgot []string
}

func (r *cacheFolderRecorder) Start(context.Context) error { return nil }
func (r *cacheFolderRecorder) Stop() error                 { return nil }
func (r *cacheFolderRecorder) Stats() map[string]any       { return nil }
func (r *cacheFolderRecorder) IsReady() bool               { return true }
func (r *cacheFolderRecorder) Type() string                { return "test" }
func (r *cacheFolderRecorder) Refresh([]string) error      { return nil }
func (r *cacheFolderRecorder) PeekCachedRange(folder, _ string, _ []byte, _ int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peeks = append(r.peeks, folder)
	return true
}
func (r *cacheFolderRecorder) ForgetCachedRange(folder, _ string, _, _ int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.forgot = append(r.forgot, folder)
}

// With folder_naming = original the mount (and so the DFS cache) files an
// entry under its OriginalFilename, not its Name. A PAR2 pass must forget
// the padded ranges, and peek intact data, under that folder: under Name
// the forget found nothing, so the zero-fill cached during the pad was
// still served after a successful repair.
func TestPar2AddressesTheDFSCacheByTheMountFolder(t *testing.T) {
	m, _ := newTestManagerForReap(t)
	config.Reset()
	config.Get().FolderNaming = config.WebDavUseOriginalName
	t.Cleanup(config.Reset)

	const (
		nzbID    = "nzb-folder"
		name     = "Show.S01E01.Display.Name"
		original = "Show.S01E01.1080p.Original-GRP"
		file     = "Show.S01E01.mkv"
	)
	addTestEntry(t, m, &storage.Entry{
		InfoHash:         nzbID,
		Name:             name,
		OriginalFilename: original,
		Protocol:         config.ProtocolNZB,
	})
	if got := (&storage.Entry{Name: name, OriginalFilename: original}).GetFolder(); got != original {
		t.Fatalf("fixture: GetFolder() = %q, want %q (folder_naming not applied)", got, original)
	}

	nzb := &storage.NZB{ID: nzbID, Files: []storage.NZBFile{{
		Name:     file,
		IsStored: true,
		Segments: []storage.NZBSegment{
			{Number: 1, MessageID: "<a>", Bytes: 100, StartOffset: 0, EndOffset: 99},
			{Number: 2, MessageID: "<b>", Bytes: 100, StartOffset: 100, EndOffset: 199},
		},
	}}}
	pending := map[string][]overlay.DeadSegment{file: {{Index: 1, MessageID: "<b>", Bytes: 100}}}

	rec := &cacheFolderRecorder{}
	m.mountManager = rec
	p := &Par2Repair{manager: m, logger: zerolog.Nop()}

	p.invalidateRepairedRanges(nzb, nzbID, name, pending)
	rec.mu.Lock()
	forgot := append([]string(nil), rec.forgot...)
	rec.mu.Unlock()
	if len(forgot) != 1 || forgot[0] != original {
		t.Fatalf("ForgetCachedRange folders = %q, want [%q] (the mount folder, not the entry name)", forgot, original)
	}

	src := p.newCacheSource(nzbID, name, rec, nzb, pending, nil, nil)
	if _, ok := src.readCached("<a>", 0, 100); !ok {
		t.Fatal("readCached of the intact segment missed")
	}
	rec.mu.Lock()
	peeks := append([]string(nil), rec.peeks...)
	rec.mu.Unlock()
	if len(peeks) != 1 || peeks[0] != original {
		t.Fatalf("PeekCachedRange folders = %q, want [%q] (the mount folder, not the entry name)", peeks, original)
	}
}

// An entry that can't be read falls back to the name the caller passed.
func TestPar2CacheFolderFallsBackWhenTheEntryIsGone(t *testing.T) {
	m, _ := newTestManagerForReap(t)
	p := &Par2Repair{manager: m, logger: zerolog.Nop()}
	if got := p.dfsCacheFolder("nzb-missing", "Fallback.Name"); got != "Fallback.Name" {
		t.Fatalf("dfsCacheFolder = %q, want the fallback", got)
	}
}
