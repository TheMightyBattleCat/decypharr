package manager

import (
	"context"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

// The geometry head read has its own cap: on a server that stalls the first
// article, checkImportGeometry returns soon after geometryHeadReadTimeout
// with the meta-only verdict, as for any failed read, and leaves the
// caller's ctx live (so the sweep does not see itself stopped). Before the
// cap the sweep's ctx, which has no per-file deadline, let the read wait
// out the connection's 60 s idle timeout on every file. A healthy file on
// the same server still gets the head-based verdict.
func TestCheckImportGeometryCapsAStalledHeadRead(t *testing.T) {
	const (
		article = 1000
		nzbID   = "geo-head-cap"
	)
	p2eStartClock(t)
	srv, err := newP2eServer()
	if err != nil {
		t.Fatalf("fake server: %v", err)
	}
	t.Cleanup(srv.close)

	// File size 3 articles; the Matroska Segment says 10,048 B, so the head
	// shows a missing volume. The meta alone shows nothing.
	const size = 3 * article
	head := append(matroskaHead(10_000), make([]byte, article-len(matroskaHead(10_000)))...)
	file := func(name string) *storage.NZBFile {
		f := &storage.NZBFile{Name: name, Size: size, FileType: storage.NZBFileTypeMedia}
		for i := range 3 {
			id := p2eMsgID(name, i+1)
			f.Segments = append(f.Segments, storage.NZBSegment{
				Number: i + 1, MessageID: id, Bytes: article,
				StartOffset: int64(i * article), EndOffset: int64((i+1)*article - 1),
			})
			if i == 0 {
				srv.addArticle(&p2eArticle{id: id, data: head, body: p2eYenc(name, size, 1, 3, 0, head)})
			}
		}
		return f
	}
	good, stalled := file("good.mkv"), file("stalled.mkv")
	srv.set(stalled.Segments[0].MessageID, p2eStall)

	p, repair := newTestPar2Repair(t)
	cfg := config.Get()
	oldUsenet := cfg.Usenet
	cfg.Usenet = config.Usenet{
		Providers:                []config.UsenetProvider{{Host: "127.0.0.1", Port: srv.port(), MaxConnections: 2, Priority: 1}},
		MaxConnections:           2,
		ProcessingMaxConnections: 2,
		DiskBufferPath:           t.TempDir(),
	}
	t.Cleanup(func() { cfg.Usenet = oldUsenet })
	u, err := usenet.New()
	if err != nil {
		t.Fatalf("usenet.New: %v", err)
	}
	t.Cleanup(func() { _ = u.Close() })
	p.manager.usenet = u
	if err := u.AddNZBForTest(&storage.NZB{ID: nzbID, Name: "geo", Files: []storage.NZBFile{*good, *stalled}}); err != nil {
		t.Fatalf("AddNZBForTest: %v", err)
	}

	old := geometryHeadReadTimeout
	geometryHeadReadTimeout = time.Second
	t.Cleanup(func() { geometryHeadReadTimeout = old })

	// Control: the head is read and shows the missing volume.
	if v := repair.checkImportGeometry(context.Background(), nzbID, "good.mkv", true); v.reason != reasonMissingVolume {
		t.Fatalf("healthy head: got %+v, want reason %q from the head", v, reasonMissingVolume)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const margin = 3 * time.Second // far below the 60 s idle timeout
	done := make(chan geometryVerdict, 1)
	start := time.Now()
	go func() { done <- repair.checkImportGeometry(ctx, nzbID, "stalled.mkv", true) }()
	var v geometryVerdict
	select {
	case v = <-done:
	case <-time.After(geometryHeadReadTimeout + margin):
		t.Fatalf("stalled head read still blocked %v after the call, with a %v cap", geometryHeadReadTimeout+margin, geometryHeadReadTimeout)
	}
	took := time.Since(start)
	if took < geometryHeadReadTimeout/2 {
		t.Fatalf("stalled head read returned after %v: the stall was not reached (BODY stalled %d)", took, srv.bodyStalled.Load())
	}
	if srv.bodyStalled.Load() == 0 {
		t.Fatal("the stalled article was never asked for")
	}
	if v != (geometryVerdict{}) {
		t.Fatalf("stalled head read: got %+v, want the meta-only (empty) verdict", v)
	}
	if ctx.Err() != nil {
		t.Fatalf("caller's ctx ended (%v): a cap expiry must not look like the sweep being stopped", ctx.Err())
	}

	// The connection the stall held was dropped, not pooled half-read.
	if v := repair.checkImportGeometry(context.Background(), nzbID, "good.mkv", true); v.reason != reasonMissingVolume {
		t.Fatalf("healthy head after the stall: got %+v, want reason %q", v, reasonMissingVolume)
	}
}
