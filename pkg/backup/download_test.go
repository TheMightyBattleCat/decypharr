package backup

import (
	"archive/tar"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

func runFull(t *testing.T, e *testEnv) string {
	t.Helper()
	if _, err := e.svc.RunJob(context.Background(), "full"); err != nil {
		t.Fatal(err)
	}
	e.waitIdle()
	snaps := e.snapshots()
	if len(snaps) != 1 {
		t.Fatalf("snapshots: %+v", snaps)
	}
	return snaps[0].ID
}

func TestDownloadTarsConfigAndDB(t *testing.T) {
	e := newEnv(t, nil)
	id := runFull(t, e)
	for comp, want := range map[Component]struct{ name, body string }{
		ComponentConfig: {"config.json", `{"a":1}`},
		ComponentDB:     {"db/entries", "db"},
	} {
		res, err := e.svc.Download(context.Background(), id, comp)
		if err != nil {
			t.Fatal(comp, err)
		}
		if !strings.HasSuffix(res.Filename, ".tar.zst") {
			t.Errorf("filename %q", res.Filename)
		}
		zr, err := zstd.NewReader(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		tr := tar.NewReader(zr)
		found := false
		for {
			h, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(comp, err)
			}
			b, _ := io.ReadAll(tr)
			if h.Name == want.name && string(b) == want.body {
				found = true
			}
		}
		zr.Close()
		res.Body.Close()
		if !found {
			t.Errorf("%s: %s not in the archive", comp, want.name)
		}
	}
	if _, err := e.svc.Download(context.Background(), id, ComponentNZB); !errors.Is(err, ErrInvalid) {
		t.Errorf("nzb download: %v", err)
	}
	if _, err := e.svc.Download(context.Background(), id, ComponentLinks); !errors.Is(err, ErrNotFound) {
		t.Errorf("links download without that component: %v", err)
	}
}

func TestRestoreNZBPutsTheRecordBackAndKeepsNewerFiles(t *testing.T) {
	e := newEnv(t, nil)
	id := runFull(t, e)
	meta := filepath.Join(e.data, "usenet", "meta", "one.meta")
	ctx := context.Background()

	resp, err := e.svc.RestoreNZB(ctx, id, RestoreNZBRequest{Entry: "one"})
	if err != nil || resp.Restored != 0 {
		t.Fatalf("present record: %+v %v", resp, err)
	}
	if err := os.Remove(meta); err != nil {
		t.Fatal(err)
	}
	resp, err = e.svc.RestoreNZB(ctx, id, RestoreNZBRequest{Entry: "one"})
	if err != nil || resp.Restored != 1 {
		t.Fatalf("restore: %+v %v", resp, err)
	}
	if b, _ := os.ReadFile(meta); string(b) != "one" {
		t.Errorf("restored content %q", b)
	}
	if _, err := e.svc.RestoreNZB(ctx, id, RestoreNZBRequest{Entry: "nope"}); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown entry: %v", err)
	}
	if _, err := e.svc.RestoreNZB(ctx, id, RestoreNZBRequest{Entry: "../x"}); !errors.Is(err, ErrInvalid) && !errors.Is(err, ErrNotFound) {
		t.Errorf("path entry: %v", err)
	}
}

func TestBuildNZBXML(t *testing.T) {
	n := &storage.NZB{Name: "R&D", Groups: []string{"alt.binaries.x"}, Files: []storage.NZBFile{
		{Name: "a.rar", Segments: []storage.NZBSegment{{Number: 2, Bytes: 5, MessageID: "<b@x>"}, {Number: 1, Bytes: 7, MessageID: "a@x"}}},
		{Name: "gone", IsDeleted: true, Segments: []storage.NZBSegment{{Number: 1, MessageID: "z@x"}}},
	}}
	out := string(BuildNZBXML(n))
	for _, want := range []string{`R&amp;D`, `subject="a.rar"`, `<group>alt.binaries.x</group>`, `number="1">a@x<`, `number="2">b@x<`} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
	if strings.Contains(out, "z@x") || strings.Index(out, "a@x") > strings.Index(out, "b@x") {
		t.Errorf("deleted file kept or segments unordered:\n%s", out)
	}
}
