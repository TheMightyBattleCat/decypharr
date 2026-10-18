package backup

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

// Download streams one component of a snapshot. The database, config, state
// and hearsay components come back as a .tar.zst of their files (the
// database as one file per store, in the export format); links come back as
// the link manifest in its .tsv form. The NZB and overlay components are
// too large to download and are restored instead.
func (s *BackupService) Download(ctx context.Context, id string, component Component) (DownloadResult, error) {
	r, err := s.repo()
	if err != nil {
		return DownloadResult{}, err
	}
	snap, err := r.ReadSnapshot(ctx, id)
	if err != nil {
		return DownloadResult{}, err
	}
	ce, ok := snap.Components[component]
	if !ok {
		return DownloadResult{}, fmt.Errorf("%w: snapshot %s has no %s component", ErrNotFound, id, component)
	}
	switch component {
	case ComponentLinks:
		if ce.Manifest == "" {
			return DownloadResult{}, fmt.Errorf("%w: snapshot %s has no link manifest", ErrNotFound, id)
		}
		body, err := r.OpenBlob(ctx, ce.Manifest)
		if err != nil {
			return DownloadResult{}, err
		}
		return DownloadResult{Filename: id + "-links.tsv", ContentType: "text/tab-separated-values", Body: body}, nil
	case ComponentDB, ComponentConfig, ComponentState, ComponentHearsay:
		files := ce.Files
		prefix := ""
		if component == ComponentDB {
			files, prefix = ce.StoreBlobs, "db/"
		}
		if len(files) == 0 {
			return DownloadResult{}, fmt.Errorf("%w: snapshot %s has no files for %s", ErrNotFound, id, component)
		}
		names := make([]string, 0, len(files))
		for name, sha := range files {
			ok, err := r.HasBlob(ctx, sha)
			if err != nil {
				return DownloadResult{}, err
			}
			if !ok {
				return DownloadResult{}, fmt.Errorf("%w: blob %s for %s is missing from the repository", ErrNotFound, sha, name)
			}
			names = append(names, name)
		}
		sort.Strings(names)
		pr, pw := io.Pipe()
		go func() {
			info := ArchiveInfo{Format: 1, SnapshotID: snap.ID, Component: component,
				Commit: snap.Decypharr.Commit, DBFormat: snap.Decypharr.DBFormat, Records: ce.Records}
			pw.CloseWithError(writeTarZst(ctx, pw, r, info, prefix, names, files))
		}()
		return DownloadResult{Filename: fmt.Sprintf("%s-%s.tar.zst", id, component), ContentType: "application/zstd", Body: pr}, nil
	}
	return DownloadResult{}, fmt.Errorf("%w: the %s component cannot be downloaded; restore it instead", ErrInvalid, component)
}

// ArchiveInfoName is the first entry of a downloaded archive. It says which
// snapshot and component the archive holds and, for the database, the
// format version and record count a restore checks. Archives made before it
// existed lack it; a restore refuses such a database archive.
const ArchiveInfoName = "backup-info.json"

// ArchiveInfo is the content of ArchiveInfoName.
type ArchiveInfo struct {
	Format     int       `json:"format"`
	SnapshotID string    `json:"snapshot_id"`
	Component  Component `json:"component"`
	Commit     string    `json:"commit,omitempty"`
	DBFormat   int       `json:"db_format,omitempty"`
	Records    int       `json:"records,omitempty"`
}

func writeTarZst(ctx context.Context, w io.Writer, bs BlobStore, info ArchiveInfo, prefix string, names []string, files map[string]string) error {
	zw, err := zstd.NewWriter(w)
	if err != nil {
		return err
	}
	tw := tar.NewWriter(zw)
	body, err := json.Marshal(info)
	if err != nil {
		return err
	}
	if err := tw.WriteHeader(&tar.Header{Name: ArchiveInfoName, Mode: 0o600, Size: int64(len(body)), Typeflag: tar.TypeReg}); err != nil {
		return err
	}
	if _, err := io.Copy(tw, bytes.NewReader(body)); err != nil {
		return err
	}
	for _, name := range names {
		if err := tarBlob(ctx, tw, bs, prefix+name, files[name]); err != nil {
			return err
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return zw.Close()
}

// tarBlob adds a blob to tw. The tar header needs the size first, so the
// blob goes to a temporary file (one at a time) before it is copied in.
func tarBlob(ctx context.Context, tw *tar.Writer, bs BlobStore, name, sha string) error {
	rc, err := bs.OpenBlob(ctx, sha)
	if err != nil {
		return err
	}
	defer rc.Close()
	tmp, err := os.CreateTemp("", "backup-download-*")
	if err != nil {
		return err
	}
	defer func() { _ = tmp.Close(); _ = os.Remove(tmp.Name()) }()
	n, err := io.Copy(tmp, rc)
	if err != nil {
		return err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: n, Typeflag: tar.TypeReg}); err != nil {
		return err
	}
	_, err = io.Copy(tw, tmp)
	return err
}

// RestoreNZB puts one entry's NZB record (and its overlay files) back from a
// snapshot. req.Entry is the record's ID (the name of its .meta file), or an
// entry name the live database can map to one. Files that exist now are
// newer than the snapshot and are kept. With Export it writes the rebuilt
// .nzb to <data>/backup-exports/ instead and touches nothing else.
func (s *BackupService) RestoreNZB(ctx context.Context, id string, req RestoreNZBRequest) (RestoreNZBResponse, error) {
	resp := RestoreNZBResponse{Entry: req.Entry}
	if strings.TrimSpace(req.Entry) == "" {
		return resp, fmt.Errorf("%w: entry is required", ErrInvalid)
	}
	r, err := s.repo()
	if err != nil {
		return resp, err
	}
	snap, err := r.ReadSnapshot(ctx, id)
	if err != nil {
		return resp, err
	}
	ce, ok := snap.Components[ComponentNZB]
	if !ok || ce.Tree == "" {
		return resp, fmt.Errorf("%w: snapshot %s has no NZB records", ErrNotFound, id)
	}
	tree, err := ReadTree(ctx, r, ce.Tree)
	if err != nil {
		return resp, err
	}
	key := req.Entry
	if _, ok := metaSha(tree, key); !ok && s.deps.Lookup != nil {
		if alt := s.deps.Lookup.EntryID(req.Entry); alt != "" {
			key = alt
		}
	}
	if !safeRel(key+".meta") || strings.Contains(key, "/") {
		return resp, fmt.Errorf("%w: %q is not a valid entry", ErrInvalid, req.Entry)
	}
	sha, ok := metaSha(tree, key)
	if !ok {
		return resp, fmt.Errorf("%w: snapshot %s holds no NZB record for %q", ErrNotFound, id, req.Entry)
	}
	usenetDir := filepath.Join(s.deps.DataDir, "usenet")

	if req.Export {
		data, err := readBlob(ctx, r, sha)
		if err != nil {
			return resp, err
		}
		nzb, err := usenet.DecodeNZB(data)
		if err != nil {
			return resp, fmt.Errorf("record %s is unreadable: %w", key, err)
		}
		dir := filepath.Join(s.deps.DataDir, "backup-exports")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return resp, err
		}
		dst := filepath.Join(dir, key+".nzb")
		if err := os.WriteFile(dst, BuildNZBXML(nzb), 0o644); err != nil {
			return resp, err
		}
		resp.Restored = 1
		resp.Message = "exported to " + dst
		return resp, nil
	}

	u := &Undelete{o: UndeleteOptions{
		Repo:           r,
		MetaDir:        filepath.Join(usenetDir, "meta"),
		OverlayDir:     filepath.Join(usenetDir, "overlay"),
		VolumeOrderDir: filepath.Join(usenetDir, "volume-order-backup"),
	}}
	files, err := u.planFiles(ctx, snap, key)
	if err != nil {
		return resp, err
	}
	for _, f := range files {
		ok, err := u.copyBlob(ctx, f)
		if err != nil {
			return resp, fmt.Errorf("restore %s: %w", filepath.Base(f.dst), err)
		}
		if ok {
			resp.Restored++
		}
	}
	if resp.Restored == 0 {
		resp.Message = "the record and its overlay files are already present; nothing was changed"
	} else {
		resp.Message = fmt.Sprintf("restored %d file(s) from snapshot %s", resp.Restored, snap.ID)
	}
	return resp, nil
}

func metaSha(t *Tree, id string) (string, bool) {
	for rel, sha := range t.Files {
		if strings.TrimPrefix(rel, "meta/") == id+".meta" {
			return sha, true
		}
	}
	return "", false
}
