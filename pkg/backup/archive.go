package backup

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// Limits for an uploaded archive. A real download holds at most a dozen
// small files and the database stores, so both caps are generous.
const (
	DefaultArchiveMaxBytes   int64 = 32 << 30
	DefaultArchiveMaxEntries       = 64
	maxArchiveInfoBytes            = 64 << 10
)

// maxZstdWindow caps the window a zstd frame may declare when reading. A
// frame header can ask for up to 512 MB of window with a handful of bytes,
// and our own writers never use more than 8 MB.
const maxZstdWindow = 64 << 20

// ArchiveRestoreOptions says how RestoreFromArchive restores a downloaded
// .tar.zst.
type ArchiveRestoreOptions struct {
	DataDir string
	// Only limits the restore to these components; empty means every
	// component the archive holds.
	Only []Component
	// ApplyDB swaps the staged database in at once (see RestoreOptions).
	ApplyDB bool
	// MaxBytes caps the decompressed size of all entries, MaxEntries their
	// number; zero means the defaults.
	MaxBytes   int64
	MaxEntries int
}

// RestoreFromArchiveFile restores the .tar.zst at path; see RestoreFromArchive.
func RestoreFromArchiveFile(ctx context.Context, p string, opts ArchiveRestoreOptions) (RestoreReport, error) {
	f, err := os.Open(p)
	if err != nil {
		return RestoreReport{Restored: map[Component]int{}, Skipped: map[Component]string{}}, err
	}
	defer f.Close()
	return RestoreFromArchive(ctx, f, opts)
}

// RestoreFromArchive restores a .tar.zst made by Download into opts.DataDir
// with the same safety as a restore from a repository. The whole archive is
// validated and unpacked into a scratch folder inside the data folder first;
// entries that are not plain files with a known name, too many entries or
// too many bytes refuse the archive before anything else is written. The
// database is then staged (it can still refuse: newer format, a restore
// already staged, counts that differ) and only after that are config, state
// and hearsay files moved into place, the old ones kept as
// <name>.before-restore-<ts>. The caller guarantees nothing is running on the
// folder (see CheckStoresFree).
func RestoreFromArchive(ctx context.Context, r io.Reader, opts ArchiveRestoreOptions) (RestoreReport, error) {
	rep := RestoreReport{Restored: map[Component]int{}, Skipped: map[Component]string{}}
	if opts.DataDir == "" {
		return rep, fmt.Errorf("%w: a data folder is required", ErrInvalid)
	}
	if opts.MaxBytes <= 0 {
		opts.MaxBytes = DefaultArchiveMaxBytes
	}
	if opts.MaxEntries <= 0 {
		opts.MaxEntries = DefaultArchiveMaxEntries
	}
	if err := os.MkdirAll(opts.DataDir, 0o755); err != nil {
		return rep, err
	}
	scratch, err := os.MkdirTemp(opts.DataDir, ".restore-archive-")
	if err != nil {
		return rep, err
	}
	defer os.RemoveAll(scratch)

	info, names, err := unpackArchive(ctx, r, scratch, opts)
	if err != nil {
		return rep, err
	}
	if info != nil {
		rep.SnapshotID = info.SnapshotID
	}

	hasDB := false
	byComp := map[Component][]string{}
	for _, n := range names {
		if strings.HasPrefix(n, "db/") {
			hasDB = true
			continue
		}
		byComp[fileComponent(n)] = append(byComp[fileComponent(n)], n)
	}
	if !hasDB && len(byComp) == 0 {
		return rep, fmt.Errorf("%w: the archive holds no database, config, state or hearsay files", ErrInvalid)
	}
	for _, c := range opts.Only {
		present := byComp[c] != nil || (c == ComponentDB && hasDB)
		if !present {
			rep.Skipped[c] = "not in the archive"
		}
	}

	// The database goes first, as for a repository restore.
	staged, filesDone := false, false
	if hasDB && wanted(opts.Only, ComponentDB) {
		if info == nil || info.DBFormat <= 0 {
			return rep, fmt.Errorf("%w: the archive does not say which database format it holds (it is not a download from this build or a later one)", ErrInvalid)
		}
		id := info.SnapshotID
		if id == "" {
			id = "archive"
		}
		resp, err := stageDB(id, BuildInfo{Commit: info.Commit, DBFormat: info.DBFormat}, info.Records, opts.DataDir,
			func(store string) (io.ReadCloser, error) {
				f, err := os.Open(filepath.Join(scratch, "db", store))
				if errors.Is(err, os.ErrNotExist) {
					return nil, nil
				}
				return f, err
			})
		if err != nil {
			return rep, err
		}
		rep.DB = &resp
		rep.Warnings = append(rep.Warnings, resp.Warnings...)
		staged = true
		defer func() {
			if !filesDone {
				_ = os.Remove(filepath.Join(opts.DataDir, PendingRestoreFile))
				_ = os.RemoveAll(resp.StagedDir)
			}
		}()
	}

	stamp := time.Now().Format("20060102-150405")
	for _, c := range []Component{ComponentConfig, ComponentState, ComponentHearsay} {
		if !wanted(opts.Only, c) {
			continue
		}
		for _, n := range byComp[c] {
			if err := ctx.Err(); err != nil {
				return rep, err
			}
			dst := filepath.Join(opts.DataDir, filepath.FromSlash(n))
			if _, err := os.Lstat(dst); err == nil {
				if err := os.Rename(dst, dst+".before-restore-"+stamp); err != nil {
					return rep, fmt.Errorf("move %s aside: %w", n, err)
				}
			}
			if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
				return rep, err
			}
			if err := os.Rename(filepath.Join(scratch, filepath.FromSlash(n)), dst); err != nil {
				return rep, fmt.Errorf("restore %s: %w", n, err)
			}
			rep.Restored[c]++
		}
	}

	filesDone = true
	if staged {
		if opts.ApplyDB {
			if err := ApplyPendingRestore(opts.DataDir); err != nil {
				return rep, err
			}
			rep.DB.RestartRequired = false
		}
		total := 0
		for _, n := range rep.DB.Counts {
			total += n
		}
		rep.Restored[ComponentDB] = total
	}
	return rep, nil
}

// fileComponent says which file-based component a data-folder file belongs to.
func fileComponent(name string) Component {
	for c, files := range componentFiles {
		for _, f := range files {
			if f == name {
				return c
			}
		}
	}
	return ""
}

// unpackArchive validates every entry of the .tar.zst in r and writes the
// accepted ones under scratch (mode 0600). It returns the archive info (nil
// when absent) and the names of the other entries, sorted.
func unpackArchive(ctx context.Context, r io.Reader, scratch string, opts ArchiveRestoreOptions) (*ArchiveInfo, []string, error) {
	zr, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(maxZstdWindow))
	if err != nil {
		return nil, nil, fmt.Errorf("%w: not a zstd file: %v", ErrInvalid, err)
	}
	defer zr.Close()
	tr := tar.NewReader(zr)

	var (
		info    *ArchiveInfo
		names   []string
		seen    = map[string]bool{}
		total   int64
		entries int
		stores  = map[string]bool{}
	)
	for _, s := range storage.StoreNames() {
		stores[s] = true
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("%w: unreadable archive: %v", ErrInvalid, err)
		}
		entries++
		if entries > opts.MaxEntries {
			return nil, nil, fmt.Errorf("%w: the archive has more than %d entries", ErrInvalid, opts.MaxEntries)
		}
		switch h.Typeflag {
		case tar.TypeReg, tar.TypeRegA:
		case tar.TypeDir:
			continue
		case tar.TypeSymlink, tar.TypeLink:
			return nil, nil, fmt.Errorf("%w: the archive contains a link (%q), which is never restored", ErrInvalid, h.Name)
		default:
			return nil, nil, fmt.Errorf("%w: the archive contains %q, which is not a plain file", ErrInvalid, h.Name)
		}
		name := h.Name
		if !safeRel(name) || strings.ContainsAny(name, "\x00:") || path.Clean(name) != name {
			return nil, nil, fmt.Errorf("%w: the archive names an unsafe path %q", ErrInvalid, name)
		}
		known := name == ArchiveInfoName || fileComponent(name) != ""
		if rest, ok := strings.CutPrefix(name, "db/"); ok && stores[rest] {
			known = true
		}
		if !known {
			return nil, nil, fmt.Errorf("%w: the archive holds %q, which a backup restore does not use", ErrInvalid, name)
		}
		if seen[name] {
			return nil, nil, fmt.Errorf("%w: the archive lists %q twice", ErrInvalid, name)
		}
		seen[name] = true
		if h.Size < 0 || h.Size > opts.MaxBytes-total {
			return nil, nil, fmt.Errorf("%w: the archive unpacks to more than %d bytes", ErrInvalid, opts.MaxBytes)
		}
		if name == ArchiveInfoName {
			if h.Size > maxArchiveInfoBytes {
				return nil, nil, fmt.Errorf("%w: %s is too large", ErrInvalid, ArchiveInfoName)
			}
			b, err := io.ReadAll(io.LimitReader(tr, maxArchiveInfoBytes+1))
			if err != nil {
				return nil, nil, fmt.Errorf("%w: unreadable archive: %v", ErrInvalid, err)
			}
			var ai ArchiveInfo
			if err := json.NewDecoder(bytes.NewReader(b)).Decode(&ai); err != nil {
				return nil, nil, fmt.Errorf("%w: %s is not valid: %v", ErrInvalid, ArchiveInfoName, err)
			}
			info = &ai
			total += int64(len(b))
			continue
		}
		dst := filepath.Join(scratch, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
			return nil, nil, err
		}
		f, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return nil, nil, err
		}
		// The reader already stops at the header's size; the limit keeps the
		// running total honest even so.
		n, cerr := io.Copy(f, io.LimitReader(tr, h.Size))
		if err := errors.Join(cerr, f.Close()); err != nil {
			return nil, nil, fmt.Errorf("%w: unreadable archive: %v", ErrInvalid, err)
		}
		if n != h.Size {
			return nil, nil, fmt.Errorf("%w: %q is truncated", ErrInvalid, name)
		}
		total += n
		names = append(names, name)
	}
	sort.Strings(names)
	return info, names, nil
}

// archiveHead is what a cheap look at an archive's first entries tells.
type archiveHead struct {
	info  *ArchiveInfo
	hasDB bool
}

// peekArchive reads the archive at p just far enough to know whether it
// holds a database and what backup-info.json says. Download writes the info
// first, so an archive from this build costs one small read; one without it
// is scanned entry by entry (bodies skipped) for a db/ entry.
func peekArchive(p string, maxEntries int) (archiveHead, error) {
	var head archiveHead
	f, err := os.Open(p)
	if err != nil {
		return head, err
	}
	defer f.Close()
	zr, err := zstd.NewReader(f, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(maxZstdWindow))
	if err != nil {
		return head, fmt.Errorf("%w: not a zstd file: %v", ErrInvalid, err)
	}
	defer zr.Close()
	tr := tar.NewReader(zr)
	for i := 0; i < maxEntries; i++ {
		h, err := tr.Next()
		if err == io.EOF {
			return head, nil
		}
		if err != nil {
			return head, fmt.Errorf("%w: unreadable archive: %v", ErrInvalid, err)
		}
		if h.Name == ArchiveInfoName {
			if h.Size < 0 || h.Size > maxArchiveInfoBytes {
				return head, fmt.Errorf("%w: %s is too large", ErrInvalid, ArchiveInfoName)
			}
			b, err := io.ReadAll(io.LimitReader(tr, maxArchiveInfoBytes+1))
			if err != nil {
				return head, fmt.Errorf("%w: unreadable archive: %v", ErrInvalid, err)
			}
			var ai ArchiveInfo
			if err := json.Unmarshal(b, &ai); err != nil {
				return head, fmt.Errorf("%w: %s is not valid: %v", ErrInvalid, ArchiveInfoName, err)
			}
			head.info = &ai
			if ai.Component == ComponentDB {
				head.hasDB = true
				return head, nil
			}
			// An archive that says it is not the database one is taken at
			// its word; unpacking still refuses a db/ entry it did not
			// announce.
			return head, nil
		}
		if strings.HasPrefix(h.Name, "db/") {
			head.hasDB = true
			return head, nil
		}
	}
	return head, nil
}

// RestoreFromArchiveFiles restores several downloaded archives (say the
// database one and the config one) as a single operation. Every archive is
// looked at first and the whole set is refused, before anything is written,
// when the database archive would be: a newer database format, no format
// information, a restore already staged, or more than one database archive.
// The database archive is then applied first, the others in the order given,
// so a refused database can never leave a replaced config behind.
func RestoreFromArchiveFiles(ctx context.Context, paths []string, opts ArchiveRestoreOptions) (RestoreReport, error) {
	if len(paths) == 1 {
		return RestoreFromArchiveFile(ctx, paths[0], opts)
	}
	rep := RestoreReport{Restored: map[Component]int{}, Skipped: map[Component]string{}}
	if len(paths) == 0 {
		return rep, fmt.Errorf("%w: no archive given", ErrInvalid)
	}
	if opts.DataDir == "" {
		return rep, fmt.Errorf("%w: a data folder is required", ErrInvalid)
	}
	maxEntries := opts.MaxEntries
	if maxEntries <= 0 {
		maxEntries = DefaultArchiveMaxEntries
	}
	var dbFirst, rest []string
	for _, p := range paths {
		head, err := peekArchive(p, maxEntries)
		if err != nil {
			return rep, err
		}
		if !head.hasDB {
			rest = append(rest, p)
			continue
		}
		if len(dbFirst) > 0 {
			return rep, fmt.Errorf("%w: more than one of the archives holds the database", ErrInvalid)
		}
		if wanted(opts.Only, ComponentDB) {
			if head.info == nil || head.info.DBFormat <= 0 {
				return rep, fmt.Errorf("%w: the archive does not say which database format it holds (it is not a download from this build or a later one)", ErrInvalid)
			}
			id := head.info.SnapshotID
			if id == "" {
				id = "archive"
			}
			if err := checkDBStageable(id, BuildInfo{Commit: head.info.Commit, DBFormat: head.info.DBFormat}, opts.DataDir); err != nil {
				return rep, err
			}
		}
		dbFirst = append(dbFirst, p)
	}
	for _, p := range append(dbFirst, rest...) {
		one, err := RestoreFromArchiveFile(ctx, p, opts)
		if rep.SnapshotID == "" {
			rep.SnapshotID = one.SnapshotID
		}
		for c, n := range one.Restored {
			rep.Restored[c] += n
		}
		for c, why := range one.Skipped {
			rep.Skipped[c] = why
		}
		if one.DB != nil {
			rep.DB = one.DB
		}
		rep.Warnings = append(rep.Warnings, one.Warnings...)
		if err != nil {
			return rep, err
		}
	}
	for c, n := range rep.Restored {
		if n > 0 {
			delete(rep.Skipped, c)
		}
	}
	return rep, nil
}
