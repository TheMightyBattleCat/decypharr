package backup

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
)

const (
	dirPerm  = 0o700
	filePerm = 0o600

	extXZ   = ".xz"
	extZstd = ".zst"
	extRaw  = ".raw"

	// minSavingPercent is how much compression has to save before a blob is
	// stored compressed.
	minSavingPercent = 3

	// maxXZDict caps the xz dictionary; a blob smaller than that gets a
	// dictionary just big enough to hold it.
	maxXZDict = 64 << 20
)

// blobExts lists the blob file extensions, in the order blobPath looks.
var blobExts = []string{extXZ, extZstd, extRaw}

// repoMeta is repo.json.
type repoMeta struct {
	Format           int       `json:"format"`
	CreatedAt        time.Time `json:"created_at"`
	Compression      string    `json:"compression"`
	MinSavingPercent int       `json:"min_saving_percent"`
}

// FileRepo is a Repo kept in a folder:
//
//	repo.json
//	blobs/<aa>/<sha256>.zst | .raw
//	snapshots/<id>.json.zst
//	index/filecache.json.zst
//	lock
type FileRepo struct {
	dir string
	now func() time.Time

	mu     sync.Mutex // guards held
	held   bool
	writes sync.Mutex // serialises snapshot manifest writes
}

var _ Repo = (*FileRepo)(nil)

// OpenExistingRepo opens the repository in dir only when it already is one
// (dir/repo.json exists). Paths that read a repository, such as listing,
// verifying and restoring, use it so a mistyped folder is never turned into a
// new repository.
func OpenExistingRepo(dir string) (*FileRepo, error) {
	if dir == "" {
		return nil, ErrNotConfigured
	}
	if _, err := os.Stat(filepath.Join(dir, "repo.json")); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s is not a backup repository (no repo.json)", ErrNotFound, dir)
		}
		return nil, err
	}
	return OpenRepo(dir)
}

// OpenRepo opens the repository in dir, creating it when dir is missing or
// empty. A non-empty folder without repo.json is refused so a wrong path is
// never turned into a repository, and a newer format is refused.
func OpenRepo(dir string) (*FileRepo, error) {
	if dir == "" {
		return nil, ErrNotConfigured
	}
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	r := &FileRepo{dir: dir, now: time.Now}
	metaPath := filepath.Join(dir, "repo.json")
	data, err := os.ReadFile(metaPath)
	switch {
	case err == nil:
		var m repoMeta
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, fmt.Errorf("backup: repo.json is unreadable: %w", err)
		}
		if m.Format > RepoFormatVersion {
			return nil, fmt.Errorf("backup: repository format %d is newer than this build supports (%d)", m.Format, RepoFormatVersion)
		}
		if m.Format < RepoFormatVersion {
			// Mark the repository before the first xz blob lands in it.
			m.Format, m.Compression = RepoFormatVersion, "xz"
			b, _ := json.MarshalIndent(m, "", "  ")
			if err := r.writeFileAtomic(metaPath, b); err != nil {
				return nil, fmt.Errorf("backup: upgrading repo.json: %w", err)
			}
		}
	case errors.Is(err, fs.ErrNotExist):
		entries, derr := os.ReadDir(dir)
		if derr != nil && !errors.Is(derr, fs.ErrNotExist) {
			return nil, derr
		}
		if len(entries) > 0 {
			return nil, fmt.Errorf("%w: %s is not empty and is not a backup repository", ErrInvalid, dir)
		}
		if err := os.MkdirAll(dir, dirPerm); err != nil {
			return nil, err
		}
		m := repoMeta{Format: RepoFormatVersion, CreatedAt: r.now().UTC(), Compression: "xz", MinSavingPercent: minSavingPercent}
		b, _ := json.MarshalIndent(m, "", "  ")
		if err := r.writeFileAtomic(metaPath, b); err != nil {
			return nil, err
		}
	default:
		return nil, err
	}
	for _, sub := range []string{"blobs", "snapshots", "index", "tmp"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), dirPerm); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// Dir returns the repository folder.
func (r *FileRepo) Dir() string { return r.dir }

// ---- files ----

// writeFileAtomic writes data to path through a temp file and a rename.
func (r *FileRepo) writeFileAtomic(path string, data []byte) error {
	tmp, err := r.tempFile(filepath.Dir(path))
	if err != nil {
		return err
	}
	name := tmp.Name()
	_, werr := tmp.Write(data)
	return r.finishTemp(tmp, name, path, werr)
}

func (r *FileRepo) tempFile(nearby string) (*os.File, error) {
	tmpDir := filepath.Join(r.dir, "tmp")
	if err := os.MkdirAll(tmpDir, dirPerm); err != nil {
		return nil, err
	}
	_ = nearby
	return os.CreateTemp(tmpDir, "w-*")
}

// finishTemp syncs and closes tmp, then renames it to dst; any earlier error
// removes the temp file instead.
func (r *FileRepo) finishTemp(tmp *os.File, name, dst string, prior error) error {
	if prior == nil {
		prior = tmp.Sync()
	}
	if cerr := tmp.Close(); prior == nil {
		prior = cerr
	}
	if prior == nil {
		if prior = os.MkdirAll(filepath.Dir(dst), dirPerm); prior == nil {
			if prior = os.Chmod(name, filePerm); prior == nil {
				prior = os.Rename(name, dst)
			}
		}
	}
	if prior != nil {
		_ = os.Remove(name)
	}
	return prior
}

func zstdCompress(data []byte, level CompressionLevel) ([]byte, error) {
	lv := zstd.SpeedDefault
	if level == CompressBest {
		lv = zstd.SpeedBestCompression
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(lv), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	defer enc.Close()
	return enc.EncodeAll(data, nil), nil
}

func zstdDecompress(data []byte) ([]byte, error) {
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(maxZstdWindow))
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	return dec.DecodeAll(data, nil)
}

// writeZstdJSON compresses v at the best level into path.
func (r *FileRepo) writeZstdJSON(path string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	z, err := zstdCompress(raw, CompressBest)
	if err != nil {
		return err
	}
	return r.writeFileAtomic(path, z)
}

func readZstdJSON(path string, v any) error {
	z, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	raw, err := zstdDecompress(z)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}

// ---- lock ----

func (r *FileRepo) lockPath() string { return filepath.Join(r.dir, "lock") }

// Lock takes the single-writer lock. It does not wait: a held lock returns
// ErrBusy. A lock file left by a process that no longer exists is replaced.
func (r *FileRepo) Lock(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.held {
		return nil, ErrBusy
	}
	path := r.lockPath()
	for attempt := 0; attempt < 2; attempt++ {
		err := createLockFile(path)
		if err == nil {
			heldLocks.Store(path, struct{}{})
			r.held = true
			return func() {
				r.mu.Lock()
				defer r.mu.Unlock()
				if r.held {
					_ = os.Remove(path)
					heldLocks.Delete(path)
					r.held = false
				}
			}, nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return nil, err
		}
		if !lockStale(path) {
			return nil, ErrBusy
		}
		_ = os.Remove(path)
	}
	return nil, ErrBusy
}

// heldLocks is the set of lock paths this process holds. A lock file with
// our own PID that is not in the set was left by an earlier run that had
// the same PID (common in containers), so it is stale.
var heldLocks sync.Map

// lockGrace is how long a lock file with no readable PID counts as busy:
// its owner may be between creating and writing it.
const lockGrace = 5 * time.Second

// createLockFile makes the lock file with the PID already inside it: the PID
// goes to a temp file that is linked into place, which is atomic and fails
// with ErrExist if the lock is taken. Where hard links are unavailable it
// falls back to an exclusive create.
func createLockFile(path string) error {
	pid := strconv.Itoa(os.Getpid())
	tmp := fmt.Sprintf("%s.%s.%d.tmp", path, pid, time.Now().UnixNano())
	if err := os.WriteFile(tmp, []byte(pid), filePerm); err != nil {
		return err
	}
	defer os.Remove(tmp)
	err := os.Link(tmp, path)
	if err == nil || errors.Is(err, fs.ErrExist) {
		return err
	}
	f, ferr := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, filePerm)
	if ferr != nil {
		return ferr
	}
	_, _ = f.WriteString(pid)
	return f.Close()
}

func lockStale(path string) bool {
	b, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	pid, perr := strconv.Atoi(strings.TrimSpace(string(b)))
	if perr != nil {
		// Empty or garbled: probably still being written unless old.
		fi, serr := os.Stat(path)
		return serr == nil && time.Since(fi.ModTime()) > lockGrace
	}
	if pid == os.Getpid() {
		_, ours := heldLocks.Load(path)
		return !ours
	}
	return !processAlive(pid)
}

func processAlive(pid int) bool {
	if runtime.GOOS == "windows" {
		return true
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = p.Signal(syscall.Signal(0))
	return err == nil || errors.Is(err, syscall.EPERM)
}

// ---- blobs ----

func validSHA(sha string) bool {
	if len(sha) != 64 {
		return false
	}
	for _, c := range sha {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func (r *FileRepo) blobBase(sha string) string {
	return filepath.Join(r.dir, "blobs", sha[:2], sha)
}

// blobPath finds the file holding sha and whether it is stored raw.
func (r *FileRepo) blobPath(sha string) (path string, raw bool, err error) {
	if !validSHA(sha) {
		return "", false, fmt.Errorf("%w: bad blob name %q", ErrInvalid, sha)
	}
	base := r.blobBase(sha)
	for _, ext := range blobExts {
		if _, err := os.Stat(base + ext); err == nil {
			return base + ext, ext == extRaw, nil
		}
	}
	return "", false, fmt.Errorf("blob %s: %w", sha, ErrNotFound)
}

// PutBlob stores r. The content is hashed while it is spooled to a temp
// file, so a large file is never held in memory; it is then compressed
// unless that saves less than 3%.
func (r *FileRepo) PutBlob(ctx context.Context, rd io.Reader, level CompressionLevel) (BlobRef, bool, error) {
	spool, err := r.tempFile("")
	if err != nil {
		return BlobRef{}, false, err
	}
	spoolName := spool.Name()
	defer os.Remove(spoolName) // no-op once renamed
	h := sha256.New()
	size, err := io.Copy(io.MultiWriter(spool, h), &ctxReader{ctx: ctx, r: rd})
	if err != nil {
		spool.Close()
		return BlobRef{}, false, err
	}
	sha := hex.EncodeToString(h.Sum(nil))
	ref := BlobRef{SHA256: sha, Size: size, Stored: size}
	if _, _, err := r.blobPath(sha); err == nil {
		spool.Close()
		return r.existingRef(sha, size), false, nil
	}

	// Try compressing.
	if size > 0 {
		if _, err := spool.Seek(0, io.SeekStart); err != nil {
			spool.Close()
			return BlobRef{}, false, err
		}
		zf, err := r.tempFile("")
		if err != nil {
			spool.Close()
			return BlobRef{}, false, err
		}
		zname := zf.Name()
		defer os.Remove(zname)
		ext, err := compressBlob(zf, spool, size, level)
		if err != nil {
			zf.Close()
			spool.Close()
			return BlobRef{}, false, err
		}
		zsize, err := zf.Seek(0, io.SeekEnd)
		if err != nil {
			zf.Close()
			spool.Close()
			return BlobRef{}, false, err
		}
		if zsize*100 <= size*(100-minSavingPercent) {
			spool.Close()
			ref.Stored = zsize
			if err := r.finishTemp(zf, zname, r.blobBase(sha)+ext, nil); err != nil {
				return BlobRef{}, false, err
			}
			return ref, true, nil
		}
		zf.Close()
	}
	if err := r.finishTemp(spool, spoolName, r.blobBase(sha)+extRaw, nil); err != nil {
		return BlobRef{}, false, err
	}
	return ref, true, nil
}

// compressBlob writes src (size bytes) to dst and returns the extension for
// the codec used: xz for CompressDefault and CompressBest, zstd for
// CompressFast. Measured on NZB records, xz stores about 77% of the raw size
// where zstd's best level stores about 82% and its default level 100%.
func compressBlob(dst io.Writer, src io.Reader, size int64, level CompressionLevel) (string, error) {
	if level == CompressFast {
		enc, err := zstd.NewWriter(dst, zstd.WithEncoderLevel(zstd.SpeedBetterCompression), zstd.WithEncoderConcurrency(1))
		if err != nil {
			return "", err
		}
		_, err = io.Copy(enc, src)
		if cerr := enc.Close(); err == nil {
			err = cerr
		}
		return extZstd, err
	}
	enc, err := xz.WriterConfig{DictCap: xzDictCap(size)}.NewWriter(dst)
	if err != nil {
		return "", err
	}
	_, err = io.Copy(enc, src)
	if cerr := enc.Close(); err == nil {
		err = cerr
	}
	return extXZ, err
}

// xzDictCap is the smallest power-of-two dictionary that holds size bytes,
// between 64 KB and maxXZDict.
func xzDictCap(size int64) int {
	d := 64 << 10
	for int64(d) < size && d < maxXZDict {
		d <<= 1
	}
	return d
}

func (r *FileRepo) existingRef(sha string, size int64) BlobRef {
	ref := BlobRef{SHA256: sha, Size: size, Stored: size}
	if p, _, err := r.blobPath(sha); err == nil {
		if st, err := os.Stat(p); err == nil {
			ref.Stored = st.Size()
		}
	}
	return ref
}

type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c *ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

type blobReader struct {
	io.Reader
	closers []func() error
}

func (b *blobReader) Close() error {
	var first error
	for _, c := range b.closers {
		if err := c(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// OpenBlob returns the decompressed contents of a blob.
func (r *FileRepo) OpenBlob(ctx context.Context, sha string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, raw, err := r.blobPath(sha)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if raw {
		return f, nil
	}
	if filepath.Ext(path) == extXZ {
		xr, err := xz.NewReader(f)
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("blob %s: %w", sha, err)
		}
		return &blobReader{Reader: xr, closers: []func() error{f.Close}}, nil
	}
	dec, err := zstd.NewReader(f, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(maxZstdWindow))
	if err != nil {
		f.Close()
		return nil, err
	}
	return &blobReader{Reader: dec, closers: []func() error{func() error { dec.Close(); return nil }, f.Close}}, nil
}

func (r *FileRepo) HasBlob(_ context.Context, sha string) (bool, error) {
	_, _, err := r.blobPath(sha)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

// readBlob reads a whole blob into memory (manifests and trees only).
func readBlob(ctx context.Context, bs BlobStore, sha string) ([]byte, error) {
	rc, err := bs.OpenBlob(ctx, sha)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// ---- snapshots ----

func (r *FileRepo) snapshotPath(id string) (string, error) {
	if id == "" || strings.ContainsAny(id, `/\`) || strings.Contains(id, "..") || strings.HasPrefix(id, ".") {
		return "", fmt.Errorf("%w: bad snapshot id %q", ErrInvalid, id)
	}
	return filepath.Join(r.dir, "snapshots", id+".json"+extZstd), nil
}

func newSnapshotID(t time.Time) string {
	var b [2]byte
	_, _ = rand.Read(b[:])
	return t.UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(b[:])
}

// WriteSnapshot stores the manifest. An empty ID is filled in from the
// creation time (set to now when zero) and a short random suffix.
func (r *FileRepo) WriteSnapshot(ctx context.Context, s *Snapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.CreatedAt.IsZero() {
		s.CreatedAt = r.now().UTC()
	}
	if s.ID == "" {
		s.ID = newSnapshotID(s.CreatedAt)
	}
	path, err := r.snapshotPath(s.ID)
	if err != nil {
		return err
	}
	r.writes.Lock()
	defer r.writes.Unlock()
	return r.writeZstdJSON(path, s)
}

// loadSnapshots reads every manifest, newest first. In strict mode an
// unreadable manifest is an error (prune must never run on a partial view);
// otherwise it is skipped and its id returned in bad.
func (r *FileRepo) loadSnapshots(ctx context.Context, strict bool) (snaps []*Snapshot, bad []string, err error) {
	entries, err := os.ReadDir(filepath.Join(r.dir, "snapshots"))
	if err != nil {
		return nil, nil, err
	}
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".json"+extZstd) {
			continue
		}
		id := strings.TrimSuffix(name, ".json"+extZstd)
		var s Snapshot
		if err := readZstdJSON(filepath.Join(r.dir, "snapshots", name), &s); err != nil {
			if strict {
				return nil, nil, fmt.Errorf("backup: snapshot %s is unreadable: %w", id, err)
			}
			bad = append(bad, id)
			continue
		}
		if s.ID == "" {
			s.ID = id
		}
		snaps = append(snaps, &s)
	}
	sort.Slice(snaps, func(i, j int) bool {
		if !snaps[i].CreatedAt.Equal(snaps[j].CreatedAt) {
			return snaps[i].CreatedAt.After(snaps[j].CreatedAt)
		}
		return snaps[i].ID > snaps[j].ID
	})
	return snaps, bad, nil
}

// ListSnapshots lists readable snapshots, newest first.
func (r *FileRepo) ListSnapshots(ctx context.Context) ([]SnapshotInfo, error) {
	snaps, _, err := r.loadSnapshots(ctx, false)
	if err != nil {
		return nil, err
	}
	out := make([]SnapshotInfo, 0, len(snaps))
	for _, s := range snaps {
		out = append(out, s.Info())
	}
	return out, nil
}

func (r *FileRepo) ReadSnapshot(ctx context.Context, id string) (*Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := r.snapshotPath(id)
	if err != nil {
		return nil, err
	}
	var s Snapshot
	if err := readZstdJSON(path, &s); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("snapshot %s: %w", id, ErrNotFound)
		}
		return nil, fmt.Errorf("backup: snapshot %s is unreadable: %w", id, err)
	}
	if s.ID == "" {
		s.ID = id
	}
	return &s, nil
}

// DeleteSnapshot removes the manifest only.
func (r *FileRepo) DeleteSnapshot(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := r.snapshotPath(id)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("snapshot %s: %w", id, ErrNotFound)
		}
		return err
	}
	return nil
}

// snapshotShas adds every blob s refers to, including the files inside
// tree blobs, to set. A tree that cannot be read is an error.
func (r *FileRepo) snapshotShas(ctx context.Context, s *Snapshot, set map[string]struct{}) error {
	for _, c := range s.Components {
		for _, v := range c.StoreBlobs {
			set[v] = struct{}{}
		}
		for _, v := range c.Files {
			set[v] = struct{}{}
		}
		if c.Manifest != "" {
			set[c.Manifest] = struct{}{}
		}
		if c.Tree != "" {
			set[c.Tree] = struct{}{}
			t, err := ReadTree(ctx, r, c.Tree)
			if err != nil {
				return fmt.Errorf("snapshot %s: tree %s: %w", s.ID, c.Tree, err)
			}
			for _, sha := range t.Files {
				set[sha] = struct{}{}
			}
		}
	}
	return nil
}
