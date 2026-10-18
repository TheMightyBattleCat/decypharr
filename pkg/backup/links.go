package backup

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/sirrobot01/decypharr/pkg/arr"
)

// LinkFS is the slice of the filesystem the link code uses. Stat is part of
// the interface only so tests can assert it is never called: a link's target
// is on the mount and must never be looked up.
type LinkFS interface {
	WalkDir(root string, fn fs.WalkDirFunc) error
	Lstat(p string) (fs.FileInfo, error)
	Readlink(p string) (string, error)
	Symlink(target, p string) error
	Rename(oldPath, newPath string) error
	MkdirAll(p string, mode fs.FileMode) error
	Remove(p string) error
	// SetLinkTime sets the modification time of the link itself, without
	// following it.
	SetLinkTime(p string, t time.Time) error
	// Stat follows links. Nothing in this package calls it.
	Stat(p string) (fs.FileInfo, error)
}

// OSLinkFS is LinkFS over the real filesystem.
type OSLinkFS struct{}

func (OSLinkFS) WalkDir(root string, fn fs.WalkDirFunc) error { return filepath.WalkDir(root, fn) }
func (OSLinkFS) Lstat(p string) (fs.FileInfo, error)          { return os.Lstat(p) }
func (OSLinkFS) Readlink(p string) (string, error)            { return os.Readlink(p) }
func (OSLinkFS) Symlink(target, p string) error               { return os.Symlink(target, p) }
func (OSLinkFS) Rename(a, b string) error                     { return os.Rename(a, b) }
func (OSLinkFS) MkdirAll(p string, m fs.FileMode) error       { return os.MkdirAll(p, m) }
func (OSLinkFS) Remove(p string) error                        { return os.Remove(p) }
func (OSLinkFS) SetLinkTime(p string, t time.Time) error      { return setLinkTime(p, t) }
func (OSLinkFS) Stat(p string) (fs.FileInfo, error)           { return os.Stat(p) }

// EntryLookup answers the questions the classifier asks of the database. It
// never touches the mount.
type EntryLookup interface {
	// EntryExists reports whether an entry folder of that name exists.
	EntryExists(entry string) bool
	// FileInEntry reports whether the entry has a file of that name.
	FileInEntry(entry, file string) bool
	// EntryByID finds an entry's current folder name from its id.
	EntryByID(id string) (string, bool)
	// EntryID returns the id of an entry, or "" when it is not known.
	EntryID(entry string) string
}

// ArrClient is what link discovery needs from an Arr.
type ArrClient interface {
	RootFolders(ctx context.Context) ([]arr.RootFolder, error)
	MediaFiles(ctx context.Context) ([]arr.MediaFile, error)
	// Rescan asks the Arr to re-read a series or movie.
	Rescan(ctx context.Context, itemID int) error
}

type arrAdapter struct{ a *arr.Arr }

// NewArrClient wraps a Sonarr or Radarr client. It returns nil for any other
// type of Arr.
func NewArrClient(a *arr.Arr) ArrClient {
	if a == nil || (a.Type != arr.Sonarr && a.Type != arr.Radarr) {
		return nil
	}
	return arrAdapter{a}
}

func (c arrAdapter) RootFolders(ctx context.Context) ([]arr.RootFolder, error) {
	return c.a.RootFolders(ctx)
}

func (c arrAdapter) MediaFiles(ctx context.Context) ([]arr.MediaFile, error) {
	if c.a.Type == arr.Sonarr {
		return c.a.EpisodeFiles(ctx)
	}
	return c.a.MovieFiles(ctx)
}

func (c arrAdapter) Rescan(ctx context.Context, itemID int) error {
	if c.a.Type == arr.Sonarr {
		return c.a.RescanSeries(ctx, itemID)
	}
	return c.a.RescanMovie(ctx, itemID)
}

// PathMapping translates a path in an Arr's own namespace (a container) to a
// local one.
type PathMapping struct {
	Arr   string `json:"arr"`
	Local string `json:"local"`
}

// ArrSource is one configured Arr with its optional path map.
type ArrSource struct {
	Name    string
	Client  ArrClient
	PathMap []PathMapping
}

// ToLocal maps an Arr path to a local one using the longest matching prefix.
func (s ArrSource) ToLocal(p string) string {
	best := -1
	out := p
	for _, m := range s.PathMap {
		if rest, ok := trimDirPrefix(p, m.Arr); ok && len(m.Arr) > best {
			best = len(m.Arr)
			out = path.Join(m.Local, rest)
		}
	}
	return filepath.FromSlash(out)
}

// ToArr maps a local path back into the Arr's namespace.
func (s ArrSource) ToArr(p string) string {
	best := -1
	out := p
	for _, m := range s.PathMap {
		if rest, ok := trimDirPrefix(p, m.Local); ok && len(m.Local) > best {
			best = len(m.Local)
			out = path.Join(m.Arr, rest)
		}
	}
	return out
}

// trimDirPrefix strips prefix from p on a directory boundary.
func trimDirPrefix(p, prefix string) (string, bool) {
	p = strings.TrimRight(filepath.ToSlash(p), "/")
	prefix = strings.TrimRight(filepath.ToSlash(prefix), "/")
	if prefix == "" {
		return "", false
	}
	if p == prefix {
		return "", true
	}
	if strings.HasPrefix(p, prefix+"/") {
		return p[len(prefix)+1:], true
	}
	return "", false
}

// LinkRoot is one folder to walk, with the Arrs that manage it.
type LinkRoot struct {
	Path string   `json:"path"` // local form
	Arrs []string `json:"arrs,omitempty"`
}

// RootSet is the result of root discovery.
type RootSet struct {
	Roots []LinkRoot
	// PathMaps is the per-Arr path map, by Arr name.
	PathMaps map[string][]PathMapping
	// Errors are Arrs whose root folders could not be read.
	Errors []string
}

// DiscoverRoots unions every Arr's root folders with the configured extras
// and removes duplicates (after mapping to local paths).
func DiscoverRoots(ctx context.Context, arrs []ArrSource, extra []string) RootSet {
	set := RootSet{PathMaps: map[string][]PathMapping{}}
	idx := map[string]int{}
	add := func(p, arrName string) {
		p = filepath.Clean(p)
		if p == "." || p == "" {
			return
		}
		i, ok := idx[p]
		if !ok {
			i = len(set.Roots)
			idx[p] = i
			set.Roots = append(set.Roots, LinkRoot{Path: p})
		}
		if arrName != "" && !containsString(set.Roots[i].Arrs, arrName) {
			set.Roots[i].Arrs = append(set.Roots[i].Arrs, arrName)
		}
	}
	for _, s := range arrs {
		if s.Client == nil {
			continue
		}
		if len(s.PathMap) > 0 {
			set.PathMaps[s.Name] = s.PathMap
		}
		roots, err := s.Client.RootFolders(ctx)
		if err != nil {
			set.Errors = append(set.Errors, fmt.Sprintf("%s: %v", s.Name, err))
			continue
		}
		for _, r := range roots {
			if r.Path != "" {
				add(s.ToLocal(r.Path), s.Name)
			}
		}
	}
	for _, e := range extra {
		if strings.TrimSpace(e) != "" {
			add(e, "")
		}
	}
	sort.Slice(set.Roots, func(i, j int) bool { return set.Roots[i].Path < set.Roots[j].Path })
	return set
}

func containsString(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// Manifest is the link manifest written with every snapshot.
type Manifest struct {
	Rows []LinkRow
	// Missing are files an Arr lists that have no link on disk.
	Missing []MissingLink
	// Roots are the roots that were walked.
	Roots []string
	// UnusedRoots are roots that no series or movie uses, or that hold only
	// dead links. Only filled when every Arr could be read.
	UnusedRoots []string
	// Errors are non-fatal problems (an Arr that did not answer).
	Errors []string
}

// ManifestInput configures BuildManifest.
type ManifestInput struct {
	FS        LinkFS
	Lookup    EntryLookup
	MountRoot string
	Roots     RootSet
	Arrs      []ArrSource
}

const allFolder = "__all__"

// classifyTarget decides a link's state from its raw target string and the
// database lookup alone.
func classifyTarget(target, mountRoot string, lk EntryLookup) (state LinkState, entry, file, entryID string) {
	mountRoot = strings.TrimRight(mountRoot, "/")
	if mountRoot == "" || !strings.HasPrefix(target, mountRoot+"/"+allFolder+"/") {
		return LinkExternal, "", "", ""
	}
	rest := target[len(mountRoot)+len(allFolder)+2:]
	parts := strings.SplitN(rest, "/", 2)
	entry = parts[0]
	if len(parts) == 2 {
		file = parts[1]
	}
	if entry == "" {
		return LinkExternal, "", "", ""
	}
	if !lk.EntryExists(entry) {
		return LinkEntryGone, entry, file, ""
	}
	entryID = lk.EntryID(entry)
	if file != "" && !lk.FileInEntry(entry, file) {
		return LinkTargetGone, entry, file, entryID
	}
	return LinkLive, entry, file, entryID
}

// BuildManifest walks every root and returns the manifest. It uses Lstat and
// Readlink only.
func BuildManifest(ctx context.Context, in ManifestInput, p Progress) (*Manifest, error) {
	m := &Manifest{Errors: append([]string(nil), in.Roots.Errors...)}
	seen := map[string]int{} // path -> index in m.Rows
	done := 0
	for _, root := range in.Roots.Roots {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, err := in.FS.Lstat(root.Path); err != nil {
			m.Errors = append(m.Errors, fmt.Sprintf("root %s: %v", root.Path, err))
			continue
		}
		m.Roots = append(m.Roots, root.Path)
		err := in.FS.WalkDir(root.Path, func(pth string, d fs.DirEntry, err error) error {
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
			if err != nil {
				if d != nil && d.IsDir() {
					return fs.SkipDir
				}
				return nil
			}
			if d.Type()&fs.ModeSymlink == 0 {
				return nil
			}
			if _, dup := seen[pth]; dup {
				return nil
			}
			target, err := in.FS.Readlink(pth)
			if err != nil {
				return nil
			}
			row := LinkRow{Path: pth, Target: target, Root: root.Path}
			if fi, err := in.FS.Lstat(pth); err == nil {
				row.Mode = uint32(fi.Mode().Perm())
				row.MTime = fi.ModTime().UTC()
			}
			row.State, row.Entry, row.File, row.EntryID = classifyTarget(target, in.MountRoot, in.Lookup)
			if row.State != LinkExternal {
				row.MountRoot = strings.TrimRight(in.MountRoot, "/")
			}
			seen[pth] = len(m.Rows)
			m.Rows = append(m.Rows, row)
			done++
			if done%500 == 0 {
				p.Report(ProgressUpdate{Component: ComponentLinks, Phase: "walk", FilesDone: done})
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	p.Report(ProgressUpdate{Component: ComponentLinks, Phase: "arr", FilesDone: done, FilesTotal: done})
	crossCheck(ctx, in, m, seen)
	sort.Slice(m.Rows, func(i, j int) bool { return m.Rows[i].Path < m.Rows[j].Path })
	sort.Slice(m.Missing, func(i, j int) bool { return m.Missing[i].Path < m.Missing[j].Path })
	p.Report(ProgressUpdate{Component: ComponentLinks, Phase: "done", FilesDone: len(m.Rows), FilesTotal: len(m.Rows)})
	return m, nil
}

// crossCheck fills the Arr ids on rows, lists files an Arr knows that have no
// link on disk, and finds unused roots.
func crossCheck(ctx context.Context, in ManifestInput, m *Manifest, seen map[string]int) {
	allRead := true
	// used roots: a root with an Arr file under it that is not a dead link.
	used := map[string]bool{}
	arrCount := 0
	for _, s := range in.Arrs {
		if s.Client == nil {
			continue
		}
		arrCount++
		files, err := s.Client.MediaFiles(ctx)
		if err != nil {
			allRead = false
			m.Errors = append(m.Errors, fmt.Sprintf("%s: %v", s.Name, err))
			continue
		}
		for _, f := range files {
			if f.Path == "" {
				continue
			}
			local := s.ToLocal(f.Path)
			var rootOf string
			for _, r := range in.Roots.Roots {
				if _, ok := trimDirPrefix(local, r.Path); ok && len(r.Path) > len(rootOf) {
					rootOf = r.Path
				}
			}
			if i, ok := seen[local]; ok {
				row := &m.Rows[i]
				row.Arr, row.ArrItemID, row.ArrFileID = s.Name, f.ItemID, f.FileID
				if len(s.PathMap) > 0 {
					row.ArrPath = f.Path
				}
				if rootOf != "" && row.State != LinkTargetGone && row.State != LinkEntryGone {
					used[rootOf] = true
				}
				continue
			}
			// Lstat does not follow the last element only: a recorded folder
			// link above the file would send it onto the mount.
			if underRecordedLink(local, rootOf, seen) {
				if rootOf != "" {
					used[rootOf] = true
				}
				continue
			}
			fi, err := in.FS.Lstat(local)
			switch {
			case err == nil:
				// A real file, or a link outside the walked roots: nothing is missing.
				_ = fi
				if rootOf != "" {
					used[rootOf] = true
				}
			case errors.Is(err, fs.ErrNotExist):
				m.Missing = append(m.Missing, MissingLink{Path: local, Arr: s.Name, ArrItemID: f.ItemID, Title: f.Title})
				if rootOf != "" {
					used[rootOf] = true
				}
			}
		}
	}
	if allRead && arrCount > 0 {
		for _, r := range m.Roots {
			if !used[r] {
				m.UnusedRoots = append(m.UnusedRoots, r)
			}
		}
	}
}

// underRecordedLink reports whether a parent folder of p, below root, is a
// symlink row of the walk.
func underRecordedLink(p, root string, seen map[string]int) bool {
	for d := filepath.Dir(p); d != root && d != filepath.Dir(d); d = filepath.Dir(d) {
		if _, ok := seen[d]; ok {
			return true
		}
	}
	return false
}

// LinkCheckResult is the outcome of one check: counts per root and state.
type LinkCheckResult struct {
	At      time.Time
	Roots   []RootSummary
	Live    int
	Dead    int
	Missing int
}

// Summarise counts the manifest per root and state.
func Summarise(m *Manifest, at time.Time) LinkCheckResult {
	res := LinkCheckResult{At: at, Missing: len(m.Missing)}
	byRoot := map[string]*RootSummary{}
	for _, r := range m.Roots {
		byRoot[r] = &RootSummary{Root: r}
	}
	for _, u := range m.UnusedRoots {
		if s := byRoot[u]; s != nil {
			s.Unused = true
		}
	}
	for _, row := range m.Rows {
		s := byRoot[row.Root]
		if s == nil {
			s = &RootSummary{Root: row.Root}
			byRoot[row.Root] = s
		}
		s.Total++
		switch row.State {
		case LinkLive:
			s.Live++
			res.Live++
		case LinkTargetGone:
			s.Target++
			res.Dead++
		case LinkEntryGone:
			s.Entry++
			res.Dead++
		case LinkExternal:
			s.External++
			res.Live++
		}
	}
	for _, s := range byRoot {
		res.Roots = append(res.Roots, *s)
	}
	sort.Slice(res.Roots, func(i, j int) bool { return res.Roots[i].Root < res.Roots[j].Root })
	return res
}

// Status converts a result to the API summary.
func (r LinkCheckResult) Status() LinksStatusResponse {
	at := r.At
	return LinksStatusResponse{Roots: r.Roots, LastCheck: &at, Missing: r.Missing}
}

// Trend converts a result to a trend point.
func (r LinkCheckResult) Trend() TrendPoint {
	return TrendPoint{At: r.At, Live: r.Live, Dead: r.Dead}
}

// ---- manifest encoding ----

const manifestMagic = "#decypharr-links v1"

func tsvEscape(s string) string {
	if !strings.ContainsAny(s, "\\\t\n\r") {
		return s
	}
	r := strings.NewReplacer("\\", `\\`, "\t", `\t`, "\n", `\n`, "\r", `\r`)
	return r.Replace(s)
}

func tsvUnescape(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' || i+1 == len(s) {
			b.WriteByte(s[i])
			continue
		}
		i++
		switch s[i] {
		case 't':
			b.WriteByte('\t')
		case 'n':
			b.WriteByte('\n')
		case 'r':
			b.WriteByte('\r')
		default:
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

func writeTSV(w *bufio.Writer, fields ...string) {
	for i, f := range fields {
		if i > 0 {
			w.WriteByte('\t')
		}
		w.WriteString(tsvEscape(f))
	}
	w.WriteByte('\n')
}

// EncodeManifest writes the manifest as zstd-compressed TSV. Lines start with
// a kind: R (root walked), U (unused root), L (link), M (missing file).
func EncodeManifest(w io.Writer, m *Manifest) error {
	zw, err := zstd.NewWriter(w)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(zw)
	bw.WriteString(manifestMagic + "\n")
	for _, r := range m.Roots {
		writeTSV(bw, "R", r)
	}
	for _, r := range m.UnusedRoots {
		writeTSV(bw, "U", r)
	}
	for _, r := range m.Rows {
		mt := ""
		if !r.MTime.IsZero() {
			mt = r.MTime.UTC().Format(time.RFC3339Nano)
		}
		writeTSV(bw, "L", r.Path, r.Target, r.MountRoot, r.Entry, r.File, r.EntryID, r.Arr,
			strconv.Itoa(r.ArrItemID), strconv.Itoa(r.ArrFileID), strconv.FormatUint(uint64(r.Mode), 8),
			mt, string(r.State), r.Root, r.ArrPath)
	}
	for _, x := range m.Missing {
		writeTSV(bw, "M", x.Path, x.Arr, strconv.Itoa(x.ArrItemID), x.Title)
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	return zw.Close()
}

// DecodeManifest reads what EncodeManifest wrote.
func DecodeManifest(r io.Reader) (*Manifest, error) {
	zr, err := zstd.NewReader(r, zstd.WithDecoderMaxWindow(maxZstdWindow))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	if !sc.Scan() || sc.Text() != manifestMagic {
		return nil, errors.New("backup: not a link manifest")
	}
	m := &Manifest{}
	line := 1
	for sc.Scan() {
		line++
		if sc.Text() == "" {
			continue
		}
		f := strings.Split(sc.Text(), "\t")
		for i := range f {
			f[i] = tsvUnescape(f[i])
		}
		bad := func() error { return fmt.Errorf("backup: bad link manifest line %d", line) }
		switch f[0] {
		case "R":
			if len(f) != 2 {
				return nil, bad()
			}
			m.Roots = append(m.Roots, f[1])
		case "U":
			if len(f) != 2 {
				return nil, bad()
			}
			m.UnusedRoots = append(m.UnusedRoots, f[1])
		case "L":
			if len(f) != 15 {
				return nil, bad()
			}
			row := LinkRow{Path: f[1], Target: f[2], MountRoot: f[3], Entry: f[4], File: f[5], EntryID: f[6],
				Arr: f[7], State: LinkState(f[12]), Root: f[13], ArrPath: f[14]}
			var e1, e2, e3, e4 error
			row.ArrItemID, e1 = strconv.Atoi(f[8])
			row.ArrFileID, e2 = strconv.Atoi(f[9])
			mode, e3 := strconv.ParseUint(f[10], 8, 32)
			row.Mode = uint32(mode)
			if f[11] != "" {
				row.MTime, e4 = time.Parse(time.RFC3339Nano, f[11])
			}
			if e1 != nil || e2 != nil || e3 != nil || e4 != nil {
				return nil, bad()
			}
			m.Rows = append(m.Rows, row)
		case "M":
			if len(f) != 5 {
				return nil, bad()
			}
			id, err := strconv.Atoi(f[3])
			if err != nil {
				return nil, bad()
			}
			m.Missing = append(m.Missing, MissingLink{Path: f[1], Arr: f[2], ArrItemID: id, Title: f[4]})
		default:
			return nil, bad()
		}
	}
	return m, sc.Err()
}
