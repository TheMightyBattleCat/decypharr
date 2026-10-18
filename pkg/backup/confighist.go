package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sjson "github.com/bytedance/sonic"
	"github.com/sirrobot01/decypharr/internal/config"
)

const (
	// ConfigHistoryFile is the history index inside the history folder.
	ConfigHistoryFile = "config-history.json"
	// DefaultConfigHistoryKeep is how many versions the history keeps, on
	// top of any a kept snapshot holds.
	DefaultConfigHistoryKeep = 200
	// configSource is recorded when a save does not say which endpoint made it.
	configSource = "config"
)

// HistoryOptions configures the settings history.
type HistoryOptions struct {
	// Dir holds config-history.json (the backup folder).
	Dir string
	// Blobs stores each version of config.json. Repo.Prune only keeps blobs
	// a snapshot references, so pass a store Prune does not sweep (for
	// example OpenRepo on a folder of its own); a version that a kept
	// snapshot holds is also read from Snapshots when Blobs lacks it.
	Blobs BlobStore
	// Snapshots is optional. Versions whose sha a kept snapshot lists under
	// the config component are never trimmed.
	Snapshots Repo
	// Keep is the number of versions kept; 0 means DefaultConfigHistoryKeep.
	Keep int
	// Apply writes a version back through Config.Save; nil uses
	// ApplyConfigJSON.
	Apply func(ctx context.Context, data []byte) (restartRequired bool, err error)
	// Applied is called after a revert has saved the settings, with the
	// settings from before and after. The wiring points it at what a save
	// from the settings page does next (Arr sync, virtual folders, repair
	// and wanted-search schedules, .strm sweep). Nil does nothing.
	Applied func(previous, published *config.Config)
	// Current returns the live config.json for "current" diffs; nil reads
	// the file of the running configuration.
	Current func() ([]byte, error)
	// Source names the endpoint behind the save being recorded; nil records
	// "config". The API sets it around the save it makes.
	Source func() string
	// Now is the clock; nil uses time.Now.
	Now func() time.Time
}

// History is the settings history. Create it with NewHistory; it implements
// HistoryService.
type History struct {
	o  HistoryOptions
	mu sync.Mutex
	// liveKeep is backup.config_history_max from the newest save, so a
	// changed setting applies without rebuilding the history. It is read
	// from the saved bytes because OnSaved runs under the config lock and
	// must not call config.Get.
	liveKeep atomic.Int64
}

var _ HistoryService = (*History)(nil)

type historyFile struct {
	// Versions run oldest first.
	Versions []ConfigVersion `json:"versions"`
}

// NewHistory returns the settings history for o.
func NewHistory(o HistoryOptions) *History {
	if o.Keep <= 0 {
		o.Keep = DefaultConfigHistoryKeep
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Apply == nil {
		applied := o.Applied
		o.Apply = func(ctx context.Context, data []byte) (bool, error) {
			prev, pub, err := applyConfigJSON(ctx, data)
			if err != nil {
				return false, err
			}
			if applied != nil {
				applied(prev, pub)
			}
			return prev.RequiresRestart(pub), nil
		}
	}
	if o.Current == nil {
		o.Current = func() ([]byte, error) { return os.ReadFile(config.Get().JsonFile()) }
	}
	return &History{o: o}
}

// keep is the number of versions to keep now.
func (h *History) keep() int {
	if n := int(h.liveKeep.Load()); n > 0 {
		return n
	}
	return h.o.Keep
}

func (h *History) path() string { return filepath.Join(h.o.Dir, ConfigHistoryFile) }

func (h *History) load() (historyFile, error) {
	var f historyFile
	b, err := os.ReadFile(h.path())
	if errors.Is(err, os.ErrNotExist) {
		return f, nil
	}
	if err != nil {
		return f, err
	}
	if err := json.Unmarshal(b, &f); err != nil {
		return f, fmt.Errorf("backup: %s is unreadable: %w", ConfigHistoryFile, err)
	}
	return f, nil
}

func (h *History) store(f historyFile) error {
	b, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(h.o.Dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(h.o.Dir, ConfigHistoryFile+".tmp-*")
	if err != nil {
		return err
	}
	_, werr := tmp.Write(b)
	cerr := tmp.Close()
	if err := errors.Join(werr, cerr); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	if err := os.Rename(tmp.Name(), h.path()); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	return nil
}

// OnSaved is the hook for config.OnSaved. A failure is dropped: a settings
// save never fails because the history could not be written.
func (h *History) OnSaved(data []byte) {
	var saved struct {
		Backup struct {
			Max int `json:"config_history_max"`
		} `json:"backup"`
	}
	if json.Unmarshal(data, &saved) == nil && saved.Backup.Max > 0 {
		h.liveKeep.Store(int64(saved.Backup.Max))
	}
	source := configSource
	if h.o.Source != nil {
		if s := h.o.Source(); s != "" {
			source = s
		}
	}
	_, _, _ = h.Record(context.Background(), data, source)
}

// Record stores data as a new version. It reports false, adding nothing,
// when data equals the newest version.
func (h *History) Record(ctx context.Context, data []byte, source string) (ConfigVersion, bool, error) {
	sum := sha256.Sum256(data)
	sha := hex.EncodeToString(sum[:])

	h.mu.Lock()
	defer h.mu.Unlock()
	f, err := h.load()
	if err != nil {
		return ConfigVersion{}, false, err
	}
	if n := len(f.Versions); n > 0 && f.Versions[n-1].SHA256 == sha {
		return f.Versions[n-1], false, nil
	}
	ref, _, err := h.o.Blobs.PutBlob(ctx, bytes.NewReader(data), CompressDefault)
	if err != nil {
		return ConfigVersion{}, false, err
	}
	if ref.SHA256 != sha {
		return ConfigVersion{}, false, fmt.Errorf("backup: blob store returned sha %s for %s", ref.SHA256, sha)
	}
	now := h.o.Now().UTC()
	v := ConfigVersion{
		ID:     fmt.Sprintf("%d-%s", now.UnixMilli(), sha[:8]),
		Time:   now,
		Source: source,
		SHA256: sha,
	}
	// Two saves in one millisecond with the same bytes cannot reach here
	// (the second is unchanged), but guard the id anyway.
	for _, old := range f.Versions {
		if old.ID == v.ID {
			v.ID += "-" + strconv.Itoa(len(f.Versions))
			break
		}
	}
	f.Versions = append(f.Versions, v)
	f.Versions = h.trim(ctx, f.Versions)
	if err := h.store(f); err != nil {
		return ConfigVersion{}, false, err
	}
	return v, true, nil
}

// pinned returns the config blob shas that kept snapshots hold.
func (h *History) pinned(ctx context.Context) map[string]bool {
	out := map[string]bool{}
	if h.o.Snapshots == nil {
		return out
	}
	infos, err := h.o.Snapshots.ListSnapshots(ctx)
	if errors.Is(err, ErrNotConfigured) {
		// No snapshot repository, so no snapshot references a version.
		return out
	}
	if err != nil {
		// Unknown: pin everything rather than trim a version a snapshot needs.
		return nil
	}
	for _, in := range infos {
		s, err := h.o.Snapshots.ReadSnapshot(ctx, in.ID)
		if err != nil {
			return nil
		}
		for _, sha := range s.Components[ComponentConfig].Files {
			out[sha] = true
		}
	}
	return out
}

// trim drops the oldest versions beyond the keep count, except those a kept
// snapshot references. The newest o.Keep always stay.
func (h *History) trim(ctx context.Context, vs []ConfigVersion) []ConfigVersion {
	keep := h.keep()
	if len(vs) <= keep {
		return vs
	}
	pinned := h.pinned(ctx)
	if pinned == nil {
		return vs
	}
	cut := len(vs) - keep
	out := make([]ConfigVersion, 0, len(vs))
	for i, v := range vs {
		if i >= cut || pinned[v.SHA256] {
			out = append(out, v)
		}
	}
	return out
}

// List returns the versions, newest first.
func (h *History) List(context.Context) ([]ConfigVersion, error) {
	h.mu.Lock()
	f, err := h.load()
	h.mu.Unlock()
	if err != nil {
		return nil, err
	}
	out := make([]ConfigVersion, len(f.Versions))
	for i, v := range f.Versions {
		out[len(out)-1-i] = v
	}
	return out, nil
}

func (h *History) read(ctx context.Context, sha string) ([]byte, error) {
	try := func(bs BlobStore) ([]byte, error) {
		rc, err := bs.OpenBlob(ctx, sha)
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		return io.ReadAll(rc)
	}
	b, err := try(h.o.Blobs)
	if err == nil {
		return b, nil
	}
	if h.o.Snapshots != nil {
		if b2, err2 := try(h.o.Snapshots); err2 == nil {
			return b2, nil
		}
	}
	return nil, fmt.Errorf("%w: config version blob %s: %v", ErrNotFound, sha, err)
}

func (h *History) find(ctx context.Context, id string) (ConfigVersion, int, []ConfigVersion, error) {
	h.mu.Lock()
	f, err := h.load()
	h.mu.Unlock()
	if err != nil {
		return ConfigVersion{}, -1, nil, err
	}
	for i, v := range f.Versions {
		if v.ID == id {
			return v, i, f.Versions, nil
		}
	}
	return ConfigVersion{}, -1, nil, fmt.Errorf("%w: config version %q", ErrNotFound, id)
}

// Diff compares version id with against: "previous" (the default, the
// version saved just before id), "current" (the live config.json) or another
// version id. Old holds the value in against and New the value in id. Only
// changed settings are listed, except that secrets are listed whether or not
// they changed, with no values.
func (h *History) Diff(ctx context.Context, id, against string) (ConfigDiff, error) {
	v, idx, all, err := h.find(ctx, id)
	if err != nil {
		return ConfigDiff{}, err
	}
	newB, err := h.read(ctx, v.SHA256)
	if err != nil {
		return ConfigDiff{}, err
	}
	if against == "" {
		against = "previous"
	}
	var oldB []byte
	switch against {
	case "previous":
		if idx > 0 {
			if oldB, err = h.read(ctx, all[idx-1].SHA256); err != nil {
				return ConfigDiff{}, err
			}
		}
	case "current":
		if oldB, err = h.o.Current(); err != nil {
			return ConfigDiff{}, err
		}
	default:
		ov, _, _, ferr := h.find(ctx, against)
		if ferr != nil {
			return ConfigDiff{}, ferr
		}
		if oldB, err = h.read(ctx, ov.SHA256); err != nil {
			return ConfigDiff{}, err
		}
	}
	lines, err := DiffConfigJSON(oldB, newB)
	if err != nil {
		return ConfigDiff{}, err
	}
	return ConfigDiff{ID: id, Against: against, Lines: lines}, nil
}

// Revert writes version id through Config.Save, which records it as a new
// version.
func (h *History) Revert(ctx context.Context, id string) (RevertResponse, error) {
	v, _, _, err := h.find(ctx, id)
	if err != nil {
		return RevertResponse{}, err
	}
	data, err := h.read(ctx, v.SHA256)
	if err != nil {
		return RevertResponse{}, err
	}
	restart, err := h.o.Apply(ctx, data)
	if err != nil {
		return RevertResponse{}, err
	}
	// The save ran OnSaved, so the newest entry is the revert (or, when the
	// bytes already matched the newest version, nothing was added).
	list, err := h.List(ctx)
	if err != nil {
		return RevertResponse{}, err
	}
	newID := ""
	if len(list) > 0 {
		newID = list[0].ID
	}
	return RevertResponse{NewID: newID, RestartRequired: restart}, nil
}

// ApplyConfigJSON replaces the running configuration with the settings in
// data, through config.Update and so Config.Save. The sign-in file, the
// session secret and the STRM signing secret are kept as they are now:
// reverting settings must not log everyone out or invalidate written .strm
// files. It reports whether the change needs a restart.
func ApplyConfigJSON(ctx context.Context, data []byte) (bool, error) {
	prev, published, err := applyConfigJSON(ctx, data)
	if err != nil {
		return false, err
	}
	return prev.RequiresRestart(published), nil
}

// applyConfigJSON does the work of ApplyConfigJSON and returns the settings
// from before and after the save.
func applyConfigJSON(_ context.Context, data []byte) (prev, published *config.Config, err error) {
	published, err = config.Update(func(c *config.Config) error {
		var next config.Config
		if err := sjson.Unmarshal(data, &next); err != nil {
			return fmt.Errorf("%w: the stored settings cannot be read: %v", ErrInvalid, err)
		}
		p, err := c.Clone()
		if err != nil {
			return err
		}
		prev = p
		next.Auth = c.Auth
		if c.SessionSecret != "" {
			next.SessionSecret = c.SessionSecret
		}
		if c.Strm.Secret != "" {
			next.Strm.Secret = c.Strm.Secret
		}
		*c = next
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return prev, published, nil
}

// ---- Diff with secrets masked ----

var secretWords = []string{"key", "pass", "token", "secret", "webhook", "cookie", "credential", "signature", "callback"}

// isSecretPath reports whether a setting at path holds a secret. The check
// runs on every segment so everything under "auth" or "api_keys" is covered.
func isSecretPath(path string, value any) bool {
	if _, isBool := value.(bool); isBool {
		return false
	}
	for _, seg := range strings.Split(path, ".") {
		seg = strings.ToLower(seg)
		if i := strings.IndexByte(seg, '['); i >= 0 {
			seg = seg[:i]
		}
		if seg == "auth" {
			return true
		}
		for _, w := range secretWords {
			if strings.Contains(seg, w) {
				return true
			}
		}
	}
	if s, ok := value.(string); ok {
		l := strings.ToLower(s)
		if strings.Contains(l, "/webhooks/") || strings.Contains(l, "hooks.slack.com") {
			return true
		}
		if i := strings.Index(s, "://"); i >= 0 && strings.Contains(s[i+3:], "@") {
			return true // credentials in a URL
		}
	}
	return false
}

func flattenJSON(prefix string, v any, out map[string]any) {
	switch t := v.(type) {
	case map[string]any:
		if len(t) == 0 && prefix != "" {
			out[prefix] = "{}"
		}
		for k, c := range t {
			p := k
			if prefix != "" {
				p = prefix + "." + k
			}
			flattenJSON(p, c, out)
		}
	case []any:
		if len(t) == 0 && prefix != "" {
			out[prefix] = "[]"
		}
		for i, c := range t {
			flattenJSON(prefix+"["+strconv.Itoa(i)+"]", c, out)
		}
	default:
		out[prefix] = v
	}
}

func render(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case nil:
		return "null"
	default:
		b, _ := json.Marshal(t)
		return string(b)
	}
}

func parseConfigJSON(b []byte) (map[string]any, error) {
	out := map[string]any{}
	if len(bytes.TrimSpace(b)) == 0 {
		return out, nil
	}
	var doc any
	if err := json.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("%w: config version is not valid JSON: %v", ErrInvalid, err)
	}
	flattenJSON("", doc, out)
	return out, nil
}

// DiffConfigJSON lists the settings that differ between two config.json
// documents, in path order. Secret settings (API keys, passwords, tokens,
// webhook URLs) are listed with Secret set and no values.
func DiffConfigJSON(oldB, newB []byte) ([]ConfigDiffLine, error) {
	o, err := parseConfigJSON(oldB)
	if err != nil {
		return nil, err
	}
	n, err := parseConfigJSON(newB)
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(o)+len(n))
	seen := map[string]bool{}
	for p := range o {
		seen[p] = true
		paths = append(paths, p)
	}
	for p := range n {
		if !seen[p] {
			paths = append(paths, p)
		}
	}
	sort.Strings(paths)
	var lines []ConfigDiffLine
	for _, p := range paths {
		ov, oOK := o[p]
		nv, nOK := n[p]
		changed := oOK != nOK || render(ov) != render(nv) || fmt.Sprintf("%T", ov) != fmt.Sprintf("%T", nv)
		secret := (oOK && isSecretPath(p, ov)) || (nOK && isSecretPath(p, nv))
		switch {
		case secret:
			lines = append(lines, ConfigDiffLine{Path: p, Secret: true, Changed: changed})
		case changed:
			l := ConfigDiffLine{Path: p, Changed: true}
			if oOK {
				l.Old = render(ov)
			}
			if nOK {
				l.New = render(nv)
			}
			lines = append(lines, l)
		}
	}
	return lines, nil
}
