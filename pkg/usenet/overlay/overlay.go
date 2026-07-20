// Package overlay tracks Usenet articles confirmed missing during playback
// (430 across every provider) and the bytes that eventually replace them.
//
// It is the shared store behind two cooperating features:
//   - playback padding: a dead article is recorded here and, within a strict
//     per-file cap, zero-filled at read time instead of stalling/killing the
//     stream.
//   - PAR2 repair: a background job reconstructs a dead article's true bytes
//     and writes them here as a patch; the reader prefers a patch over both
//     the live NNTP fetch and padding, so a repaired range plays perfectly.
//
// State is persisted per NZB ID under a manifest.json plus one blob file per
// patched segment, so padding/repair state survives process restarts and a
// segment already known dead never needs to be re-classified from scratch.
package overlay

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"
)

const manifestVersion = 1

const manifestFileName = "manifest.json"

// Verdict summarizes a single logical file's damage state.
type Verdict string

const (
	VerdictClean    Verdict = "clean"    // no confirmed-dead segments
	VerdictDegraded Verdict = "degraded" // dead segments within the padding caps - playable with glitches
	VerdictFailed   Verdict = "failed"   // beyond the caps, or not a paddable container - unwatchable, legacy repair
)

// SegmentStatus is the lifecycle state of one dead segment record.
type SegmentStatus string

const (
	StatusDead    SegmentStatus = "dead"    // confirmed missing, not yet padded or repaired
	StatusPadded  SegmentStatus = "padded"  // being served as zero-fill
	StatusPatched SegmentStatus = "patched" // real bytes recovered via PAR2 and stored as a patch blob
)

// Decision is the outcome of consulting the padding policy for one segment.
type Decision int

const (
	DecisionFail Decision = iota
	DecisionPad
)

// DeadSegment is one confirmed-missing article recorded against a logical file.
type DeadSegment struct {
	Index     int           `json:"index"`
	MessageID string        `json:"message_id"`
	Bytes     int64         `json:"bytes"`
	Status    SegmentStatus `json:"status"`
}

// FileEntry is the per-logical-filename record inside an NZB's manifest.
type FileEntry struct {
	DeadSegments []DeadSegment `json:"dead_segments"`
	Verdict      Verdict       `json:"verdict"`
}

// Manifest is the whole-NZB record persisted as overlay/<nzbID>/manifest.json.
type Manifest struct {
	Version int                   `json:"version"`
	Files   map[string]*FileEntry `json:"files"`
}

// Store is the on-disk overlay store, rooted next to the stored-NZB records
// directory (see pkg/manager/stale_nzb.go for that location convention) -
// deliberately NOT under the DFS cache, which gets cleared routinely and
// would silently forget every dead-segment/patch record on a cache wipe.
type Store struct {
	root   string
	logger zerolog.Logger

	locksMu sync.Mutex
	locks   map[string]*sync.Mutex

	// loggedPads dedupes the "segment padded" log line to once per
	// (entry, segment) per process, across every reader session that hits it.
	loggedPads sync.Map // map[string]struct{}

	// repairEnqueue is set by the manager-level PAR2 worker (commit 4); nil
	// until then, in which case EnqueueRepair is a no-op.
	repairEnqueue atomic.Pointer[func(nzbID string)]
}

// NewStore creates (if needed) the overlay root directory and returns a Store
// rooted there.
func NewStore(root string, logger zerolog.Logger) (*Store, error) {
	if root == "" {
		return nil, fmt.Errorf("overlay: root path is required")
	}
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, fmt.Errorf("overlay: create root dir: %w", err)
	}
	return &Store{
		root:   root,
		logger: logger,
		locks:  make(map[string]*sync.Mutex),
	}, nil
}

// SetRepairEnqueuer installs the callback EnqueueRepair invokes after a
// segment is padded. fn may be nil to disable enqueuing.
func (s *Store) SetRepairEnqueuer(fn func(nzbID string)) {
	if s == nil {
		return
	}
	if fn == nil {
		s.repairEnqueue.Store(nil)
		return
	}
	s.repairEnqueue.Store(&fn)
}

func (s *Store) enqueueRepair(nzbID string) {
	if s == nil {
		return
	}
	if p := s.repairEnqueue.Load(); p != nil && *p != nil {
		(*p)(nzbID)
	}
}

// Handle binds a Store to one nzbID, for the common case of a reader/sweep
// working against a single NZB. Every method is nil-receiver safe so a
// caller can hold a possibly-nil *Handle (overlay disabled) without an extra
// branch at every call site.
type Handle struct {
	store *Store
	nzbID string
}

// Handle returns a Handle bound to nzbID. Safe to call on a nil Store (the
// returned Handle is then also effectively nil/no-op via its own nil check).
func (s *Store) Handle(nzbID string) *Handle {
	if s == nil {
		return nil
	}
	return &Handle{store: s, nzbID: nzbID}
}

func (h *Handle) NzbID() string {
	if h == nil {
		return ""
	}
	return h.nzbID
}

func (h *Handle) PatchBytes(file string, segIndex int) ([]byte, bool) {
	if h == nil {
		return nil, false
	}
	return h.store.PatchBytes(h.nzbID, file, segIndex)
}

func (h *Handle) WritePatch(file string, segIndex int, data []byte) error {
	if h == nil {
		return fmt.Errorf("overlay: nil handle")
	}
	return h.store.WritePatch(h.nzbID, file, segIndex, data)
}

func (h *Handle) RecordDead(file string, segIndex int, msgID string, bytes int64) error {
	if h == nil {
		return fmt.Errorf("overlay: nil handle")
	}
	return h.store.RecordDead(h.nzbID, file, segIndex, msgID, bytes)
}

func (h *Handle) Decide(file string, segIndex int, msgID string, segBytes, fileSize int64) (Decision, Verdict) {
	if h == nil {
		return DecisionFail, VerdictFailed
	}
	return h.store.Decide(h.nzbID, file, segIndex, msgID, segBytes, fileSize)
}

func (h *Handle) Verdict(file string) Verdict {
	if h == nil {
		return VerdictClean
	}
	return h.store.Verdict(h.nzbID, file)
}

func (h *Handle) ShouldLogPad(file string, segIndex int) bool {
	if h == nil {
		return false
	}
	return h.store.ShouldLogPad(h.nzbID, file, segIndex)
}

func (h *Handle) EnqueueRepair() {
	if h == nil {
		return
	}
	h.store.enqueueRepair(h.nzbID)
}

// lockFor returns the per-nzbID mutex, creating it on first use. Guards the
// manifest read-modify-write cycle so concurrent segment failures across
// multiple files/readers of the same NZB never race each other's save.
func (s *Store) lockFor(nzbID string) *sync.Mutex {
	s.locksMu.Lock()
	defer s.locksMu.Unlock()
	l, ok := s.locks[nzbID]
	if !ok {
		l = &sync.Mutex{}
		s.locks[nzbID] = l
	}
	return l
}

func (s *Store) entryDir(nzbID string) string {
	return filepath.Join(s.root, nzbID)
}

func (s *Store) manifestPath(nzbID string) string {
	return filepath.Join(s.entryDir(nzbID), manifestFileName)
}

// patchPath names a patch blob deterministically from the logical filename's
// FNV-1a hash plus its segment index, so arbitrary filenames (any character,
// any length) never need sanitizing into a filesystem-safe path component.
func (s *Store) patchPath(nzbID, file string, segIndex int) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(file))
	return filepath.Join(s.entryDir(nzbID), fmt.Sprintf("patch_%x_%d.bin", h.Sum64(), segIndex))
}

// loadManifestLocked reads the manifest for nzbID, or returns a fresh empty
// one if none exists yet. Caller must hold lockFor(nzbID).
func (s *Store) loadManifestLocked(nzbID string) (*Manifest, error) {
	data, err := os.ReadFile(s.manifestPath(nzbID))
	if err != nil {
		if os.IsNotExist(err) {
			return &Manifest{Version: manifestVersion, Files: make(map[string]*FileEntry)}, nil
		}
		return nil, fmt.Errorf("overlay: read manifest: %w", err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("overlay: decode manifest: %w", err)
	}
	if m.Files == nil {
		m.Files = make(map[string]*FileEntry)
	}
	if m.Version == 0 {
		m.Version = manifestVersion
	}
	return &m, nil
}

// saveManifestLocked writes the manifest atomically (tmp file + rename) so a
// crash or concurrent read never observes a partially-written manifest.
// Caller must hold lockFor(nzbID).
func (s *Store) saveManifestLocked(nzbID string, m *Manifest) error {
	dir := s.entryDir(nzbID)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("overlay: create entry dir: %w", err)
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("overlay: encode manifest: %w", err)
	}
	tmp, err := os.CreateTemp(dir, "manifest-*.tmp")
	if err != nil {
		return fmt.Errorf("overlay: create tmp manifest: %w", err)
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("overlay: write tmp manifest: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("overlay: close tmp manifest: %w", err)
	}
	if err := os.Rename(tmpPath, s.manifestPath(nzbID)); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("overlay: rename tmp manifest: %w", err)
	}
	return nil
}

func writeFileAtomic(dir, finalPath string, data []byte) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("overlay: create dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, "patch-*.tmp")
	if err != nil {
		return fmt.Errorf("overlay: create tmp patch: %w", err)
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("overlay: write tmp patch: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("overlay: close tmp patch: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("overlay: rename tmp patch: %w", err)
	}
	return nil
}

func fileEntryLocked(m *Manifest, file string) *FileEntry {
	fe := m.Files[file]
	if fe == nil {
		fe = &FileEntry{Verdict: VerdictClean}
		m.Files[file] = fe
	}
	return fe
}

func sortDeadSegments(fe *FileEntry) {
	sort.Slice(fe.DeadSegments, func(i, j int) bool {
		return fe.DeadSegments[i].Index < fe.DeadSegments[j].Index
	})
}

// RecordDead records segIndex as a confirmed-dead article for file. Idempotent:
// a segment already present (in any status) is left untouched.
func (s *Store) RecordDead(nzbID, file string, segIndex int, msgID string, bytes int64) error {
	mu := s.lockFor(nzbID)
	mu.Lock()
	defer mu.Unlock()

	m, err := s.loadManifestLocked(nzbID)
	if err != nil {
		return err
	}
	fe := fileEntryLocked(m, file)
	for i := range fe.DeadSegments {
		if fe.DeadSegments[i].Index == segIndex {
			return nil
		}
	}
	fe.DeadSegments = append(fe.DeadSegments, DeadSegment{
		Index: segIndex, MessageID: msgID, Bytes: bytes, Status: StatusDead,
	})
	sortDeadSegments(fe)
	return s.saveManifestLocked(nzbID, m)
}

// PatchBytes returns the recovered bytes for a patched segment, if PAR2
// repair has already written one. The bytes are stored fully post-processed
// (final logical file bytes) - callers serve them as-is, with no further
// SegmentDataStart/trim handling.
func (s *Store) PatchBytes(nzbID, file string, segIndex int) ([]byte, bool) {
	mu := s.lockFor(nzbID)
	mu.Lock()
	m, err := s.loadManifestLocked(nzbID)
	mu.Unlock()
	if err != nil {
		return nil, false
	}
	fe := m.Files[file]
	if fe == nil {
		return nil, false
	}
	patched := false
	for _, d := range fe.DeadSegments {
		if d.Index == segIndex && d.Status == StatusPatched {
			patched = true
			break
		}
	}
	if !patched {
		return nil, false
	}
	data, err := os.ReadFile(s.patchPath(nzbID, file, segIndex))
	if err != nil {
		return nil, false
	}
	return data, true
}

// WritePatch stores repaired bytes for segIndex and marks it patched. Called
// by the PAR2 repair worker (commit 4) once a segment's true bytes have been
// reconstructed and verified.
func (s *Store) WritePatch(nzbID, file string, segIndex int, data []byte) error {
	mu := s.lockFor(nzbID)
	mu.Lock()
	defer mu.Unlock()

	path := s.patchPath(nzbID, file, segIndex)
	if err := writeFileAtomic(s.entryDir(nzbID), path, data); err != nil {
		return err
	}

	m, err := s.loadManifestLocked(nzbID)
	if err != nil {
		return err
	}
	fe := fileEntryLocked(m, file)
	found := false
	for i := range fe.DeadSegments {
		if fe.DeadSegments[i].Index == segIndex {
			fe.DeadSegments[i].Status = StatusPatched
			fe.DeadSegments[i].Bytes = int64(len(data))
			found = true
			break
		}
	}
	if !found {
		fe.DeadSegments = append(fe.DeadSegments, DeadSegment{
			Index: segIndex, Bytes: int64(len(data)), Status: StatusPatched,
		})
		sortDeadSegments(fe)
	}
	recomputeVerdictLocked(fe)
	return s.saveManifestLocked(nzbID, m)
}

// Verdict returns file's current damage verdict (clean if it has never had a
// dead segment recorded).
func (s *Store) Verdict(nzbID, file string) Verdict {
	mu := s.lockFor(nzbID)
	mu.Lock()
	m, err := s.loadManifestLocked(nzbID)
	mu.Unlock()
	if err != nil {
		return VerdictClean
	}
	fe := m.Files[file]
	if fe == nil {
		return VerdictClean
	}
	return fe.Verdict
}

// PendingRepair returns, for every file in nzbID's manifest with at least
// one non-patched (dead or padded) segment, that file's dead segments -
// patched ones are excluded, since they're already fixed. Returns an empty
// map (not an error) if nzbID has no manifest yet or nothing pending. Used
// by the PAR2 repair job to discover what needs reconstructing without
// having observed every RecordDead/Decide call itself.
func (s *Store) PendingRepair(nzbID string) (map[string][]DeadSegment, error) {
	mu := s.lockFor(nzbID)
	mu.Lock()
	m, err := s.loadManifestLocked(nzbID)
	mu.Unlock()
	if err != nil {
		return nil, err
	}

	out := make(map[string][]DeadSegment)
	for file, fe := range m.Files {
		var pending []DeadSegment
		for _, d := range fe.DeadSegments {
			if d.Status != StatusPatched {
				pending = append(pending, d)
			}
		}
		if len(pending) > 0 {
			out[file] = pending
		}
	}
	return out, nil
}

// DeleteEntry removes nzbID's entire overlay directory (manifest + every
// patch blob). Called wherever a stored NZB record is torn down - keyed by
// nzbID (unique), avoiding the name-twin hazard the DFS cache has.
func (s *Store) DeleteEntry(nzbID string) error {
	mu := s.lockFor(nzbID)
	mu.Lock()
	err := os.RemoveAll(s.entryDir(nzbID))
	mu.Unlock()

	s.locksMu.Lock()
	delete(s.locks, nzbID)
	s.locksMu.Unlock()

	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("overlay: delete entry: %w", err)
	}
	return nil
}

// ShouldLogPad reports whether this is the first time (this process) that
// (nzbID, file, segIndex) has been padded, atomically marking it logged.
func (s *Store) ShouldLogPad(nzbID, file string, segIndex int) bool {
	key := nzbID + "\x00" + file + "\x00" + strconv.Itoa(segIndex)
	_, loaded := s.loggedPads.LoadOrStore(key, struct{}{})
	return !loaded
}

// videoContainerExts gates the padding policy: only these extensions are
// eligible to be padded/degraded. Every other file type fails immediately on
// a confirmed-dead segment, exactly like today.
var videoContainerExts = map[string]struct{}{
	".mkv": {}, ".mp4": {}, ".avi": {}, ".ts": {}, ".m2ts": {}, ".mov": {}, ".wmv": {},
}

// IsVideoContainer reports whether filename's extension is eligible for
// playback padding.
func IsVideoContainer(filename string) bool {
	ext := strings.ToLower(filepath.Ext(filename))
	_, ok := videoContainerExts[ext]
	return ok
}
