package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/appendstore"
	"github.com/sirrobot01/decypharr/internal/logger"
	"google.golang.org/protobuf/proto"
)

var storeNames = []string{"entries", "queue", "items", "repair_state", "repair_runs", "par2_repair_attempts", "par2_repair_state", "regrab_guard", "pending_deletes"}

// legacyStoreNames are buckets from the v1 repair system. They are removed
// on startup so they don't accumulate dead data.
var legacyStoreNames = []string{"repair_jobs", "repair_keys"}

// Storage handles persistence using appendstore
type Storage struct {
	entries         *appendstore.Store
	queue           *appendstore.Store
	entryItems      *appendstore.Store
	repairState     *appendstore.Store
	repairRuns      *appendstore.Store
	par2Repairs     *appendstore.Store
	par2RepairState *appendstore.Store
	regrabGuard     *appendstore.Store
	pendingDeletes  *appendstore.Store
	dir             string
	logger          zerolog.Logger

	healthCountsMu      sync.Mutex
	healthCounts        map[HealthStatus]int
	healthCountsBuiltAt time.Time
}

// rebuildableStoreNames are the stores that hold only history or state the
// program rebuilds as it runs. When one of them is damaged, start-up moves
// the file aside and starts it empty instead of refusing to start.
var rebuildableStoreNames = map[string]bool{
	"repair_runs":          true,
	"par2_repair_attempts": true,
	"regrab_guard":         true,
}

// corruptStoreSuffix starts the name a damaged rebuildable store is moved to:
// <name>.db.corrupt-<time>.
const corruptStoreSuffix = ".corrupt-"

func createItemStores(baseDir string, baseOptions appendstore.Options, log zerolog.Logger) (map[string]*appendstore.Store, error) {
	items := make(map[string]*appendstore.Store)
	closeOpened := func() {
		for _, it := range items {
			_ = it.Close()
		}
	}
	for _, name := range storeNames {
		path := filepath.Join(baseDir, name+".db")
		store, err := appendstore.Open(path, baseOptions)
		if err != nil && errors.Is(err, appendstore.ErrCorruptedData) && rebuildableStoreNames[name] {
			store, err = reopenCorruptStore(path, baseOptions, err, log)
		}
		if err != nil {
			closeOpened()
			if errors.Is(err, appendstore.ErrUnsupportedVersion) {
				return nil, fmt.Errorf("the %s database was written by a newer version of Decypharr and this build cannot read it. "+
					"Upgrade Decypharr, or restore the copy saved before the upgrade (a .bak file beside %s): %w", name, path, err)
			}
			if errors.Is(err, appendstore.ErrCorruptedData) {
				return nil, fmt.Errorf("the %s database (%s) is damaged and cannot be opened. "+
					"Stop Decypharr and restore the db folder from a backup: %w", name, path, err)
			}
			return nil, fmt.Errorf("failed to create %s store: %w", name, err)
		}
		items[name] = store
	}
	return items, nil
}

// reopenCorruptStore moves a damaged store file aside and opens a fresh one
// in its place. openErr is the error the damaged file gave.
func reopenCorruptStore(path string, options appendstore.Options, openErr error, log zerolog.Logger) (*appendstore.Store, error) {
	aside := path + corruptStoreSuffix + time.Now().UTC().Format("20060102T150405Z")
	if err := os.Rename(path, aside); err != nil {
		return nil, errors.Join(openErr, fmt.Errorf("move the damaged file aside: %w", err))
	}
	store, err := appendstore.Open(path, options)
	if err != nil {
		return nil, errors.Join(openErr, fmt.Errorf("open a fresh store after moving the damaged file to %s: %w", aside, err))
	}
	log.Error().
		Err(openErr).
		Str("database", path).
		Str("moved_to", aside).
		Msg("Database was damaged: moved it aside and started it empty. Its history is lost unless the db folder is restored from a backup")
	return store, nil
}

func dropLegacyStores(baseDir string, log zerolog.Logger) {
	for _, name := range legacyStoreNames {
		path := filepath.Join(baseDir, name+".db")
		if _, err := os.Stat(path); err == nil {
			if err := os.RemoveAll(path); err != nil {
				log.Warn().Err(err).Str("path", path).Msg("Failed to remove legacy repair bucket")
			} else {
				log.Info().Str("path", path).Msg("Removed legacy repair bucket")
			}
		}
	}
}

func NewStorage(dbPath string) (*Storage, error) {
	dbPath = filepath.Clean(dbPath)
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create db directory: %w", err)
	}

	log := logger.New("storage")

	dropLegacyStores(dbPath, log)

	baseOptions := appendstore.Options{
		CacheSize:           5000,
		SyncInterval:        time.Second,
		CompactionThreshold: 0.5,
		AutoCompact:         true,
		IndexedFields:       []string{attributeCategory, attributeProvider, attributeStatus},
		OnError: func(err error) {
			log.Warn().Err(err).Msg("Storage background operation failed")
		},
		// appendstore keeps a copy of a log before migrating it to the current
		// format; log where it went, because that copy is the only way back to
		// an older build.
		OnMigrate: func(info appendstore.MigrationInfo) error {
			log.Warn().
				Str("database", info.Path).
				Uint32("from_version", info.FromVersion).
				Uint32("to_version", info.ToVersion).
				Str("backup", info.Backup).
				Msg("Upgrading database format. Older Decypharr builds cannot read it; restore the backup to go back")
			return nil
		},
	}

	itemStores, err := createItemStores(dbPath, baseOptions, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create item stores: %w", err)
	}

	s := &Storage{
		entries:         itemStores["entries"],
		queue:           itemStores["queue"],
		entryItems:      itemStores["items"],
		repairState:     itemStores["repair_state"],
		repairRuns:      itemStores["repair_runs"],
		par2Repairs:     itemStores["par2_repair_attempts"],
		par2RepairState: itemStores["par2_repair_state"],
		regrabGuard:     itemStores["regrab_guard"],
		pendingDeletes:  itemStores["pending_deletes"],
		dir:             dbPath,
		logger:          log,
	}

	if count, err := s.MigrateMetadata(); err != nil {
		log.Warn().Err(err).Msg("Metadata migration failed")
	} else if count > 0 {
		log.Info().Int("count", count).Msg("Migrated entry metadata to new format")
	}

	return s, nil
}

// WriteCount is the number of writes and deletes the stores have taken since
// they were opened. It only ever goes up, so two equal readings mean the
// database did not change in between.
func (s *Storage) WriteCount() int64 {
	var n int64
	for _, store := range []*appendstore.Store{s.entries, s.queue, s.entryItems, s.repairState, s.repairRuns, s.par2Repairs, s.par2RepairState, s.regrabGuard, s.pendingDeletes} {
		if store == nil {
			continue
		}
		st := store.Stats()
		n += st.Writes + st.Deletes
	}
	return n
}

func (s *Storage) Close() error {
	var errs []error
	stores := []*appendstore.Store{s.entries, s.queue, s.entryItems, s.repairState, s.repairRuns, s.par2Repairs, s.par2RepairState, s.regrabGuard, s.pendingDeletes}
	for _, store := range stores {
		if store == nil {
			continue
		}
		if err := store.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors closing storage: %v", errs)
	}
	return nil
}

// DiskSize returns the total on-disk size of all stores (O(1), no filesystem walk).
func (s *Storage) DiskSize() int64 {
	var size int64
	for _, store := range []*appendstore.Store{s.entries, s.queue, s.entryItems, s.repairState, s.repairRuns, s.par2Repairs, s.par2RepairState, s.regrabGuard, s.pendingDeletes} {
		if store != nil {
			size += store.DiskSize()
		}
	}
	return size
}

// SaveMigrationStatus saves the system migration status
func (s *Storage) SaveMigrationStatus(status *SystemMigrationStatus) error {
	pb := SystemMigrationStatusToProto(status)
	data, err := proto.Marshal(pb)
	if err != nil {
		return err
	}
	return s.entries.Put("__migration_status__", data, nil)
}

// GetMigrationStatus retrieves the system migration status
func (s *Storage) GetMigrationStatus() (*SystemMigrationStatus, error) {
	data, err := s.entries.Get("__migration_status__")
	if err != nil {
		return nil, err
	}
	var pb SystemMigrationStatusProto
	if err := proto.Unmarshal(data, &pb); err != nil {
		return nil, err
	}
	return ProtoToSystemMigrationStatus(&pb), nil
}

func (s *Storage) copyFrom(other *Storage) error {
	pairs := []struct {
		name string
		from *appendstore.Store
		to   *appendstore.Store
	}{
		{"entries", other.entries, s.entries},
		{"queue", other.queue, s.queue},
		{"items", other.entryItems, s.entryItems},
		{"repair_state", other.repairState, s.repairState},
		{"repair_runs", other.repairRuns, s.repairRuns},
		{"par2_repair_attempts", other.par2Repairs, s.par2Repairs},
		{"par2_repair_state", other.par2RepairState, s.par2RepairState},
		{"regrab_guard", other.regrabGuard, s.regrabGuard},
		{"pending_deletes", other.pendingDeletes, s.pendingDeletes},
	}

	for _, p := range pairs {
		if p.from == nil || p.to == nil {
			continue
		}
		if err := p.from.ForEach(func(key string, value []byte) error {
			return p.to.Put(key, value, nil)
		}); err != nil {
			return fmt.Errorf("failed to copy %s: %w", p.name, err)
		}
	}
	return nil
}
