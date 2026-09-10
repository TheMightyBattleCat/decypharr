package dfs

import (
	"context"
	"fmt"
	"maps"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/backend"
	_ "github.com/sirrobot01/decypharr/pkg/mount/dfs/backend/register"
	fuseconfig "github.com/sirrobot01/decypharr/pkg/mount/dfs/config"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs"
)

// Manager manages FUSE filesystem instances with proper caching
type Manager struct {
	manager            *manager.Manager
	logger             zerolog.Logger
	ready              atomic.Bool
	backend            backend.Backend
	defaultBackendType backend.Type
	config             *fuseconfig.FuseConfig
	vfs                *vfs.Manager
}

// NewManager creates a new  FUSE filesystem manager
func NewManager(manager *manager.Manager) *Manager {
	fuseConfig := fuseconfig.ParseFuseConfig()

	m := &Manager{
		manager:            manager,
		logger:             logger.New("dfs"),
		defaultBackendType: backend.GetDefaultBackendType(),
		config:             fuseConfig,
	}
	return m
}

// Start starts the FUSE filesystem manager
func (m *Manager) Start(ctx context.Context) error {
	// Create VFS manager

	m.logger.Info().
		Str("mount_path", m.config.MountPath).
		Str("backend", string(m.defaultBackendType)).
		Msg("Starting DFS with backend")

	vfsManager, err := vfs.NewManager(context.Background(), m.manager, m.config)
	if err != nil {
		return fmt.Errorf("failed to create VFS manager: %w", err)
	}
	m.vfs = vfsManager

	// Create backend
	bck, err := backend.New(m.defaultBackendType, vfsManager, m.config)
	if err != nil {
		return fmt.Errorf("failed to create backend: %w", err)
	}
	m.backend = bck

	// Mount using the backend
	if err := m.backend.Mount(ctx); err != nil {
		return fmt.Errorf("backend mount failed: %w", err)
	}

	m.ready.Store(true)
	m.logger.Info().
		Str("mount_path", m.config.MountPath).
		Str("backend", string(m.defaultBackendType)).
		Msg("DFS started successfully")
	return nil
}

// Stop stops the  FUSE filesystem manager
func (m *Manager) Stop() error {
	if m.backend == nil {
		m.logger.Info().Msg("Backend not initialized, nothing to stop")
		return nil
	}
	m.logger.Info().
		Str("backend", string(m.backend.Type())).
		Msg("Stopping FUSE filesystem")

	// Write every open cache item's RAM-only bytes to disk first. The unmount
	// below closes the VFS inside a 10s budget and shutdown carries on when that
	// runs out, so bytes still in RAM would be lost while the cache metadata
	// already lists them.
	if m.vfs != nil {
		m.vfs.FlushCaches()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Unmount using backend, this also ensures the VFS manager is properly closed
	if err := m.backend.Unmount(ctx); err != nil {
		m.logger.Warn().Err(err).Msg("Backend unmount error")
	}
	m.ready.Store(false)
	return nil
}

func (m *Manager) IsReady() bool {
	return m.ready.Load()
}

func (m *Manager) Refresh(dirs []string) error {
	if m.backend == nil {
		return fmt.Errorf("backend not initialized")
	}
	for _, dir := range dirs {
		m.backend.Refresh(dir)
	}
	return nil
}

// Stats returns unified statistics across all DFS mounts
func (m *Manager) Stats() map[string]any {
	// Aggregate stats from all mounts
	stats := map[string]any{
		"enabled": true,
		"ready":   m.ready.Load(),
		"type":    m.Type(),
		"backend": string(m.defaultBackendType),
	}
	if m.vfs != nil {
		maps.Copy(stats, m.vfs.GetStats())
	}
	return stats
}

func (m *Manager) CleanupCache() (map[string]any, error) {
	if m.vfs == nil {
		return nil, fmt.Errorf("VFS manager is not initialized")
	}
	return m.vfs.CleanupCache(), nil
}

func (m *Manager) PurgeCache() (map[string]any, error) {
	if m.vfs == nil {
		return nil, fmt.Errorf("VFS manager is not initialized")
	}
	return m.vfs.PurgeCache(), nil
}

// PeekCachedRange reads [off, off+len(p)) of filename's cache item under
// entryName directly from local disk, if already fully cached - see
// vfs.Manager.PeekCachedRange. Never creates a cache item or triggers a
// download; false means there is nothing to read from here (no VFS mount,
// or the range isn't fully cached).
func (m *Manager) PeekCachedRange(entryName, filename string, p []byte, off int64) bool {
	if m.vfs == nil {
		return false
	}
	return m.vfs.PeekCachedRange(entryName, filename, p, off)
}

// WriteCachedRange durably writes p at [off, off+len(p)) into filename's
// cache item under entryName, creating it (sized by fileSize) if needed -
// see vfs.Manager.WriteCachedRange. The write-side mirror of
// PeekCachedRange.
func (m *Manager) WriteCachedRange(entryName, filename string, fileSize int64, p []byte, off int64) error {
	if m.vfs == nil {
		return fmt.Errorf("VFS manager is not initialized")
	}
	return m.vfs.WriteCachedRange(entryName, filename, fileSize, p, off)
}

// ForgetCachedRange drops [off, off+length) from filename's cache item
// under entryName so a later read re-downloads it - see
// vfs.Manager.ForgetCachedRange. No-op when there is no VFS mount or the
// file was never cached.
func (m *Manager) ForgetCachedRange(entryName, filename string, off, length int64) {
	if m.vfs == nil {
		return
	}
	m.vfs.ForgetCachedRange(entryName, filename, off, length)
}

// CacheCoverage returns filename's cache coverage under entryName - cached
// bytes against the file's total declared size, plus its last write time -
// see vfs.Manager.CacheCoverage. ok=false means there is nothing to report
// (no VFS mount, or the file has never been cached).
func (m *Manager) CacheCoverage(entryName, filename string) (cached, total int64, modTime time.Time, ok bool) {
	if m.vfs == nil {
		return 0, 0, time.Time{}, false
	}
	return m.vfs.CacheCoverage(entryName, filename)
}

func (m *Manager) Type() string {
	return "dfs"
}
