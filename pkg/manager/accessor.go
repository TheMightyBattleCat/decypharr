package manager

import (
	"context"

	"github.com/go-co-op/gocron/v2"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/hearsay"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/storage/migration"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

func (m *Manager) SetMountManager(mountMgr MountManager) {
	m.mountManager = mountMgr
}

// Repair returns the repair service. It is created during init() so callers
// can rely on a non-nil value once the manager has been constructed.
func (m *Manager) Repair() *Repair {
	return m.repair
}

// PrecacheStatus returns a snapshot of the read-ahead/next-episode precache
// feature's live config and state, for the repair/overlay GUI and API.
func (m *Manager) PrecacheStatus() PrecacheSummary {
	return m.precache.Summary()
}

// PurgeIncompletePrecache reaps part-cached precache entries - see
// Precache.PurgeIncomplete.
func (m *Manager) PurgeIncompletePrecache(execute bool) (deleted, skippedInflight []string, failed []PurgeFailure, freedBytes int64, err error) {
	return m.precache.PurgeIncomplete(execute)
}

// RescanPrecache re-runs the on-disk cache scan - see Precache.Rescan.
func (m *Manager) RescanPrecache() {
	m.precache.Rescan()
}

// SetPrecachePaused sets or clears the precache feature's global runtime
// pause - see Precache.SetPaused.
func (m *Manager) SetPrecachePaused(paused bool) {
	m.precache.SetPaused(paused)
}

// SetPrecacheEntryPaused sets or clears the runtime pause for one
// (infoHash,filename) pair - see Precache.SetKeyPaused.
func (m *Manager) SetPrecacheEntryPaused(infoHash, filename string, paused bool) {
	m.precache.SetKeyPaused(infoHash, filename, paused)
}

// Hearsay returns the hearsay service, or nil when disabled. A nil
// service is safe to call.
func (m *Manager) Hearsay() *hearsay.Service {
	return m.hearsay
}

func (m *Manager) Scheduler() gocron.Scheduler {
	return m.scheduler
}

// Migrator returns the migrator instance
func (m *Manager) Migrator() *migration.Migrator {
	return m.migrator
}

// Arr returns the Arr storage instance
// PlexReaper returns the Plex stale-version reaper.
func (m *Manager) PlexReaper() *PlexReaper {
	if m == nil {
		return nil
	}
	return m.plexReaper
}

// WantedSearch returns the scheduled per-Arr wanted-search service.
func (m *Manager) WantedSearch() *WantedSearch {
	if m == nil {
		return nil
	}
	return m.wantedSearch
}

func (m *Manager) Arr() *arr.Storage {
	return m.arr
}

func (m *Manager) Queue() *Queue {
	return m.queue
}

func (m *Manager) JobQueue() *JobQueue {
	return m.jobQueue
}

func (m *Manager) Clients() *xsync.Map[string, debrid.Client] {
	return m.clients
}

func (m *Manager) MountManager() MountManager {
	return m.mountManager
}

func (m *Manager) Storage() *storage.Storage {
	return m.storage
}

func (m *Manager) Context() context.Context {
	return m.ctx
}

func (m *Manager) Usenet() *usenet.Usenet {
	return m.usenet
}

// Par2Repair returns the PAR2 repair worker. May be nil if the usenet client
// failed to initialize.
func (m *Manager) Par2Repair() *Par2Repair {
	return m.par2Repair
}

// GetDebridSpeedTestResult returns stored speed test result for a specific debrid provider
func (m *Manager) GetDebridSpeedTestResult(provider string) (debridTypes.SpeedTestResult, bool) {
	return m.debridSpeedTestResults.Load(provider)
}
