package manager

import (
	"fmt"
	"reflect"

	"github.com/sirrobot01/decypharr/internal/config"
)

// ApplySavedConfig carries out what a saved settings change needs besides the
// save itself: it syncs the Arr storage and, when no restart is needed, the
// virtual folders, the .strm sweep, the repair schedule and the wanted
// searches. It reports whether a restart is needed; the caller restarts. The
// settings page and a settings revert both come through here.
func (m *Manager) ApplySavedConfig(previous, updated *config.Config) (restartRequired bool, err error) {
	m.Arr().SyncFromConfig(updated.Arrs, previous.Arrs)

	// A base-URL or STRM settings change moves the desired content of every
	// .strm file; resweep now that the new config is published.
	strmChanged := previous.AppURL != updated.AppURL ||
		!reflect.DeepEqual(previous.Strm, updated.Strm)

	// Only a field that needs it forces a restart (HTTP bind, debrid/usenet
	// clients, or the mount). Everything else is already live.
	if previous.RequiresRestart(updated) {
		return true, nil
	}
	// Backup settings are read live; only the schedule needs re-registering
	// (the one-minute watcher would catch it too, this is just prompt).
	if m.backupSvc != nil && !reflect.DeepEqual(previous.Backup, updated.Backup) {
		if err := m.backupSvc.ApplyConfig(); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to apply backup schedule after live update")
		}
	}
	if strmChanged {
		m.Strm().SweepAsync("config_change")
	}
	if err := m.ApplyVirtualFolders(updated.VirtualFolders); err != nil {
		m.logger.Error().Err(err).Msg("Failed to apply virtual folders after live config update")
		return false, fmt.Errorf("configuration was saved, but virtual folders could not be applied: %w", err)
	}
	// Reschedule the repair sweep from the saved settings. A sweep already
	// running carries on (see Repair.ApplyConfig); the new settings apply
	// from the next sweep.
	if svc := m.Repair(); svc != nil {
		if err := svc.ApplyConfig(); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to apply repair config after live update")
		}
	}
	// Re-register each Arr's scheduled wanted search.
	if ws := m.WantedSearch(); ws != nil {
		if err := ws.ApplyConfig(); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to schedule a wanted search after live update")
		}
	}
	return false, nil
}
