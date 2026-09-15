package config

import "time"

// PlexConfig gates Precache's read-ahead/next-episode bursts (see
// pkg/manager.Precache.Observe) behind an active Plex "now playing" session
// for the file being read, so a background library scan, thumbnail
// generation pass, or metadata analysis pass - which issue the same ranged
// reads through the mount - can't spuriously trigger precache. URL empty
// disables the gate entirely: precache fires unconditionally, exactly as it
// did before this existed.
type PlexConfig struct {
	// URL is the Plex server's base URL (e.g. http://localhost:32400).
	// Empty disables the gate.
	URL string `json:"plex_url,omitempty"`

	// Token is the Plex auth token sent as X-Plex-Token when querying
	// /status/sessions.
	Token string `json:"plex_token,omitempty"`

	// SessionCacheTTL bounds how long a fetched Plex session list is reused
	// before being refreshed. Observe runs inline in the ranged-read path
	// (see stream.go), so this keeps a live Plex query off every read.
	// Default 10s when unset/non-positive.
	SessionCacheTTL time.Duration `json:"plex_session_cache_ttl,omitempty"`

	// ReapMode controls removing a title's stale Plex versions - the ones
	// Plex marks deleted (Media deletedAt, shown as "Unavailable") after an
	// Arr upgrade, rename, or decypharr re-grab replaced the file - see
	// pkg/manager.PlexReaper. Only ever acts per title, never section-wide
	// like Plex's "Empty trash". Empty/"off" disables it, "dry_run" logs and
	// lists what it would remove, "on" removes.
	ReapMode PlexReapMode `json:"plex_reap_mode,omitempty"`
}

// PlexReapMode is PlexConfig.ReapMode.
type PlexReapMode string

const (
	PlexReapOff    PlexReapMode = "off"
	PlexReapDryRun PlexReapMode = "dry_run"
	PlexReapOn     PlexReapMode = "on"
)

// Reap returns the effective reap mode: off unless Plex is enabled and the
// mode is one of the known values.
func (p PlexConfig) Reap() PlexReapMode {
	if !p.Enabled() {
		return PlexReapOff
	}
	switch p.ReapMode {
	case PlexReapDryRun, PlexReapOn:
		return p.ReapMode
	}
	return PlexReapOff
}

func (p PlexConfig) IsZero() bool {
	return p.URL == "" && p.Token == "" && p.SessionCacheTTL == 0 && p.ReapMode == ""
}

// Enabled reports whether the Plex session gate is active - false (gate
// disabled, precache fires unconditionally) when URL is unset.
func (p PlexConfig) Enabled() bool {
	return p.URL != ""
}

// SessionTTL returns the configured session-list cache TTL, defaulting to
// 10s when unset/non-positive.
func (p PlexConfig) SessionTTL() time.Duration {
	if p.SessionCacheTTL <= 0 {
		return 10 * time.Second
	}
	return p.SessionCacheTTL
}
