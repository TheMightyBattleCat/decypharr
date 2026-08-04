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
}

func (p PlexConfig) IsZero() bool {
	return p.URL == "" && p.Token == "" && p.SessionCacheTTL == 0
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
