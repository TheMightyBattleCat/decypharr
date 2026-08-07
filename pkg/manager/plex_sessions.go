// Package manager: plexSessionChecker gates Precache's read-ahead/next-
// episode bursts behind an actual Plex "now playing" session for the file
// being read, so a library scan, thumbnail-generation pass, or metadata
// analysis pass - which issue the same ranged reads through the mount -
// can't spuriously trigger precache. Disabled entirely when
// config.PlexConfig.URL is unset (see PlexConfig's doc comment). Once
// enabled, a failed poll is retried a few times before being treated as a
// failure, and a short grace window lets a momentary Plex blip keep serving
// the last known sessions; only once Plex has stayed unreachable past the
// grace window does the gate fail closed and deny precache (see refresh).
package manager

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	json "github.com/bytedance/sonic"
	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// plexSessionFetchTimeout bounds one /status/sessions request so an
// unreachable/hanging Plex server can't stall the calling stream request
// (Observe runs inline in the ranged-read path - see stream.go) beyond a
// bounded worst case.
const plexSessionFetchTimeout = 5 * time.Second

// plexWarnInterval throttles the "Plex unreachable" log line - refresh may
// re-run as often as once per PlexConfig.SessionCacheTTL, which would
// otherwise spam the log for as long as Plex stays down.
const plexWarnInterval = 5 * time.Minute

// plexSessionFetchRetries bounds how many times refresh retries a failed
// /status/sessions poll, within one refresh call, before treating it as a
// failure - a single dropped connection or transient 5xx shouldn't be
// enough to move the gate toward fail-closed.
const plexSessionFetchRetries = 3

// plexSessionRetryDelay separates retry attempts within one refresh call.
// Kept short since plexSessionFetchTimeout already bounds each attempt and
// Observe runs inline in the ranged-read path.
const plexSessionRetryDelay = 250 * time.Millisecond

// plexSessionGraceMinimum floors the grace window (see refresh) so a very
// short SessionCacheTTL doesn't shrink the window to nearly nothing.
const plexSessionGraceMinimum = 30 * time.Second

// plexSessionsResponse is the subset of Plex's /status/sessions payload
// this checker needs: the file path backing each currently-playing item.
type plexSessionsResponse struct {
	MediaContainer struct {
		Metadata []struct {
			Media []struct {
				Part []struct {
					File string `json:"file"`
				} `json:"Part"`
			} `json:"Media"`
		} `json:"Metadata"`
	} `json:"MediaContainer"`
}

// plexSessionChecker answers "is this entry+filename part of an active Plex
// playing session right now", backed by a short-lived cache of Plex's
// current session list so Observe (called on every ranged read) doesn't
// query Plex on every call.
type plexSessionChecker struct {
	manager *Manager
	logger  zerolog.Logger
	client  *http.Client

	mu          sync.Mutex
	fetched     time.Time
	lastSuccess time.Time           // last time a poll succeeded; zero if it never has
	degraded    bool                // true once a failed poll has exceeded the grace window; isPlexWatching denies everything while degraded
	resolved    map[string]struct{} // resolved absolute file paths of files in active sessions, from the last successful poll

	lastWarnMu sync.Mutex
	lastWarn   time.Time
}

func newPlexSessionChecker(m *Manager) *plexSessionChecker {
	return &plexSessionChecker{
		manager:  m,
		logger:   logger.New("plex-sessions"),
		client:   &http.Client{Timeout: plexSessionFetchTimeout},
		resolved: make(map[string]struct{}),
	}
}

// isPlexWatching reports whether filename, as resolved for entry, is part
// of an active Plex playing session. Always true when the gate is disabled
// (PlexConfig.URL unset). Once enabled, false whenever degraded - Plex has
// stayed unreachable past the grace window in refresh - so an unreachable
// Plex server denies precache rather than firing on background activity.
func (c *plexSessionChecker) isPlexWatching(entry *storage.Entry, filename string) bool {
	cfg := config.Get().Plex
	if !cfg.Enabled() {
		return true
	}
	c.refresh(cfg)

	expected := filepath.Join(c.manager.GetTorrentMountPath(entry), filename)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.degraded {
		return false
	}
	_, ok := c.resolved[expected]
	return ok
}

// refresh re-fetches and re-resolves Plex's session list if the cached one
// is older than cfg.SessionTTL(). A failing poll is retried up to
// plexSessionFetchRetries times before being treated as a failure; if every
// attempt fails, the checker keeps serving the last resolved session list
// as though healthy until time.Since(lastSuccess) exceeds the grace window
// (max(2*cfg.SessionTTL(), plexSessionGraceMinimum)), at which point it
// flips degraded so isPlexWatching starts denying precache.
func (c *plexSessionChecker) refresh(cfg config.PlexConfig) {
	c.mu.Lock()
	stale := time.Since(c.fetched) >= cfg.SessionTTL()
	c.mu.Unlock()
	if !stale {
		return
	}

	var files []string
	var err error
	for attempt := 0; attempt < plexSessionFetchRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(plexSessionRetryDelay)
		}
		files, err = c.fetchSessions(cfg)
		if err == nil {
			break
		}
	}
	now := time.Now()

	if err != nil {
		grace := 2 * cfg.SessionTTL()
		if grace < plexSessionGraceMinimum {
			grace = plexSessionGraceMinimum
		}
		c.mu.Lock()
		c.fetched = now
		c.degraded = c.lastSuccess.IsZero() || time.Since(c.lastSuccess) >= grace
		c.mu.Unlock()
		c.warnDebounced(err)
		return
	}

	resolved := resolvePlexSessionPaths(files)
	c.mu.Lock()
	c.fetched = now
	c.lastSuccess = now
	c.degraded = false
	c.resolved = resolved
	c.mu.Unlock()
}

// fetchSessions queries Plex's /status/sessions and returns the raw file
// path of each currently-playing item's media part.
func (c *plexSessionChecker) fetchSessions(cfg config.PlexConfig) ([]string, error) {
	url, err := utils.JoinURL(cfg.URL, "/status/sessions")
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), plexSessionFetchTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Plex-Token", cfg.Token)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("plex sessions request failed: %s", resp.Status)
	}

	var parsed plexSessionsResponse
	dec := json.ConfigDefault.NewDecoder(resp.Body)
	if err := dec.Decode(&parsed); err != nil {
		return nil, err
	}

	var files []string
	for _, m := range parsed.MediaContainer.Metadata {
		for _, media := range m.Media {
			for _, part := range media.Part {
				if part.File != "" {
					files = append(files, part.File)
				}
			}
		}
	}
	return files, nil
}

// TestPlexConnection performs an ad-hoc /status/sessions probe against cfg,
// for the repair page's "Test connection" button - independent of the
// long-lived plexSessionChecker cache newPlexSessionChecker builds for
// Precache.Observe, so a test never disturbs the live session cache.
func TestPlexConnection(cfg config.PlexConfig) error {
	c := &plexSessionChecker{client: &http.Client{Timeout: plexSessionFetchTimeout}}
	_, err := c.fetchSessions(cfg)
	return err
}

// resolvePlexSessionPaths resolves each Plex session file path to the
// underlying location precache tracks entries by: readSymlinkTarget first
// (the DownloadActionSymlink setup collectArrFiles/the repair sweep already
// trust), falling back to the raw path itself when it isn't a symlink
// (Plex mounted directly on the DFS/rclone mount, no symlink layer).
func resolvePlexSessionPaths(files []string) map[string]struct{} {
	resolved := make(map[string]struct{}, len(files))
	for _, f := range files {
		target := readSymlinkTarget(f)
		if target == "" {
			target = filepath.Clean(f)
		}
		resolved[target] = struct{}{}
	}
	return resolved
}

// warnDebounced logs a Plex-unreachable warning at most once per
// plexWarnInterval, so a prolonged outage doesn't spam the log every time
// refresh retries.
func (c *plexSessionChecker) warnDebounced(err error) {
	c.lastWarnMu.Lock()
	defer c.lastWarnMu.Unlock()
	if time.Since(c.lastWarn) < plexWarnInterval {
		return
	}
	c.lastWarn = time.Now()
	c.logger.Warn().Err(err).Msg("failed to fetch Plex sessions after retries; precache session gate will deny once the grace window elapses")
}
