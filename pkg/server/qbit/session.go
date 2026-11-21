package qbit

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// A qBittorrent client logs in once and sends the SID cookie on every later
// call. The SID is a random handle; the credentials it stands for stay in
// memory here and never travel in the cookie, so a log that records cookies
// records nothing a caller could reuse once the session is gone.
const (
	sidIdleExpiry  = 24 * time.Hour
	sidMaxSessions = 1000
)

// sidNow is the clock; tests replace it.
var sidNow = time.Now

type sidSession struct {
	username string
	// password is kept so every request re-runs authenticate with the
	// category it carries: with the login off that is what finds the Arr's
	// host and API key, and with it on it notices a later change of login.
	password string
	version  string
	lastUsed time.Time
}

type sidStore struct {
	mu       sync.Mutex
	sessions map[string]*sidSession
}

var sids = &sidStore{sessions: make(map[string]*sidSession)}

// currentAuthVersion is the stored credentials' version; every save of the
// login or the API token changes it, which ends every SID issued before.
func currentAuthVersion() string {
	if auth := config.Get().Auth; auth != nil {
		return auth.SessionVersion
	}
	return ""
}

func (s *sidStore) issue(username, password string) (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	sid := hex.EncodeToString(buf)
	now := sidNow()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.sessions) >= sidMaxSessions {
		for id, sess := range s.sessions {
			if now.Sub(sess.lastUsed) > sidIdleExpiry {
				delete(s.sessions, id)
			}
		}
	}
	for len(s.sessions) >= sidMaxSessions {
		oldestID := ""
		var oldest time.Time
		for id, sess := range s.sessions {
			if oldestID == "" || sess.lastUsed.Before(oldest) {
				oldestID, oldest = id, sess.lastUsed
			}
		}
		delete(s.sessions, oldestID)
	}
	s.sessions[sid] = &sidSession{
		username: username,
		password: password,
		version:  currentAuthVersion(),
		lastUsed: now,
	}
	return sid, nil
}

// lookup returns the credentials behind sid. An unknown SID, one idle for
// longer than the expiry, or one issued before the credentials last changed
// is dropped and reported as not found.
func (s *sidStore) lookup(sid string) (string, string, bool) {
	now := sidNow()
	version := currentAuthVersion()
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sid]
	if !ok {
		return "", "", false
	}
	if now.Sub(sess.lastUsed) > sidIdleExpiry || sess.version != version {
		delete(s.sessions, sid)
		return "", "", false
	}
	sess.lastUsed = now
	return sess.username, sess.password, true
}

func (s *sidStore) drop(sid string) {
	s.mu.Lock()
	delete(s.sessions, sid)
	s.mu.Unlock()
}

func (s *sidStore) reset() {
	s.mu.Lock()
	s.sessions = make(map[string]*sidSession)
	s.mu.Unlock()
}
