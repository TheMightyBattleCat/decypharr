package server

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	json "github.com/bytedance/sonic"

	"github.com/sirrobot01/decypharr/internal/config"
)

func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check if setup is needed
		cfg := config.Get()
		if !cfg.UseAuth {
			next.ServeHTTP(w, r)
			return
		}

		isAPI := s.isAPIRequest(r)

		if cfg.NeedsAuth() {
			if isAPI {
				s.sendJSONError(w, "Authentication setup required", http.StatusUnauthorized)
			} else {
				http.Redirect(w, r, "/register", http.StatusSeeOther)
			}
			return
		}

		if s.validSession(r, cfg) {
			next.ServeHTTP(w, r)
			return
		}
		if s.isValidAPIToken(r) {
			next.ServeHTTP(w, markTokenOnly(r))
			return
		}
		if isAPI {
			s.sendJSONError(w, "Authentication required. Please provide a valid API token in the Authorization header (Bearer <token>) or authenticate via session cookies.", http.StatusUnauthorized)
		} else {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
		}
	})
}

type tokenOnlyKey struct{}

// markTokenOnly records that r got in with the API token alone, with no
// signed-in session.
func markTokenOnly(r *http.Request) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), tokenOnlyKey{}, true))
}

// authedBySession reports whether r is the owner's own session rather than a
// caller holding only the API token. The token is also the password the Arrs
// store for the qBittorrent and SABnzbd APIs, so it must not be able to read
// the stored secrets, change the login or replace the config. With the login
// off, or before one is stored, there is no session to ask for and every
// request counts as the owner's.
func authedBySession(r *http.Request) bool {
	tokenOnly, _ := r.Context().Value(tokenOnlyKey{}).(bool)
	return !tokenOnly
}

// requireSession refuses a request that authenticated with the API token
// alone (see authedBySession).
func (s *Server) requireSession(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !authedBySession(r) {
			s.sendJSONError(w, "This action needs a signed-in session; the API token is not enough", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// validSession reports whether r carries a session cookie that is valid for
// the stored credentials. A session is valid only for the credentials it
// logged in under: saving the credentials again (a new password, a new API
// token) changes their session version.
func (s *Server) validSession(r *http.Request, cfg *config.Config) bool {
	if s.cookie == nil {
		return false
	}
	session, err := s.cookie.Get(r, "auth-session")
	auth, ok := session.Values["authenticated"].(bool)
	version, hasVersion := session.Values["auth_version"].(string)
	currentAuth := cfg.GetAuth()
	return err == nil && ok && auth && hasVersion && currentAuth != nil && version == currentAuth.SessionVersion
}

// setupAuthMiddleware guards the setup wizard's write routes. They are public
// only while no credential is stored (a fresh install, or the login turned
// off). Once one is, they need the owner's signed-in session, whatever
// SetupComplete says: a config that stops validating must not hand the
// wizard (turn the login off, install a provider, restore an archive over
// config.json and auth.json) to anyone who can reach the port, nor to a
// caller holding only the API token (see authedBySession).
func (s *Server) setupAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cfg := config.Get()
		if !cfg.UseAuth || cfg.NeedsAuth() || s.validSession(r, cfg) {
			next.ServeHTTP(w, r)
			return
		}
		if s.isValidAPIToken(r) {
			s.sendJSONError(w, "The setup wizard needs a signed-in session; the API token is not enough", http.StatusForbidden)
			return
		}
		s.sendJSONError(w, "Authentication required: a login is already set up", http.StatusUnauthorized)
	})
}

// isAPIRequest reports whether an authentication failure should be returned as
// JSON instead of redirecting the caller to the login page.
func (s *Server) isAPIRequest(r *http.Request) bool {
	path := r.URL.Path
	if urlBase := strings.TrimSuffix(s.urlBase, "/"); urlBase != "" {
		path = strings.TrimPrefix(path, urlBase)
	}
	return strings.HasPrefix(path, "/api/") || strings.HasPrefix(path, "/webhooks/")
}

// sendJSONError sends a JSON error response
func (s *Server) sendJSONError(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	err := json.ConfigDefault.NewEncoder(w).Encode(map[string]any{
		"error":  message,
		"status": statusCode,
	})
	if err != nil {
		return
	}
}

// setupRedirectMiddleware redirects to /setup if setup is not completed
func (s *Server) setupRedirectMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cfg := config.Get()

		// Skip setup check for setup-related routes
		if strings.HasPrefix(r.URL.Path, "/setup") ||
			strings.HasPrefix(r.URL.Path, "/api/setup") ||
			strings.HasPrefix(r.URL.Path, "/api/login") ||
			strings.HasPrefix(r.URL.Path, "/api/logout") ||
			strings.HasPrefix(r.URL.Path, "/api/config") ||
			strings.HasPrefix(r.URL.Path, "/assets") ||
			strings.HasPrefix(r.URL.Path, "/images") ||
			r.URL.Path == "/version" {
			next.ServeHTTP(w, r)
			return
		}
		// With a login stored, the owner can still sign in and open the
		// settings page to undo a config that stopped validating; both stay
		// behind the login.
		if (r.URL.Path == "/login" || r.URL.Path == "/settings") && cfg.UseAuth && !cfg.NeedsAuth() {
			next.ServeHTTP(w, r)
			return
		}

		// Check if setup is completed
		if err := cfg.SetupComplete(); err != nil {
			isAPI := s.isAPIRequest(r)
			if isAPI {
				s.sendJSONError(w, fmt.Sprintf("[error] %s Setup wizard must be completed first. Please visit /setup", err), http.StatusServiceUnavailable)
			} else {
				http.Redirect(w, r, "/setup", http.StatusSeeOther)
			}
			return
		}

		next.ServeHTTP(w, r)
	})
}
