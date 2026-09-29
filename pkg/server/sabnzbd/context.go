package sabnzbd

import (
	"context"
	"crypto/subtle"
	"fmt"
	"net/http"
	"strings"

	"github.com/sirrobot01/decypharr/internal/config"

	"github.com/sirrobot01/decypharr/pkg/arr"
)

type contextKey string

const (
	apiKeyKey   contextKey = "apikey"
	modeKey     contextKey = "mode"
	arrKey      contextKey = "arr"
	categoryKey contextKey = "category"
)

func getMode(ctx context.Context) string {
	if mode, ok := ctx.Value(modeKey).(string); ok {
		return mode
	}
	return ""
}

func (s *SABnzbd) categoryContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		category := r.URL.Query().Get("category")
		if category == "" {
			// Check form data
			_ = r.ParseForm()
			category = r.Form.Get("category")
		}
		if category == "" {
			category = r.FormValue("category")
		}

		ctx := context.WithValue(r.Context(), categoryKey, strings.TrimSpace(category))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func getArrFromContext(ctx context.Context) *arr.Arr {
	if a, ok := ctx.Value(arrKey).(*arr.Arr); ok {
		return a
	}
	return nil
}

func getCategory(ctx context.Context) string {
	if category, ok := ctx.Value(categoryKey).(string); ok {
		return category
	}
	return ""
}

// modeContext extracts the mode parameter from the request
func (s *SABnzbd) modeContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mode := r.URL.Query().Get("mode")
		if mode == "" {
			// Check form data
			_ = r.ParseForm()
			mode = r.Form.Get("mode")
		}

		// Extract category for Arr integration
		category := r.URL.Query().Get("cat")
		if category == "" {
			category = r.Form.Get("cat")
		}

		// Create a default Arr instance for the category
		downloadUncached := false
		a := arr.New(category, "", "", false, &downloadUncached, "", "auto")

		ctx := context.WithValue(r.Context(), modeKey, strings.TrimSpace(mode))
		ctx = context.WithValue(ctx, arrKey, a)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// authContext creates a middleware that extracts the Arr host and token from the Authorization header
// and adds it to the request context.
// This is used to identify the Arr instance for the request.
// Only a valid host and token will be added to the context/config. The rest are manual
func (s *SABnzbd) authContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.URL.Query().Get("ma_username")
		token := r.URL.Query().Get("ma_password")
		category := getCategory(r.Context())
		a, err := s.authenticate(category, host, token)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		ctx := context.WithValue(r.Context(), arrKey, a)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *SABnzbd) authenticate(category, username, password string) (*arr.Arr, error) {
	cfg := config.Get()
	a := s.manager.Arr().Get(category)
	known := a != nil
	if !known {
		// Arr is not yet in runtime storage — look for a matching config entry
		// so we inherit its download_uncached setting. If no config match,
		// leave nil so SendToDebrid falls back to the debrid provider's setting.
		var downloadUncached *bool
		for _, cfgArr := range config.Get().Arrs {
			if cfgArr.Name == category {
				downloadUncached = cfgArr.DownloadUncached
				break
			}
		}
		a = arr.New(category, "", "", false, downloadUncached, "", string(arr.SourceAuto))
	}
	if cfg.UseAuth {
		// With auth on, the caller must prove who it is: the web login, the
		// API token as the password, or the exact host and API key of an Arr
		// configured in Settings. An auto-detected Arr's host and key came from
		// an earlier caller, so they are not a credential. Nothing is probed or
		// stored here, so the stored Arr keeps its own host and key.
		if config.VerifyAuth(username, password) || config.VerifyToken(password) {
			return a, nil
		}
		if known && a.Source != arr.SourceAuto && username == a.Host && password != "" &&
			subtle.ConstantTimeCompare([]byte(password), []byte(a.Token)) == 1 {
			return a, nil
		}
		return nil, fmt.Errorf("unauthorized: invalid credentials")
	}

	// Validate the sent credentials on a candidate: the stored Arr is shared,
	// so it must not change unless the candidate validates.
	arrValidated := false
	if username != "" && password != "" {
		candidate := arr.New(category, username, password, a.SkipRepair, a.DownloadUncached, a.SelectedDebrid, string(arr.SourceAuto))
		arrValidated = candidate.Validate() == nil
	}

	if arrValidated && a.Source == arr.SourceAuto {
		updated := *a
		updated.Host = username
		updated.Token = password
		s.manager.Arr().AddOrUpdate(&updated)
		a = &updated
	}
	return a, nil
}
