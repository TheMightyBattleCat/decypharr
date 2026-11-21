package qbit

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
)

type contextKey string

const (
	categoryKey contextKey = "category"
	hashesKey   contextKey = "hashes"
	arrKey      contextKey = "arr"
)

func getCategory(ctx context.Context) string {
	if category, ok := ctx.Value(categoryKey).(string); ok {
		return category
	}
	return ""
}

func getHashes(ctx context.Context) []string {
	if hashes, ok := ctx.Value(hashesKey).([]string); ok {
		return hashes
	}
	return nil
}

func getArrFromContext(ctx context.Context) *arr.Arr {
	if a, ok := ctx.Value(arrKey).(*arr.Arr); ok {
		return a
	}
	return nil
}

func decodeAuthHeader(header string) (string, string, error) {
	encodedTokens := strings.Split(header, " ")
	if len(encodedTokens) != 2 {
		return "", "", nil
	}
	encodedToken := encodedTokens[1]

	bytes, err := base64.StdEncoding.DecodeString(encodedToken)
	if err != nil {
		return "", "", err
	}

	bearer := string(bytes)

	colonIndex := strings.LastIndex(bearer, ":")
	if colonIndex < 0 {
		// strings.LastIndex returns -1 when the substring is absent; without
		// this guard `bearer[:colonIndex]` would panic with
		// "slice bounds out of range [:-1]". Triggers on any Authorization
		// header whose decoded base64 payload contains no ':' separator
		// (e.g. an empty payload, or garbage bytes that decode but lack a
		// 'user:pass' shape).
		return "", "", fmt.Errorf("malformed credentials: missing colon separator")
	}
	username := bearer[:colonIndex]
	password := bearer[colonIndex+1:]

	if username == "" || password == "" {
		return username, password, fmt.Errorf("empty username or password")
	}

	return strings.TrimSpace(username), strings.TrimSpace(password), nil
}

func (q *QBit) categoryContext(next http.Handler) http.Handler {
	// Print full URL for debugging

	// Try to get category from URL query first
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Print request method and URL
		category := strings.Trim(r.URL.Query().Get("category"), "")
		if category == "" {
			// GetReader from form
			_ = r.ParseForm()
			category = r.Form.Get("category")
			if category == "" {
				// GetReader from multipart form
				_ = r.ParseMultipartForm(32 << 20)
				category = r.FormValue("category")
			}
		}
		ctx := context.WithValue(r.Context(), categoryKey, strings.TrimSpace(category))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// authContext creates a middleware that extracts the Arr host and token from the Authorization header
// and adds it to the request context.
// This is used to identify the Arr instance for the utils.
// Only a valid host and token will be added to the context/config. The rest are manual
//
// A request that carries a SID cookie the store does not know, or whose
// credentials no longer pass, gets 403 as qBittorrent answers: the Arrs log
// in again on a 403, and on nothing else.
func (q *QBit) authContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, sid := getCredentials(r)
		if sid != "" {
			var ok bool
			username, password, ok = sids.lookup(sid)
			if !ok {
				http.Error(w, "Forbidden", http.StatusForbidden)
				return
			}
		}
		category := getCategory(r.Context())
		a, err := q.authenticate(category, username, password)
		if err != nil {
			if sid != "" {
				sids.drop(sid)
				http.Error(w, "Forbidden", http.StatusForbidden)
				return
			}
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		ctx := context.WithValue(r.Context(), arrKey, a)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// getCredentials returns the Basic credentials when the request has them,
// otherwise the SID cookie (sid or SID) for the caller to look up.
func getCredentials(r *http.Request) (username, password, sid string) {
	username, password, err := decodeAuthHeader(r.Header.Get("Authorization"))
	if err == nil && username != "" {
		return username, password, ""
	}
	cookie, err := r.Cookie("sid")
	if err != nil {
		cookie, err = r.Cookie("SID")
	}
	if err == nil && cookie.Value != "" {
		return "", "", cookie.Value
	}
	return "", "", ""
}

func (q *QBit) authenticate(category, username, password string) (*arr.Arr, error) {
	cfg := config.Get()
	a := q.manager.Arr().Get(category)
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
		q.manager.Arr().AddOrUpdate(&updated)
		a = &updated
	}
	return a, nil
}

func hashesContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_hashes := chi.URLParam(r, "hashes")
		var hashes []string
		if _hashes != "" {
			hashes = strings.Split(_hashes, "|")
		}
		if hashes == nil {
			// GetReader hashes from form
			_ = r.ParseForm()
			hashes = r.Form["hashes"]
		}
		for i, hash := range hashes {
			hashes[i] = strings.TrimSpace(hash)
		}
		ctx := context.WithValue(r.Context(), hashesKey, hashes)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
