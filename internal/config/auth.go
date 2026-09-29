package config

import (
	"crypto/subtle"

	"golang.org/x/crypto/bcrypt"
)

func VerifyAuth(username, password string) bool {
	// If you're storing hashed password, use bcrypt to compare
	if username == "" {
		return false
	}
	auth := Get().GetAuth()
	if auth == nil {
		return false
	}
	if username != auth.Username {
		return false
	}
	err := bcrypt.CompareHashAndPassword([]byte(auth.Password), []byte(password))
	return err == nil
}

// VerifyToken reports whether token matches the configured API token,
// compared in constant time.
//
// This is kept out of VerifyAuth on purpose. The token authenticates the HTTP
// API surfaces (web API, qBittorrent, SABnzbd) only; WebDAV goes through
// VerifyAuth and must never be unlocked by an API token.
func VerifyToken(token string) bool {
	if token == "" {
		return false
	}
	auth := Get().GetAuth()
	if auth == nil || auth.APIToken == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(auth.APIToken)) == 1
}
