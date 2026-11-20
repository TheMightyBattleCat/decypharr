//go:build !windows

package config

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

const atomicWriteChildEnv = "ATOMICWRITE_CHILD"

// runAtomicWriteChild re-runs this test binary for one child test with a
// file-size limit, so a write past the limit fails with EFBIG the way a full
// disk fails with ENOSPC.
func runAtomicWriteChild(t *testing.T, name, dir string) string {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^"+name+"$", "-test.v")
	cmd.Env = append(os.Environ(), atomicWriteChildEnv+"=1", "ATOMICWRITE_DIR="+dir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("child %s failed: %v\n%s", name, err, out)
	}
	return string(out)
}

func atomicWriteLimitFileSize(t *testing.T, limit uint64) {
	t.Helper()
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &syscall.Rlimit{Cur: limit, Max: limit}); err != nil {
		t.Fatalf("setrlimit: %v", err)
	}
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, " | ")
}

// A settings save that fails part way (full disk, crash) leaves config.json
// truncated, and the next start exits in config.Get.
func TestFailedConfigSaveKeepsOldFile(t *testing.T) {
	if os.Getenv(atomicWriteChildEnv) != "" {
		t.Skip("parent only")
	}
	t.Setenv("DECYPHARR_SECRET_KEY", "")
	Reset()
	t.Cleanup(Reset)
	dir := t.TempDir()
	SetConfigPath(dir)
	good := `{"port":"8282","download_folder":"/downloads","usenet":{"providers":[{"host":"news.example","port":563,"username":"u","password":"p","connections":10,"ssl":true}]}}`
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(good), 0600); err != nil {
		t.Fatal(err)
	}
	if Get().Port != "8282" {
		t.Fatal("first load failed")
	}
	before, _ := os.ReadFile(filepath.Join(dir, "config.json"))
	t.Logf("config.json before the failed save: %d bytes, valid JSON %v", len(before), json.Valid(before))

	out := runAtomicWriteChild(t, "TestFailedConfigSaveChild", dir)
	t.Logf("child: %s", strings.TrimSpace(lastLines(out, 3)))

	after, err := os.ReadFile(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("config.json after the failed save: %d bytes, valid JSON %v", len(after), json.Valid(after))

	if string(after) != string(before) {
		t.Errorf("a failed settings save changed config.json (%d -> %d bytes)", len(before), len(after))
	}
	Reset()
	var c Config
	if err := c.loadConfig(); err != nil {
		t.Errorf("next start cannot load config.json: %v", err)
	}
}

func TestFailedConfigSaveChild(t *testing.T) {
	if os.Getenv(atomicWriteChildEnv) == "" {
		t.Skip("child only")
	}
	Reset()
	SetConfigPath(os.Getenv("ATOMICWRITE_DIR"))
	Get()
	atomicWriteLimitFileSize(t, 300)
	_, err := Update(func(c *Config) error {
		c.LogLevel = "debug"
		return nil
	})
	t.Logf("settings save error: %v", err)
	if err == nil {
		t.Fatal("save succeeded under the size limit")
	}
}

// A leftover temp file from an interrupted save does not stop a load.
func TestLeftoverTempFileDoesNotStopLoad(t *testing.T) {
	t.Setenv("DECYPHARR_SECRET_KEY", "")
	Reset()
	t.Cleanup(Reset)
	dir := t.TempDir()
	SetConfigPath(dir)
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(`{"port":"8282"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.json.tmp-123"), []byte(`{"por`), 0600); err != nil {
		t.Fatal(err)
	}
	if Get().Port != "8282" {
		t.Fatal("load failed with a leftover temp file")
	}
}

// The exit message for a broken config.json does not quote the file.
func TestBrokenConfigErrorDoesNotQuoteFile(t *testing.T) {
	t.Setenv("DECYPHARR_SECRET_KEY", "")
	Reset()
	t.Cleanup(Reset)
	dir := t.TempDir()
	SetConfigPath(dir)
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(`{"session_secret":"hunter2-secret","usenet":{"providers":[{"password":"p4ss-word"`), 0600); err != nil {
		t.Fatal(err)
	}
	var c Config
	err := c.loadConfig()
	if err == nil {
		t.Fatal("broken config.json loaded")
	}
	if strings.Contains(err.Error(), "hunter2") || strings.Contains(err.Error(), "p4ss-word") {
		t.Errorf("error quotes the file: %v", err)
	}
	if !strings.Contains(err.Error(), filepath.Join(dir, "config.json")) {
		t.Errorf("error does not name the file: %v", err)
	}
}

const (
	authTestConfig = `{"port":"8282","use_auth":true,"download_folder":"/downloads","usenet":{"providers":[{"host":"news.example","port":563,"username":"u","password":"p","connections":10,"ssl":true}]}}`
	authTestToken  = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	authTestFile   = `{"session_version":"v1","username":"admin","password":"$2a$10$abcdefghijklmnopqrstuuJ8cG4b5Qy0Hc0r0G1qv7Yd9b2nYbVWa","api_token":"` + authTestToken + `"}`
)

func writeAuthTestDir(t *testing.T, cfg, auth string) string {
	t.Helper()
	dir := t.TempDir()
	SetConfigPath(dir)
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(cfg), 0600); err != nil {
		t.Fatal(err)
	}
	if auth != "" {
		if err := os.WriteFile(filepath.Join(dir, "auth.json"), []byte(auth), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// A credential save that fails part way leaves auth.json as it was, and the
// next start keeps the stored login and API token.
func TestFailedAuthSaveKeepsOldFile(t *testing.T) {
	if os.Getenv(atomicWriteChildEnv) != "" {
		t.Skip("parent only")
	}
	t.Setenv("DECYPHARR_SECRET_KEY", "")
	Reset()
	t.Cleanup(Reset)
	dir := writeAuthTestDir(t, authTestConfig, authTestFile)
	if Get().NeedsAuth() {
		t.Fatal("control: an install with a stored credential needs no registration")
	}
	before, _ := os.ReadFile(filepath.Join(dir, "auth.json"))

	out := runAtomicWriteChild(t, "TestFailedAuthSaveChild", dir)
	t.Logf("child: %s", strings.TrimSpace(lastLines(out, 3)))

	after, _ := os.ReadFile(filepath.Join(dir, "auth.json"))
	if string(after) != string(before) {
		t.Fatalf("a failed credential save changed auth.json (%d -> %d bytes, valid JSON %v)", len(before), len(after), json.Valid(after))
	}
	Reset()
	next := Get()
	if next.NeedsAuth() {
		t.Error("next start opens registration after a failed credential save")
	}
	if a := next.GetAuth(); a == nil || a.Username != "admin" || a.APIToken != authTestToken {
		t.Errorf("next start lost the stored login or token: %+v", a)
	}
}

func TestFailedAuthSaveChild(t *testing.T) {
	if os.Getenv(atomicWriteChildEnv) == "" {
		t.Skip("child only")
	}
	Reset()
	SetConfigPath(os.Getenv("ATOMICWRITE_DIR"))
	Get()
	atomicWriteLimitFileSize(t, 64)
	_, err := Update(func(n *Config) error {
		a := n.GetAuth()
		a.APIToken = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
		return n.SaveAuth(a)
	})
	t.Logf("credential save error: %v", err)
	if err == nil {
		t.Fatal("save succeeded under the size limit")
	}
}

// An auth.json that exists but cannot be decoded stops the load, and the
// file is left as it is: no new token, no open registration.
func TestUnreadableAuthFileStopsLoad(t *testing.T) {
	t.Setenv("DECYPHARR_SECRET_KEY", "")
	Reset()
	t.Cleanup(Reset)
	torn := authTestFile[:64]
	dir := writeAuthTestDir(t, authTestConfig, torn)

	var c Config
	err := c.loadConfig()
	if err == nil {
		t.Fatalf("an unreadable auth.json loaded: NeedsAuth=%v", c.NeedsAuth())
	}
	if !strings.Contains(err.Error(), "auth.json is unreadable") || !strings.Contains(err.Error(), filepath.Join(dir, "auth.json")) {
		t.Errorf("error does not name auth.json: %v", err)
	}
	if strings.Contains(err.Error(), "$2a$") {
		t.Errorf("error quotes the file: %v", err)
	}
	after, _ := os.ReadFile(filepath.Join(dir, "auth.json"))
	if string(after) != torn {
		t.Errorf("auth.json was rewritten: %q", after)
	}
}

// Turning auth on in a settings save over an unreadable auth.json fails the
// save instead of writing a new token over it.
func TestEnableAuthOverUnreadableAuthFileFailsSave(t *testing.T) {
	t.Setenv("DECYPHARR_SECRET_KEY", "")
	Reset()
	t.Cleanup(Reset)
	cfg := strings.Replace(authTestConfig, `"use_auth":true,`, "", 1)
	dir := writeAuthTestDir(t, cfg, "")
	if Get().UseAuth {
		t.Fatal("control: auth is off")
	}
	torn := authTestFile[:64]
	if err := os.WriteFile(filepath.Join(dir, "auth.json"), []byte(torn), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := Update(func(n *Config) error {
		n.UseAuth = true
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "auth.json is unreadable") {
		t.Errorf("enabling auth over an unreadable auth.json: err = %v", err)
	}
	after, _ := os.ReadFile(filepath.Join(dir, "auth.json"))
	if string(after) != torn {
		t.Errorf("auth.json was rewritten: %q", after)
	}
}

// Control: with no auth.json and auth on, the first start opens
// registration and stores a new API token, as before.
func TestMissingAuthFileOpensRegistration(t *testing.T) {
	t.Setenv("DECYPHARR_SECRET_KEY", "")
	Reset()
	t.Cleanup(Reset)
	dir := writeAuthTestDir(t, authTestConfig, "")
	c := Get()
	if !c.NeedsAuth() {
		t.Error("first start with auth on and no auth.json needs registration")
	}
	data, err := os.ReadFile(filepath.Join(dir, "auth.json"))
	if err != nil {
		t.Fatalf("no auth.json written: %v", err)
	}
	var a Auth
	if err := json.Unmarshal(data, &a); err != nil || a.APIToken == "" || a.Username != "" {
		t.Errorf("auth.json after first start: %+v, %v", a, err)
	}
}
