package config

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestSessionSecretPersistsAcrossLoads(t *testing.T) {
	t.Setenv("DECYPHARR_SECRET_KEY", "")
	Reset()
	directory := t.TempDir()
	SetConfigPath(directory)
	t.Cleanup(Reset)
	if err := os.WriteFile(filepath.Join(directory, "config.json"), []byte(`{"port":"8282"}`), 0644); err != nil {
		t.Fatal(err)
	}
	first := Get().SecretKey()
	if len(first) < 32 {
		t.Fatalf("session key length = %d, want at least 32", len(first))
	}
	info, err := os.Stat(filepath.Join(directory, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0600 {
		t.Fatalf("config permissions = %o, want 600", info.Mode().Perm())
	}
	Reset()
	if Get().SecretKey() != first {
		t.Fatal("session key changed after loading the same installation")
	}
	Reset()
	SetConfigPath(t.TempDir())
	if Get().SecretKey() == first {
		t.Fatal("independent installations share a session key")
	}
}

// The first-load save of a new session secret must not write environment
// overrides into config.json.
func TestSessionSecretSaveLeavesEnvOverridesOut(t *testing.T) {
	t.Setenv("DECYPHARR_SECRET_KEY", "")
	t.Setenv("DECYPHARR_PORT", "9999")
	Reset()
	directory := t.TempDir()
	SetConfigPath(directory)
	t.Cleanup(Reset)
	if err := os.WriteFile(filepath.Join(directory, "config.json"), []byte(`{"port":"8282"}`), 0644); err != nil {
		t.Fatal(err)
	}
	if got := Get().Port; got != "9999" {
		t.Fatalf("Port = %q, want the environment override 9999", got)
	}
	data, err := os.ReadFile(filepath.Join(directory, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"session_secret"`) {
		t.Fatal("config.json has no session_secret after the first load")
	}
	if strings.Contains(string(data), "9999") {
		t.Fatal("the environment override was written to config.json")
	}
}

func TestSessionSecretEnvironmentOverride(t *testing.T) {
	t.Setenv("DECYPHARR_SECRET_KEY", "explicit-session-key")
	cfg := &Config{SessionSecret: "persisted-session-key"}
	if got := cfg.SecretKey(); got != "explicit-session-key" {
		t.Fatalf("SecretKey() = %q, want the environment override", got)
	}
	t.Setenv("DECYPHARR_SECRET_KEY", "")
	if got := cfg.SecretKey(); got != cfg.SessionSecret {
		t.Fatal("removing the override did not restore the persisted key")
	}
}

func TestSaveAuthChangesSessionVersionAndIsOwnerOnly(t *testing.T) {
	Reset()
	directory := t.TempDir()
	SetConfigPath(directory)
	t.Cleanup(Reset)
	c := Get()
	c.UseAuth = true
	if err := c.SaveAuth(&Auth{Username: "admin", Password: "hash", APIToken: "tok"}); err != nil {
		t.Fatal(err)
	}
	first := c.GetAuth().SessionVersion
	if first == "" {
		t.Fatal("SaveAuth left the session version empty")
	}
	if err := c.SaveAuth(c.GetAuth()); err != nil {
		t.Fatal(err)
	}
	if c.GetAuth().SessionVersion == first {
		t.Fatal("saving the credentials again kept the session version")
	}
	info, err := os.Stat(c.AuthFile())
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0600 {
		t.Fatalf("auth.json permissions = %o, want 600", info.Mode().Perm())
	}
	if err := c.SaveAuth(nil); err == nil {
		t.Fatal("SaveAuth(nil) succeeded")
	}
}
