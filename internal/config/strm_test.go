package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A config written before STRM existed has a session secret and no STRM
// secret. The first load must save a new STRM secret, keep it on later loads,
// and not write an environment override into config.json while doing so.
func TestStrmSecretIsSavedOnceWithoutEnvOverrides(t *testing.T) {
	t.Setenv("DECYPHARR_SECRET_KEY", "")
	t.Setenv("DECYPHARR_PORT", "9999")
	Reset()
	directory := t.TempDir()
	SetConfigPath(directory)
	t.Cleanup(Reset)
	path := filepath.Join(directory, "config.json")
	if err := os.WriteFile(path, []byte(`{"port":"8282","session_secret":"an-existing-session-secret"}`), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := Get()
	first := cfg.Strm.Secret
	if len(first) < 32 {
		t.Fatalf("STRM secret length = %d, want at least 32", len(first))
	}
	if cfg.Port != "9999" {
		t.Fatalf("Port = %q, want the environment override 9999", cfg.Port)
	}
	if cfg.Strm.Active() {
		t.Fatal("STRM is active on a config that never enabled it")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), first) {
		t.Fatal("config.json does not hold the generated STRM secret")
	}
	if strings.Contains(string(data), "9999") {
		t.Fatal("the environment override was written to config.json")
	}
	if !strings.Contains(string(data), "an-existing-session-secret") {
		t.Fatal("the session secret was lost by the save")
	}

	Reset()
	if got := Get().Strm.Secret; got != first {
		t.Fatal("STRM secret changed after loading the same installation")
	}
}
