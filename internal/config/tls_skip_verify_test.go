package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// tls_skip_verify is kept per provider through a load and a settings save,
// and is off when the file does not set it.
func TestProviderTLSSkipVerifyRoundTrip(t *testing.T) {
	t.Setenv("DECYPHARR_SECRET_KEY", "")
	Reset()
	t.Cleanup(Reset)
	dir := t.TempDir()
	SetConfigPath(dir)
	raw := `{"port":"8282","download_folder":"/downloads","usenet":{"providers":[` +
		`{"host":"a.example","port":563,"username":"u","password":"p","ssl":true,"tls_skip_verify":true},` +
		`{"host":"b.example","port":563,"username":"u","password":"p","ssl":true}]}}`
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	check := func(stage string, c *Config) {
		t.Helper()
		got := map[string]bool{}
		for _, p := range c.Usenet.Providers {
			got[p.Host] = p.TLSSkipVerify
		}
		if !got["a.example"] || got["b.example"] {
			t.Errorf("%s: tls_skip_verify a=%v b=%v, want a=true b=false", stage, got["a.example"], got["b.example"])
		}
	}
	check("load", Get())

	if _, err := Update(func(c *Config) error {
		c.LogLevel = "debug"
		return nil
	}); err != nil {
		t.Fatalf("save: %v", err)
	}
	saved, err := os.ReadFile(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(saved), `"tls_skip_verify": true`) + strings.Count(string(saved), `"tls_skip_verify":true`); n != 1 {
		t.Errorf("saved config.json has tls_skip_verify true %d times, want 1", n)
	}

	Reset()
	SetConfigPath(dir)
	check("reload", Get())
}
