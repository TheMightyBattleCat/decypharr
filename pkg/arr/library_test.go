package arr

import (
	"strings"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/internal/config"
)

func TestDistinctByHost(t *testing.T) {
	s := &Storage{arrs: xsync.NewMap[string, *Arr]()}
	s.arrs.Store("radarr", New("radarr", "http://192.0.2.10:7878", "k", false, nil, "", "auto"))
	s.arrs.Store("bh-radarr", New("bh-radarr", "http://192.0.2.10:7878/", "k", false, nil, "", "auto"))
	s.arrs.Store("sonarr", New("sonarr", "http://192.0.2.10:8989", "k", false, nil, "", "auto"))
	s.arrs.Store("nohost", New("nohost", "", "k", false, nil, "", "auto"))

	got := s.DistinctByHost()
	if len(got) != 2 {
		t.Fatalf("got %d arrs, want 2", len(got))
	}
	names := map[string]bool{}
	for _, a := range got {
		names[a.Name] = true
	}
	if !names["bh-radarr"] || !names["sonarr"] {
		t.Fatalf("got %v, want bh-radarr (first by name) and sonarr", names)
	}
}

// Editing an Arr's host in Settings must take effect: SyncFromConfig kept the
// old in-memory host whenever the new one was a valid URL.
func TestSyncFromConfigAppliesEditedHost(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	s := &Storage{arrs: xsync.NewMap[string, *Arr]()}
	s.AddOrUpdate(New("sonarr", "http://old-host:8989", "k", false, nil, "", "manual"))

	s.SyncFromConfig([]config.Arr{{Name: "sonarr", Host: "http://new-host:8989", Token: "k"}})

	if got := s.Get("sonarr").Host; got != "http://new-host:8989" {
		t.Fatalf("host after save = %q, want the edited host", got)
	}
	if got := s.SyncToConfig(); len(got) != 1 || got[0].Host != "http://new-host:8989" {
		t.Fatalf("SyncToConfig = %+v, want the edited host", got)
	}
}

// A config entry with no usable host keeps the one the download-client
// handshake resolved.
func TestSyncFromConfigKeepsResolvedHostWhenConfigHostInvalid(t *testing.T) {
	s := &Storage{arrs: xsync.NewMap[string, *Arr]()}
	s.AddOrUpdate(New("tv", "http://sonarr:8989", "k", false, nil, "", "auto"))

	s.SyncFromConfig([]config.Arr{{Name: "tv", Host: "", Token: "k", Source: "auto"}})

	if got := s.Get("tv").Host; got != "http://sonarr:8989" {
		t.Fatalf("host = %q, want the resolved host kept", got)
	}
}

// The Settings page lists Arrs in SyncToConfig's order: it must be the
// config's order, not a map's, or the cards reshuffle on every load.
func TestSyncToConfigKeepsConfigOrder(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	live := config.Get()
	saved := live.Arrs
	t.Cleanup(func() { config.Get().Arrs = saved })
	live.Arrs = []config.Arr{
		{Name: "sonarr", Host: "http://h:8989", Token: "a"},
		{Name: "radarr", Host: "http://h:7878", Token: "b"},
		{Name: "tv", Host: "http://h:8989", Token: "a", Source: "auto"},
	}
	s := &Storage{arrs: xsync.NewMap[string, *Arr]()}
	for _, a := range live.Arrs {
		s.AddOrUpdate(New(a.Name, a.Host, a.Token, false, nil, "", a.Source))
	}
	s.AddOrUpdate(New("music", "http://h:8686", "c", false, nil, "", "auto"))
	s.AddOrUpdate(New("books", "http://h:8787", "d", false, nil, "", "auto"))

	want := []string{"sonarr", "radarr", "tv", "books", "music"}
	for range 20 {
		got := s.SyncToConfig()
		names := make([]string, len(got))
		for i, a := range got {
			names[i] = a.Name
		}
		if strings.Join(names, ",") != strings.Join(want, ",") {
			t.Fatalf("order = %v, want %v", names, want)
		}
	}
}
