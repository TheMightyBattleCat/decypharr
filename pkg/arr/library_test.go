package arr

import (
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
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
