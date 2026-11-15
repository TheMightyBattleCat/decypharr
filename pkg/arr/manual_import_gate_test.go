package arr

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

func TestManualImportGateCooldownAndInflight(t *testing.T) {
	now := time.Unix(0, 0)
	g := newManualImportGate()
	g.now = func() time.Time { return now }

	if !g.acquire("a") {
		t.Fatal("first acquire refused")
	}
	if g.acquire("a") {
		t.Fatal("second acquire allowed while in flight")
	}
	if !g.acquire("b") {
		t.Fatal("other download refused")
	}
	g.release("a")
	now = now.Add(manualImportCooldown - time.Second)
	if g.acquire("a") {
		t.Fatal("acquire allowed inside cooldown")
	}
	now = now.Add(2 * time.Second)
	if !g.acquire("a") {
		t.Fatal("acquire refused after cooldown")
	}
}

// Two cleanup ticks for the same stuck download must cost the arr one scan,
// and a failed scan must not be retried.
func TestManualImportItemsScansOncePerCooldown(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	old := importGate
	importGate = newManualImportGate()
	defer func() { importGate = old }()

	var scans atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v3/manualimport" {
			scans.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	a := New("sonarr", srv.URL, "k", false, nil, "", "manual")
	b := New("sonarr-dup", srv.URL, "k", false, nil, "", "manual")

	items := map[string]bool{"dl-1": true}
	_ = a.ManualImportItems(items)
	_ = a.ManualImportItems(items)
	_ = b.ManualImportItems(items)

	if got := scans.Load(); got != 1 {
		t.Fatalf("manualimport scans = %d, want 1", got)
	}
}
