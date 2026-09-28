package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// copyV3Fixtures copies the log-format-3 databases in testdata/v3 into a new
// data directory. They were written by the in-tree store this package used
// before appendstore, one record per store, so they stand for the data an
// existing install already has. They are kept as .v3 because .gitignore
// ignores *.db.
func copyV3Fixtures(t *testing.T) string {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	dir := t.TempDir()
	fixtures, err := filepath.Glob(filepath.Join("testdata", "v3", "*.v3"))
	if err != nil || len(fixtures) != len(storeNames) {
		t.Fatalf("want %d fixtures, found %d (%v)", len(storeNames), len(fixtures), err)
	}
	for _, f := range fixtures {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		name := strings.TrimSuffix(filepath.Base(f), ".v3") + ".db"
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestNewStorageReadsDataWrittenByThePreviousStore(t *testing.T) {
	dir := copyV3Fixtures(t)
	s, err := NewStorage(dir)
	if err != nil {
		t.Fatalf("open v3 data: %v", err)
	}

	entry, err := s.Get("fixturehash01")
	if err != nil {
		t.Fatalf("entry: %v", err)
	}
	if entry.Name != "Example.Release.2024" || entry.Category != "sonarr" || entry.Size != 4242 {
		t.Errorf("entry = %q %q %d", entry.Name, entry.Category, entry.Size)
	}
	var metas []*EntryMetaInfo
	if err := s.ForEachMeta(func(m *EntryMetaInfo) error {
		metas = append(metas, m)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(metas) != 1 || metas[0].Name != "Example.Release.2024" || metas[0].Size != 4242 ||
		metas[0].Protocol != string(config.ProtocolNZB) || metas[0].AddedOn.Unix() != 1700000000 {
		t.Errorf("entry metadata = %+v", metas)
	}
	if queued, err := s.GetQueued("fixturequeued01"); err != nil || queued.Status != debridTypes.TorrentStatusQueued {
		t.Errorf("queued entry = %+v, %v", queued, err)
	}
	if item, err := s.GetEntryItem("Example.Release.2024"); err != nil || len(item.Files) != 1 {
		t.Errorf("entry item = %+v, %v", item, err)
	}
	if runs, err := s.ListRepairRuns(); err != nil || len(runs) != 1 || runs[0].ID != "run-1" {
		t.Errorf("repair runs = %+v, %v", runs, err)
	}
	if h, err := s.GetEntryHealth("Example.Release.2024"); err != nil || h.Status != HealthHealthy {
		t.Errorf("entry health = %+v, %v", h, err)
	}
	if counts := s.CountEntryHealthByStatus(); counts[HealthHealthy] != 1 {
		t.Errorf("health counts = %v", counts)
	}
	if a, err := s.GetPar2RepairAttempt("attempt-1"); err != nil || a.NzbID != "fixturehash01" {
		t.Errorf("par2 attempt = %+v, %v", a, err)
	}
	if st, err := s.GetPar2RepairState("fixturehash01"); err != nil || st.AttemptCount != 2 {
		t.Errorf("par2 state = %+v, %v", st, err)
	}
	var guards, deletes []string
	_ = s.ForEachRegrabGuardRecord(func(r *RegrabGuardRecord) { guards = append(guards, r.Identity) })
	_ = s.ForEachPendingDelete(func(p *PendingDelete) { deletes = append(deletes, p.InfoHash) })
	if len(guards) != 1 || guards[0] != "fixture-identity" {
		t.Errorf("regrab guard records = %v", guards)
	}
	if len(deletes) != 1 || deletes[0] != "fixturehash02" {
		t.Errorf("pending deletes = %v", deletes)
	}

	// Every store keeps its pre-upgrade copy, the way back to an older build.
	for _, name := range storeNames {
		backups, _ := filepath.Glob(filepath.Join(dir, name+".db.v3*"))
		if len(backups) != 1 {
			t.Errorf("%s: want one pre-upgrade copy, found %v", name, backups)
		}
	}

	// Data written after the upgrade survives a reopen alongside the old data.
	added := time.Unix(1710000000, 0).UTC()
	if err := s.AddOrUpdate(&Entry{
		Protocol: config.ProtocolNZB, InfoHash: "fixturehash03", Name: "Later.Release.2025",
		Size: 99, Status: debridTypes.TorrentStatusDownloaded, AddedOn: added,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = NewStorage(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s.Close()
	for _, hash := range []string{"fixturehash01", "fixturehash03"} {
		if _, err := s.Get(hash); err != nil {
			t.Errorf("after reopen, %s: %v", hash, err)
		}
	}
}
