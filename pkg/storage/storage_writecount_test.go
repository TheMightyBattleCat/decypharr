package storage

import (
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
)

func TestWriteCountRisesWithWritesAndDeletes(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	s, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	n0 := s.WriteCount()
	if s.WriteCount() != n0 {
		t.Fatal("count moved without a write")
	}
	if err := s.SaveRepairRun(&RepairRun{ID: "r1"}); err != nil {
		t.Fatal(err)
	}
	n1 := s.WriteCount()
	if n1 <= n0 {
		t.Fatalf("a write did not raise the count: %d -> %d", n0, n1)
	}
	if err := s.DeleteRepairRun("r1"); err != nil {
		t.Fatal(err)
	}
	if s.WriteCount() <= n1 {
		t.Fatal("a delete did not raise the count")
	}
}
