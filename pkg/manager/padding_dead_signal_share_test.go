package manager

import "testing"

// Two overlapping probes of one file (a manual recheck during the sweep, an
// import-gate retry): the second registration used to replace the first, so
// the first probe's reads tripped nothing and it could pass a file whose
// dead segment it read (review N2).
func TestDeadSignalReachesEveryOverlappingProbe(t *testing.T) {
	const hash, file = "nzb-sig", "movie.mkv"
	first, second := NewDeadSegmentSignal(), NewDeadSegmentSignal()
	registerDeadSignal(hash, file, first)
	registerDeadSignal(hash, file, second)

	read := DeadSignalForVerificationRead(hash, file)
	if read == nil {
		t.Fatalf("no signal for a registered file")
	}
	read.Trip()
	if !first.Detected() || !second.Detected() {
		t.Fatalf("trip reached first=%v second=%v, want both", first.Detected(), second.Detected())
	}

	// A probe registering while the file's hub is tripped starts tripped.
	late := NewDeadSegmentSignal()
	registerDeadSignal(hash, file, late)
	if !late.Detected() {
		t.Fatalf("late probe of a file with a confirmed-dead segment is not tripped")
	}

	for _, s := range []*DeadSegmentSignal{first, second, late} {
		unregisterDeadSignal(hash, file, s)
	}
	if DeadSignalForVerificationRead(hash, file) != nil {
		t.Fatalf("registry kept a file with no probes")
	}
}

// Unregistering the first probe must leave the second one's reads wired.
func TestDeadSignalSurvivesTheOtherProbeFinishing(t *testing.T) {
	const hash, file = "nzb-sig-2", "movie.mkv"
	first, second := NewDeadSegmentSignal(), NewDeadSegmentSignal()
	registerDeadSignal(hash, file, first)
	registerDeadSignal(hash, file, second)
	unregisterDeadSignal(hash, file, first)

	read := DeadSignalForVerificationRead(hash, file)
	if read == nil {
		t.Fatalf("second probe's reads lost their signal when the first finished")
	}
	read.Trip()
	if !second.Detected() {
		t.Fatalf("second probe not tripped")
	}
	if first.Detected() {
		t.Fatalf("a finished probe was tripped by a later read")
	}
	unregisterDeadSignal(hash, file, second)
}
