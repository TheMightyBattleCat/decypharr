package usenet

import (
	"context"
	"fmt"
)

// RecoveryAvailability is one nzbID's live-server verdict on its own
// retained PAR2 recovery data: of every article the release's Par2Files
// (index file plus recovery volumes) point at, how many are still
// fetchable right now versus already gone from the provider.
//
// This is distinct from Par2Repair.Availability (pkg/manager), which only
// confirms PAR2 *metadata* was retained - it never touches the network.
// Recovery segments can rot off a provider's retention window the same way
// any other article can, so metadata presence alone doesn't mean a repair
// attempt will actually succeed; this is the network-checking counterpart.
type RecoveryAvailability struct {
	TotalSegments     int // every article referenced by Par2Files
	AvailableSegments int // confirmed present on the server
	MissingSegments   int // confirmed gone (definitive article-not-found)
	ErrorSegments     int // STAT attempts that errored without a definitive answer (connection issues, etc.) - neither present nor confirmed missing
}

// RecoveryAvailability batch-STATs every article referenced by nzbID's
// retained Par2Files (the PAR2 index file and recovery volumes), reporting
// how much of that recovery data is actually alive on the server right now.
//
// Uses BatchStatComplete rather than BatchStat: this is a pre-flight check
// run before committing to a PAR2 job, not a hot per-segment path, so an
// exhaustive count (every ID STAT-ed, no early bailout) is worth the extra
// round trips - callers need the real missing count to judge whether
// enough recovery data survives, not just a pass/fail signal.
func (u *Usenet) RecoveryAvailability(ctx context.Context, nzbID string) (RecoveryAvailability, error) {
	nzb, err := u.GetNZB(nzbID)
	if err != nil {
		return RecoveryAvailability{}, fmt.Errorf("failed to load NZB: %w", err)
	}
	if len(nzb.Par2Files) == 0 {
		return RecoveryAvailability{}, fmt.Errorf("no par2 metadata retained for this release")
	}

	var messageIDs []string
	for _, f := range nzb.Par2Files {
		for _, seg := range f.Segments {
			messageIDs = append(messageIDs, seg.MessageID)
		}
	}
	if len(messageIDs) == 0 {
		return RecoveryAvailability{}, fmt.Errorf("par2 metadata retained no segments")
	}

	stat, err := u.nntp.BatchStatComplete(ctx, messageIDs)
	if err != nil {
		return RecoveryAvailability{}, fmt.Errorf("recovery availability check failed: %w", err)
	}

	return RecoveryAvailability{
		TotalSegments:     stat.TotalCount,
		AvailableSegments: stat.FoundCount,
		MissingSegments:   stat.TotalCount - stat.FoundCount - stat.ErrorCount,
		ErrorSegments:     stat.ErrorCount,
	}, nil
}
