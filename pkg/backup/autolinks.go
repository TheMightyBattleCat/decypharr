package backup

import (
	"context"
	"fmt"
	"io/fs"

	"github.com/sirrobot01/decypharr/internal/config"
)

// maxKeptRecreated bounds the list of automatically re-created links kept
// for the Library Links page.
const maxKeptRecreated = 200

// autoRecreate puts back links that the Arrs list but the disk lacks, when
// the opt-in setting is on and a manual link restore has been applied before.
// Only `create` rows whose newest snapshot holds a live link are used; a
// link that exists is never retargeted. Over the per-check limit nothing is
// created and links_dropped is sent instead.
func (s *BackupService) autoRecreate(ctx context.Context, m *Manifest) {
	ar := s.cfg().AutoRecreate
	if !ar.Enabled || m == nil || len(m.Missing) == 0 || !LinkRestoreApplied(s.deps.DataDir) {
		return
	}
	limit := ar.Limit
	if limit <= 0 {
		limit = 25
	}
	r, err := s.repo()
	if err != nil {
		return // no destination: nothing to restore from
	}
	infos, err := r.ListSnapshots(ctx)
	if err != nil {
		s.log.Warn().Err(err).Msg("Automatic link re-creation could not list snapshots")
		return
	}

	want := make(map[string]struct{}, len(m.Missing))
	for _, ml := range m.Missing {
		want[ml.Path] = struct{}{}
	}
	// The newest snapshot holding a path decides; an older one is not tried
	// when the newest has the link in a state other than live.
	type pick struct {
		snap string
		rows []LinkRow
	}
	var picks []*pick
	for _, info := range infos { // newest first
		if len(want) == 0 {
			break
		}
		if ctx.Err() != nil {
			return
		}
		sm, err := s.loadSnapshotManifest(ctx, r, info.ID)
		if err != nil {
			continue
		}
		var p *pick
		for _, row := range sm.Rows {
			if _, ok := want[row.Path]; !ok {
				continue
			}
			delete(want, row.Path)
			if row.State != LinkLive {
				continue
			}
			if p == nil {
				p = &pick{snap: info.ID}
				picks = append(picks, p)
			}
			p.rows = append(p.rows, row)
		}
	}

	fsys := s.deps.FS
	if fsys == nil {
		fsys = OSLinkFS{}
	}
	mount := ""
	if s.deps.MountRoot != nil {
		mount = s.deps.MountRoot()
	}
	now := s.now().UTC()
	var plans []LinkPlan
	total := 0
	for _, p := range picks {
		plan := BuildLinkPlan(LinkPlanInput{FS: fsys, Lookup: s.deps.Lookup, MountRoot: mount,
			Rows: p.rows, SnapshotID: p.snap, Now: now})
		creates := plan.Rows[:0:0]
		for _, row := range plan.Rows {
			if row.Class == PlanCreate {
				creates = append(creates, row)
			}
		}
		if len(creates) == 0 {
			continue
		}
		plan.Rows = creates
		plan.Counts = map[PlanClass]int{PlanCreate: len(creates)}
		plans = append(plans, plan)
		total += len(creates)
	}
	if total == 0 {
		return
	}
	if total > limit {
		s.log.Warn().Int("candidates", total).Int("limit", limit).
			Msg("Automatic link re-creation skipped: over the limit")
		s.notify(config.EventLinksDropped, "warning",
			fmt.Sprintf("%d library links could be re-created automatically, over the limit of %d, so none were", total, limit), nil)
		return
	}

	for _, plan := range plans {
		res := ApplyLinkPlan(ctx, fsys, plan, LinkApplyOptions{Rescan: ar.Rescan, Rescanner: s.deps.Rescanner})
		for _, e := range res.Errors {
			s.log.Warn().Str("snapshot", plan.SnapshotID).Msg("Automatic link re-creation: " + e)
		}
		for _, w := range res.Warnings {
			s.log.Warn().Str("snapshot", plan.SnapshotID).Msg("Automatic link re-creation: " + w)
		}
		if res.Created == 0 {
			continue
		}
		// ApplyLinkPlan reports counts only, so a row counts as created when
		// a link now sits at its path.
		for _, row := range plan.Rows {
			if fi, err := fsys.Lstat(row.Path); err != nil || fi.Mode()&fs.ModeSymlink == 0 {
				continue
			}
			s.log.Info().Str("path", row.Path).Str("snapshot", plan.SnapshotID).
				Msg("Library link re-created automatically")
			s.addRecreated(AutoRecreated{Path: row.Path, SnapshotID: plan.SnapshotID, At: now})
		}
	}
}

func (s *BackupService) addRecreated(a AutoRecreated) {
	s.linkMu.Lock()
	defer s.linkMu.Unlock()
	s.recreated = append(s.recreated, a)
	if len(s.recreated) > maxKeptRecreated {
		s.recreated = s.recreated[len(s.recreated)-maxKeptRecreated:]
	}
}

// recreatedList returns a copy of the automatic re-creations, newest first.
// Callers hold linkMu.
func (s *BackupService) recreatedList() []AutoRecreated {
	out := make([]AutoRecreated, len(s.recreated))
	for i, a := range s.recreated {
		out[len(out)-1-i] = a
	}
	return out
}
