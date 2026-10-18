package backup

import (
	"context"
)

// notImplemented is the Service used until the real one is wired in.
type notImplemented struct{}

// NotImplemented returns a Service whose every method returns
// ErrNotImplemented (PreAction is a no-op).
func NotImplemented() Service { return notImplemented{} }

func (notImplemented) Status(context.Context) (StatusResponse, error) {
	return StatusResponse{}, ErrNotImplemented
}
func (notImplemented) RunJob(context.Context, string) (RunJobResponse, error) {
	return RunJobResponse{}, ErrNotImplemented
}
func (notImplemented) ListSnapshots(context.Context) ([]SnapshotInfo, error) {
	return nil, ErrNotImplemented
}
func (notImplemented) GetSnapshot(context.Context, string) (*Snapshot, error) {
	return nil, ErrNotImplemented
}
func (notImplemented) DeleteSnapshot(context.Context, string) error { return ErrNotImplemented }
func (notImplemented) VerifySnapshot(context.Context, string, bool) (VerifyReport, error) {
	return VerifyReport{}, ErrNotImplemented
}
func (notImplemented) Download(context.Context, string, Component) (DownloadResult, error) {
	return DownloadResult{}, ErrNotImplemented
}
func (notImplemented) RestoreDB(context.Context, string) (RestoreDBResponse, error) {
	return RestoreDBResponse{}, ErrNotImplemented
}
func (notImplemented) RestoreNZB(context.Context, string, RestoreNZBRequest) (RestoreNZBResponse, error) {
	return RestoreNZBResponse{}, ErrNotImplemented
}
func (notImplemented) PlanLinksRestore(context.Context, string, LinksPlanRequest) (LinkPlan, error) {
	return LinkPlan{}, ErrNotImplemented
}
func (notImplemented) ApplyLinksRestore(context.Context, string, LinksApplyRequest) (LinksApplyResponse, error) {
	return LinksApplyResponse{}, ErrNotImplemented
}
func (notImplemented) LinksStatus(context.Context) (LinksStatusResponse, error) {
	return LinksStatusResponse{}, ErrNotImplemented
}
func (notImplemented) LinksCheck(context.Context) (LinksStatusResponse, error) {
	return LinksStatusResponse{}, ErrNotImplemented
}
func (notImplemented) ListLinks(context.Context, LinksQuery) (LinksPage, error) {
	return LinksPage{}, ErrNotImplemented
}
func (notImplemented) LinksMissing(context.Context) ([]MissingLink, error) {
	return nil, ErrNotImplemented
}
func (notImplemented) PlanLinksCleanup(context.Context, LinksCleanupPlanRequest) (CleanupPlan, error) {
	return CleanupPlan{}, ErrNotImplemented
}
func (notImplemented) ApplyLinksCleanup(context.Context, LinksCleanupApplyRequest) (CleanupResult, error) {
	return CleanupResult{}, ErrNotImplemented
}
func (notImplemented) ListRemoved(context.Context, RemovedQuery) ([]RemovedEntry, error) {
	return nil, ErrNotImplemented
}
func (notImplemented) RestoreRemoved(context.Context, string, RestoreRemovedRequest) (RestoreRemovedResponse, error) {
	return RestoreRemovedResponse{}, ErrNotImplemented
}
func (notImplemented) ListConfigHistory(context.Context) ([]ConfigVersion, error) {
	return nil, ErrNotImplemented
}
func (notImplemented) DiffConfigHistory(context.Context, string, string) (ConfigDiff, error) {
	return ConfigDiff{}, ErrNotImplemented
}
func (notImplemented) RevertConfig(context.Context, string) (RevertResponse, error) {
	return RevertResponse{}, ErrNotImplemented
}
func (notImplemented) RetentionPreview(context.Context, RetentionPreviewRequest) (PrunePlan, error) {
	return PrunePlan{}, ErrNotImplemented
}
func (notImplemented) PreAction(ctx context.Context, action string) (string, error) {
	return PreAction(ctx, nil, action)
}
