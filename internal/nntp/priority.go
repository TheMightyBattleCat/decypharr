package nntp

import "context"

// Priority is a per-request bandwidth-tier hint carried on a request's
// context. Everything implicitly requests PriorityNormal by simply not
// setting one; only the URGENT PAR2 repair lane (see pkg/manager.Par2Repair)
// sets PriorityUrgent.
type Priority int

const (
	PriorityNormal Priority = iota

	// PriorityUrgent lets a request drawn from a provider currently in its
	// reserve band (QuotaReserve) be served at lead tier instead of being
	// demoted to fills-only - e.g. so urgent playback-adjacent PAR2 repair
	// isn't starved behind normal bulk traffic during active playback. Never
	// promotes a hard-blocked (QuotaBlocked) provider, and never promotes a
	// configured backup: the bandwidth monitor still gates urgent work
	// exactly as it gates everything else, just with the reserve-band
	// fills-only demotion waived for primaries.
	PriorityUrgent
)

type priorityCtxKey struct{}

// WithPriority attaches p to ctx for every NNTP request made using it (and
// every context derived from it, e.g. via context.WithTimeout).
func WithPriority(ctx context.Context, p Priority) context.Context {
	return context.WithValue(ctx, priorityCtxKey{}, p)
}

func priorityFrom(ctx context.Context) Priority {
	if ctx == nil {
		return PriorityNormal
	}
	if p, ok := ctx.Value(priorityCtxKey{}).(Priority); ok {
		return p
	}
	return PriorityNormal
}
