package manager

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/overlay"
)

// A partial outage: provider 1 (127.0.0.1) holds every article, provider 2
// (localhost) lacks one article of the file. While provider 1 refuses
// connections, the sweep's STAT check passes (provider 2 has the sampled
// articles) and ffprobe's verification read reaches the missing article:
// provider 2 says 430 and provider 1 is never asked. That 430 is no evidence
// the article is gone, so the read must fail as a plain read failure (which
// checkConfirmed ends inconclusive after two), not trip the dead-segment
// signal that makes it a broken verdict the sweep heals. When both providers
// answer 430 the signal still trips, and a playback read in the partial case
// still pads as before.
func TestPartialOutageVerificationReadIsNotADeadSegment(t *testing.T) {
	for _, tc := range []struct {
		name          string
		refuseFirst   bool // provider 1 refuses connections
		firstAlso430  bool // provider 1 lacks the article too
		wantDetected  bool
		wantRead430   bool
		wantReadError bool
	}{
		{name: "control_both_up", wantDetected: false},
		{name: "first_provider_refuses", refuseFirst: true, wantDetected: false, wantRead430: false, wantReadError: true},
		{name: "control_both_430", firstAlso430: true, wantDetected: true, wantRead430: true, wantReadError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			servers, fx := newP2eEnv(t, 2)
			h := newP2eHarness(t, servers[0], fx, "partial-outage-"+tc.name)
			servers[1].reset()
			f := fx.posted[0]
			k := 2
			servers[1].set(f.ids[k], p2eNotFound)
			if tc.firstAlso430 {
				servers[0].set(f.ids[k], p2eNotFound)
			}
			if tc.refuseFirst {
				servers[0].refuse.Store(true)
				servers[0].dropConns()
			}

			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()

			statErr := h.u.CheckFile(ctx, h.nzbID, f.name)

			sig := usenet.NewDeadSegmentSignal()
			vctx := usenet.ContextWithDeadSignal(usenet.ContextForVerificationRead(ctx), sig)
			var buf bytes.Buffer
			readErr := h.u.Stream(vctx, h.nzbID, f.name, 0, int64(len(f.data))-1, &buf)

			t.Logf("%s: CheckFile=%v read_err=%v (430=%v) read=%d/%d dead_signal=%v srv1=%+v srv2=%+v",
				tc.name, statErr, readErr, nntp.IsArticleNotFoundError(readErr), buf.Len(), len(f.data), sig.Detected(),
				servers[0].counts(), servers[1].counts())

			if !tc.firstAlso430 && statErr != nil {
				t.Errorf("CheckFile = %v, want nil (the sweep's STAT step passes, so ffprobe reads the file)", statErr)
			}
			if sig.Detected() != tc.wantDetected {
				t.Errorf("dead signal detected=%v, want %v", sig.Detected(), tc.wantDetected)
			}
			if (readErr != nil) != tc.wantReadError {
				t.Errorf("verification read err = %v, want an error: %v", readErr, tc.wantReadError)
			}
			if got := nntp.IsArticleNotFoundError(readErr); got != tc.wantRead430 {
				t.Errorf("verification read error is a 430 = %v, want %v (err %v)", got, tc.wantRead430, readErr)
			}
			if pending := pendingFor(t, h, f.name); pending != 0 {
				t.Errorf("verification read recorded %d dead segments in the overlay, want 0", pending)
			}
		})
	}
}

// Control for the change above: a playback read in the same partial outage
// still gets the 430 and offers the segment to the overlay's pad decision,
// as before (only the verification read and the sweep latch treat a
// partial 430 differently). This fixture's file is too small for the pad
// caps (one of its segments is more than 10% of it), so the overlay
// declines the pad and records the file failed; what matters is that the
// playback path reached it.
func TestPartialOutagePlaybackReadReachesThePadDecision(t *testing.T) {
	servers, fx := newP2eEnv(t, 2)
	h := newP2eHarness(t, servers[0], fx, "partial-outage-playback")
	servers[1].reset()
	f := fx.posted[0]
	servers[1].set(f.ids[2], p2eNotFound)
	servers[0].refuse.Store(true)
	servers[0].dropConns()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	var buf bytes.Buffer
	err := h.u.Stream(ctx, h.nzbID, f.name, 0, int64(len(f.data))-1, &buf)
	verdict := h.u.OverlayVerdict(h.nzbID, f.name)
	t.Logf("playback read: err=%v (430=%v partial=%v) read=%d/%d verdict=%v pending=%d",
		err, nntp.IsArticleNotFoundError(err), nntp.IsPartialNotFound(err), buf.Len(), len(f.data), verdict, pendingFor(t, h, f.name))
	if !nntp.IsArticleNotFoundError(err) {
		t.Errorf("playback read err = %v, want the 430 playback pads on", err)
	}
	if verdict == overlay.VerdictClean {
		t.Errorf("overlay verdict %v: the playback read never reached the pad decision", verdict)
	}
	if pending := pendingFor(t, h, f.name); pending != 1 {
		t.Errorf("overlay holds %d dead segments after the playback read, want 1 (recorded as before)", pending)
	}
}
