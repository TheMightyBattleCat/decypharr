package qbit

import (
	"net/http"
	"testing"
	"time"
)

func useSIDClock(t *testing.T, start time.Time) *time.Time {
	t.Helper()
	now := start
	old := sidNow
	sidNow = func() time.Time { return now }
	sids.reset()
	t.Cleanup(func() {
		sidNow = old
		sids.reset()
	})
	return &now
}

func TestQBitSIDExpiresAfterADayIdle(t *testing.T) {
	_, routes := newSIDTestQBit(t)
	now := useSIDClock(t, time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC))
	cookie := sidLogin(t, routes, sidTestUser, sidTestPassword)

	// Use keeps the session alive past a day from the login.
	*now = now.Add(23 * time.Hour)
	if code := sidGet(routes, "SID", cookie.Value); code != http.StatusOK {
		t.Fatalf("after 23 h: status = %d, want 200", code)
	}
	*now = now.Add(23 * time.Hour)
	if code := sidGet(routes, "SID", cookie.Value); code != http.StatusOK {
		t.Fatalf("23 h after the last use: status = %d, want 200", code)
	}
	*now = now.Add(sidIdleExpiry + time.Second)
	if code := sidGet(routes, "SID", cookie.Value); code != http.StatusForbidden {
		t.Fatalf("a day idle: status = %d, want 403", code)
	}
	// An expired session is gone for good, even if the clock went back.
	*now = now.Add(-2 * sidIdleExpiry)
	if code := sidGet(routes, "SID", cookie.Value); code != http.StatusForbidden {
		t.Fatalf("expired SID reused: status = %d, want 403", code)
	}
}

func TestQBitSIDStoreDropsTheOldestAtTheCap(t *testing.T) {
	newAuthenticationTestQBit(t)
	now := useSIDClock(t, time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC))
	first, err := sids.issue("u", "p")
	if err != nil {
		t.Fatal(err)
	}
	*now = now.Add(time.Second)
	second, _ := sids.issue("u", "p")
	for i := 0; i < sidMaxSessions-2; i++ {
		*now = now.Add(time.Second)
		if _, err := sids.issue("u", "p"); err != nil {
			t.Fatal(err)
		}
	}
	// The first session is used, so the second is now the oldest.
	*now = now.Add(time.Second)
	if _, _, ok := sids.lookup(first); !ok {
		t.Fatal("first session missing before the cap")
	}
	*now = now.Add(time.Second)
	if _, err := sids.issue("u", "p"); err != nil {
		t.Fatal(err)
	}
	if n := len(sids.sessions); n != sidMaxSessions {
		t.Fatalf("sessions = %d, want %d", n, sidMaxSessions)
	}
	if _, _, ok := sids.lookup(second); ok {
		t.Fatal("least recently used session kept past the cap")
	}
	if _, _, ok := sids.lookup(first); !ok {
		t.Fatal("recently used session dropped at the cap")
	}
}
