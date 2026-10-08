package reader

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// A fetch's time is reported in four parts that add up to its total, so a
// slow one shows whether the reader's slots, the connection, the news server
// or the scratch-cache write held it.
func TestFetchPhasesSplitTheTotal(t *testing.T) {
	p := fetchPhases{
		slotWait: 100 * time.Millisecond,
		exec:     3 * time.Second,
		onConn:   2900 * time.Millisecond,
		store:    2800 * time.Millisecond,
	}
	if got, want := p.total(), 3100*time.Millisecond; got != want {
		t.Fatalf("total = %v, want %v", got, want)
	}
	if got, want := p.connect(), 100*time.Millisecond; got != want {
		t.Fatalf("connect = %v, want %v", got, want)
	}
	if got, want := p.server(), 100*time.Millisecond; got != want {
		t.Fatalf("server = %v, want %v", got, want)
	}
	if sum := p.slotWait + p.connect() + p.server() + p.store; sum != p.total() {
		t.Fatalf("parts sum to %v, total is %v", sum, p.total())
	}
}

type slowWriter struct{ delay time.Duration }

func (w slowWriter) Write(p []byte) (int, error) {
	time.Sleep(w.delay)
	return len(p), nil
}

func TestTimedWriterAddsUpTimeInTheWriter(t *testing.T) {
	tw := &timedWriter{w: slowWriter{delay: 20 * time.Millisecond}}
	for range 2 {
		if n, err := tw.Write([]byte("abc")); n != 3 || err != nil {
			t.Fatalf("Write = %d, %v", n, err)
		}
	}
	if tw.spent < 40*time.Millisecond {
		t.Fatalf("spent = %v, want at least 40ms", tw.spent)
	}
}

// One line per interval, carrying how many slow fetches went unlogged in
// between: a burst with every segment slow must not log several a second.
func TestSlowFetchLogSpacesLinesAndCountsTheSkipped(t *testing.T) {
	var l slowFetchLog
	start := time.Unix(1_000_000, 0)
	if skipped, ok := l.allow(start, 5*time.Second); !ok || skipped != 0 {
		t.Fatalf("first = %d, %v; want logged with none skipped", skipped, ok)
	}
	for i := 1; i <= 3; i++ {
		if _, ok := l.allow(start.Add(time.Duration(i)*time.Second), 5*time.Second); ok {
			t.Fatalf("fetch %d s after a line was logged again", i)
		}
	}
	if skipped, ok := l.allow(start.Add(5*time.Second), 5*time.Second); !ok || skipped != 3 {
		t.Fatalf("after the interval = %d, %v; want logged with 3 skipped", skipped, ok)
	}
}

func TestNoteSlowFetchLogsOnlyPastTheThreshold(t *testing.T) {
	var buf bytes.Buffer
	sf := &SegmentFetcher{logger: zerolog.New(&buf)}

	sf.noteSlowFetch(context.Background(), 7, fetchPhases{exec: slowFetchLogThreshold - time.Millisecond}, nil)
	if buf.Len() != 0 {
		t.Fatalf("a fetch under the threshold was logged: %s", buf.String())
	}

	p := fetchPhases{slotWait: time.Second, exec: 3 * time.Second, onConn: 2500 * time.Millisecond, store: 2 * time.Second}
	sf.noteSlowFetch(ContextForBufferedPlayback(ContextForPlayback(context.Background())), 7, p, errors.New("boom"))
	line := buf.String()
	for _, want := range []string{
		`"message":"slow segment fetch"`,
		`"segment":7`,
		`"read":"mount read-ahead"`,
		`"total":4000`,
		`"slot_wait":1000`,
		`"connect":500`,
		`"server":500`,
		`"store":2000`,
		`"error":"boom"`,
	} {
		if !strings.Contains(line, want) {
			t.Fatalf("line lacks %s: %s", want, line)
		}
	}
}
