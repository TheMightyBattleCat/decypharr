package nntp

// A message ID reaches the command line as given. One holding CR LF sends a
// second command; the server answers both, and the connection goes back to
// the pool one reply behind, so each later user of it reads the reply to the
// request before it. These tests cover the guard that refuses such an ID
// before writing, and the reply checks that close a connection that is out
// of step anyway (an echoed message ID other than asked, or a status the
// command cannot return).

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

func messageIDPayload(seed byte) []byte {
	data := make([]byte, 64*1024)
	for i := range data {
		data[i] = byte(i*7+i/300) ^ seed
	}
	return data
}

// oneConnClient is a client on one provider with one connection, so every
// request shares the connection an earlier one left behind. retries lets
// failover ask again on a fresh connection after one is closed.
func oneConnClient(t *testing.T, s *fakeNNTP) *Client {
	t.Helper()
	p := config.UsenetProvider{Host: "127.0.0.1", Port: s.port(), Priority: 1, MaxConnections: 1}
	c := newStatTestClient(t, []config.UsenetProvider{p}, 100)
	c.retries = 2
	return c
}

// An ID holding CR LF is refused before anything is written: the server sees
// neither the STAT nor the injected DATE, the error is not a missing article,
// and the connection stays in step, so a present article still reads present.
func TestStatRefusesLineBreakInMessageID(t *testing.T) {
	s := startFakeNNTP(t, 0, "gone@test")
	c := oneConnClient(t, s)
	ctx := context.Background()

	_, _, err := c.Stat(ctx, "a@test>\r\nDATE")
	if !errors.Is(err, errBadCommandArg) {
		t.Fatalf("STAT with a line break in the ID: err = %v, want errBadCommandArg", err)
	}
	if IsArticleNotFoundError(err) {
		t.Fatalf("refused ID read as a missing article: %v", err)
	}

	if _, _, err := c.Stat(ctx, "gone@test"); !IsArticleNotFoundError(err) {
		t.Fatalf("STAT gone@test (missing): err = %v, want a 430", err)
	}
	if _, echoed, err := c.Stat(ctx, "a@test"); err != nil {
		t.Fatalf("STAT a@test (present) after the refused ID: echoed %q, err = %v", echoed, err)
	}
	if n := s.stats.Load(); n != 2 {
		t.Errorf("server saw %d STAT commands, want 2 (the refused one never sent)", n)
	}
	if n := s.dates.Load(); n != 0 {
		t.Errorf("server saw %d DATE commands: the injected line was sent", n)
	}
	if n := s.accepts.Load(); n != 1 {
		t.Errorf("server accepted %d connections, want 1: a refused ID costs no connection", n)
	}
}

// A pipelined window holding one bad ID is refused whole, with nothing
// written or left buffered to go out ahead of the next command.
func TestStatBatchRefusesLineBreakBeforeWriting(t *testing.T) {
	c, server := newBodyTestConn(t)
	got := make(chan int, 1)
	go func() {
		buf := make([]byte, 4096)
		n, _ := server.Read(buf)
		got <- n
	}()

	type out struct {
		results []StatResult
		err     error
	}
	done := make(chan out, 1)
	go func() {
		r, _, err := c.StatBatch([]string{"ok@test", "a@test>\r\nDATE", "next@test"})
		done <- out{r, err}
	}()
	var res out
	select {
	case res = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("StatBatch wrote the window and waited for replies")
	}
	if !errors.Is(res.err, errBadCommandArg) {
		t.Fatalf("err = %v, want errBadCommandArg", res.err)
	}
	for i, r := range res.results {
		if r.Available || IsArticleNotFoundError(r.Error) || !errors.Is(r.Error, errBadCommandArg) {
			t.Errorf("result %d = %+v, want not available, refused, not a 430", i, r)
		}
	}
	if n := c.writer.Buffered(); n != 0 {
		t.Errorf("%d bytes left in the write buffer", n)
	}
	select {
	case n := <-got:
		t.Errorf("server received %d bytes", n)
	case <-time.After(100 * time.Millisecond):
	}
	if c.IsClosed() {
		t.Error("connection closed: nothing was written, it should stay usable")
	}
}

// A BODY reply that echoes another article's ID is another article's body,
// which passes its own CRC and size. The connection is closed and the read
// asked again, so b@test returns b@test's bytes, not a@test's.
func TestBodyReplyForAnotherArticleClosesConnection(t *testing.T) {
	skipWithoutCRCDecoder(t)
	dataA, dataB := messageIDPayload(0x00), messageIDPayload(0x5a)
	s := startFakeNNTP(t, 0)
	s.bodies = map[string]string{"a@test": yencWire(dataA, 0), "b@test": yencWire(dataB, 0)}
	// a@test's body twice: the second copy is what the next BODY reads.
	s.extraReply = map[string]string{"a@test": "222 0 <a@test>\r\n" + yencWire(dataA, 0) + ".\r\n"}
	c := oneConnClient(t, s)

	var out bytes.Buffer
	if err := fetchBody(c, "a@test", &out); err != nil || !bytes.Equal(out.Bytes(), dataA) {
		t.Fatalf("read a@test: err = %v, a@test's bytes: %v", err, bytes.Equal(out.Bytes(), dataA))
	}
	err := fetchBody(c, "b@test", &out)
	if err != nil {
		t.Fatalf("read b@test: %v", err)
	}
	if bytes.Equal(out.Bytes(), dataA) {
		t.Fatal("read b@test returned a@test's bytes: another article served as good")
	}
	if !bytes.Equal(out.Bytes(), dataB) {
		t.Fatalf("read b@test returned %d bytes that are not b@test's", out.Len())
	}
	if n := s.accepts.Load(); n != 2 {
		t.Errorf("server accepted %d connections, want 2 (the out-of-step one closed)", n)
	}
}

// A STAT reply that echoes another article's ID is taken for no article: a
// missing article still reads missing, and the connection is replaced.
func TestStatReplyForAnotherArticleClosesConnection(t *testing.T) {
	s := startFakeNNTP(t, 0, "gone@test")
	s.extraLine = map[string]bool{"a@test": true} // "223 0 <stray@test>" after a@test's reply
	c := oneConnClient(t, s)
	ctx := context.Background()

	if _, _, err := c.Stat(ctx, "a@test"); err != nil {
		t.Fatalf("STAT a@test: %v", err)
	}
	if _, echoed, err := c.Stat(ctx, "gone@test"); !IsArticleNotFoundError(err) {
		t.Fatalf("STAT gone@test (missing): echoed %q, err = %v; want a 430, not the stray reply", echoed, err)
	}
	if _, _, err := c.Stat(ctx, "a@test"); err != nil {
		t.Fatalf("STAT a@test after the stray reply: %v", err)
	}
	if n := s.accepts.Load(); n != 2 {
		t.Errorf("server accepted %d connections, want 2 (the out-of-step one closed)", n)
	}
}

// A status the command cannot return (a 111 or 500 meant for some other
// command) closes the connection instead of pooling it one reply behind; the
// read is asked again and gets its own reply.
func TestStrayStatusClosesConnection(t *testing.T) {
	skipWithoutCRCDecoder(t)
	dataB := messageIDPayload(0x5a)

	t.Run("111 read by BODY", func(t *testing.T) {
		s := startFakeNNTP(t, 0)
		s.bodies = map[string]string{"b@test": yencWire(dataB, 0)}
		s.extraReply = map[string]string{"a@test": "111 20261120120000\r\n"}
		c := oneConnClient(t, s)
		if _, _, err := c.Stat(context.Background(), "a@test"); err != nil {
			t.Fatalf("STAT a@test: %v", err)
		}
		var out bytes.Buffer
		if err := fetchBody(c, "b@test", &out); err != nil || !bytes.Equal(out.Bytes(), dataB) {
			t.Fatalf("read b@test: err = %v, b@test's bytes: %v", err, bytes.Equal(out.Bytes(), dataB))
		}
		if n := s.accepts.Load(); n != 2 {
			t.Errorf("server accepted %d connections, want 2", n)
		}
	})

	t.Run("500 read by STAT", func(t *testing.T) {
		s := startFakeNNTP(t, 0)
		s.extraReply = map[string]string{"x@test": "500 unknown command\r\n"}
		c := oneConnClient(t, s)
		if _, _, err := c.Stat(context.Background(), "x@test"); err != nil {
			t.Fatalf("STAT x@test: %v", err)
		}
		if _, echoed, err := c.Stat(context.Background(), "a@test"); err != nil || echoed != "<a@test>" {
			t.Fatalf("STAT a@test after the stray 500: echoed %q, err = %v", echoed, err)
		}
		if n := s.accepts.Load(); n != 2 {
			t.Errorf("server accepted %d connections, want 2", n)
		}
	})
}

// Controls: a reply that echoes no ID, or the asked ID in another case, is
// accepted, and a 430 leaves the connection open for the pool.
func TestBodyReplyEchoChecksKeepNormalReplies(t *testing.T) {
	skipWithoutCRCDecoder(t)
	c, server := newBodyTestConn(t)
	payload := bodyPayload(16 * 1024)
	serveResponses(t, server,
		"222 body follows\r\n"+encodeBody(payload)+".\r\n",
		"222 0 <A@B> body\r\n"+encodeBody(payload)+".\r\n",
		"430 no such article\r\n",
	)
	for i, id := range []string{"<a@b>", "<a@b>"} {
		res, err := c.requestBody(id, timeouts.StreamBodyTimeout)
		if err != nil || !bytes.Equal(res.Data, payload) {
			t.Fatalf("reply %d: err = %v", i+1, err)
		}
		putBodyBuf(res.Data)
	}
	if _, err := c.requestBody("<gone@b>", timeouts.StreamBodyTimeout); !IsArticleNotFoundError(err) {
		t.Fatalf("430: err = %v", err)
	}
	if c.IsClosed() {
		t.Error("connection closed after normal replies")
	}
}

func TestReplyForOtherArticle(t *testing.T) {
	for _, tc := range []struct {
		message string
		other   bool
	}{
		{"222 0 <a@test> body", false},
		{"0 <a@test>", false},
		{"0 <A@TEST>", false},
		{"222 0 body", false},
		{"0 0", false},
		{"222 0 <b@test> body", true},
		{"0 <stray@test>", true},
	} {
		if _, other := replyForOtherArticle("<a@test>", tc.message); other != tc.other {
			t.Errorf("%q: other = %v, want %v", tc.message, other, tc.other)
		}
	}
}
