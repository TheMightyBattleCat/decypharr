package nntp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	nntpyenc "github.com/sirrobot01/decypharr/internal/nntp/yenc"
	"github.com/sirrobot01/decypharr/internal/utils"
)

// Note: Timeout values are defined in TimeoutConfig (client.go).
// Use timeouts.StreamBodyTimeout for read deadlines.

// bodyBufPool supplies output buffers for whole-article yEnc decodes. 1 MB
// covers the decoder's expected-size estimate for typical ~750 KB articles,
// so a decode never regrows mid-article. Buffers handed to callers that keep
// them (GetDecodedBody) escape; the pool refills through New.
var bodyBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 1<<20)
		return &b
	},
}

func getBodyBuf() []byte { return *bodyBufPool.Get().(*[]byte) }

func putBodyBuf(b []byte) {
	if cap(b) == 0 {
		return
	}
	b = b[:0]
	bodyBufPool.Put(&b)
}

// progressUpdateStride throttles the per-Read clock cost: bodyReader bumps a
// counter on every source read and only reads the clock every stride'th one
// (a clock read per Read once showed up as 19% of process CPU). The decoder
// pulls up to 32 KB per read, so progress is at most ~128 KB stale, far
// below any idle deadline.
const progressUpdateStride = 4

// shortIdleLimit is the idle deadline below which a body decode keeps a real
// socket read deadline instead of relying on the janitor, whose 5 s sweep
// would otherwise more than double a short, latency-sensitive timeout (the
// parser's PAR2 size probe uses 5 s).
const shortIdleLimit = 2 * bodyJanitorInterval

// bodyReader is the reader a connection's yEnc decoder holds for the life of
// the connection. It follows c.reader, which a STARTTLS upgrade replaces,
// and records read progress for the idle janitor.
type bodyReader struct {
	c     *Connection
	reads uint8
}

// respTerminator is the multiline reply terminator "\r\n.\r\n", packed the
// way Connection.bodyTail packs the newest bytes (last byte lowest).
const (
	respTerminator     = 0x0d0a2e0d0a
	respTerminatorMask = 1<<40 - 1
)

// errBodyNotConsumed marks a body the decoder did not read to its end: it
// panicked, or it stopped consuming before the terminator. It wraps
// ErrDataCorruption, so failover treats the copy as corrupt and asks the
// next provider. Unlike rapidyenc's own corrupt-article errors, part of the
// reply is still in the decoder's buffer, so requestBody closes the
// connection.
var errBodyNotConsumed = fmt.Errorf("yEnc decoder did not read the reply to its end: %w", nntpyenc.ErrDataCorruption)

func (b *bodyReader) Read(p []byte) (int, error) {
	if b.c.bodyTail&respTerminatorMask == respTerminator {
		// The reply's terminator has been handed over and the decoder
		// still wants more: it stopped consuming (rapidyenc does this in
		// uuencode mode past its first read window). The server has
		// nothing more to send, so a read would only wait out the idle
		// deadline.
		return 0, fmt.Errorf("%w: it asked for more after the terminator", errBodyNotConsumed)
	}
	n, err := b.c.reader.Read(p)
	if n > 0 {
		for _, ch := range p[max(0, n-5):n] {
			b.c.bodyTail = b.c.bodyTail<<8 | uint64(ch)
		}
		if b.c.firstReadNS == 0 {
			b.c.firstReadNS = nanotimeNow()
		}
		b.reads++
		if b.reads >= progressUpdateStride {
			b.reads = 0
			b.c.lastProgressNS.Store(nanotimeNow())
			if d := b.c.shortIdle; d > 0 {
				_ = b.c.conn.SetReadDeadline(time.Now().Add(d))
			}
		}
	}
	return n, err
}

// decoder returns the connection's yEnc body decoder, creating it on first
// use. A connection is never used concurrently, so no locking is needed.
func (c *Connection) decoder() *nntpyenc.BodyDecoder {
	if c.bodyDec == nil {
		c.bodyDec = nntpyenc.NewBodyDecoder(&bodyReader{c: c}, getBodyBuf)
	}
	return c.bodyDec
}

// nextBodyWithIdleDeadline decodes one complete NNTP response, status line
// included, with an idle deadline: a stall (no bytes arriving for `idle`)
// closes the connection so the decoder's in-flight Read unblocks with an
// error.
//
// History: the first design called SetReadDeadline per Read (pollSetDeadline
// ~41% CPU in profile). The replacement used a time.AfterFunc + Timer.Reset
// per Read, which also dominated CPU (Timer.Reset ~27% of total, scheduler
// timer-heap scans another ~13%; see production profile 2026-05-31). Both
// shared the same root cause: a runtime-managed timer entry per active body
// copy, hammered with ops on every bufio Read across 100+ concurrent streams.
//
// Current design: one process-wide janitor goroutine sweeps connections in a
// body decode every few seconds and closes anything past its deadline.
// bodyReader periodically refreshes an atomic monotonic timestamp rather than
// touching a runtime timer on every read. Stall detection latency becomes
// "idle + janitor interval"; for a 60s idle that is acceptable. A deadline
// under shortIdleLimit instead keeps a socket read deadline, pushed forward
// every progressUpdateStride reads.
func (c *Connection) nextBodyWithIdleDeadline(idle time.Duration) (nntpyenc.BodyResult, error) {
	if idle <= 0 {
		idle = 60 * time.Second
	}
	c.lastProgressNS.Store(nanotimeNow())
	if idle < shortIdleLimit {
		c.shortIdle = idle
		_ = c.conn.SetReadDeadline(time.Now().Add(idle))
		defer func() {
			c.shortIdle = 0
			_ = c.conn.SetReadDeadline(time.Time{})
		}()
	} else {
		// Disable any deadline carried in from earlier on this connection
		// and arm the janitor. idleNS=0 on exit tells it to skip.
		_ = c.conn.SetReadDeadline(time.Time{})
		c.idleNS.Store(int64(idle))
		bodyIdleJanitor.add(c)
		defer func() {
			bodyIdleJanitor.remove(c)
			c.idleNS.Store(0)
		}()
	}

	c.bodyTail = 0
	res, err := c.decodeNext()
	if err != nil && !nntpyenc.IsCorruptArticle(err) {
		// The janitor sets idleNS to 0 after closing a stalled conn, but
		// the race-free signal is "did we make progress within the
		// deadline?". If not, format as a stall error.
		if nanotimeNow()-c.lastProgressNS.Load() > int64(idle) {
			err = fmt.Errorf("stream idle for %s: %w", idle, err)
		}
	}
	return res, err
}

// decodeNext runs the decoder on one reply. A panic inside it (rapidyenc's
// format sniffing slices out of range on some lines before "=ybegin") is
// returned as errBodyNotConsumed: the article is corrupt and the rest of
// the reply is still buffered.
func (c *Connection) decodeNext() (res nntpyenc.BodyResult, err error) {
	defer func() {
		if p := recover(); p != nil {
			res, err = nntpyenc.BodyResult{}, fmt.Errorf("%w: decoder panic: %v", errBodyNotConsumed, p)
		}
	}()
	return c.decoder().Next()
}

// nanotimeNow returns the monotonic clock in nanoseconds. Uses time.Now's
// monotonic reading via Sub(zero): one runtime.nanotime call, no wall-clock
// overhead, no allocation.
var nanotimeEpoch = time.Now()

func nanotimeNow() int64 {
	return int64(time.Since(nanotimeEpoch))
}

// bodyIdleJanitor sweeps connections currently in nextBodyWithIdleDeadline
// and closes any whose last-progress timestamp is older than their idle
// deadline. One goroutine per process, started lazily on first add().
var bodyIdleJanitor = newBodyJanitor()

const bodyJanitorInterval = 5 * time.Second

type bodyJanitor struct {
	mu      sync.Mutex
	conns   map[*Connection]struct{}
	started atomic.Bool
}

func newBodyJanitor() *bodyJanitor {
	return &bodyJanitor{conns: make(map[*Connection]struct{})}
}

func (j *bodyJanitor) ensureRunning() {
	if !j.started.CompareAndSwap(false, true) {
		return
	}
	go j.run()
}

func (j *bodyJanitor) add(c *Connection) {
	j.ensureRunning()
	j.mu.Lock()
	j.conns[c] = struct{}{}
	j.mu.Unlock()
}

func (j *bodyJanitor) remove(c *Connection) {
	j.mu.Lock()
	delete(j.conns, c)
	j.mu.Unlock()
}

func (j *bodyJanitor) run() {
	tick := time.NewTicker(bodyJanitorInterval)
	defer tick.Stop()
	// Snapshot under the lock and act outside it so a slow Close() can't
	// hold up other registrations.
	var stalled []*Connection
	for range tick.C {
		now := nanotimeNow()
		stalled = stalled[:0]
		j.mu.Lock()
		for c := range j.conns {
			idle := c.idleNS.Load()
			if idle <= 0 {
				continue
			}
			if now-c.lastProgressNS.Load() > idle {
				stalled = append(stalled, c)
			}
		}
		j.mu.Unlock()
		for _, c := range stalled {
			_ = c.conn.Close() // unblocks the in-flight Read
		}
	}
}

func (c *Connection) readResponseWithDeadline(timeout time.Duration) (Response, error) {
	if timeout <= 0 {
		timeout = timeouts.StreamBodyTimeout
	}
	_ = c.conn.SetReadDeadline(utils.Now().Add(timeout))
	defer func() { _ = c.conn.SetReadDeadline(time.Time{}) }()
	return c.readResponse()
}

func (c *Connection) readResponseCodeWithDeadline(timeout time.Duration) (int, []byte, error) {
	if timeout <= 0 {
		timeout = timeouts.StreamBodyTimeout
	}
	_ = c.conn.SetReadDeadline(utils.Now().Add(timeout))
	defer func() { _ = c.conn.SetReadDeadline(time.Time{}) }()
	return c.readResponseCode()
}

// Connection represents an NNTP connection
type Connection struct {
	username, password, address string
	port                        int
	conn                        net.Conn
	text                        *textproto.Reader
	reader                      *bufio.Reader
	writer                      *bufio.Writer
	logger                      zerolog.Logger
	closed                      atomic.Bool

	// bodyDec decodes complete BODY responses, reading through bodyReader.
	// Created on first use (decoder); it keeps a reusable 32 KB read
	// buffer. Safe only because commands and responses strictly alternate:
	// the decoder never reads past the current response's terminator.
	bodyDec *nntpyenc.BodyDecoder
	// bodyTail holds the last bytes bodyReader handed the decoder for the
	// current reply, newest in the lowest byte; nextBodyWithIdleDeadline
	// clears it before each reply.
	bodyTail uint64

	// Body-decode idle tracking. lastProgressNS is refreshed by bodyReader
	// while reads make progress; idleNS is armed by nextBodyWithIdleDeadline
	// and read by the shared janitor goroutine when sweeping for stalls.
	// Stored in monotonic nanoseconds (nanotimeNow). idleNS 0 means this
	// connection isn't in a janitor-watched decode and the janitor should
	// skip it. shortIdle is the idle deadline of a decode that keeps a
	// socket read deadline instead (see shortIdleLimit); 0 otherwise.
	lastProgressNS atomic.Int64
	idleNS         atomic.Int64
	shortIdle      time.Duration

	// Fetch timing (fetch_timing.go). timing is the owning pool's recorder,
	// nil outside a client pool. firstReadNS is when the current body's
	// first byte arrived, checkedOutNS and releasedNS when the connection
	// last left and went back to its pool; all nanotimeNow, 0 when unset.
	timing       *providerTiming
	firstReadNS  int64
	checkedOutNS int64
	releasedNS   int64

	// onBody receives each successful article body's decoded size and how
	// long it took from sending BODY to the end of the body, for the owning
	// client's body routing (body_routing.go). Nil outside a client pool.
	onBody func(n int64, d time.Duration)
}

func (c *Connection) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	return c.conn.Close()
}

func (c *Connection) IsClosed() bool {
	return c.closed.Load()
}

func (c *Connection) authenticate() error {
	// Send AUTHINFO USER command
	if err := c.sendCommandArg("AUTHINFO USER", c.username); err != nil {
		return NewConnectionError(fmt.Errorf("failed to send username: %w", err))
	}

	resp, err := c.readResponse()
	if err != nil {
		return NewConnectionError(fmt.Errorf("failed to read user response: %w", err))
	}

	if resp.Code != 381 {
		return classifyNNTPError(resp.Code, fmt.Sprintf("unexpected response to AUTHINFO USER: %s", resp.Message))
	}

	// Send AUTHINFO PASS command
	if err := c.sendCommandArg("AUTHINFO PASS", c.password); err != nil {
		return NewConnectionError(fmt.Errorf("failed to send password: %w", err))
	}

	resp, err = c.readResponse()
	if err != nil {
		return NewConnectionError(fmt.Errorf("failed to read password response: %w", err))
	}

	if resp.Code != 281 {
		return classifyNNTPError(resp.Code, fmt.Sprintf("[%s] authentication failed: %s", c.address, resp.Message))
	}
	return nil
}

// startTLS initiates TLS encryption with proper error handling
func (c *Connection) startTLS() error {
	if err := c.sendCommand("STARTTLS"); err != nil {
		return NewConnectionError(fmt.Errorf("failed to send STARTTLS: %w", err))
	}

	resp, err := c.readResponse()
	if err != nil {
		return NewConnectionError(fmt.Errorf("failed to read STARTTLS response: %w", err))
	}

	if resp.Code != 382 {
		return classifyNNTPError(resp.Code, fmt.Sprintf("STARTTLS not supported: %s", resp.Message))
	}

	// Upgrade connection to TLS
	tlsConn := tls.Client(c.conn, &tls.Config{
		ServerName:         c.address,
		InsecureSkipVerify: true, // Match createConnection behavior
		MinVersion:         tls.VersionTLS12,
	})

	c.conn = tlsConn
	c.reader = bufio.NewReaderSize(tlsConn, 256*1024)
	c.writer = bufio.NewWriterSize(tlsConn, 256*1024)
	c.text = textproto.NewReader(c.reader)

	c.logger.Debug().Msg("TLS encryption enabled")
	return nil
}

// ping sends a simple command to test the connection
func (c *Connection) ping() error {
	if c.conn == nil {
		return NewConnectionError(errors.New("connection is nil"))
	}
	_ = c.conn.SetDeadline(utils.Now().Add(timeouts.PingTimeout))
	defer func() { _ = c.conn.SetDeadline(time.Time{}) }()

	// Write without sendCommandArg: it would swap the PingTimeout write
	// deadline for HandshakeTimeout, so a peer that stopped reading held
	// this 1.5 s check for 10 s (upstream cabeb8a3).
	if err := c.writeCommandArg("DATE", ""); err != nil {
		return NewConnectionError(err)
	}
	if err := c.writer.Flush(); err != nil {
		return NewConnectionError(err)
	}
	resp, err := c.readResponse()
	if err != nil {
		return NewConnectionError(err)
	}
	if resp.Code != 111 {
		return NewConnectionError(fmt.Errorf("unexpected DATE response: %d %s", resp.Code, resp.Message))
	}
	return nil
}

// sendCommand sends a command to the NNTP server
func (c *Connection) sendCommand(command string) error {
	return c.sendCommandArg(command, "")
}

func (c *Connection) sendCommandArg(command, arg string) error {
	_ = c.conn.SetWriteDeadline(utils.Now().Add(timeouts.HandshakeTimeout))
	defer func() { _ = c.conn.SetWriteDeadline(time.Time{}) }()

	if err := c.writeCommandArg(command, arg); err != nil {
		return err
	}
	return c.writer.Flush()
}

// errBadCommandArg refuses a command argument that would end the command
// line early. A message ID holding CR LF sends a second command; the server
// answers both, and the connection is left one reply behind for its next
// user. Nothing is written, so the connection stays in step and can be
// pooled. It is not an *Error: never read as a missing article or a dead
// connection, and failover returns it at once.
var errBadCommandArg = errors.New("command argument holds CR, LF or NUL; not sent")

// writeCommandArg buffers one command line without flushing it or touching
// the connection's deadlines. An argument holding CR, LF or NUL is refused
// before anything is buffered.
func (c *Connection) writeCommandArg(command, arg string) error {
	if strings.ContainsAny(arg, "\r\n\x00") {
		return fmt.Errorf("%s %q: %w", command, arg, errBadCommandArg)
	}
	if _, err := c.writer.WriteString(command); err != nil {
		return err
	}
	if arg != "" {
		if err := c.writer.WriteByte(' '); err != nil {
			return err
		}
		if _, err := c.writer.WriteString(arg); err != nil {
			return err
		}
	}
	_, err := c.writer.WriteString("\r\n")
	return err
}

// sendFailure wraps an error from sendCommandArg. A refused argument passes
// through as is (see errBadCommandArg); anything else is a connection error.
func sendFailure(what string, err error) error {
	if errors.Is(err, errBadCommandArg) {
		return err
	}
	return NewConnectionError(fmt.Errorf("%s: %w", what, err))
}

// echoedMessageID returns the message ID a 222/223 status line echoes: its
// first field in angle brackets. Empty when the line echoes none. message may
// start with the status code ("222 0 <id>") or not ("0 <id>").
func echoedMessageID(message string) string {
	for _, f := range strings.Fields(message) {
		if len(f) > 2 && f[0] == '<' && f[len(f)-1] == '>' {
			return f
		}
	}
	return ""
}

// replyForOtherArticle reports whether a 222/223 status line echoes a message
// ID other than asked (already through FormatMessageID). Such a reply belongs
// to an earlier command: the connection is out of step with its replies.
// Case is ignored so a server that folds case is not read as out of step.
func replyForOtherArticle(asked, message string) (echoed string, other bool) {
	echoed = echoedMessageID(message)
	return echoed, echoed != "" && !strings.EqualFold(echoed, asked)
}

// errReplyOutOfStep marks a reply that does not belong to the command just
// sent: an echoed message ID other than asked, or a status the command
// cannot return. The connection is closed, never pooled, and the error is a
// connection error so failover asks again on another connection.
var errReplyOutOfStep = errors.New("reply out of step with the command")

// strayReply reports whether code cannot be the reply to a BODY or STAT whose
// success code is success: any other code below 400 (a 111 or 2xx belongs to
// some other command), or 500, unknown command, which both mandatory
// commands never get. Every other 4xx/5xx is a reply either may receive.
func strayReply(code, success int) bool {
	return code != success && (code < 400 || code == 500)
}

// outOfStep closes the connection, logs why, and returns the connection
// error for a reply that does not belong to the command sent.
func (c *Connection) outOfStep(command, asked, detail string) error {
	_ = c.Close()
	c.logger.Warn().Str("provider", c.address).Str("command", command).Str("asked", asked).Str("reply", detail).
		Msg("NNTP reply does not belong to the command sent; closed the connection")
	return NewConnectionError(fmt.Errorf("%w: %s %s answered %q", errReplyOutOfStep, command, asked, detail))
}

// readResponse reads a response from the NNTP server
func (c *Connection) readResponse() (Response, error) {
	code, message, err := c.readResponseCode()
	if err != nil {
		return Response{}, err
	}

	return Response{
		Code:    code,
		Message: string(message),
	}, nil
}

// readResponseCode parses a short NNTP status line in-place from the connection
// buffer. Most BODY callers only need the code on success, so keeping the
// message as bytes avoids materializing a response string for every article.
func (c *Connection) readResponseCode() (int, []byte, error) {
	line, err := c.reader.ReadSlice('\n')
	if err != nil {
		return 0, nil, err
	}
	line = bytes.TrimSuffix(line, []byte{'\n'})
	line = bytes.TrimSuffix(line, []byte{'\r'})
	if len(line) < 3 ||
		line[0] < '0' || line[0] > '9' ||
		line[1] < '0' || line[1] > '9' ||
		line[2] < '0' || line[2] > '9' ||
		(len(line) > 3 && line[3] != ' ') {
		return 0, nil, fmt.Errorf("invalid response code: %s", line)
	}

	code := int(line[0]-'0')*100 + int(line[1]-'0')*10 + int(line[2]-'0')
	if len(line) == 3 {
		return code, nil, nil
	}
	return code, line[4:], nil
}

// readMultilineResponse reads a multiline response
func (c *Connection) readMultilineResponse() (*Response, error) {
	resp, err := c.readResponse()
	if err != nil {
		return nil, err
	}

	// Check if this is a multiline response
	if resp.Code < 200 || resp.Code >= 300 {
		return &resp, nil
	}

	lines, err := c.text.ReadDotLines()
	if err != nil {
		return nil, err
	}

	resp.Lines = lines
	return &resp, nil
}

// GetArticle retrieves an article by message ID with proper error classification
func (c *Connection) GetArticle(messageID string) (*Article, error) {
	messageID = FormatMessageID(messageID)
	if err := c.sendCommandArg("ARTICLE", messageID); err != nil {
		return nil, sendFailure("failed to send ARTICLE command", err)
	}

	resp, err := c.readMultilineResponse()
	if err != nil {
		return nil, NewConnectionError(fmt.Errorf("failed to read article response: %w", err))
	}

	if resp.Code != 220 {
		return nil, classifyNNTPError(resp.Code, resp.Message)
	}

	return c.parseArticle(messageID, resp.Lines)
}

// requestBody sends BODY and decodes the complete response through the
// connection's decoder: status line, yEnc payload and ".\r\n" terminator in
// one pass, with size and CRC checks. The returned Data buffer comes from
// bodyBufPool and the caller owns it. A failure that may have left part of
// the response unread closes the connection, since the decoder's buffer and
// the wire no longer agree on where the next response starts.
func (c *Connection) requestBody(messageID string, idle time.Duration) (nntpyenc.BodyResult, error) {
	messageID = FormatMessageID(messageID)
	sentNS := nanotimeNow()
	c.firstReadNS = 0
	if err := c.sendCommandArg("BODY", messageID); err != nil {
		return nntpyenc.BodyResult{}, sendFailure("failed to send BODY command", err)
	}

	res, err := c.nextBodyWithIdleDeadline(idle)
	if err != nil {
		putBodyBuf(res.Data)
		res.Data = nil
		if errors.Is(err, errBodyNotConsumed) {
			// Corrupt, but part of the reply is still in the decoder's
			// buffer: the connection can't serve another command.
			_ = c.Close()
			return res, NewYencDecodeError(fmt.Errorf("streaming yenc decode failed: %w", err))
		}
		if !nntpyenc.IsCorruptArticle(err) {
			// Not one of the checks run at the terminator: the response
			// may be half read.
			_ = c.Close()
		}
		if res.StatusCode == 0 {
			return res, NewConnectionError(fmt.Errorf("failed to read body response: %w", err))
		}
		return res, classifyTransferError("streaming yenc decode failed", err)
	}
	if res.StatusCode != 222 {
		putBodyBuf(res.Data)
		res.Data = nil
		if strayReply(res.StatusCode, 222) {
			return res, c.outOfStep("BODY", messageID, res.Message)
		}
		return res, classifyNNTPError(res.StatusCode, res.Message)
	}
	if _, other := replyForOtherArticle(messageID, res.Message); other {
		// Another article's body, already read in full: it passes its own
		// CRC and size, so only the echoed ID tells it apart.
		putBodyBuf(res.Data)
		res.Data = nil
		return res, c.outOfStep("BODY", messageID, res.Message)
	}
	if !res.Yenc {
		// No "=ybegin": rapidyenc decoded the body as uuencode, which it
		// checks for neither size nor CRC. The reply was read to its end,
		// but its bytes can't be trusted as the article.
		putBodyBuf(res.Data)
		res.Data = nil
		return res, NewYencDecodeError(fmt.Errorf("article body has no yEnc header: %w", nntpyenc.ErrDataMissing))
	}
	if c.timing != nil && c.firstReadNS > 0 {
		doneNS := nanotimeNow()
		c.timing.latency.observe(time.Duration(c.firstReadNS - sentNS))
		c.timing.transfer.observe(time.Duration(doneNS - c.firstReadNS))
	}
	return res, nil
}

func metadataFromResult(meta nntpyenc.DecoderMeta, snippet []byte) *YencMetadata {
	return &YencMetadata{
		Name:     meta.FileName,
		Size:     meta.FileSize,
		Part:     meta.PartNumber,
		Total:    meta.TotalParts,
		Offset:   meta.Offset,
		PartSize: meta.PartSize,
		Begin:    meta.Begin(),
		End:      meta.End(),
		Snippet:  snippet,
	}
}

// GetHeaderPrefix retrieves exact yEnc metadata plus a small decoded prefix.
// The whole article is read, so the connection stays reusable.
func (c *Connection) GetHeaderPrefix(messageID string, maxSnippet int) (*YencMetadata, error) {
	return c.GetHeaderPrefixWithTimeout(messageID, maxSnippet, timeouts.StreamBodyTimeout)
}

// GetHeaderPrefixWithTimeout is GetHeaderPrefix with the idle deadline
// overridden - for latency-sensitive, best-effort callers (e.g. the parser's
// PAR2 source-size probe, see nntp.Client.ExecuteOnce) that want a fast
// failure rather than waiting out the full StreamBodyTimeout on a dead
// article.
func (c *Connection) GetHeaderPrefixWithTimeout(messageID string, maxSnippet int, timeout time.Duration) (*YencMetadata, error) {
	res, err := c.requestBody(messageID, timeout)
	if err != nil {
		return nil, err
	}
	var snippet []byte
	if maxSnippet > 0 {
		snippet = bytes.Clone(res.Data[:min(maxSnippet, len(res.Data))])
	}
	putBodyBuf(res.Data)
	return metadataFromResult(res.Meta, snippet), nil
}

// GetHeaderPrefixContext is GetHeaderPrefix that ends when ctx does: the
// connection is closed under the read, so a stalled server costs the caller
// its own deadline rather than the full StreamBodyTimeout. A connection
// closed this way is never pooled again (the pool drops closed ones), so no
// half-read response is left for the next user. When ctx ends first the
// error is ctx's own, not an NNTP one: the caller gave up, the provider
// didn't fail, so failover neither retries nor excludes it.
func (c *Connection) GetHeaderPrefixContext(ctx context.Context, messageID string, maxSnippet int) (*YencMetadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	meta, err := c.GetHeaderPrefix(messageID, maxSnippet)
	if !stop() {
		// The close has started, maybe not finished: finish it here so the
		// connection is closed before the pool sees it.
		_ = c.Close()
		if err != nil {
			return nil, ctx.Err()
		}
	}
	return meta, err
}

// GetBody retrieves article body by message ID as raw bytes (used by GetHeader)
func (c *Connection) GetBody(messageID string) ([]byte, error) {
	messageID = FormatMessageID(messageID)
	if err := c.sendCommandArg("BODY", messageID); err != nil {
		return nil, sendFailure("failed to send BODY command", err)
	}

	code, message, err := c.readResponseCodeWithDeadline(timeouts.StreamBodyTimeout)
	if err != nil {
		return nil, NewConnectionError(fmt.Errorf("failed to read body response: %w", err))
	}

	if code != 222 {
		return nil, classifyNNTPError(code, string(message))
	}

	// Set read deadline to prevent hanging on stalled servers
	_ = c.conn.SetReadDeadline(utils.Now().Add(timeouts.StreamBodyTimeout))
	defer func() { _ = c.conn.SetReadDeadline(time.Time{}) }()

	body, err := c.readDotBytes()
	if err != nil {
		return nil, classifyTransferError("failed to read body", err)
	}
	return body, nil
}

// GetDecodedBody retrieves and decodes an article body in one pass.
func (c *Connection) GetDecodedBody(messageID string) ([]byte, error) {
	decoded, _, err := c.GetDecodedBodyWithMetadata(messageID)
	return decoded, err
}

// GetDecodedBodyWithMetadata retrieves and decodes the article body while also
// returning the parsed yEnc metadata from the same pass. The returned slice
// is sized to the body: callers keep these (PAR2 holds many at once), and a
// pooled 1 MB buffer each would multiply that memory.
func (c *Connection) GetDecodedBodyWithMetadata(messageID string) ([]byte, *YencMetadata, error) {
	start := time.Now()
	res, err := c.requestBody(messageID, timeouts.StreamBodyTimeout)
	if err != nil {
		return nil, nil, err
	}
	c.noteBody(int64(len(res.Data)), time.Since(start))
	decoded := bytes.Clone(res.Data)
	putBodyBuf(res.Data)
	return decoded, metadataFromResult(res.Meta, nil), nil
}

func (c *Connection) StreamBody(messageID string, w io.Writer) (int64, error) {
	n, _, err := c.StreamBodyMeta(messageID, w)
	return n, err
}

// StreamBodyMeta decodes one article body and writes it to w in a single
// Write, returning the article's yEnc headers (no snippet) so the caller can
// check it received the part it asked for. Providers can hold a different
// upload's article under a reused Message-ID; it decodes cleanly and passes
// its own size and CRC checks, so only its part number and size give it
// away. meta is nil when no body was read.
//
// On the streaming path w is the segment cache, where every Write costs a
// pwrite plus an exclusive buffer-lock acquisition; segment readers only see
// bytes after Finalize, so one Write per article adds no visible latency.
func (c *Connection) StreamBodyMeta(messageID string, w io.Writer) (int64, *YencMetadata, error) {
	start := time.Now()
	res, err := c.requestBody(messageID, timeouts.StreamBodyTimeout)
	if err != nil {
		return 0, nil, err
	}
	// Body routing times the download, not the caller: the write to w (a
	// disk cache stalled by a sweep) comes after and is left out.
	c.noteBody(int64(len(res.Data)), time.Since(start))
	n, err := w.Write(res.Data)
	if err == nil && n != len(res.Data) {
		err = io.ErrShortWrite
	}
	putBodyBuf(res.Data)
	if err != nil {
		return int64(n), nil, classifyTransferError("streaming yenc decode failed", err)
	}
	return int64(n), metadataFromResult(res.Meta, nil), nil
}

// noteBody hands a completed body download to the owning client's body
// routing.
func (c *Connection) noteBody(n int64, d time.Duration) {
	if c.onBody != nil {
		c.onBody(n, d)
	}
}

// readDotBytes reads dot-terminated NNTP data using textproto.DotReader
// This matches Python nntplib's efficient buffered approach
func (c *Connection) readDotBytes() ([]byte, error) {
	// Use textproto's DotReader which efficiently handles dot-stuffing
	// and terminator detection with optimized buffered reading
	dotReader := c.text.DotReader()

	// Pre-allocate for typical usenet segment (~750KB)
	// Using io.ReadAll with pre-sized buffer hint
	buf := bytes.NewBuffer(make([]byte, 0, 800*1024))

	// Copy from DotReader to buffer
	_, err := io.Copy(buf, dotReader)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// GetHead retrieves article headers by message ID
func (c *Connection) GetHead(messageID string) ([]byte, error) {
	messageID = FormatMessageID(messageID)
	if err := c.sendCommandArg("HEAD", messageID); err != nil {
		return nil, sendFailure("failed to send HEAD command", err)
	}

	// Read the initial response
	resp, err := c.readResponse()
	if err != nil {
		return nil, NewConnectionError(fmt.Errorf("failed to read head response: %w", err))
	}

	if resp.Code != 221 {
		return nil, classifyNNTPError(resp.Code, resp.Message)
	}

	// Read the header data using textproto
	lines, err := c.text.ReadDotLines()
	if err != nil {
		return nil, NewConnectionError(fmt.Errorf("failed to read header data: %w", err))
	}

	// Join with \r\n to preserve original line endings and add final \r\n
	headers := strings.Join(lines, "\r\n")
	if len(lines) > 0 {
		headers += "\r\n"
	}

	return []byte(headers), nil
}

func (c *Connection) Post(messageID, filename string, body []byte) error {
	now := utils.Now().Format("2006-01-02 15:04:05")
	if err := c.sendCommand("POST"); err != nil {
		return NewConnectionError(fmt.Errorf("failed to send POST command: %w", err))
	}

	resp, err := c.readResponse()
	if err != nil {
		return NewConnectionError(fmt.Errorf("failed to read POST response: %w", err))
	}

	// 340 = send article to be posted
	if resp.Code != 340 {
		// 440, 441, etc should be classified properly
		return classifyNNTPError(resp.Code, fmt.Sprintf("unexpected response to POST: %s", resp.Message))
	}

	// 2. Build RFC-822 style article (headers + blank line + body)
	var buf bytes.Buffer

	if filename != "" {
		buf.WriteString("Subject: " + filename + "\r\n")
	}

	buf.WriteString("Date: " + now + "\r\n")
	buf.WriteString("Newsgroups: " + "alt.binaries.friends" + "\r\n")
	if messageID != "" {
		// ensure proper <id> format
		msgID := FormatMessageID(messageID)
		buf.WriteString("Message-ID: " + msgID + "\r\n")
	}

	// End of headers
	buf.WriteString("\r\n")

	// 3. Body with CRLF normalization + dot-stuffing
	if len(body) > 0 {
		// Normalize to \n, then re-add \r\n
		body := bytes.ReplaceAll(body, []byte("\r\n"), []byte("\n"))
		lines := bytes.SplitSeq(body, []byte("\n"))

		for line := range lines {
			// Last split after trailing \n will give empty line; still write CRLF.
			if len(line) > 0 && line[0] == '.' {
				// dot-stuff per NNTP
				buf.WriteByte('.')
			}
			buf.Write(line)
			buf.WriteString("\r\n")
		}
	}

	// 4. Terminator line
	buf.WriteString(".\r\n")

	// 5. Send article data
	if _, err := c.writer.Write(buf.Bytes()); err != nil {
		return NewConnectionError(fmt.Errorf("failed to send article data: %w", err))
	}
	if err := c.writer.Flush(); err != nil {
		return NewConnectionError(fmt.Errorf("failed to flush article data: %w", err))
	}

	// 6. Final response
	resp, err = c.readResponse()
	if err != nil {
		return NewConnectionError(fmt.Errorf("failed to read post completion response: %w", err))
	}

	if resp.Code != 240 { // 240 = article received OK
		return classifyNNTPError(resp.Code, resp.Message)
	}

	return nil
}

// Stat retrieves article statistics by message ID with proper error classification
func (c *Connection) Stat(messageID string) (articleNumber int, echoedID string, err error) {
	messageID = FormatMessageID(messageID)

	if err = c.sendCommandArg("STAT", messageID); err != nil {
		return 0, "", sendFailure("failed to send STAT", err)
	}

	resp, err := c.readResponseWithDeadline(timeouts.StreamBodyTimeout)
	if err != nil {
		return 0, "", NewConnectionError(fmt.Errorf("failed to read STAT response: %w", err))
	}
	if strayReply(resp.Code, 223) {
		return 0, "", c.outOfStep("STAT", messageID, fmt.Sprintf("%d %s", resp.Code, resp.Message))
	}
	if resp.Code == 223 {
		if _, other := replyForOtherArticle(messageID, resp.Message); other {
			return 0, "", c.outOfStep("STAT", messageID, fmt.Sprintf("%d %s", resp.Code, resp.Message))
		}
	}
	return parseStatResponse(resp)
}

func parseStatResponse(resp Response) (articleNumber int, echoedID string, err error) {
	if resp.Code != 223 {
		return 0, "", classifyNNTPError(resp.Code, resp.Message)
	}

	fields := strings.Fields(resp.Message)
	if len(fields) < 2 {
		return 0, "", NewProtocolError(resp.Code, fmt.Sprintf("unexpected STAT response format: %q", resp.Message))
	}

	if articleNumber, err = strconv.Atoi(fields[0]); err != nil {
		return 0, "", NewProtocolError(resp.Code, fmt.Sprintf("invalid article number %q: %v", fields[0], err))
	}
	echoedID = fields[1]

	return articleNumber, echoedID, nil
}

// errStatDesync marks a STAT pipeline whose replies did not line up with its
// commands. Every result in that window is untrusted.
var errStatDesync = errors.New("STAT pipeline replies out of step with commands")

// StatBatch sends every STAT in one write, followed by a DATE, and reads the
// replies in order. It returns the time charged to found articles: each hit
// is charged the time since the previous reply (the first since the write),
// so a slow 430 is not billed to the hit behind it.
//
// Replies carry no command tag, so a server that drops or adds one line
// shifts every later reply onto the wrong message ID, turning a present
// article into a false 430. The DATE is the check: its 111 must come exactly
// after the last STAT reply. A 111 in a STAT slot, anything but 111 after the
// last one, bytes left over after it, a reply that is neither 223 nor
// not-found, a 223 echoing a message ID other than asked, or a transport
// error fails the whole window as a connection error. The connection is
// then unusable and the caller must release it. A message ID holding CR, LF
// or NUL fails the window with errBadCommandArg before anything is written;
// the connection is still usable.
func (c *Connection) StatBatch(messageIDs []string) ([]StatResult, time.Duration, error) {
	results := make([]StatResult, len(messageIDs))
	for i, id := range messageIDs {
		results[i].MessageID = id
	}
	if len(messageIDs) == 0 {
		return results, 0, nil
	}
	fail := func(err error) ([]StatResult, time.Duration, error) {
		connErr := NewConnectionError(err)
		for i := range results {
			results[i].Available = false
			results[i].Error = connErr
		}
		return results, 0, connErr
	}

	// Refuse the whole window before buffering anything: a line refused
	// part way would leave the STATs before it in the write buffer, to go
	// out ahead of the next command on this connection.
	asked := make([]string, len(messageIDs))
	for i, id := range messageIDs {
		asked[i] = FormatMessageID(id)
		if strings.ContainsAny(asked[i], "\r\n\x00") {
			err := fmt.Errorf("STAT %q: %w", asked[i], errBadCommandArg)
			for j := range results {
				results[j].Error = err
			}
			return results, 0, err
		}
	}

	_ = c.conn.SetWriteDeadline(utils.Now().Add(timeouts.HandshakeTimeout))
	var werr error
	for _, id := range asked {
		if werr = c.writeCommandArg("STAT", id); werr != nil {
			break
		}
	}
	if werr == nil {
		werr = c.writeCommandArg("DATE", "")
	}
	if werr == nil {
		werr = c.writer.Flush()
	}
	_ = c.conn.SetWriteDeadline(time.Time{})
	if werr != nil {
		return fail(fmt.Errorf("write STAT pipeline of %d: %w", len(messageIDs), werr))
	}

	var hitTime time.Duration
	prev := time.Now()
	for i := range results {
		resp, err := c.readResponseWithDeadline(timeouts.StreamBodyTimeout)
		if err != nil {
			return fail(fmt.Errorf("read STAT pipeline at %d/%d: %w", i+1, len(results), err))
		}
		now := time.Now()
		_, _, statErr := parseStatResponse(resp)
		if statErr == nil {
			if echoed, other := replyForOtherArticle(asked[i], resp.Message); other {
				return fail(fmt.Errorf("%w: reply %d/%d was for %s, asked %s", errStatDesync, i+1, len(results), echoed, asked[i]))
			}
		}
		switch {
		case statErr == nil:
			results[i].Available = true
			hitTime += now.Sub(prev)
		case IsArticleNotFoundError(statErr):
			results[i].Error = statErr
		default:
			return fail(fmt.Errorf("%w: reply %d/%d was %d %q", errStatDesync, i+1, len(results), resp.Code, resp.Message))
		}
		prev = now
	}
	resp, err := c.readResponseWithDeadline(timeouts.StreamBodyTimeout)
	if err != nil {
		return fail(fmt.Errorf("read STAT pipeline DATE check: %w", err))
	}
	if resp.Code != 111 {
		return fail(fmt.Errorf("%w: expected 111 after %d replies, got %d %q", errStatDesync, len(results), resp.Code, resp.Message))
	}
	if n := c.reader.Buffered(); n > 0 {
		return fail(fmt.Errorf("%w: %d unexpected bytes after the DATE check", errStatDesync, n))
	}
	return results, hitTime, nil
}

// SelectGroup selects a newsgroup and returns group information
func (c *Connection) SelectGroup(groupName string) (*GroupInfo, error) {
	if err := c.sendCommandArg("GROUP", groupName); err != nil {
		return nil, NewConnectionError(fmt.Errorf("failed to send GROUP command: %w", err))
	}

	resp, err := c.readResponse()
	if err != nil {
		return nil, NewConnectionError(fmt.Errorf("failed to read GROUP response: %w", err))
	}

	if resp.Code != 211 {
		return nil, classifyNNTPError(resp.Code, resp.Message)
	}

	// Parse GROUP response: "211 number low high group-name"
	fields := strings.Fields(resp.Message)
	if len(fields) < 4 {
		return nil, NewProtocolError(resp.Code, fmt.Sprintf("unexpected GROUP response format: %q", resp.Message))
	}

	groupInfo := &GroupInfo{
		Name: groupName,
	}

	if count, err := strconv.Atoi(fields[0]); err == nil {
		groupInfo.Count = count
	}
	if low, err := strconv.Atoi(fields[1]); err == nil {
		groupInfo.Low = low
	}
	if high, err := strconv.Atoi(fields[2]); err == nil {
		groupInfo.High = high
	}

	return groupInfo, nil
}

// parseArticle parses article data from response lines
func (c *Connection) parseArticle(messageID string, lines []string) (*Article, error) {
	article := &Article{
		MessageID: messageID,
		Groups:    []string{},
	}

	headerEnd := -1
	for i, line := range lines {
		if line == "" {
			headerEnd = i
			break
		}

		// Parse headers
		if after, ok := strings.CutPrefix(line, "Subject: "); ok {
			article.Subject = after
		} else if after, ok := strings.CutPrefix(line, "From: "); ok {
			article.From = after
		} else if after, ok := strings.CutPrefix(line, "Date: "); ok {
			article.Date = after
		} else if after, ok := strings.CutPrefix(line, "Newsgroups: "); ok {
			groups := after
			article.Groups = strings.Split(groups, ",")
			for i := range article.Groups {
				article.Groups[i] = strings.TrimSpace(article.Groups[i])
			}
		}
	}

	// Join body lines
	if headerEnd != -1 && headerEnd+1 < len(lines) {
		body := strings.Join(lines[headerEnd+1:], "\n")
		article.Body = []byte(body)
		article.Size = int64(len(article.Body))
	}

	return article, nil
}

// FormatMessageID ensures message ID has proper format
func FormatMessageID(messageID string) string {
	messageID = strings.TrimSpace(messageID)
	if !strings.HasPrefix(messageID, "<") {
		messageID = "<" + messageID
	}
	if !strings.HasSuffix(messageID, ">") {
		messageID = messageID + ">"
	}
	return messageID
}
