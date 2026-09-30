package nntp

import (
	"bufio"
	"bytes"
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

func (b *bodyReader) Read(p []byte) (int, error) {
	n, err := b.c.reader.Read(p)
	if n > 0 {
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

	res, err := c.decoder().Next()
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

// writeCommandArg buffers one command line without flushing it or touching
// the connection's deadlines.
func (c *Connection) writeCommandArg(command, arg string) error {
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
		return nil, NewConnectionError(fmt.Errorf("failed to send ARTICLE command: %w", err))
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
		return nntpyenc.BodyResult{}, NewConnectionError(fmt.Errorf("failed to send BODY command: %w", err))
	}

	res, err := c.nextBodyWithIdleDeadline(idle)
	if err != nil {
		putBodyBuf(res.Data)
		res.Data = nil
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
		return res, classifyNNTPError(res.StatusCode, res.Message)
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

// GetBody retrieves article body by message ID as raw bytes (used by GetHeader)
func (c *Connection) GetBody(messageID string) ([]byte, error) {
	messageID = FormatMessageID(messageID)
	if err := c.sendCommandArg("BODY", messageID); err != nil {
		return nil, NewConnectionError(fmt.Errorf("failed to send BODY command: %w", err))
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
		return nil, NewConnectionError(fmt.Errorf("failed to send HEAD command: %w", err))
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
		return 0, "", NewConnectionError(fmt.Errorf("failed to send STAT: %w", err))
	}

	resp, err := c.readResponseWithDeadline(timeouts.StreamBodyTimeout)
	if err != nil {
		return 0, "", NewConnectionError(fmt.Errorf("failed to read STAT response: %w", err))
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
// not-found, or a transport error fails the whole window as a connection
// error. The connection is then unusable and the caller must release it.
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

	_ = c.conn.SetWriteDeadline(utils.Now().Add(timeouts.HandshakeTimeout))
	var werr error
	for _, id := range messageIDs {
		if werr = c.writeCommandArg("STAT", FormatMessageID(id)); werr != nil {
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
