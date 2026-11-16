package manager

// P2E harness, part 1: a minimal fake NNTP server on 127.0.0.1 with
// per-message-ID and server-wide fault injection. Speaks exactly what
// internal/nntp sends: a 200 greeting, DATE (ping + pipelined-STAT sentinel,
// conns.go:367/867), STAT (conns.go:797/862, reply needs "223 <n> <id>",
// conns.go:813), BODY (conns.go:504), QUIT. AUTHINFO is only sent when a
// username is configured (client.go:1275) - the harness configures none.

import (
	"bufio"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type p2eBehavior int

const (
	p2eOK            p2eBehavior = iota
	p2eNotFound                  // 430 on BODY and STAT
	p2eStatLie                   // STAT 223, BODY 430
	p2eStall                     // BODY never answered (STAT answered 223)
	p2eStallAll                  // BODY and STAT never answered
	p2eTruncateClose             // half the yEnc body, then the connection is closed
	p2eTruncateShort             // a complete, CRC-valid article half as long
	p2eWrongBody                 // another article's complete, CRC-valid body
)

func (b p2eBehavior) String() string {
	return [...]string{"ok", "notfound", "statlie", "stall", "stall_all", "truncate_close", "truncate_short", "wrongbody"}[b]
}

type p2eArticle struct {
	id        string
	body      string // full yEnc text (=ybegin .. =yend lines), no status, no terminator
	shortBody string // CRC-valid half-length variant
	data      []byte // decoded payload
}

type p2eServer struct {
	ln net.Listener

	mu        sync.Mutex
	articles  map[string]*p2eArticle
	behav     map[string]p2eBehavior
	wrongWith map[string]string // id -> id whose body is served instead
	conns     map[net.Conn]struct{}

	refuse      atomic.Bool
	quotaAfter  atomic.Int64 // BODY count after which every BODY gets quotaCode; 0 = off
	quotaCode   string
	slowNS      atomic.Int64 // delay before each reply
	connsServed atomic.Int64

	// counters
	cmdStat, cmdBody, cmdDate, cmdQuit, cmdOther atomic.Int64
	bodyOK, body430, bodyQuota, bodyStalled      atomic.Int64
	bodyWrong, bodyTrunc, bodyShort              atomic.Int64
	stat223, stat430                             atomic.Int64
	bytesOut                                     atomic.Int64
	refused                                      atomic.Int64
	otherCmds                                    sync.Map // string -> *atomic.Int64
	bodyByID                                     sync.Map // id -> *atomic.Int64
}

func newP2eServer() (*p2eServer, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	s := &p2eServer{
		ln:        ln,
		articles:  map[string]*p2eArticle{},
		behav:     map[string]p2eBehavior{},
		wrongWith: map[string]string{},
		conns:     map[net.Conn]struct{}{},
		quotaCode: "502 Download quota exceeded",
	}
	go s.accept()
	return s, nil
}

func (s *p2eServer) port() int { return s.ln.Addr().(*net.TCPAddr).Port }

func (s *p2eServer) close() {
	_ = s.ln.Close()
	s.dropConns()
}

// dropConns resets every open connection (RST via linger 0).
func (s *p2eServer) dropConns() {
	s.mu.Lock()
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	for _, c := range conns {
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetLinger(0)
		}
		_ = c.Close()
	}
}

// reset clears behaviours and counters and drops every connection.
func (s *p2eServer) reset() {
	s.dropConns()
	s.mu.Lock()
	s.behav = map[string]p2eBehavior{}
	s.wrongWith = map[string]string{}
	s.mu.Unlock()
	s.refuse.Store(false)
	s.quotaAfter.Store(0)
	s.slowNS.Store(0)
	s.resetCounters()
}

func (s *p2eServer) resetCounters() {
	for _, c := range []*atomic.Int64{&s.cmdStat, &s.cmdBody, &s.cmdDate, &s.cmdQuit, &s.cmdOther,
		&s.bodyOK, &s.body430, &s.bodyQuota, &s.bodyStalled, &s.bodyWrong, &s.bodyTrunc, &s.bodyShort,
		&s.stat223, &s.stat430, &s.bytesOut, &s.refused, &s.connsServed} {
		c.Store(0)
	}
	s.otherCmds.Range(func(k, _ any) bool { s.otherCmds.Delete(k); return true })
	s.bodyByID.Range(func(k, _ any) bool { s.bodyByID.Delete(k); return true })
}

func (s *p2eServer) addArticle(a *p2eArticle) {
	s.mu.Lock()
	s.articles[a.id] = a
	s.mu.Unlock()
}

func (s *p2eServer) set(id string, b p2eBehavior) {
	s.mu.Lock()
	s.behav[id] = b
	s.mu.Unlock()
}

func (s *p2eServer) setWrong(id, servedID string) {
	s.mu.Lock()
	s.behav[id] = p2eWrongBody
	s.wrongWith[id] = servedID
	s.mu.Unlock()
}

func (s *p2eServer) lookup(id string) (*p2eArticle, p2eBehavior, *p2eArticle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.articles[id]
	b := s.behav[id]
	var other *p2eArticle
	if w, ok := s.wrongWith[id]; ok {
		other = s.articles[w]
	}
	return a, b, other
}

func (s *p2eServer) accept() {
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		if s.refuse.Load() {
			s.refused.Add(1)
			if tc, ok := c.(*net.TCPConn); ok {
				_ = tc.SetLinger(0)
			}
			_ = c.Close()
			continue
		}
		s.mu.Lock()
		s.conns[c] = struct{}{}
		s.mu.Unlock()
		s.connsServed.Add(1)
		go s.p2eServeConn(c)
	}
}

func (s *p2eServer) write(w *bufio.Writer, str string) {
	n, _ := w.WriteString(str)
	s.bytesOut.Add(int64(n))
}

func (s *p2eServer) p2eServeConn(c net.Conn) {
	defer func() {
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
		_ = c.Close()
	}()
	r := bufio.NewReader(c)
	w := bufio.NewWriterSize(c, 256*1024)
	s.write(w, "200 p2e fake NNTP ready\r\n")
	if w.Flush() != nil {
		return
	}
	stall := func() {
		// Never answer; keep reading (and discarding) until the client closes.
		_, _ = io.Copy(io.Discard, r)
	}
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		if d := s.slowNS.Load(); d > 0 {
			time.Sleep(time.Duration(d))
		}
		if s.refuse.Load() {
			if tc, ok := c.(*net.TCPConn); ok {
				_ = tc.SetLinger(0)
			}
			return
		}
		cmd, arg, _ := strings.Cut(line, " ")
		switch strings.ToUpper(cmd) {
		case "DATE":
			s.cmdDate.Add(1)
			s.write(w, "111 20261116120000\r\n")
		case "QUIT":
			s.cmdQuit.Add(1)
			s.write(w, "205 bye\r\n")
			_ = w.Flush()
			return
		case "STAT":
			s.cmdStat.Add(1)
			id := strings.Trim(arg, "<>")
			a, b, _ := s.lookup(id)
			switch {
			case a == nil || b == p2eNotFound:
				s.stat430.Add(1)
				s.write(w, "430 no such article\r\n")
			case b == p2eStallAll:
				_ = w.Flush()
				stall()
				return
			default:
				s.stat223.Add(1)
				s.write(w, "223 0 <"+id+">\r\n")
			}
		case "BODY":
			s.cmdBody.Add(1)
			id := strings.Trim(arg, "<>")
			v, _ := s.bodyByID.LoadOrStore(id, new(atomic.Int64))
			v.(*atomic.Int64).Add(1)
			if q := s.quotaAfter.Load(); q > 0 && s.cmdBody.Load() > q {
				s.bodyQuota.Add(1)
				s.write(w, s.quotaCode+"\r\n")
				break
			}
			a, b, other := s.lookup(id)
			switch {
			case a == nil || b == p2eNotFound || b == p2eStatLie:
				s.body430.Add(1)
				s.write(w, "430 no such article\r\n")
			case b == p2eStall || b == p2eStallAll:
				s.bodyStalled.Add(1)
				_ = w.Flush()
				stall()
				return
			case b == p2eTruncateClose:
				s.bodyTrunc.Add(1)
				s.write(w, "222 0 <"+id+">\r\n"+a.body[:len(a.body)/2])
				_ = w.Flush()
				return
			case b == p2eTruncateShort:
				s.bodyShort.Add(1)
				s.write(w, "222 0 <"+id+">\r\n"+a.shortBody+".\r\n")
			case b == p2eWrongBody && other != nil:
				s.bodyWrong.Add(1)
				s.write(w, "222 0 <"+id+">\r\n"+other.body+".\r\n")
			default:
				s.bodyOK.Add(1)
				s.write(w, "222 0 <"+id+">\r\n"+a.body+".\r\n")
			}
		default:
			s.cmdOther.Add(1)
			v, _ := s.otherCmds.LoadOrStore(line, new(atomic.Int64))
			v.(*atomic.Int64).Add(1)
			s.write(w, "500 unknown command\r\n")
		}
		if w.Flush() != nil {
			return
		}
	}
}

type p2eServerCounts struct {
	Stat, Body, Date, Other                              int64
	BodyOK, Body430, BodyQuota, BodyStalled, BodyWrong   int64
	BodyTrunc, BodyShort, Stat223, Stat430, Bytes, Conns int64
	Refused                                              int64
	OtherCmds                                            []string
}

func (s *p2eServer) counts() p2eServerCounts {
	c := p2eServerCounts{
		Stat: s.cmdStat.Load(), Body: s.cmdBody.Load(), Date: s.cmdDate.Load(), Other: s.cmdOther.Load(),
		BodyOK: s.bodyOK.Load(), Body430: s.body430.Load(), BodyQuota: s.bodyQuota.Load(),
		BodyStalled: s.bodyStalled.Load(), BodyWrong: s.bodyWrong.Load(), BodyTrunc: s.bodyTrunc.Load(),
		BodyShort: s.bodyShort.Load(), Stat223: s.stat223.Load(), Stat430: s.stat430.Load(),
		Bytes: s.bytesOut.Load(), Conns: s.connsServed.Load(), Refused: s.refused.Load(),
	}
	s.otherCmds.Range(func(k, _ any) bool { c.OtherCmds = append(c.OtherCmds, k.(string)); return true })
	return c
}

func (s *p2eServer) bodyCount(id string) int64 {
	if v, ok := s.bodyByID.Load(id); ok {
		return v.(*atomic.Int64).Load()
	}
	return 0
}
