package shim

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil/mysqlwire"
)

// The status flags on the wire (#2110), packet by packet. Every test here
// talks to a real go-mysql server connection behind NewConn and Session, with
// a client that shows the status of each packet (mysqlwire).

// servePort serves connections the way the port's hosts do. bind prepares
// each connection's handler; a nil handler from newHandler serves h as is.
func servePort(t *testing.T, authMethod string, handler func() server.Handler) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	srv, err := NewMySQLServer(authMethod)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(30 * time.Second))
				h := handler()
				auth, _ := NewTenantAuth(map[string]string{"u": "p"})
				mc, err := NewConn(c, srv, auth, h)
				if err != nil {
					return
				}
				if sh, ok := h.(*Handler); ok {
					sh.BindConn(mc)
				}
				session := NewSession(mc, h)
				for session.HandleCommand() == nil {
				}
			}()
		}
	}()
	return ln.Addr().String()
}

func dialPort(t *testing.T, addr string) *mysqlwire.Conn {
	t.Helper()
	c, err := mysqlwire.Dial(addr, "u", "p", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

const (
	stAuto      = mysql.SERVER_STATUS_AUTOCOMMIT
	stAutoTrans = mysql.SERVER_STATUS_AUTOCOMMIT | mysql.SERVER_STATUS_IN_TRANS
	stTrans     = mysql.SERVER_STATUS_IN_TRANS
	stNone      = uint16(0)
	stReadOnly  = mysql.SERVER_STATUS_AUTOCOMMIT | mysql.SERVER_STATUS_IN_TRANS | mysql.SERVER_STATUS_IN_TRANS_READONLY
	stNoBack    = mysql.SERVER_STATUS_AUTOCOMMIT | mysql.SERVER_STATUS_NO_BACKSLASH_ESCAPED

	// What a source may send that the port must not pass on: each promises
	// something the port does not deliver.
	stNotDelivered = mysql.SERVER_MORE_RESULTS_EXISTS | mysql.SERVER_STATUS_CURSOR_EXISTS | mysql.SERVER_STATUS_LAST_ROW_SEND |
		mysql.SERVER_SESSION_STATE_CHANGED | mysql.SERVER_PS_OUT_PARAMS | mysql.SERVER_STATUS_METADATA_CHANGED
	// What it does pass on about the statement itself.
	stAboutStatement = mysql.SERVER_STATUS_NO_INDEX_USED | mysql.SERVER_QUERY_WAS_SLOW
)

func wantStatus(t *testing.T, what string, got, want uint16) {
	t.Helper()
	if got != want {
		t.Errorf("%s: status 0x%04x, want 0x%04x", what, got, want)
	}
}

// The handshake and the OK that ends authentication announce autocommit, as
// MySQL and MariaDB do. Both are written before the port knows which server
// the client wants, and go-mysql builds them from a status nobody can set
// beforehand: this pins that NewConn sets it anyway.
func TestStatus_handshakeAnnouncesAutocommit(t *testing.T) {
	addr := servePort(t, "", func() server.Handler { return NewHandler(nil, nil) })
	hs, err := mysqlwire.ReadHandshake(addr)
	if err != nil {
		t.Fatal(err)
	}
	wantStatus(t, "handshake", hs.Status, stAuto)
	c := dialPort(t, addr)
	wantStatus(t, "handshake", c.Handshake.Status, stAuto)
	wantStatus(t, "OK that ends authentication", c.AuthStatus, stAuto)

	// The version the handshake announces is a fixed one, on purpose (see
	// portServerVersion); @@version answers the same on a copy-only
	// connection.
	if c.Handshake.ServerVersion != portServerVersion {
		t.Errorf("handshake version %q, want %q", c.Handshake.ServerVersion, portServerVersion)
	}
}

// The other auth methods write the same handshake through a server built
// another way.
func TestStatus_handshakeAnnouncesAutocommit_sha2Server(t *testing.T) {
	addr := servePort(t, mysql.AUTH_CACHING_SHA2_PASSWORD, func() server.Handler { return NewHandler(nil, nil) })
	hs, err := mysqlwire.ReadHandshake(addr)
	if err != nil {
		t.Fatal(err)
	}
	wantStatus(t, "handshake", hs.Status, stAuto)
	if hs.AuthPlugin != mysql.AUTH_CACHING_SHA2_PASSWORD {
		t.Errorf("auth plugin %q", hs.AuthPlugin)
	}
	if hs.ServerVersion != portServerVersion {
		t.Errorf("handshake version %q, want %q", hs.ServerVersion, portServerVersion)
	}
}

func TestSetHandshakeStatus(t *testing.T) {
	// A protocol 10 handshake as go-mysql writes it: header, 10, version,
	// NUL, connection id, salt, filler, capabilities, charset, STATUS, ...
	build := func(version string) []byte {
		p := []byte{0, 0, 0, 0, 10}
		p = append(p, version...)
		p = append(p, 0)
		p = append(p, 1, 2, 3, 4)             // connection id
		p = append(p, "saltsalt"...)          // salt part 1
		p = append(p, 0)                      // filler
		p = append(p, 0xff, 0xf7)             // capabilities, low
		p = append(p, 33)                     // charset
		p = append(p, 0, 0)                   // status
		p = append(p, 0xff, 0x81, 21)         // capabilities high, salt length
		p = append(p, make([]byte, 10+13)...) // reserved, salt part 2
		p = append(p, "mysql_native_password\x00"...)
		binary.LittleEndian.PutUint16(p, uint16(len(p)-4))
		return p
	}
	for _, version := range []string{"8.0.11", "11.4.13-MariaDB-ubu2404-log", ""} {
		p := build(version)
		orig := append([]byte(nil), p...)
		out, ok := setHandshakeStatus(p, stAuto)
		if !ok {
			t.Fatalf("version %q: not patched", version)
		}
		off := 4 + 1 + len(version) + 1 + 4 + 8 + 1 + 2 + 1
		if got := binary.LittleEndian.Uint16(out[off:]); got != stAuto {
			t.Errorf("version %q: status 0x%04x", version, got)
		}
		if string(p) != string(orig) {
			t.Errorf("version %q: the caller's buffer was written to", version)
		}
		out[off], out[off+1] = 0, 0
		if string(out) != string(orig) {
			t.Errorf("version %q: bytes other than the status changed", version)
		}
	}
	// Anything that is not a whole protocol 10 handshake is left alone.
	whole := build("8.0.11")
	for name, p := range map[string][]byte{
		"empty":            nil,
		"header only":      {1, 0, 0, 0},
		"protocol 9":       append([]byte{0, 0, 0, 0, 9}, whole[5:]...),
		"an error packet":  {3, 0, 0, 0, 0xff, 0x10, 0x04},
		"cut short":        whole[:20],
		"not sequence 0":   append([]byte{whole[0], whole[1], whole[2], 1}, whole[4:]...),
		"length disagrees": append([]byte{whole[0] + 1}, whole[1:]...),
		"no version end":   {2, 0, 0, 0, 10, 'x'},
	} {
		if _, ok := setHandshakeStatus(p, stAuto); ok {
			t.Errorf("%s: patched", name)
		}
	}
}

// A copy-only connection has no transactions: every packet says autocommit,
// not in a transaction, including after a SET autocommit=0 the port accepts
// as connection chatter.
func TestStatus_copyOnlyConnection(t *testing.T) {
	addr := servePort(t, "", func() server.Handler {
		h := NewHandler(nil, nil)
		h.BindFreeSQL(&fakeFreeSQL{res: oneCell("n", "BIGINT", "3")})
		return h
	})
	c := dialPort(t, addr)
	rep, err := c.Ping()
	if err != nil {
		t.Fatal(err)
	}
	wantStatus(t, "PING", rep.Status, stAuto)
	for _, q := range []string{
		"SELECT count(*) AS n FROM t", // the copy
		"SELECT @@autocommit",         // answered from the port's variables
		"SET autocommit=0",            // connection chatter
		"SELECT @@autocommit",
		"SET time_zone = '+00:00'", // a session setting the port applies
		"SHOW WARNINGS",
		"USE shop",
	} {
		rep, err := c.Exec(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if rep.Resultset {
			wantStatus(t, q+" (column EOF)", rep.HeaderStatus, stAuto)
		}
		wantStatus(t, q, rep.Status, stAuto)
		if q == "SELECT @@autocommit" && (len(rep.Rows) != 1 || *rep.Rows[0][0] != "1") {
			t.Errorf("@@autocommit = %v, want 1: the variable and the flag must agree", rep.Rows)
		}
	}
	if rep, err = c.InitDB("shop"); err != nil {
		t.Fatal(err)
	}
	wantStatus(t, "COM_INIT_DB", rep.Status, stAuto)
	id, _, err := c.Prepare("SELECT count(*) AS n FROM t")
	if err != nil {
		t.Fatal(err)
	}
	if rep, err = c.Execute(id); err != nil {
		t.Fatal(err)
	}
	wantStatus(t, "COM_STMT_EXECUTE (column EOF)", rep.HeaderStatus, stAuto)
	wantStatus(t, "COM_STMT_EXECUTE", rep.Status, stAuto)
	if rep, err = c.ResetStmt(id); err != nil {
		t.Fatal(err)
	}
	wantStatus(t, "COM_STMT_RESET", rep.Status, stAuto)
	if _, err := c.Exec("BEGIN"); err == nil {
		// The copy takes no transaction; whatever it answers, the flags
		// stay the same.
		wantStatus(t, "after BEGIN", c.Status, stAuto)
	}
	if rep, err = c.Ping(); err != nil {
		t.Fatal(err)
	}
	wantStatus(t, "PING after BEGIN", rep.Status, stAuto)
}

// A handler that says nothing about its session (not a *Handler) is served
// as autocommit, not in a transaction.
func TestStatus_handlerWithoutSessionStatus(t *testing.T) {
	addr := servePort(t, "", func() server.Handler { return server.EmptyHandler{} })
	c := dialPort(t, addr)
	wantStatus(t, "OK that ends authentication", c.AuthStatus, stAuto)
	rep, err := c.Ping()
	if err != nil {
		t.Fatal(err)
	}
	wantStatus(t, "PING", rep.Status, stAuto)
}

// Under read routing every packet carries the source session's state as the
// source last reported it, whoever answered the statement.
func TestStatus_routedConnection(t *testing.T) {
	for _, st := range []uint16{stAuto, stAutoTrans, stNone, stTrans, stReadOnly, stNoBack} {
		t.Run(fmt.Sprintf("session 0x%04x", st), func(t *testing.T) {
			inTrans := st&mysql.SERVER_STATUS_IN_TRANS != 0
			// What the source's own answers carry: the session's state, a
			// flag about the statement, and every flag the port must drop.
			fromSource := st | stAboutStatement | stNotDelivered
			r := &fakeRouter{sessStatus: st, sessKnown: true, inTxn: inTrans, resultStatus: &fromSource}
			f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
			var h *Handler
			addr := servePort(t, "", func() server.Handler {
				h = NewHandler(nil, nil)
				h.BindFreeSQL(f)
				h.BindRouter(r, RouterConfig{MaxCopyAge: time.Hour, ReadOnly: true})
				return h
			})
			c := dialPort(t, addr)
			// Written before the port knew the server: always autocommit.
			wantStatus(t, "handshake", c.Handshake.Status, stAuto)
			wantStatus(t, "OK that ends authentication", c.AuthStatus, stAuto)

			ping := func(what string) {
				t.Helper()
				rep, err := c.Ping()
				if err != nil {
					t.Fatalf("%s: %v", what, err)
				}
				wantStatus(t, what, rep.Status, st)
			}
			exec := func(q string, wantFinal uint16) mysqlwire.Reply {
				t.Helper()
				rep, err := c.Exec(q)
				if err != nil {
					t.Fatalf("%s: %v", q, err)
				}
				if rep.Resultset {
					// The fake source hands its rows back whole, so the
					// library writes this EOF from the same status as the
					// last one; a streamed result (the prepared statement
					// below, and every forwarded result in production) gets
					// the session's flags alone.
					wantStatus(t, q+" (column EOF)", rep.HeaderStatus&^stAboutStatement, st)
				}
				wantStatus(t, q, rep.Status, wantFinal)
				return rep
			}
			ping("PING")

			// Forwarded, a result set: the source's flags, less the ones the
			// port does not deliver.
			r.toCopy = false
			if rep := exec("SELECT side FROM t WHERE id = 1", st|stAboutStatement); *rep.Rows[0][0] != "mysql" {
				t.Fatalf("not forwarded: %q", *rep.Rows[0][0])
			}
			// What the statement's own flags said is not carried over to the
			// next packet.
			ping("PING after a forwarded statement")
			// SHOW WARNINGS after a forwarded statement is forwarded too.
			exec("SHOW WARNINGS", st|stAboutStatement)

			// Forwarded, an OK packet.
			r.forwardOK = true
			rep := exec("COMMIT", st|stAboutStatement)
			if rep.Resultset {
				t.Fatal("COMMIT answered a result set")
			}
			r.forwardOK = false

			// Forwarded, a prepared statement streamed in the binary protocol.
			c.Status = 0xffff
			id, _, err := c.Prepare("SELECT side FROM t WHERE id = 1")
			if err != nil {
				t.Fatal(err)
			}
			wantStatus(t, "COM_STMT_PREPARE (EOF after the definitions)", c.Status, st)
			rep, err = c.Execute(id)
			if err != nil {
				t.Fatal(err)
			}
			if rep.RowCount != 1 || len(r.prepared) != 1 || len(r.prepared[0].executed) != 1 {
				t.Fatalf("the prepared statement was not executed on the source: %d rows", rep.RowCount)
			}
			wantStatus(t, "COM_STMT_EXECUTE (column EOF)", rep.HeaderStatus, st)
			wantStatus(t, "COM_STMT_EXECUTE", rep.Status, st|stAboutStatement)
			if rep, err = c.ResetStmt(id); err != nil {
				t.Fatal(err)
			}
			wantStatus(t, "COM_STMT_RESET", rep.Status, st)

			// Answered by the port itself.
			if !inTrans {
				r.toCopy = true
				if rep := exec("SELECT side FROM t", st); *rep.Rows[0][0] != "copy" {
					t.Fatalf("not answered by the copy: %q", *rep.Rows[0][0])
				}
				exec("SHOW WARNINGS", st) // the port's own, after a copy-served statement
				if rep, err = c.Execute(id); err != nil {
					t.Fatal(err)
				}
				if f.calls != 2 {
					t.Fatalf("the prepared statement was not answered by the copy (%d copy runs)", f.calls)
				}
				wantStatus(t, "COM_STMT_EXECUTE on the copy (column EOF)", rep.HeaderStatus, st)
				wantStatus(t, "COM_STMT_EXECUTE on the copy", rep.Status, st)
				r.toCopy = false
			}
			exec("USE shop", st)
			if rep, err = c.InitDB("shop"); err != nil {
				t.Fatal(err)
			}
			wantStatus(t, "COM_INIT_DB", rep.Status, st)

			// Refused by read-only mode: an error packet has no status, and
			// the packets after it still tell the session's state.
			if _, err := c.Exec("INSERT INTO t VALUES (1)"); err == nil {
				t.Fatal("the write was not refused")
			}
			exec("SHOW WARNINGS", st) // the refusal's diagnostics, served by the port
			ping("PING after a refusal")
		})
	}
}

// The state moves with the source's answers: BEGIN, then COMMIT.
func TestStatus_routedFollowsTheSource(t *testing.T) {
	r := &fakeRouter{sessStatus: stAuto, sessKnown: true, forwardOK: true}
	r.onForward = func(stmt string) {
		switch stmt {
		case "BEGIN":
			r.sessStatus, r.inTxn = stAutoTrans, true
		case "COMMIT":
			r.sessStatus, r.inTxn = stAuto, false
		case "SET autocommit=0":
			r.sessStatus = stNone
		}
		r.resultStatus = &r.sessStatus
	}
	addr := servePort(t, "", func() server.Handler {
		h := NewHandler(nil, nil)
		h.BindFreeSQL(&fakeFreeSQL{})
		h.BindRouter(r, RouterConfig{MaxCopyAge: time.Hour})
		return h
	})
	c := dialPort(t, addr)
	for _, step := range []struct {
		q    string
		want uint16
	}{
		{"BEGIN", stAutoTrans}, {"COMMIT", stAuto}, {"SET autocommit=0", stNone},
	} {
		rep, err := c.Exec(step.q)
		if err != nil {
			t.Fatal(err)
		}
		wantStatus(t, step.q, rep.Status, step.want)
		if rep, err = c.Ping(); err != nil {
			t.Fatal(err)
		}
		wantStatus(t, "PING after "+step.q, rep.Status, step.want)
	}
}

// Before the connection to the source exists, and once it is lost, there is
// no session on the source: a new one starts in autocommit, and a lost one
// took its transaction with it.
func TestStatus_routedWithoutASourceSession(t *testing.T) {
	r := &fakeRouter{sessStatus: stTrans, sessKnown: false}
	addr := servePort(t, "", func() server.Handler {
		h := NewHandler(nil, nil)
		h.BindFreeSQL(&fakeFreeSQL{})
		h.BindRouter(r, RouterConfig{MaxCopyAge: time.Hour})
		return h
	})
	c := dialPort(t, addr)
	rep, err := c.Ping()
	if err != nil {
		t.Fatal(err)
	}
	wantStatus(t, "PING", rep.Status, stAuto)
}

func TestHandler_SessionStatus(t *testing.T) {
	if got := NewHandler(nil, nil).SessionStatus(); got != stAuto {
		t.Errorf("no router: 0x%04x, want autocommit", got)
	}
	r := &fakeRouter{sessKnown: true}
	h := routingHandler(t, r, &fakeFreeSQL{}, time.Hour)
	for _, tc := range []struct{ source, want uint16 }{
		{stNone, stNone},
		{stAutoTrans, stAutoTrans},
		{stReadOnly, stReadOnly},
		{stNoBack, stNoBack},
		// Flags about a statement or a result are not the session's.
		{stAuto | stAboutStatement | stNotDelivered | mysql.SERVER_STATUS_DB_DROPPED, stAuto},
	} {
		r.sessStatus = tc.source
		if got := h.SessionStatus(); got != tc.want {
			t.Errorf("source 0x%04x: 0x%04x, want 0x%04x", tc.source, got, tc.want)
		}
	}
}

// firstWrite records what reaches the socket.
type firstWrite struct {
	net.Conn
	wrote [][]byte
}

func (f *firstWrite) Write(p []byte) (int, error) {
	f.wrote = append(f.wrote, bytes.Clone(p))
	return len(p), nil
}

// The port sets a time limit on every write (portWriteTimeout).
func (f *firstWrite) SetWriteDeadline(time.Time) error { return nil }

// A first packet that is not the handshake this code knows (a library that
// writes it another way) goes out as written, and is said in the log, once
// per process: the status it announces is then most likely 0, which is the
// bug of #2110, and nothing else would show it at run time. An error sent in
// place of the handshake is not that case and is not logged.
func TestHandshakeConn_unrecognisedHandshakeIsLogged(t *testing.T) {
	var logged bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn})))
	handshakeNotSet = sync.Once{}
	t.Cleanup(func() { slog.SetDefault(old); handshakeNotSet = sync.Once{} })

	write := func(first []byte) *firstWrite {
		t.Helper()
		under := &firstWrite{}
		c := &handshakeConn{Conn: under}
		for _, p := range [][]byte{first, {1, 0, 0, 1, 0x00}} {
			if n, err := c.Write(p); err != nil || n != len(p) {
				t.Fatalf("Write = %d, %v", n, err)
			}
		}
		if len(under.wrote) != 2 || !bytes.Equal(under.wrote[0], first) || !bytes.Equal(under.wrote[1], []byte{1, 0, 0, 1, 0x00}) {
			t.Fatalf("an unrecognised first packet was changed on its way out: %v", under.wrote)
		}
		return under
	}

	write([]byte{3, 0, 0, 0, 0xff, 0x10, 0x04}) // an error packet in place of the handshake
	if logged.Len() != 0 {
		t.Fatalf("an error packet sent in place of the handshake was logged: %s", logged.String())
	}
	write([]byte{2, 0, 0, 0, 11, 'x'}) // a protocol this code does not know
	if !strings.Contains(logged.String(), "level=WARN") || !strings.Contains(logged.String(), handshakeNotSetWarning) {
		t.Fatalf("the unrecognised handshake was not logged as a warning with its text; log: %q", logged.String())
	}
	for _, want := range []string{"handshake was not recognised", "autocommit is off", "logged once"} {
		if !strings.Contains(handshakeNotSetWarning, want) {
			t.Errorf("the warning no longer says %q: %s", want, handshakeNotSetWarning)
		}
	}
	write([]byte{2, 0, 0, 0, 11, 'x'})
	if n := strings.Count(logged.String(), "level=WARN"); n != 1 {
		t.Errorf("logged %d times, want once per process", n)
	}
}

// A PING on a routed connection is the source's to answer: the port only
// remembers the last status it was sent, and a statement the source refused
// (an error packet, no status) can have opened a transaction since.
func TestStatus_routedPingAsksTheSource(t *testing.T) {
	r := &fakeRouter{sessStatus: stNone, sessKnown: true}
	r.onPing = func() { r.sessStatus = stTrans } // what the source's OK says
	addr := servePort(t, "", func() server.Handler {
		h := NewHandler(nil, nil)
		h.BindFreeSQL(&fakeFreeSQL{})
		h.BindRouter(r, RouterConfig{MaxCopyAge: time.Hour, ReadOnly: true})
		return h
	})
	c := dialPort(t, addr)
	rep, err := c.Ping()
	if err != nil {
		t.Fatal(err)
	}
	if r.pings != 1 {
		t.Fatalf("the source was sent %d PINGs, want 1", r.pings)
	}
	wantStatus(t, "PING", rep.Status, stTrans)

	// A PING the source does not answer is the loss of the session.
	r.pingErr = mysql.NewError(readrouter.CodeUpstreamLost, "MySQL server has gone away")
	if _, err := c.Ping(); !isWireError(err, readrouter.CodeUpstreamLost) {
		t.Errorf("PING with the source gone: err = %v, want error %d", err, readrouter.CodeUpstreamLost)
	}
}

func isWireError(err error, code uint16) bool {
	var me *mysqlwire.Error
	return errors.As(err, &me) && me.Code == code
}

// Once the session on the source is lost, every command answers with that
// loss, the ones the port would answer itself included, and nothing more is
// asked of the source.
func TestStatus_routedLostAnswersEveryCommand(t *testing.T) {
	r := &fakeRouter{sessStatus: stAutoTrans, sessKnown: true, inTxn: true}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	var observed []string
	addr := servePort(t, "", func() server.Handler {
		h := NewHandler(nil, nil)
		h.BindFreeSQL(f)
		h.BindRouter(r, RouterConfig{MaxCopyAge: time.Hour, Observe: func(side RouteSide, reason RouteReason) {
			observed = append(observed, string(side)+"/"+string(reason))
		}})
		return h
	})
	c := dialPort(t, addr)
	id, _, err := c.Prepare("SELECT side FROM t WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec("SELECT side FROM t WHERE id = 1"); err != nil {
		t.Fatal(err)
	}

	r.lostErr = mysql.NewError(readrouter.CodeUpstreamLost, "MySQL server has gone away (the port's connection to the source was lost: test); reconnect to continue")
	r.sessKnown, r.inTxn = false, false // what a real lost connection reports
	forwarded, pings, useDBs, copyRuns, executed := len(r.forwarded), r.pings, len(r.useDBs), f.calls, len(r.prepared[0].executed)
	observed = nil

	lost := func(what string, err error) {
		t.Helper()
		if !isWireError(err, readrouter.CodeUpstreamLost) {
			t.Errorf("%s: err = %v, want error %d", what, err, readrouter.CodeUpstreamLost)
		}
	}
	_, err = c.Ping()
	lost("PING", err)
	for _, q := range []string{"SELECT side FROM t WHERE id = 1", "SHOW WARNINGS", "USE shop", "COMMIT", "SELECT 1"} {
		_, err := c.Exec(q)
		lost(q, err)
	}
	_, err = c.InitDB("shop")
	lost("COM_INIT_DB", err)
	_, err = c.FieldList("t")
	lost("COM_FIELD_LIST", err)
	_, _, err = c.Prepare("SELECT 1")
	lost("COM_STMT_PREPARE", err)
	_, err = c.Execute(id)
	lost("COM_STMT_EXECUTE", err)
	_, err = c.ResetStmt(id)
	lost("COM_STMT_RESET", err)

	if len(r.forwarded) != forwarded || r.pings != pings || len(r.useDBs) != useDBs || f.calls != copyRuns || len(r.prepared[0].executed) != executed || len(r.prepared) != 1 {
		t.Errorf("a lost connection still asked the source or the copy for something: forwarded %d->%d, pings %d->%d, USE %d->%d, copy %d->%d",
			forwarded, len(r.forwarded), pings, r.pings, useDBs, len(r.useDBs), copyRuns, f.calls)
	}
	// Seven statements (five texts, a PREPARE, an EXECUTE): each counted
	// once as one nobody answered. The other commands are not statements.
	if want := strings.TrimSuffix(strings.Repeat("mysql/upstream_lost,", 7), ","); strings.Join(observed, ",") != want {
		t.Errorf("observed %v, want seven mysql/upstream_lost", observed)
	}
	// Closing a statement has no answer, and still frees it.
	if err := c.CloseStmt(id); err != nil {
		t.Fatal(err)
	}
	_, err = c.Ping()
	lost("PING after COM_STMT_CLOSE", err)
	if r.prepared[0].closed != 1 {
		t.Errorf("the statement was closed %d times, want 1", r.prepared[0].closed)
	}
}

// A connection that is not routed has no session on the source to lose or to
// ping: the port answers.
func TestHandler_noRouterHasNoSourceSession(t *testing.T) {
	h := NewHandler(nil, nil)
	if err := h.SourceLost(true); err != nil {
		t.Errorf("SourceLost = %v", err)
	}
	if err := h.PingSource(); err != nil {
		t.Errorf("PingSource = %v", err)
	}
}
