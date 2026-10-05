package readrouter

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/dbtrail/dbtrail/internal/config"
)

// statusSource is a source whose session state the test moves: each
// statement's answer carries the status the handler set on the connection, as
// MySQL's does.
type statusSource struct {
	server.EmptyHandler
	conn   *server.Conn
	onPing func()
}

// HandleOtherCommand is never reached by COM_PING (the library answers it),
// so the pings are counted where the test can see them: see serve.
func (h *statusSource) ping() {
	if h.onPing != nil {
		h.onPing()
	}
}

// Prepared statements without parameters, run as their text.
func (h *statusSource) HandleStmtPrepare(string) (int, int, any, error) { return 0, 0, nil, nil }
func (h *statusSource) HandleStmtClose(any) error                       { return nil }
func (h *statusSource) HandleStmtExecute(_ any, query string, _ []any) (*mysql.Result, error) {
	return h.HandleQuery(query)
}

func (h *statusSource) set(status uint16) {
	h.conn.UnsetStatus(^uint16(0))
	h.conn.SetStatus(status)
}

func (h *statusSource) HandleQuery(q string) (*mysql.Result, error) {
	const auto, trans = mysql.SERVER_STATUS_AUTOCOMMIT, mysql.SERVER_STATUS_IN_TRANS
	switch q {
	case "BEGIN":
		h.set(auto | trans)
	case "START TRANSACTION READ ONLY":
		h.set(auto | trans | mysql.SERVER_STATUS_IN_TRANS_READONLY)
	case "COMMIT":
		h.set(auto)
	case "SET autocommit=0":
		h.set(0)
	case "SET sql_mode='NO_BACKSLASH_ESCAPES'":
		h.set(auto | mysql.SERVER_STATUS_NO_BACKSLASH_ESCAPED)
	case "INSERT fails and opens a transaction":
		// What a duplicate key does with autocommit off: an error packet,
		// which has no status, and a transaction left open.
		h.conn.SetStatus(trans)
		return nil, mysql.NewError(mysql.ER_DUP_ENTRY, "Duplicate entry")
	case "SELECT nope":
		return nil, mysql.NewError(mysql.ER_BAD_FIELD_ERROR, "Unknown column 'nope'")
	case "SELECT 1":
		rs, err := mysql.BuildSimpleTextResultset([]string{"1"}, [][]any{{int64(1)}})
		if err != nil {
			return nil, err
		}
		// A flag about the statement, which is not the session's. It stays
		// on this fake's connection until a statement sets the state anew.
		h.conn.SetStatus(mysql.SERVER_STATUS_NO_INDEX_USED)
		return &mysql.Result{Resultset: rs}, nil
	}
	return mysql.NewResultReserveResultset(0), nil
}

// pingCounter sees the commands the source reads: a COM_PING is one packet of
// one byte, 0x0e.
type pingCounter struct {
	net.Conn
	h    *statusSource
	live bool
	head []byte
}

func (p *pingCounter) Read(b []byte) (int, error) {
	n, err := p.Conn.Read(b)
	if p.live {
		p.head = append(p.head, b[:n]...)
		for len(p.head) >= 4 {
			size := int(p.head[0]) | int(p.head[1])<<8 | int(p.head[2])<<16
			if len(p.head) < 4+size {
				break
			}
			if size == 1 && p.head[4] == mysql.COM_PING {
				p.h.ping()
			}
			p.head = p.head[4+size:]
		}
	}
	return n, err
}

// statusAtLogin makes the source announce a session state in the OK that ends
// the login, as MySQL does for a session its configuration opens that way.
type statusAtLogin struct {
	server.AuthenticationHandler
	status uint16
}

func (a statusAtLogin) OnAuthSuccess(c *server.Conn) error {
	c.SetStatus(a.status)
	return a.AuthenticationHandler.OnAuthSuccess(c)
}

// newStatusSource starts a source whose sessions open in the state atLogin.
func newStatusSource(t *testing.T, atLogin uint16) string {
	return newStatusSourceWith(t, atLogin, nil)
}

// newStatusSourceWith also hands each session's handler to prepare.
func newStatusSourceWith(t *testing.T, atLogin uint16, prepare func(*statusSource)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	conf := server.NewServer("8.0.11", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, nil, nil)
	auth := server.NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	if err := auth.AddUser("u", "p"); err != nil {
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
				h := &statusSource{}
				pc := &pingCounter{Conn: c, h: h}
				mc, err := server.NewCustomizedConn(pc, conf, statusAtLogin{auth, atLogin}, h)
				if err != nil {
					return
				}
				h.conn = mc
				if prepare != nil {
					prepare(h)
				}
				pc.live = true
				for mc.HandleCommand() == nil {
				}
			}()
		}
	}()
	return ln.Addr().String()
}

// Status is the source session's state as the source's own packets last
// reported it (#2110): what the port tells its client about the session.
func TestForwarder_Status(t *testing.T) {
	const (
		auto     = mysql.SERVER_STATUS_AUTOCOMMIT
		trans    = mysql.SERVER_STATUS_IN_TRANS
		readOnly = mysql.SERVER_STATUS_IN_TRANS_READONLY
		noBack   = mysql.SERVER_STATUS_NO_BACKSLASH_ESCAPED
	)
	f, err := NewForwarder("u:p@tcp("+newStatusSource(t, auto)+")/db?tls=false", config.SSL{Mode: "disabled"}, DefaultPolicy(), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if st, known := f.Status(); known {
		t.Fatalf("before the connection is opened: status 0x%04x reported as known", st)
	}
	ctx := context.Background()
	for _, step := range []struct {
		stmt    string
		want    uint16
		wantErr bool
		inTrans bool
	}{
		// The source's answer also says "no index used": a flag about the
		// statement, not about the session.
		{stmt: "SELECT 1", want: auto},
		{stmt: "BEGIN", want: auto | trans, inTrans: true},
		{stmt: "SELECT 1", want: auto | trans, inTrans: true},
		// An error packet has no status: the state is the one before it.
		{stmt: "SELECT nope", want: auto | trans, wantErr: true, inTrans: true},
		{stmt: "COMMIT", want: auto},
		{stmt: "START TRANSACTION READ ONLY", want: auto | trans | readOnly, inTrans: true},
		{stmt: "COMMIT", want: auto},
		{stmt: "SET autocommit=0", want: 0},
		{stmt: "SET sql_mode='NO_BACKSLASH_ESCAPES'", want: auto | noBack},
	} {
		_, err := f.Forward(ctx, step.stmt, &BufferSink{})
		if (err != nil) != step.wantErr {
			t.Fatalf("%s: err = %v", step.stmt, err)
		}
		st, known := f.Status()
		if !known || st != step.want {
			t.Errorf("after %s: status 0x%04x (known %v), want 0x%04x", step.stmt, st, known, step.want)
		}
		if f.InTransaction() != step.inTrans {
			t.Errorf("after %s: InTransaction() = %v", step.stmt, f.InTransaction())
		}
	}

	f.lose(context.Canceled)
	if st, known := f.Status(); known {
		t.Errorf("after the connection is lost: status 0x%04x reported as known", st)
	}
}

// A source that opens its sessions in another state than "autocommit" says so
// in the OK that ends the login. The port must report it from the first
// answer after the connection is opened, whatever that first statement is:
// a driver escapes its strings by the NO_BACKSLASH_ESCAPES flag, and a
// statement the source refuses carries no status of its own.
func TestForwarder_Status_fromTheLogin(t *testing.T) {
	const atLogin = mysql.SERVER_STATUS_NO_BACKSLASH_ESCAPED // and autocommit off
	f, err := NewForwarder("u:p@tcp("+newStatusSource(t, atLogin)+")/db?tls=false", config.SSL{Mode: "disabled"}, DefaultPolicy(), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Forward(context.Background(), "SELECT nope", &BufferSink{}); err == nil {
		t.Fatal("the statement did not fail")
	}
	if st, known := f.Status(); !known || st != atLogin {
		t.Errorf("after a first statement the source refused: status 0x%04x (known %v), want 0x%04x", st, known, atLogin)
	}
}

// A connection interrupted from another goroutine (the query deadline, the
// client hanging up) is marked lost before its statement's goroutine lets go
// of it. When that lands just as a statement succeeded, the statement's own
// answer still carries the flags the source sent with it (Status), and every
// command after it is refused (Lost): no successful answer is ever stamped
// "autocommit, no transaction" for a session that was killed mid-transaction.
func TestForwarder_Status_interrupted(t *testing.T) {
	f, err := NewForwarder("u:p@tcp("+newStatusSource(t, mysql.SERVER_STATUS_AUTOCOMMIT)+")/db?tls=false", config.SSL{Mode: "disabled"}, DefaultPolicy(), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Forward(context.Background(), "BEGIN", &BufferSink{}); err != nil {
		t.Fatal(err)
	}
	if st, known := f.Status(); !known || st&mysql.SERVER_STATUS_IN_TRANS == 0 {
		t.Fatalf("in a transaction: status 0x%04x (known %v)", st, known)
	}
	if err := f.Lost(); err != nil {
		t.Fatalf("a live connection reports a loss: %v", err)
	}
	f.interrupt(context.DeadlineExceeded)
	if st, known := f.Status(); !known || st&mysql.SERVER_STATUS_IN_TRANS == 0 {
		t.Errorf("the answer of the statement the interrupt raced with: status 0x%04x (known %v), want the source's last flags (in a transaction)", st, known)
	}
	if err := f.Lost(); !IsLost(err) {
		t.Errorf("Lost() after an interrupt = %v, want the lost error", err)
	}
	if err := f.Ping(context.Background()); !IsLost(err) {
		t.Errorf("Ping after an interrupt = %v, want the lost error", err)
	}
	// Once the statement's goroutine lets go of the connection there is no
	// session at all.
	f.lose(context.DeadlineExceeded)
	if st, known := f.Status(); known {
		t.Errorf("after the connection is let go of: status 0x%04x reported as known", st)
	}
}

// A PING is the source's to answer once there is a session on it: it tells
// the session's state as it is NOW. After a statement the source refused, an
// error packet with no status, the port only remembers the state from before
// (here: autocommit off, no transaction, while the refused INSERT opened one).
func TestForwarder_Ping(t *testing.T) {
	const auto, trans = mysql.SERVER_STATUS_AUTOCOMMIT, mysql.SERVER_STATUS_IN_TRANS
	var pinged, sessions atomic.Int32
	addr := newStatusSourceWith(t, auto, func(h *statusSource) {
		sessions.Add(1)
		h.onPing = func() { pinged.Add(1) }
	})
	f, err := NewForwarder("u:p@tcp("+addr+")/db?tls=false", config.SSL{Mode: "disabled"}, DefaultPolicy(), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	ctx := context.Background()

	// No session yet: nothing is sent, nothing is opened.
	if err := f.Ping(ctx); err != nil {
		t.Fatalf("Ping before the connection is opened: %v", err)
	}
	if sessions.Load() != 0 || pinged.Load() != 0 {
		t.Fatalf("a PING opened a connection to the source (%d sessions, %d pings)", sessions.Load(), pinged.Load())
	}
	if _, known := f.Status(); known {
		t.Fatal("a PING made a session known")
	}

	for _, q := range []string{"SET autocommit=0", "INSERT fails and opens a transaction"} {
		_, _ = f.Forward(ctx, q, &BufferSink{})
	}
	if st, _ := f.Status(); st != 0 {
		t.Fatalf("premise: after the refused INSERT the remembered status is 0x%04x, want 0x0000", st)
	}
	if err := f.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if pinged.Load() != 1 {
		t.Fatalf("the source saw %d pings, want 1", pinged.Load())
	}
	if st, known := f.Status(); !known || st != trans {
		t.Errorf("after the PING: status 0x%04x (known %v), want 0x%04x, what the source answered", st, known, trans)
	}

	// The source going away is found by the PING, and stays found.
	f.mu.Lock()
	raw := f.raw
	f.mu.Unlock()
	raw.Close()
	if err := f.Ping(ctx); !IsLost(err) {
		t.Fatalf("Ping on a dead socket = %v, want the lost error", err)
	}
	if err := f.Lost(); !IsLost(err) {
		t.Errorf("Lost() = %v after a failed PING", err)
	}
}

// OnSession is told once how the source opened the session, with the
// source's first answer to a statement: an init_connect runs after the login
// is answered, so the login's own status does not show what it did.
func TestForwarder_OnSession(t *testing.T) {
	const auto, noBack = mysql.SERVER_STATUS_AUTOCOMMIT, mysql.SERVER_STATUS_NO_BACKSLASH_ESCAPED
	open := func(t *testing.T, atLogin uint16, initConnect func(*statusSource)) (*Forwarder, *[]uint16) {
		t.Helper()
		f, err := NewForwarder("u:p@tcp("+newStatusSourceWith(t, atLogin, initConnect)+")/db?tls=false", config.SSL{Mode: "disabled"}, DefaultPolicy(), 10*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(f.Close)
		var got []uint16
		f.OnSession = func(st uint16) { got = append(got, st) }
		return f, &got
	}
	ctx := context.Background()

	t.Run("a state the login announces", func(t *testing.T) {
		f, got := open(t, noBack, nil)
		// A refused statement carries no status: nothing is told yet.
		if _, err := f.Forward(ctx, "SELECT nope", &BufferSink{}); err == nil {
			t.Fatal("the statement did not fail")
		}
		if len(*got) != 0 {
			t.Fatalf("told after a statement the source refused: %#v", *got)
		}
		for range 2 {
			if _, err := f.Forward(ctx, "DO 1", &BufferSink{}); err != nil {
				t.Fatal(err)
			}
		}
		if len(*got) != 1 || (*got)[0] != noBack {
			t.Errorf("OnSession calls = %#v, want one, with 0x%04x", *got, noBack)
		}
	})

	t.Run("a state an init_connect sets after the login", func(t *testing.T) {
		// The login is answered "autocommit"; the session is then set to
		// autocommit off, which the first answer shows.
		f, got := open(t, auto, func(h *statusSource) { h.set(0) })
		if _, err := f.Forward(ctx, "DO 1", &BufferSink{}); err != nil {
			t.Fatal(err)
		}
		if len(*got) != 1 || (*got)[0] != 0 {
			t.Errorf("OnSession calls = %#v, want one, with 0x0000 (autocommit off)", *got)
		}
	})

	t.Run("a first statement that is SET NAMES", func(t *testing.T) {
		// What PyMySQL sends first: it changes neither flag.
		f, got := open(t, auto, func(h *statusSource) { h.set(0) })
		if _, err := f.Forward(ctx, "SET NAMES utf8mb4", &BufferSink{}); err != nil {
			t.Fatal(err)
		}
		if len(*got) != 1 || (*got)[0] != 0 {
			t.Errorf("OnSession calls = %#v, want one, with 0x0000", *got)
		}
	})

	t.Run("a first statement that is a SET of autocommit", func(t *testing.T) {
		// The state after it is the client's own doing, not the source's
		// default; nothing is told, then or later.
		f, got := open(t, auto, nil)
		for _, q := range []string{"SET autocommit=0", "DO 1"} {
			if _, err := f.Forward(ctx, q, &BufferSink{}); err != nil {
				t.Fatal(err)
			}
		}
		if len(*got) != 0 {
			t.Errorf("OnSession was told %#v about a state the client set itself", *got)
		}
	})

	t.Run("a prepared statement first", func(t *testing.T) {
		f, got := open(t, auto, func(h *statusSource) { h.set(0) })
		st, err := f.Prepare(ctx, "DO 1")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.Execute(ctx, nil, &BufferSink{}); err != nil {
			t.Fatal(err)
		}
		if len(*got) != 1 || (*got)[0] != 0 {
			t.Errorf("OnSession calls = %#v, want one, with 0x0000", *got)
		}
	})
}
