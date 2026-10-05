package readrouter

import (
	"context"
	"net"
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
	conn *server.Conn
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

func newStatusSource(t *testing.T) string {
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
				mc, err := server.NewCustomizedConn(c, conf, auth, h)
				if err != nil {
					return
				}
				h.conn = mc
				// This source opens its sessions in autocommit.
				h.set(mysql.SERVER_STATUS_AUTOCOMMIT)
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
	f, err := NewForwarder("u:p@tcp("+newStatusSource(t)+")/db?tls=false", config.SSL{Mode: "disabled"}, DefaultPolicy(), 10*time.Second)
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

	// A read-only-transaction flag left from an answer seen earlier does not
	// outlive the transaction: the connection's own "in a transaction" is the
	// fresher of the two.
	f.mu.Lock()
	f.lastStatus = auto | trans | readOnly
	f.mu.Unlock()
	if st, _ := f.Status(); st != auto {
		t.Errorf("stale flags of an ended transaction: status 0x%04x, want 0x%04x", st, auto)
	}

	f.lose(context.Canceled)
	if st, known := f.Status(); known {
		t.Errorf("after the connection is lost: status 0x%04x reported as known", st)
	}
}
