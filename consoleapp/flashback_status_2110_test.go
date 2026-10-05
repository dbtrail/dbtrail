package consoleapp

import (
	"context"
	"net"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/shim"
	"github.com/dbtrail/dbtrail/internal/testutil/mysqlwire"
)

// The embedded port's handshake and the OK that ends authentication announce
// autocommit (#2110), through the port's own accept path. Before, both said
// status 0, and a driver that trusts them (PyMySQL) concluded autocommit was
// off and never turned it off on the source.
func TestFlashbackPortAnnouncesAutocommit(t *testing.T) {
	srv := newFlashbackConsole(t, "tok")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveFlashback(ctx, srv, ln, flashbackConfig{}) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Error("serveFlashback did not return")
		}
	}()

	c, err := mysqlwire.Dial(ln.Addr().String(), "no-such-server", "tok", "")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	const auto = mysqlwire.StatusAutocommit
	if c.Handshake.Status != auto {
		t.Errorf("handshake status 0x%04x, want 0x%04x (autocommit)", c.Handshake.Status, auto)
	}
	if c.AuthStatus != auto {
		t.Errorf("status of the OK that ends authentication 0x%04x, want 0x%04x", c.AuthStatus, auto)
	}
	// No server is bound to this connection (the name is unknown): the port
	// still answers PING, as a session that has run nothing.
	rep, err := c.Ping()
	if err != nil {
		t.Fatal(err)
	}
	if rep.Status != auto {
		t.Errorf("PING status 0x%04x, want 0x%04x", rep.Status, auto)
	}
}

// statusRouter is a shim.Router that only reports a session state.
type statusRouter struct {
	shim.Router
	status uint16
}

func (r statusRouter) Status() (uint16, bool) { return r.status, true }
func (statusRouter) Close()                   {}

// The proxy the port hands go-mysql before the server is known reports the
// bound handler's session state, and a new session's while none is bound.
func TestRoutingHandlerSessionStatus(t *testing.T) {
	r := &routingHandler{}
	if got := r.SessionStatus(); got != gomysql.SERVER_STATUS_AUTOCOMMIT {
		t.Errorf("nothing bound: status 0x%04x, want autocommit", got)
	}
	h := shim.NewHandler(nil, nil)
	const inTransaction = gomysql.SERVER_STATUS_IN_TRANS
	h.BindRouter(statusRouter{status: inTransaction}, shim.RouterConfig{})
	r.inner = h
	if got := r.SessionStatus(); got != inTransaction {
		t.Errorf("bound to a session in a transaction with autocommit off: status 0x%04x, want 0x%04x", got, inTransaction)
	}
}
