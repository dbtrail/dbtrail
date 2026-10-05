package consoleapp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
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
	status        uint16
	lost, pingErr error
	pings         *int
}

func (r statusRouter) Status() (uint16, bool) { return r.status, true }
func (r statusRouter) Lost() error            { return r.lost }
func (r statusRouter) Ping(context.Context) error {
	*r.pings++
	return r.pingErr
}
func (statusRouter) Close() {}

// The proxy the port hands go-mysql before the server is known reports the
// bound handler's session state, and a new session's while none is bound.
func TestRoutingHandlerSessionStatus(t *testing.T) {
	r := &routingHandler{}
	if got := r.SessionStatus(); got != gomysql.SERVER_STATUS_AUTOCOMMIT {
		t.Errorf("nothing bound: status 0x%04x, want autocommit", got)
	}
	h := shim.NewHandler(nil, nil)
	const inTransaction = gomysql.SERVER_STATUS_IN_TRANS
	if err := r.SourceLost(true); err != nil {
		t.Errorf("nothing bound: SourceLost = %v", err)
	}
	if err := r.PingSource(); err != nil {
		t.Errorf("nothing bound: PingSource = %v", err)
	}
	lost, refused := errors.New("the session is lost"), errors.New("the ping failed")
	pings := 0
	h.BindRouter(statusRouter{status: inTransaction, lost: lost, pingErr: refused, pings: &pings}, shim.RouterConfig{})
	r.inner = h
	// The loss and the PING are the bound handler's too: the Session asks
	// the proxy, never the handler behind it.
	if err := r.SourceLost(false); !errors.Is(err, lost) {
		t.Errorf("SourceLost = %v, want the bound handler's", err)
	}
	if err := r.PingSource(); !errors.Is(err, refused) || pings != 1 {
		t.Errorf("PingSource = %v after %d pings, want the bound handler's, after 1", err, pings)
	}
	if got := r.SessionStatus(); got != inTransaction {
		t.Errorf("bound to a session in a transaction with autocommit off: status 0x%04x, want 0x%04x", got, inTransaction)
	}
}

// A source that opens its sessions in another state than the port's
// handshake announces is said in the log, once per server: the handshake
// cannot know the server, and PyMySQL decides from it once, for good.
func TestWarnSessionDefaults(t *testing.T) {
	const auto, noBackslash = gomysql.SERVER_STATUS_AUTOCOMMIT, gomysql.SERVER_STATUS_NO_BACKSLASH_ESCAPED
	if d, c := sessionDefaultsDiffer(auto); d != "" || c != "" {
		t.Errorf("a session in autocommit differs: %q, %q", d, c)
	}
	// In a transaction or not is not a default.
	if d, _ := sessionDefaultsDiffer(auto | gomysql.SERVER_STATUS_IN_TRANS); d != "" {
		t.Errorf("a flag that is not a default differs: %q", d)
	}
	for _, tc := range []struct {
		status uint16
		diff   string
		says   []string
	}{
		{0, "autocommit off", []string{"SET autocommit", "discarded", "PyMySQL"}},
		{auto | noBackslash, "NO_BACKSLASH_ESCAPES in sql_mode", []string{"escapes", "prepared statements"}},
		{noBackslash, "autocommit off and NO_BACKSLASH_ESCAPES in sql_mode", []string{"SET autocommit", "prepared statements"}},
	} {
		d, c := sessionDefaultsDiffer(tc.status)
		if d != tc.diff {
			t.Errorf("status 0x%04x: difference %q, want %q", tc.status, d, tc.diff)
		}
		for _, want := range tc.says {
			if !strings.Contains(c, want) {
				t.Errorf("status 0x%04x: the consequence does not mention %q: %s", tc.status, want, c)
			}
		}
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	// The once-per-server memory is the process's: an id of this run's own.
	id := fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
	warnSessionDefaults(logger, id, "srva", auto)
	if buf.Len() != 0 {
		t.Fatalf("a source whose sessions open as announced was logged: %s", buf.String())
	}
	for range 3 {
		warnSessionDefaults(logger, id, "srva", 0)
	}
	out := buf.String()
	if strings.Count(out, "level=WARN") != 1 {
		t.Fatalf("logged %d times for one server, want once:\n%s", strings.Count(out, "level=WARN"), out)
	}
	for _, want := range []string{"server=srva", "autocommit off", "SET autocommit", "logged once per server"} {
		if !strings.Contains(out, want) {
			t.Errorf("the warning does not say %q:\n%s", want, out)
		}
	}
	// Another server, and another difference on the same one, are told too.
	warnSessionDefaults(logger, id+"-other", "srvb", 0)
	warnSessionDefaults(logger, id, "srva", auto|noBackslash)
	if n := strings.Count(buf.String(), "level=WARN"); n != 3 {
		t.Errorf("logged %d times, want 3 (two servers, two differences)", n)
	}
}
