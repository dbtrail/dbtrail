package console

import (
	"net"
	"strings"
	"sync"
	"testing"
)

// Test connection is open to servers:read (authz.go), and the body picks the
// destination. The stored password must reach only the stored server's own
// host and user: a changed host with the password omitted would otherwise
// forward the saved credential to a server the caller chose, where a hostile
// MySQL endpoint captures it during the handshake.

// acceptCounter is a TCP listener that records how many connections reached
// it. It answers nothing, so the driver handshake fails fast — the point is
// only whether a connection was attempted at all.
type acceptCounter struct {
	ln   net.Listener
	mu   sync.Mutex
	n    int
	host string
	port string
}

func newAcceptCounter(t *testing.T) *acceptCounter {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a := &acceptCounter{ln: ln}
	a.host, a.port, _ = net.SplitHostPort(ln.Addr().String())
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			a.mu.Lock()
			a.n++
			a.mu.Unlock()
			c.Close()
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return a
}

func (a *acceptCounter) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.n
}

func TestServersTest_storedPasswordStaysOnItsOwnDestination(t *testing.T) {
	clearStores(t)
	srv := newRegistryServer(t)
	evil := newAcceptCounter(t)
	// A stored server with a password, pointed at a dead local port so an
	// allowed probe fails fast instead of dialing a real host.
	dead := newAcceptCounter(t)
	dead.ln.Close()
	e, err := srv.cm.reg.Add(ServerEntry{
		Name: "prod",
		DSN:  "forensics:s3cr3tPW@tcp(" + dead.host + ":" + dead.port + ")/binlog_index",
	})
	if err != nil {
		t.Fatal(err)
	}
	path := "/api/servers/" + e.ID + "/test"

	// Refused: a changed host, port or user with the password omitted, and
	// the chosen host is never contacted.
	for name, body := range map[string]string{
		"host": `{"host":"` + evil.host + `","port":"` + evil.port + `"}`,
		"port": `{"port":"` + evil.port + `"}`,
		"user": `{"user":"root"}`,
	} {
		rec, out := doServersReq(t, srv, "POST", path, body)
		if rec.Code != 400 {
			t.Errorf("%s changed, password omitted: %d %s, want 400", name, rec.Code, out)
		}
		if !strings.Contains(string(out), "re-enter the password") {
			t.Errorf("%s: refusal does not tell the operator to re-enter the password: %s", name, out)
		}
		if strings.Contains(string(out), "s3cr3tPW") {
			t.Errorf("%s: the refusal echoes the password: %s", name, out)
		}
	}
	if evil.count() != 0 {
		t.Errorf("the stored password's probe reached the chosen host %d time(s)", evil.count())
	}

	// Allowed: same host and user, only the database name changed. The
	// password still goes where it already went.
	if rec, out := doServersReq(t, srv, "POST", path, `{"dbname":"other_index"}`); rec.Code == 400 {
		t.Errorf("a dbname-only change was refused: %s", out)
	}
	// Allowed: empty body tests the stored server as-is.
	if rec, out := doServersReq(t, srv, "POST", path, ``); rec.Code == 400 {
		t.Errorf("an empty body was refused: %s", out)
	}
	// Allowed: the operator supplies the password for the new host. It is
	// their own credential to send.
	if rec, out := doServersReq(t, srv, "POST", path,
		`{"host":"`+evil.host+`","port":"`+evil.port+`","password":"typedbyhand"}`); rec.Code == 400 {
		t.Errorf("a typed password for a new host was refused: %s", out)
	}
	// Allowed: a raw dsn carries its own password; the stored one is never
	// merged into it, so nothing to protect.
	if rec, out := doServersReq(t, srv, "POST", path,
		`{"dsn":"u:p@tcp(`+evil.host+`:`+evil.port+`)/db"}`); rec.Code == 400 {
		t.Errorf("a raw dsn to a new host was refused: %s", out)
	}
}

// A stored server with no password has nothing to forward, so a changed host
// is not refused.
func TestServersTest_passwordlessStoredServerIsNotGuarded(t *testing.T) {
	clearStores(t)
	srv := newRegistryServer(t)
	evil := newAcceptCounter(t)
	e, err := srv.cm.reg.Add(ServerEntry{Name: "nopw", DSN: "u@tcp(127.0.0.1:1)/db"})
	if err != nil {
		t.Fatal(err)
	}
	if rec, out := doServersReq(t, srv, "POST", "/api/servers/"+e.ID+"/test",
		`{"host":"`+evil.host+`","port":"`+evil.port+`"}`); rec.Code == 400 {
		t.Errorf("a passwordless stored server was guarded: %s", out)
	}
}
