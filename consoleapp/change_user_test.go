package consoleapp

import (
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"net"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/client"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/shim"
)

// COM_CHANGE_USER and COM_RESET_CONNECTION on the router's port. A proxy such
// as ProxySQL keeps a pool of connections to each backend and hands one
// opened for one application to another by changing its user on it; the port
// answered "command 17 is not supported now", which reached the application
// as an error. The client library has no change-user call, so the packets are
// written here by hand, which also lets the tests send broken ones.

// changeUserResult is what the port answered a change-user: 0 for OK, else
// the MySQL error code; closed when the connection ended instead.
type changeUserResult struct {
	code   uint16
	msg    string
	closed bool
}

// changeUser sends COM_CHANGE_USER for user/password/schema on c and answers
// the port's auth switch the way libmysqlclient does. oldAuth is what the
// client puts in the packet itself, scrambled with the handshake's salt; the
// port must ignore it and check the answer to its own fresh salt.
func changeUser(t *testing.T, c *client.Conn, user, password, schema string) changeUserResult {
	t.Helper()
	pkt := []byte{0, 0, 0, 0, gomysql.COM_CHANGE_USER}
	pkt = append(pkt, user...)
	pkt = append(pkt, 0)
	oldAuth := []byte("scrambled-with-the-old-salt")
	pkt = append(pkt, byte(len(oldAuth)))
	pkt = append(pkt, oldAuth...)
	pkt = append(pkt, schema...)
	pkt = append(pkt, 0)
	pkt = append(pkt, 0x21, 0x00) // utf8_general_ci
	pkt = append(pkt, gomysql.AUTH_NATIVE_PASSWORD...)
	pkt = append(pkt, 0)
	return sendChangeUser(t, c, pkt, password)
}

func sendChangeUser(t *testing.T, c *client.Conn, pkt []byte, password string) changeUserResult {
	t.Helper()
	c.ResetSequence()
	if err := c.WritePacket(pkt); err != nil {
		return changeUserResult{closed: true}
	}
	for {
		resp, err := c.ReadPacket()
		if err != nil {
			return changeUserResult{closed: true}
		}
		switch resp[0] {
		case gomysql.OK_HEADER:
			return changeUserResult{}
		case gomysql.ERR_HEADER:
			return changeUserResult{code: binary.LittleEndian.Uint16(resp[1:3]), msg: string(resp[3:])}
		case gomysql.EOF_HEADER: // auth switch request: plugin NUL salt NUL
			plugin, rest, ok := strings.Cut(string(resp[1:]), "\x00")
			if !ok {
				t.Fatalf("auth switch without a plugin: %q", resp)
			}
			salt := []byte(strings.TrimSuffix(rest, "\x00"))
			var scramble []byte
			switch plugin {
			case gomysql.AUTH_NATIVE_PASSWORD:
				scramble = gomysql.CalcNativePassword(salt, []byte(password))
			case gomysql.AUTH_CACHING_SHA2_PASSWORD:
				scramble = gomysql.CalcCachingSha2Password(salt, []byte(password))
			default:
				t.Fatalf("auth switch to %q", plugin)
			}
			if err := c.WritePacket(append([]byte{0, 0, 0, 0}, scramble...)); err != nil {
				return changeUserResult{closed: true}
			}
		case gomysql.MORE_DATE_HEADER: // caching_sha2 fast auth ok: the OK follows
			continue
		default:
			t.Fatalf("unexpected answer to change user: %q", resp)
		}
	}
}

// resetConnection sends COM_RESET_CONNECTION.
func resetConnection(t *testing.T, c *client.Conn) changeUserResult {
	t.Helper()
	return sendChangeUser(t, c, []byte{0, 0, 0, 0, gomysql.COM_RESET_CONNECTION}, "")
}

// boundTo is the id of the server the connection is bound to now, as
// SHOW ROUTER STATUS names it on its "server" row.
func boundTo(t *testing.T, c *client.Conn) string {
	t.Helper()
	r, err := c.Execute("SHOW ROUTER STATUS")
	if err != nil {
		t.Fatalf("SHOW ROUTER STATUS: %v", err)
	}
	for i := range r.Values {
		if k, _ := r.GetString(i, 0); k == "server" {
			v, _ := r.GetString(i, 1)
			return v
		}
	}
	t.Fatal("SHOW ROUTER STATUS has no server row")
	return ""
}

// twoServerPort is a router port with server-a (index 127.0.0.1:1, login
// app1/pw1) and server-b (index 127.0.0.1:2, login app2/pw2), port password
// port-pw.
type twoServerPort struct {
	f    routerFiles
	addr string
	a, b console.ServerEntry
}

func newTwoServerPort(t *testing.T) twoServerPort {
	t.Helper()
	f := newRouterFiles(t)
	a, err := f.owner.Add(console.ServerEntry{Name: "server-a", DSN: "idxuser:idxsecret@tcp(127.0.0.1:1)/idx_a"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := f.owner.Add(console.ServerEntry{Name: "server-b", DSN: "idxuser:idxsecret@tcp(127.0.0.1:2)/idx_b"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.owner.SetPortLogin(a.ID, "app1", "pw1"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.owner.SetPortLogin(b.ID, "app2", "pw2"); err != nil {
		t.Fatal(err)
	}
	f.savePortFile(t, true, "port-pw")
	srv, runner, err := newRouterConsole(f.settings(""), console.SQLPool{Workers: 1, Threads: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.Close)
	addr := freeAddr(t)
	bound, stop, _ := startRouterSaying(t, srv, addr)
	t.Cleanup(stop)
	waitUntil(t, "the port to open", func() bool { return bound() != "" })
	return twoServerPort{f: f, addr: addr, a: a, b: b}
}

func (p twoServerPort) connect(t *testing.T, user, password string) *client.Conn {
	t.Helper()
	c, err := client.Connect(p.addr, user, password, "")
	if err != nil {
		t.Fatalf("connect as %s: %v", user, err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestChangeUser_toAnotherLogin(t *testing.T) {
	p := newTwoServerPort(t)
	c := p.connect(t, "app1", "pw1")
	if got := boundTo(t, c); got != p.a.ID {
		t.Fatalf("before: %s, want server-a %s", got, p.a.ID)
	}
	if r := changeUser(t, c, "app2", "pw2", ""); r.code != 0 || r.closed {
		t.Fatalf("change to app2: %+v", r)
	}
	if got := boundTo(t, c); got != p.b.ID {
		t.Fatalf("after changing to app2 the connection is bound to %s, want server-b %s", got, p.b.ID)
	}
	// And back, and to the same user again.
	if r := changeUser(t, c, "app1", "pw1", ""); r.code != 0 || r.closed {
		t.Fatalf("change back to app1: %+v", r)
	}
	if got := boundTo(t, c); got != p.a.ID {
		t.Fatalf("after changing back: %s", got)
	}
	if r := changeUser(t, c, "app1", "pw1", ""); r.code != 0 || r.closed {
		t.Fatalf("change to the same user: %+v", r)
	}
	if got := boundTo(t, c); got != p.a.ID {
		t.Fatalf("after changing to the same user: %s", got)
	}
}

// Every refusal answers 1045 and closes the connection: it is never left on
// the user it had.
func TestChangeUser_refusalsCloseTheConnection(t *testing.T) {
	p := newTwoServerPort(t)
	cases := []struct{ user, pw, why string }{
		{"app2", "wrong", "a wrong password"},
		{"nobody", "pw2", "an unknown user with a login's password"},
		{"server-b", "pw2", "a server's name with its login's password"},
		{"app2", "pw1", "a login with another login's password"},
		{"APP2", "pw2", "the login in another case"},
	}
	for _, cs := range cases {
		c := p.connect(t, "app1", "pw1")
		r := changeUser(t, c, cs.user, cs.pw, "")
		if r.code != gomysql.ER_ACCESS_DENIED_ERROR {
			t.Errorf("%s: %+v, want 1045", cs.why, r)
		}
		if err := c.Ping(); err == nil {
			t.Errorf("%s: the connection stayed open on its old user", cs.why)
		}
	}
}

func TestChangeUser_portPasswordOpensAnyName(t *testing.T) {
	p := newTwoServerPort(t)
	c := p.connect(t, "app1", "pw1")
	for _, cs := range []struct{ user, want string }{
		{"server-b", p.b.ID},
		{"app1", p.a.ID},
		{"app2", p.b.ID},
	} {
		if r := changeUser(t, c, cs.user, "port-pw", ""); r.code != 0 || r.closed {
			t.Fatalf("change to %s with the port password: %+v", cs.user, r)
		}
		if got := boundTo(t, c); got != cs.want {
			t.Fatalf("%s with the port password reads %s, want %s", cs.user, got, cs.want)
		}
	}
}

func TestChangeUser_malformedPacketsClose(t *testing.T) {
	p := newTwoServerPort(t)
	for i, pkt := range [][]byte{
		{0, 0, 0, 0, gomysql.COM_CHANGE_USER},                                                // nothing at all
		append([]byte{0, 0, 0, 0, gomysql.COM_CHANGE_USER}, "app2"...),                       // no NUL after the user
		append(append([]byte{0, 0, 0, 0, gomysql.COM_CHANGE_USER}, "app2\x00"...), 200, 'x'), // auth longer than the packet
	} {
		c := p.connect(t, "app1", "pw1")
		r := sendChangeUser(t, c, pkt, "pw2")
		if r.code == 0 && !r.closed {
			t.Errorf("malformed packet %d accepted", i)
		}
		if err := c.Ping(); err == nil {
			t.Errorf("malformed packet %d left the connection open", i)
		}
	}
}

// What the old user had does not survive: its prepared statements are gone,
// and the old login's revocation no longer concerns the connection, while
// the new login's does.
func TestChangeUser_stateAndRevocation(t *testing.T) {
	p := newTwoServerPort(t)
	c := p.connect(t, "app1", "pw1")
	st, err := c.Prepare("SELECT ?")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if r := changeUser(t, c, "app2", "pw2", ""); r.code != 0 || r.closed {
		t.Fatalf("change to app2: %+v", r)
	}
	if _, err := st.Execute(1); !isMySQLCode(err, gomysql.ER_UNKNOWN_STMT_HANDLER) {
		t.Errorf("a statement prepared by the old user, after the change: %v, want unknown statement (1243)", err)
	}
	// Revoking app1 (the old login) leaves the connection alone.
	if _, err := p.f.owner.SetPortLogin(p.a.ID, "", ""); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "app1's removal to reach the router", func() bool { return loginAs(t, p.addr, "app1", "pw1") != 0 })
	if err := c.Ping(); err != nil {
		t.Fatalf("revoking the OLD login closed the changed connection: %v", err)
	}
	// Revoking app2 (the current one) closes it.
	if _, err := p.f.owner.SetPortLogin(p.b.ID, "", ""); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "the connection to close", func() bool { return c.Ping() != nil })
}

func TestResetConnection_keepsTheUser(t *testing.T) {
	p := newTwoServerPort(t)
	c := p.connect(t, "app2", "pw2")
	st, err := c.Prepare("SELECT ?")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if r := resetConnection(t, c); r.code != 0 || r.closed {
		t.Fatalf("reset: %+v", r)
	}
	if got := boundTo(t, c); got != p.b.ID {
		t.Fatalf("after reset the connection is bound to %s", got)
	}
	if _, err := st.Execute(1); !isMySQLCode(err, gomysql.ER_UNKNOWN_STMT_HANDLER) {
		t.Errorf("a statement prepared before the reset, after it: %v, want unknown statement (1243)", err)
	}
}

// The port turned off while a connection is open: a change of user is
// refused, like a new login, and the connection closes.
func TestChangeUser_portOffRefuses(t *testing.T) {
	p := newTwoServerPort(t)
	c := p.connect(t, "app1", "pw1")
	p.f.savePortFile(t, false, "port-pw")
	waitUntil(t, "the port to stop taking logins", func() bool {
		cc, err := client.Connect(p.addr, "app2", "pw2", "")
		if err == nil {
			cc.Close()
		}
		return err != nil
	})
	if r := changeUser(t, c, "app2", "pw2", ""); r.code == 0 && !r.closed {
		t.Fatalf("a change of user on a port turned off was accepted: %+v", r)
	}
}

// A connection whose first binding failed (its username names no server) can
// change to a user that does, and is served.
func TestChangeUser_afterAFailedBinding(t *testing.T) {
	p := newTwoServerPort(t)
	c := p.connect(t, "no-such-server", "port-pw")
	if _, err := c.Execute("SHOW ROUTER STATUS"); err == nil {
		t.Fatal("a connection to no server answered")
	}
	if r := changeUser(t, c, "app2", "pw2", ""); r.code != 0 || r.closed {
		t.Fatalf("change after a failed binding: %+v", r)
	}
	if got := boundTo(t, c); got != p.b.ID {
		t.Fatalf("bound to %s, want server-b", got)
	}
}

// The rule of the handshake holds for a change of user: the server a login
// password opens is decided in the reading that offered the password. Here
// the registry changes between that reading and the binding (visible, not
// yet applied): server-b's login app2 is removed and server-a renamed to
// "app2". A client that changed user with server-b's password must be bound
// to server-b, never to server-a, which the username names now.
func TestChangeUser_bindsTheServerWhosePasswordItChecked(t *testing.T) {
	f := newRouterFiles(t)
	a, err := f.owner.Add(console.ServerEntry{Name: "server-a", DSN: "idxuser:idxsecret@tcp(127.0.0.1:1)/idx_a"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := f.owner.Add(console.ServerEntry{Name: "server-b", DSN: "idxuser:idxsecret@tcp(127.0.0.1:2)/idx_b"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.owner.SetPortLogin(b.ID, "app2", "pw2"); err != nil {
		t.Fatal(err)
	}
	follower, err := console.LoadRegistryFollower(f.servers)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: follower})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	mysrv, err := shim.NewMySQLServer("")
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	creds := hookCreds{flashbackCreds: flashbackCreds{srv: srv}, after: func() {
		calls++
		if calls != 2 { // 1 = the handshake, 2 = the change of user
			return
		}
		if _, err := f.owner.SetPortLogin(b.ID, "", ""); err != nil {
			t.Error(err)
		}
		renamed := a
		renamed.Name = "app2"
		if err := f.owner.Update(renamed); err != nil {
			t.Error(err)
		}
		if _, err := follower.Reload(); err != nil {
			t.Error(err)
		}
	}}
	cfg := routerFlashbackConfig(routerSettings{Policy: readrouter.DefaultPolicy()})
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		handleFlashbackConn(context.Background(), srv, c, mysrv, creds, shim.NewGate(0), cfg, slog.Default())
	}()
	c, err := client.Connect(ln.Addr().String(), "server-a", "tok", "")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// This console opens a server's index when it binds, and there is none:
	// the binding fails, naming the server it went for.
	r := changeUser(t, c, "app2", "pw2", "")
	if strings.Contains(r.msg, `"server-a"`) || !strings.Contains(r.msg, `server "server-b"`) {
		t.Fatalf("a change of user checked with server-b's password went for another server: %+v", r)
	}
}

func isMySQLCode(err error, code uint16) bool {
	var me *gomysql.MyError
	return errors.As(err, &me) && me.Code == code
}

// The answers the docs name for a refused change: a wrong password 1045, a
// malformed packet 1835, a username that names no server (with the port's
// password, which every username takes) 1049.
func TestChangeUser_refusalCodes(t *testing.T) {
	p := newTwoServerPort(t)
	c := p.connect(t, "app1", "pw1")
	if r := changeUser(t, c, "app2", "wrong", ""); r.code != gomysql.ER_ACCESS_DENIED_ERROR {
		t.Errorf("wrong password: %+v, want 1045", r)
	}
	c = p.connect(t, "app1", "pw1")
	if r := sendChangeUser(t, c, []byte{0, 0, 0, 0, gomysql.COM_CHANGE_USER}, ""); r.code != gomysql.ER_MALFORMED_PACKET {
		t.Errorf("malformed packet: %+v, want 1835", r)
	}
	c = p.connect(t, "app1", "pw1")
	if r := changeUser(t, c, "no-such-server", "port-pw", ""); r.code != gomysql.ER_BAD_DB_ERROR {
		t.Errorf("a username that names no server: %+v, want 1049", r)
	}
}
