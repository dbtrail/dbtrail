package consoleapp

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/client"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/shim"
)

// A server's application login on the router's port, through a real MySQL
// handshake: the username and password a proxy such as ProxySQL logs in with
// (the application's own) open the port for that server alone, never under
// another username, and a login removed in the web interface closes the
// connections it opened.

// loginAs reports the MySQL error code a handshake as user ends with (0 = let in).
func loginAs(t *testing.T, addr, user, password string) uint16 {
	t.Helper()
	conn, err := client.Connect(addr, user, password, "")
	if err == nil {
		conn.Close()
		return 0
	}
	var me *gomysql.MyError
	if errors.As(err, &me) {
		return me.Code
	}
	t.Fatalf("handshake as %s: %v (not a MySQL error)", user, err)
	return 0
}

func TestRouterPort_applicationLogin(t *testing.T) {
	f := newRouterFiles(t)
	prod, err := f.owner.Add(console.ServerEntry{Name: "prod", DSN: "u:p@tcp(127.0.0.1:1)/idx_prod"})
	if err != nil {
		t.Fatal(err)
	}
	staging, err := f.owner.Add(console.ServerEntry{Name: "staging", DSN: "u:p@tcp(127.0.0.1:1)/idx_staging"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.owner.SetPortLogin(prod.ID, "app1", "pw-one"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.owner.SetPortLogin(staging.ID, "app2", "pw-two"); err != nil {
		t.Fatal(err)
	}
	dev, err := f.owner.Add(console.ServerEntry{Name: "dev", DSN: "u:p@tcp(127.0.0.1:1)/idx_dev"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.owner.SetPortLogin(dev.ID, "app3", `it's "q\\" ñ`); err != nil {
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
	defer stop()
	waitUntil(t, "the port to open", func() bool { return bound() != "" })

	cases := []struct {
		user, pw string
		want     uint16
		why      string
	}{
		{"app1", "pw-one", 0, "a login with its own password"},
		{"app2", "pw-two", 0, "the other login with its own password"},
		{"app1", "port-pw", 0, "a login with the port's password, which opens every server"},
		{"app1", "pw-two", gomysql.ER_ACCESS_DENIED_ERROR, "a login with another login's password"},
		{"app2", "pw-one", gomysql.ER_ACCESS_DENIED_ERROR, "the other way round"},
		{"nobody", "pw-one", gomysql.ER_ACCESS_DENIED_ERROR, "an unknown user with a login's password"},
		{"prod", "pw-one", gomysql.ER_ACCESS_DENIED_ERROR, "the server's own name with its login's password"},
		{"App1", "pw-one", gomysql.ER_ACCESS_DENIED_ERROR, "the login in another case"},
		{"app1", "wrong", gomysql.ER_ACCESS_DENIED_ERROR, "a wrong password"},
		{"app3", `it's "q\\" ñ`, 0, "a password with quotes, a backslash and a ñ"},
		{"app1@copy", "pw-one", gomysql.ER_ACCESS_DENIED_ERROR, "the copy-only suffix with the login's password"},
		{"app1@copy", "port-pw", 0, "the copy-only suffix with the port's password"},
	}
	for _, c := range cases {
		if got := loginAs(t, addr, c.user, c.pw); got != c.want {
			t.Errorf("%s (%s / %s): MySQL error %d, want %d", c.why, c.user, c.pw, got, c.want)
		}
	}

	// A new port password leaves the logins alone.
	f.savePortFile(t, true, "port-pw-2")
	waitUntil(t, "the new port password", func() bool { return loginAs(t, addr, "prod", "port-pw-2") == 0 })
	if got := loginAs(t, addr, "app1", "pw-one"); got != 0 {
		t.Fatalf("a new port password closed a login: %d", got)
	}
	// A rotated login password closes the connections opened with the old
	// one; the old password no longer opens the port, the new one does.
	rot, err := client.Connect(addr, "app1", "pw-one", "")
	if err != nil {
		t.Fatal(err)
	}
	defer rot.Close()
	if _, err := f.owner.SetPortLogin(prod.ID, "app1", "pw-rotated"); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "the rotated login's connection to be closed", func() bool { return rot.Ping() != nil })
	if got := loginAs(t, addr, "app1", "pw-one"); got != gomysql.ER_ACCESS_DENIED_ERROR {
		t.Fatalf("the old password still opens the port: %d", got)
	}
	if got := loginAs(t, addr, "app1", "pw-rotated"); got != 0 {
		t.Fatalf("the rotated password does not open the port: %d", got)
	}

	// A connection opened with the login is closed when the login is
	// removed, and the login no longer opens the port.
	conn, err := client.Connect(addr, "app1", "pw-rotated", "")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Ping(); err != nil {
		t.Fatalf("ping on the login's connection: %v", err)
	}
	if _, err := f.owner.SetPortLogin(prod.ID, "", ""); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "the login's connection to be closed", func() bool { return conn.Ping() != nil })
	if got := loginAs(t, addr, "app1", "pw-rotated"); got != gomysql.ER_ACCESS_DENIED_ERROR {
		t.Fatalf("a removed login still opens the port: %d", got)
	}
	if got := loginAs(t, addr, "app2", "pw-two"); got != 0 {
		t.Fatalf("removing one login closed the other: %d", got)
	}
}

// hookCreds runs after the handshake has read the passwords it will check
// and before the connection is bound: the moment a login can change under a
// connection.
type hookCreds struct {
	flashbackCreds
	after func()
}

func (h hookCreds) GetCredential(user string) (server.Credential, bool, error) {
	cred, found, err := h.flashbackCreds.GetCredential(user)
	if h.after != nil {
		h.after()
	}
	return cred, found, err
}

// A login removed, and a server renamed to its username, while a client
// authenticates with that login's password: the connection must not be
// served the renamed server, though the username is no login any more.
func TestFlashbackConn_loginChangedDuringHandshakeIsRefused(t *testing.T) {
	f := newRouterFiles(t)
	a, err := f.owner.Add(console.ServerEntry{Name: "server-a", DSN: "u:p@tcp(127.0.0.1:1)/idx_a"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := f.owner.Add(console.ServerEntry{Name: "server-b", DSN: "u:p@tcp(127.0.0.1:1)/idx_b"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.owner.SetPortLogin(a.ID, "app", "pw-a"); err != nil {
		t.Fatal(err)
	}
	f.savePortFile(t, true, "port-pw")
	srv, runner, err := newRouterConsole(f.settings(""), console.SQLPool{Workers: 1, Threads: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.Close)
	srv.RefreshFollowedFiles()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	mysrv, err := shim.NewMySQLServer("")
	if err != nil {
		t.Fatal(err)
	}
	cfg := routerFlashbackConfig(routerSettings{Policy: readrouter.DefaultPolicy()})
	once := false
	creds := hookCreds{flashbackCreds: flashbackCreds{srv: srv}, after: func() {
		if once {
			return
		}
		once = true
		if _, err := f.owner.SetPortLogin(a.ID, "", ""); err != nil {
			t.Error(err)
		}
		renamed := b
		renamed.Name = "app"
		if err := f.owner.Update(renamed); err != nil {
			t.Error(err)
		}
		srv.RefreshFollowedFiles() // the router sees both at once
	}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := ln.Accept()
		if err != nil {
			return
		}
		handleFlashbackConn(context.Background(), srv, c, mysrv, creds, shim.NewGate(0), cfg, slog.Default())
	}()
	conn, err := client.Connect(ln.Addr().String(), "app", "pw-a", "")
	if err != nil {
		t.Fatalf("handshake with pw-a: %v", err)
	}
	_, err = conn.Execute("SELECT 1")
	conn.Close()
	<-done
	if err == nil || !strings.Contains(err.Error(), "changed while connecting") {
		t.Fatalf("a connection authenticated with a removed login's password was served server-b: %v", err)
	}
}
