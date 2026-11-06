package consoleapp

import (
	"errors"
	"testing"

	"github.com/go-mysql-org/go-mysql/client"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/console"
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
	}
	for _, c := range cases {
		if got := loginAs(t, addr, c.user, c.pw); got != c.want {
			t.Errorf("%s (%s / %s): MySQL error %d, want %d", c.why, c.user, c.pw, got, c.want)
		}
	}

	// A connection opened with the login is closed when the login is
	// removed, and the login no longer opens the port.
	conn, err := client.Connect(addr, "app1", "pw-one", "")
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
	if got := loginAs(t, addr, "app1", "pw-one"); got != gomysql.ER_ACCESS_DENIED_ERROR {
		t.Fatalf("a removed login still opens the port: %d", got)
	}
	if got := loginAs(t, addr, "app2", "pw-two"); got != 0 {
		t.Fatalf("removing one login closed the other: %d", got)
	}
}
