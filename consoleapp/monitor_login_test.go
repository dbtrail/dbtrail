package consoleapp

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/go-mysql-org/go-mysql/client"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/console"
)

// The port's monitor login: the username and password a proxy's health
// checks log in with (console/monitor_login.go). The statements are the ones
// ProxySQL 3.1.12's monitor sends (lib/MySQL_Monitor.cpp).

// monitorPort is twoServerPort with a monitor login monitor/mon-pw saved in
// the port's settings file.
type monitorPort struct {
	twoServerPort
	srv *console.Server
}

func (p monitorPort) saveMonitor(t *testing.T, user, password string) {
	t.Helper()
	body := "version: 1\nenabled: true\npassword: port-pw\n"
	if user != "" {
		body += "monitor_user: " + user + "\nmonitor_password: " + password + "\n"
	}
	tmp := p.f.portFile + ".tmp"
	if err := os.WriteFile(tmp, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, p.f.portFile); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "the router to read the monitor login", func() bool {
		pw, ok, _ := p.srv.MonitorPasswordFor(user)
		if user == "" {
			_, ok, _ = p.srv.MonitorPasswordFor("monitor")
			return !ok
		}
		return ok && pw == password
	})
}

func newMonitorPort(t *testing.T) monitorPort {
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
	p := monitorPort{twoServerPort: twoServerPort{f: f, addr: addr, a: a, b: b}, srv: srv}
	p.saveMonitor(t, "monitor", "mon-pw")
	return p
}

func connectErr(addr, user, password string) error {
	c, err := client.Connect(addr, user, password, "")
	if err == nil {
		defer c.Close()
		// Every username gets past the handshake with the port's password;
		// the first statement says whether it opened anything.
		err = c.Ping()
	}
	return err
}

// What ProxySQL's monitor sends is answered; everything else is refused and
// the connection stays.
func TestMonitorLogin_answersTheHealthChecks(t *testing.T) {
	p := newMonitorPort(t)
	c := p.connect(t, "monitor", "mon-pw")
	if err := c.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	for _, q := range []string{"SET wait_timeout=20", "set wait_timeout = 3600;", "SET SESSION wait_timeout=1", "SET @@wait_timeout=5"} {
		if _, err := c.Execute(q); err != nil {
			t.Errorf("%q: %v", q, err)
		}
	}
	for _, q := range []string{
		"SELECT @@global.read_only read_only",
		"SELECT @@global.read_only read_only ",
		"SELECT @@global.innodb_read_only read_only",
		"SELECT @@global.super_read_only read_only",
		"SELECT @@global.read_only&@@global.innodb_read_only read_only",
		"SELECT @@global.read_only|@@global.innodb_read_only read_only",
	} {
		r, err := c.Execute(q)
		if err != nil {
			t.Errorf("%q: %v", q, err)
			continue
		}
		if len(r.Fields) != 1 || string(r.Fields[0].Name) != "read_only" || r.RowNumber() != 1 {
			t.Errorf("%q: %d fields, %d rows", q, len(r.Fields), r.RowNumber())
			continue
		}
		if v, _ := r.GetInt(0, 0); v != 1 {
			t.Errorf("%q answered read_only=%d, want 1 (the port is a reader)", q, v)
		}
	}
	for _, q := range []string{"SELECT 1 FROM shop.orders", "SHOW SLAVE STATUS", "SHOW ROUTER STATUS", "SET wait_timeout=1; DROP TABLE x", "SELECT @@global.read_only"} {
		if _, err := c.Execute(q); !isMySQLCode(err, gomysql.ER_SPECIFIC_ACCESS_DENIED_ERROR) {
			t.Errorf("%q: %v, want 1227", q, err)
		}
	}
	if err := c.UseDB("idx_a"); !isMySQLCode(err, gomysql.ER_SPECIFIC_ACCESS_DENIED_ERROR) {
		t.Errorf("USE on a monitor connection: %v, want 1227", err)
	}
	if _, err := c.Prepare("SELECT 1"); err == nil {
		t.Error("a prepared statement on a monitor connection was accepted")
	}
	if err := c.Ping(); err != nil {
		t.Fatalf("ping after the refusals: %v", err)
	}
}

// The monitor's username is the monitor's alone, its password opens nothing
// else, and usernames match exactly.
func TestMonitorLogin_refusals(t *testing.T) {
	p := newMonitorPort(t)
	cases := []struct{ user, password string }{
		{"monitor", "port-pw"},
		{"monitor", "pw1"},
		{"app1", "mon-pw"},
		{"server-a", "mon-pw"},
		{"Monitor", "mon-pw"},
		{"monitor", ""},
	}
	for _, c := range cases {
		err := connectErr(p.addr, c.user, c.password)
		if !isMySQLCode(err, gomysql.ER_ACCESS_DENIED_ERROR) && !isMySQLCode(err, gomysql.ER_BAD_DB_ERROR) {
			t.Errorf("%s / %s: %v, want refused", c.user, c.password, err)
		}
	}
	// The port's password still opens a server by its name.
	if err := connectErr(p.addr, "server-a", "port-pw"); isMySQLCode(err, gomysql.ER_ACCESS_DENIED_ERROR) {
		t.Errorf("server-a with the port password: %v", err)
	}
}

// A changed or removed monitor login closes the connections opened with it,
// and no application's.
func TestMonitorLogin_changeClosesItsConnections(t *testing.T) {
	p := newMonitorPort(t)
	mon := p.connect(t, "monitor", "mon-pw")
	app := p.connect(t, "app1", "pw1")
	if err := mon.Ping(); err != nil {
		t.Fatal(err)
	}
	p.saveMonitor(t, "monitor", "mon-pw-2")
	waitUntil(t, "the monitor connection to close", func() bool { return mon.Ping() != nil })
	if got := boundTo(t, app); got != p.a.ID {
		t.Fatalf("the application connection: bound to %s", got)
	}
	if err := connectErr(p.addr, "monitor", "mon-pw"); !isMySQLCode(err, gomysql.ER_ACCESS_DENIED_ERROR) {
		t.Fatalf("old monitor password: %v", err)
	}
	mon2 := p.connect(t, "monitor", "mon-pw-2")
	p.saveMonitor(t, "", "")
	waitUntil(t, "the monitor connection to close on removal", func() bool { return mon2.Ping() != nil })
	if err := connectErr(p.addr, "monitor", "mon-pw-2"); err == nil {
		t.Fatal("a removed monitor login still opens the port")
	}
}

// ProxySQL moves pooled connections between users, the monitor included.
func TestMonitorLogin_changeUserAndReset(t *testing.T) {
	p := newMonitorPort(t)
	c := p.connect(t, "app1", "pw1")
	// ProxySQL names information_schema when it changes the user.
	if r := changeUserLikeProxySQL(t, c, "monitor", "mon-pw", "information_schema"); r.code != 0 || r.closed {
		t.Fatalf("app1 -> monitor: %+v", r)
	}
	if _, err := c.Execute("SET wait_timeout=10"); err != nil {
		t.Fatalf("monitor after change: %v", err)
	}
	if _, err := c.Execute("SELECT 1 FROM t"); !isMySQLCode(err, gomysql.ER_SPECIFIC_ACCESS_DENIED_ERROR) {
		t.Fatalf("a query after changing to the monitor: %v", err)
	}
	if r := resetConnection(t, c); r.code != 0 || r.closed {
		t.Fatalf("reset on a monitor connection: %+v", r)
	}
	if _, err := c.Execute("SELECT 1 FROM t"); !isMySQLCode(err, gomysql.ER_SPECIFIC_ACCESS_DENIED_ERROR) {
		t.Fatalf("after the reset it is no longer a monitor connection: %v", err)
	}
	if r := changeUserLikeProxySQL(t, c, "app2", "pw2", ""); r.code != 0 || r.closed {
		t.Fatalf("monitor -> app2: %+v", r)
	}
	if got := boundTo(t, c); got != p.b.ID {
		t.Fatalf("after monitor -> app2: bound to %s", got)
	}
	w := p.connect(t, "app1", "pw1")
	if r := changeUserLikeProxySQL(t, w, "monitor", "port-pw", ""); r.code != gomysql.ER_ACCESS_DENIED_ERROR {
		t.Fatalf("change to the monitor with the port password: %+v, want 1045", r)
	}
	m := p.connect(t, "monitor", "mon-pw")
	p.saveMonitor(t, "monitor", "mon-pw-3")
	if r := resetConnection(t, m); r.code == 0 && !r.closed {
		t.Fatal("a reset after the monitor login changed was accepted")
	}
}

// A monitor connection answers PING even where a failed binding fails it,
// and the monitor login is not offered under caching_sha2_password.
func TestMonitorLogin_pingAndAuthMethod(t *testing.T) {
	r := &routingHandler{monitor: true, fail: errors.New("bind failed"), failPing: true}
	if err := r.PingSource(); err != nil {
		t.Fatalf("monitor ping under FailedBindFailsPing: %v", err)
	}
	p := newMonitorPort(t)
	native := flashbackCreds{srv: p.srv}
	if cred, found, ls, _ := native.credentialWithLogin("monitor"); !found || ls != console.MonitorLoginKey || len(cred.Passwords) != 1 || cred.Passwords[0] != "mon-pw" {
		t.Fatalf("native: %+v %v %q", cred, found, ls)
	}
	sha := flashbackCreds{srv: p.srv, authMethod: gomysql.AUTH_CACHING_SHA2_PASSWORD}
	if cred, found, ls, _ := sha.credentialWithLogin("monitor"); found || ls != "" {
		t.Fatalf("caching_sha2: %+v %v %q, want not offered", cred, found, ls)
	}
}

// A monitor connection whose login changed during its handshake is closed,
// not left answering PING untracked.
func TestMonitorLogin_changedDuringTheHandshakeCloses(t *testing.T) {
	p := newMonitorPort(t)
	epoch := p.srv.LoginEpoch()
	p.saveMonitor(t, "monitor", "mon-pw-2") // moves the epoch on
	closed := 0
	proxy := &routingHandler{}
	_, err := bindFlashbackHandler(context.Background(), p.srv, proxy, nil, "monitor", console.MonitorLoginKey, epoch,
		nil, flashbackConfig{}, slog.New(slog.NewTextHandler(io.Discard, nil)), func() { closed++ })
	if !isMySQLCode(err, gomysql.ER_ACCESS_DENIED_ERROR) || closed != 1 || proxy.monitor {
		t.Fatalf("bind with a stale epoch: %v, closed %d, monitor %v; want 1045, closed once, not a monitor session", err, closed, proxy.monitor)
	}
}
