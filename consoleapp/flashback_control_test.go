package consoleapp

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/console"
)

// freeAddr returns a loopback address nothing listens on right now.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func accepts(addr string) bool {
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}
	c.Close()
	return true
}

func newControl(t *testing.T) *flashbackControl {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	c := &flashbackControl{ctx: ctx, srv: newFlashbackConsole(t, "tok")}
	t.Cleanup(func() { c.Close(); cancel() })
	return c
}

// TestFlashbackControl_OpenMoveClose: the port follows Apply while the
// process runs, one address at a time.
func TestFlashbackControl_OpenMoveClose(t *testing.T) {
	c := newControl(t)
	a, b := freeAddr(t), freeAddr(t)

	if err := c.Apply(a); err != nil || !accepts(a) {
		t.Fatalf("open %s: err %v, accepting %v", a, err, accepts(a))
	}
	if err := c.Apply(a); err != nil || !accepts(a) {
		t.Fatalf("the same address again: err %v, accepting %v", err, accepts(a))
	}
	if err := c.Apply(b); err != nil || !accepts(b) || accepts(a) {
		t.Fatalf("move to %s: err %v, new accepting %v, old accepting %v", b, err, accepts(b), accepts(a))
	}
	if err := c.Apply(""); err != nil || accepts(b) {
		t.Fatalf("close: err %v, still accepting %v", err, accepts(b))
	}
	if err := c.Apply(""); err != nil {
		t.Fatalf("closing a closed port: %v", err)
	}
	if err := c.Apply(a); err != nil || !accepts(a) {
		t.Fatalf("open after close: err %v, accepting %v", err, accepts(a))
	}
}

// TestFlashbackControl_RefusedAddressKeepsThePort: an address that cannot be
// bound leaves the port where it was.
func TestFlashbackControl_RefusedAddressKeepsThePort(t *testing.T) {
	c := newControl(t)
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()

	if err := c.Apply(taken.Addr().String()); err == nil || !strings.Contains(err.Error(), "cannot bind") {
		t.Fatalf("a taken address from closed: err %v, want cannot bind", err)
	}
	a := freeAddr(t)
	if err := c.Apply(a); err != nil {
		t.Fatal(err)
	}
	// A client connected before the refusals must still be connected after:
	// "still accepting" alone would also be true of a port torn down and
	// rebuilt, which drops everyone on it.
	held, err := net.Dial("tcp", a)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	greeting := make([]byte, 1)
	if _, err := held.Read(greeting); err != nil {
		t.Fatalf("no handshake greeting from the port: %v", err)
	}
	c.mu.Lock()
	run := c.done
	c.mu.Unlock()
	for _, bad := range []string{taken.Addr().String(), "not-an-address"} {
		if err := c.Apply(bad); err == nil {
			t.Fatalf("%q was accepted", bad)
		}
		c.mu.Lock()
		same := c.done == run
		c.mu.Unlock()
		if !same || c.Listening() != a || !accepts(a) {
			t.Fatalf("after refusing %q: same run %v, listening on %q; a refused address must leave the port untouched", bad, same, c.Listening())
		}
	}
	// Read past the rest of the greeting: an open connection then goes quiet
	// (a timeout); a dropped one ends (EOF or a reset).
	_ = held.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	var rerr error
	for buf := make([]byte, 256); rerr == nil; {
		_, rerr = held.Read(buf)
	}
	var ne net.Error
	if !errors.As(rerr, &ne) || !ne.Timeout() {
		t.Fatalf("the client connected before the refusals was dropped (read: %v)", rerr)
	}
}

// TestFlashbackControl_SamePortOtherHost: moving between hosts on one port
// number cannot bind the new before releasing the old; it must still end on
// the new address.
func TestFlashbackControl_SamePortOtherHost(t *testing.T) {
	c := newControl(t)
	a := freeAddr(t)
	_, port, _ := net.SplitHostPort(a)
	if err := c.Apply(a); err != nil {
		t.Fatal(err)
	}
	if err := c.Apply("0.0.0.0:" + port); err != nil {
		t.Fatalf("same port, every interface: %v", err)
	}
	if !accepts(a) {
		t.Fatal("not accepting after the move")
	}
	c.mu.Lock()
	got := c.addr
	c.mu.Unlock()
	if got != "0.0.0.0:"+port {
		t.Fatalf("serving on %q, want the new address", got)
	}
}

// TestFlashbackControl_StartupConflictRefusesEveryOpen: contradictory routing
// flags do not stop the daemon; they refuse the port, with the reason.
func TestFlashbackControl_StartupConflictRefusesEveryOpen(t *testing.T) {
	c := newControl(t)
	c.cfgErr = errors.New("read routing: both thresholds are 0")
	a := freeAddr(t)
	if err := c.Apply(a); err == nil || !strings.Contains(err.Error(), "read routing") || accepts(a) {
		t.Fatalf("err %v, accepting %v", err, accepts(a))
	}
}

// mysqlLogin reports the MySQL error code a handshake ends with (0 = let in).
func mysqlLogin(t *testing.T, addr, password string) uint16 {
	t.Helper()
	conn, err := client.Connect(addr, "default", password, "")
	if err == nil {
		conn.Close()
		return 0
	}
	var me *gomysql.MyError
	if errors.As(err, &me) {
		return me.Code
	}
	t.Fatalf("handshake with %s: %v (not a MySQL error)", addr, err)
	return 0
}

// TestFlashbackTurnedOnFromTheWebInterface is the issue's own sentence, end to
// end: no address and no token at startup, the port turned on through the
// HTTP route, and a MySQL client let in with the password that route returned
// and with nothing else. Then the same after a restart, from the saved file.
func TestFlashbackTurnedOnFromTheWebInterface(t *testing.T) {
	saved := upConsoleFlashbackListen
	t.Cleanup(func() { upConsoleFlashbackListen = saved })
	upConsoleFlashbackListen = ""

	path := filepath.Join(t.TempDir(), console.FlashbackFileName)
	start := func(t *testing.T) (*console.Server, func()) {
		t.Helper()
		// The token here only authenticates the HTTP calls below. It also
		// opens the port, which is why the client logins use the generated
		// password and a wrong one, never this.
		srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "api-tok", FlashbackPath: path})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		stop, err := startFlashbackPort(ctx, srv)
		if err != nil {
			t.Fatalf("startup with no address must not fail: %v", err)
		}
		return srv, func() { cancel(); stop() }
	}
	call := func(t *testing.T, srv *console.Server, method, route, body string) (int, map[string]any) {
		t.Helper()
		req := httptest.NewRequest(method, "http://127.0.0.1"+route, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer api-tok")
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		var got map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &got)
		return rec.Code, got
	}

	srv, shutdown := start(t)
	addr := freeAddr(t)
	if accepts(addr) {
		t.Fatal("precondition: the port is already open")
	}
	code, got := call(t, srv, http.MethodPut, "/api/flashback", `{"enabled":true,"listen":"`+addr+`"}`)
	pw, _ := got["password"].(string)
	if code != 200 || pw == "" {
		t.Fatalf("turn on = %d %v", code, got)
	}
	if c := mysqlLogin(t, addr, pw); c == gomysql.ER_ACCESS_DENIED_ERROR {
		t.Fatalf("the password the web interface returned was refused (code %d)", c)
	}
	if c := mysqlLogin(t, addr, "wrong"); c != gomysql.ER_ACCESS_DENIED_ERROR {
		t.Fatalf("a wrong password: code %d, want access denied", c)
	}

	// A new password: the old one stops opening the port at once.
	code, got = call(t, srv, http.MethodPost, "/api/flashback/password", "")
	pw2, _ := got["password"].(string)
	if code != 200 || pw2 == "" || pw2 == pw {
		t.Fatalf("new password = %d %v", code, got)
	}
	if c := mysqlLogin(t, addr, pw); c != gomysql.ER_ACCESS_DENIED_ERROR {
		t.Fatalf("the replaced password still opens the port (code %d)", c)
	}
	if c := mysqlLogin(t, addr, pw2); c == gomysql.ER_ACCESS_DENIED_ERROR {
		t.Fatal("the new password was refused")
	}

	// Restart: the saved setting brings the port back with the same password.
	shutdown()
	if accepts(addr) {
		t.Fatal("the port outlived the daemon")
	}
	// A daemon with no token at all: the saved password is the only thing
	// that opens the port, and it does.
	bare, err := console.New(console.Config{Listen: "127.0.0.1:0", FlashbackPath: path})
	if err != nil {
		t.Fatal(err)
	}
	bareCtx, bareCancel := context.WithCancel(context.Background())
	bareStop, err := startFlashbackPort(bareCtx, bare)
	if err != nil {
		t.Fatal(err)
	}
	if c := mysqlLogin(t, addr, pw2); c == gomysql.ER_ACCESS_DENIED_ERROR {
		t.Fatal("with no token, the saved password was refused")
	}
	if c := mysqlLogin(t, addr, ""); c != gomysql.ER_ACCESS_DENIED_ERROR {
		t.Fatalf("with no token, an empty password: code %d, want access denied", c)
	}
	bareCancel()
	bareStop()
	srv, shutdown = start(t)
	if c := mysqlLogin(t, addr, pw2); c == gomysql.ER_ACCESS_DENIED_ERROR {
		t.Fatal("after a restart the saved password was refused")
	}

	// Off: nothing listens, and it stays off across a restart.
	if code, got = call(t, srv, http.MethodPut, "/api/flashback", `{"enabled":false}`); code != 200 || accepts(addr) {
		t.Fatalf("turn off = %d %v, still accepting %v", code, got, accepts(addr))
	}
	shutdown()
	_, shutdown = start(t)
	defer shutdown()
	if accepts(addr) {
		t.Fatal("a port saved as off came back after a restart")
	}
}

// TestStartFlashbackPort_SavedAddressTakenDoesNotStopTheDaemon: the process
// that would refuse to start is the one capturing changes.
func TestStartFlashbackPort_SavedAddressTakenDoesNotStopTheDaemon(t *testing.T) {
	saved := upConsoleFlashbackListen
	t.Cleanup(func() { upConsoleFlashbackListen = saved })
	upConsoleFlashbackListen = ""

	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	path := filepath.Join(t.TempDir(), console.FlashbackFileName)
	if err := os.WriteFile(path, []byte("version: 1\nenabled: true\nlisten: "+taken.Addr().String()+"\npassword: bfp_saved\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "api-tok", FlashbackPath: path})
	if err != nil {
		t.Fatal(err)
	}
	stop, err := startFlashbackPort(context.Background(), srv)
	if err != nil {
		t.Fatalf("a saved address that is taken stopped startup: %v", err)
	}
	defer stop()
	req := httptest.NewRequest("GET", "http://127.0.0.1/api/flashback", nil)
	req.Header.Set("Authorization", "Bearer api-tok")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if !strings.Contains(rec.Body.String(), `"enabled":false`) || !strings.Contains(rec.Body.String(), "cannot bind") {
		t.Fatalf("status does not say why the port is off: %s", rec.Body.String())
	}
}

// TestStartFlashbackPort_ReadOnlyWithoutAnAddress: with no address at startup
// the port is the web interface's to turn on, so --route-read-only is not a
// flag about a port that is off: it must be accepted (and then holds for a
// port turned on later). It is still refused with routing off, where it would
// guard nothing whoever turns the port on.
func TestStartFlashbackPort_ReadOnlyWithoutAnAddress(t *testing.T) {
	listen, age, ro, cost, rows := upConsoleFlashbackListen, upRouteMaxCopyAge, upRouteReadOnly, upRouteCostThreshold, upRouteScanRows
	t.Cleanup(func() {
		upConsoleFlashbackListen, upRouteMaxCopyAge, upRouteReadOnly, upRouteCostThreshold, upRouteScanRows = listen, age, ro, cost, rows
	})
	srv := newFlashbackConsole(t, "tok")

	upConsoleFlashbackListen, upRouteReadOnly, upRouteMaxCopyAge, upRouteCostThreshold, upRouteScanRows = "", true, 15*time.Minute, 1000, 0
	if got := flashbackConfigFromFlags(); !got.RouteReadOnly || got.RouteMaxCopyAge != 15*time.Minute {
		t.Fatalf("setup: the port's configuration is %+v", got)
	}
	stop, err := startFlashbackPort(context.Background(), srv)
	if err != nil {
		t.Fatalf("read-only with routing on and no address at startup: %v; the web interface can still turn the port on", err)
	}
	stop()

	upRouteMaxCopyAge = 0
	_, err = startFlashbackPort(context.Background(), srv)
	if err == nil || !strings.Contains(err.Error(), "read routing, which is off") {
		t.Fatalf("read-only with routing off: err = %v, want the routing-off refusal", err)
	}
	if strings.Contains(err.Error(), "port, which is off") {
		t.Fatalf("the refusal blames a port that the web interface can turn on: %v", err)
	}
}
