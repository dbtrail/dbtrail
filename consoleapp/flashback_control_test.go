package consoleapp

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"

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
