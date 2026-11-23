package consoleapp

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/testutil/mysqlwire"
)

// servePortFor3162 serves the port with cfg and returns its address. The
// cleanup proves that every connection's goroutine ended: serveFlashback
// returns only after its WaitGroup does.
func servePortFor3162(t *testing.T, cfg flashbackConfig) string {
	t.Helper()
	srv := newFlashbackConsole(t, "tok")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveFlashback(ctx, srv, ln, cfg) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Error("serveFlashback did not return: a connection's goroutine is still there")
		}
	})
	return ln.Addr().String()
}

// readPacket3162 reads one MySQL packet and returns its payload.
func readPacket3162(t *testing.T, c net.Conn) ([]byte, error) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	var head [4]byte
	if _, err := io.ReadFull(c, head[:]); err != nil {
		return nil, err
	}
	payload := make([]byte, int(head[0])|int(head[1])<<8|int(head[2])<<16)
	_, err := io.ReadFull(c, payload)
	return payload, err
}

// refused3162 reports whether a new connection is answered with error 1040
// in place of the handshake, and then closed.
func refused3162(t *testing.T, addr string) bool {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	p, err := readPacket3162(t, c)
	if err != nil {
		t.Fatalf("first packet: %v", err)
	}
	if len(p) < 3 || p[0] != 0xff {
		return false // a handshake: the connection was taken
	}
	if code := binary.LittleEndian.Uint16(p[1:3]); code != 1040 {
		t.Fatalf("refused with error %d, want 1040", code)
	}
	if msg := string(p[3:]); msg != "Too many connections" {
		t.Fatalf("refused with %q, want MySQL's own text", msg)
	}
	if _, err := readPacket3162(t, c); !errors.Is(err, io.EOF) {
		t.Fatalf("after the refusal: %v, want the connection closed", err)
	}
	return true
}

// loginSoon3162 logs in, retrying while the port still answers 1040: a place
// is given back when the connection that held it has ended on the port's
// side, a moment after the client closed it.
func loginSoon3162(t *testing.T, addr string) *mysqlwire.Conn {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		c, err := mysqlwire.Dial(addr, "no-such-server", "tok", "")
		if err == nil {
			return c
		}
		if time.Now().After(deadline) {
			t.Fatalf("no place was given back: still %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// The port takes MaxConns clients; one more is answered with MySQL's own
// "too many connections" before the handshake, and a place is given back
// when a client leaves (#3162). Refused connections take no place: after
// many of them the port still serves exactly MaxConns.
func TestFlashbackPort_refusesPastTheConnectionLimit(t *testing.T) {
	const limit = 2
	addr := servePortFor3162(t, flashbackConfig{MaxConns: limit})

	var held []*mysqlwire.Conn
	for range limit {
		c, err := mysqlwire.Dial(addr, "no-such-server", "tok", "")
		if err != nil {
			t.Fatalf("connection within the limit: %v", err)
		}
		defer c.Close()
		held = append(held, c)
	}
	for i := range 20 {
		if !refused3162(t, addr) {
			t.Fatalf("connection %d past the limit of %d was given a handshake", i+1, limit)
		}
	}
	// The connections within the limit are still served.
	for _, c := range held {
		if _, err := c.Ping(); err != nil {
			t.Fatalf("a connection within the limit stopped answering: %v", err)
		}
	}

	// One leaves: exactly one place comes back.
	held[0].Close()
	again := loginSoon3162(t, addr)
	defer again.Close()
	if !refused3162(t, addr) {
		t.Fatalf("after one client left and one took its place, one more was given a handshake")
	}

	// All leave: the count is back to zero, so the port takes limit again.
	again.Close()
	held[1].Close()
	for range limit {
		c := loginSoon3162(t, addr)
		defer c.Close()
	}
	if !refused3162(t, addr) {
		t.Fatalf("after every client left, the port took more than %d", limit)
	}
}

// MaxConns 0 is no limit: the port inside watch, as before.
func TestFlashbackPort_noLimitWhenUnset(t *testing.T) {
	addr := servePortFor3162(t, flashbackConfig{})
	for range 8 {
		c, err := mysqlwire.Dial(addr, "no-such-server", "tok", "")
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
	}
}

// shortLogin3162 gives clients d to log in for the length of the test.
func shortLogin3162(t *testing.T, d time.Duration) {
	t.Helper()
	old := flashbackLoginTimeout
	flashbackLoginTimeout = d
	t.Cleanup(func() { flashbackLoginTimeout = old })
}

// A client that connects and never sends anything is closed when the time
// to log in is over, and the place it held is given back (#3162).
func TestFlashbackPort_closesAClientThatNeverLogsIn(t *testing.T) {
	shortLogin3162(t, 300*time.Millisecond)
	addr := servePortFor3162(t, flashbackConfig{MaxConns: 1})

	silent, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()
	if p, err := readPacket3162(t, silent); err != nil || len(p) == 0 || p[0] == 0xff {
		t.Fatalf("handshake: %v %x", err, p)
	}
	// It holds the only place while it may still log in.
	if !refused3162(t, addr) {
		t.Fatal("a second client was taken while the first held the only place")
	}
	start := time.Now()
	if _, err := readPacket3162(t, silent); err == nil {
		t.Fatal("the silent client was sent something more, not closed")
	} else if errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "i/o timeout") {
		t.Fatalf("the silent client was still connected after %v", time.Since(start))
	}
	loginSoon3162(t, addr).Close()
}

// A client that stops in the middle of the handshake (part of its reply
// sent, then nothing) is closed the same way.
func TestFlashbackPort_closesAClientThatStopsMidHandshake(t *testing.T) {
	shortLogin3162(t, 300*time.Millisecond)
	addr := servePortFor3162(t, flashbackConfig{MaxConns: 1})

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := readPacket3162(t, c); err != nil {
		t.Fatal(err)
	}
	// The header of a 100-byte reply, and 10 of its bytes.
	if _, err := c.Write(append([]byte{100, 0, 0, 1}, make([]byte, 10)...)); err != nil {
		t.Fatal(err)
	}
	if _, err := readPacket3162(t, c); err == nil || strings.Contains(err.Error(), "i/o timeout") {
		t.Fatalf("the stalled client was not closed: %v", err)
	}
	loginSoon3162(t, addr).Close()
}

// A client that hangs up in the middle of the handshake gives its place
// back at once, without waiting for the time to log in.
func TestFlashbackPort_aHangUpMidHandshakeGivesThePlaceBack(t *testing.T) {
	addr := servePortFor3162(t, flashbackConfig{MaxConns: 1}) // 30 seconds to log in
	for range 5 {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		if p, err := readPacket3162(t, c); err != nil || p[0] == 0xff {
			// The one before has not ended on the port's side yet.
			c.Close()
			time.Sleep(20 * time.Millisecond)
			continue
		}
		_, _ = c.Write([]byte{100, 0, 0, 1, 0, 0})
		c.Close()
	}
	start := time.Now()
	loginSoon3162(t, addr).Close()
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("the place came back after %v: it waited for the login time, not the hang-up", d)
	}
}

// A client that logs in within the time is not closed when the time passes.
func TestFlashbackPort_theLoginTimeDoesNotEndALoggedInClient(t *testing.T) {
	shortLogin3162(t, 300*time.Millisecond)
	addr := servePortFor3162(t, flashbackConfig{})
	c, err := mysqlwire.Dial(addr, "no-such-server", "tok", "")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	time.Sleep(700 * time.Millisecond)
	if _, err := c.Ping(); err != nil {
		t.Fatalf("a logged-in client was closed by the login time: %v", err)
	}
}

// --max-connections: 256 unless set, the flag over the variable, and a value
// that cannot be a number of clients stops the start, naming where it came
// from.
func TestRouterSettings_maxConnections(t *testing.T) {
	t.Setenv("BINTRAIL_CONSOLE_TOKEN", "t")
	t.Setenv("BINTRAIL_ROUTER_MAX_CONNECTIONS", "")
	st, err := maxWaitSettings(t)
	if err != nil || st.MaxConns != 256 {
		t.Fatalf("unset: MaxConns = %d, %v; want 256", st.MaxConns, err)
	}
	if cfg := routerFlashbackConfig(st); cfg.MaxConns != 256 {
		t.Fatalf("the port is served with MaxConns %d, want what was resolved", cfg.MaxConns)
	}
	t.Setenv("BINTRAIL_ROUTER_MAX_CONNECTIONS", " 512 ")
	if st, err = maxWaitSettings(t); err != nil || st.MaxConns != 512 {
		t.Fatalf("env: MaxConns = %d, %v; want 512", st.MaxConns, err)
	}
	if st, err = maxWaitSettings(t, "--max-connections", "8"); err != nil || st.MaxConns != 8 {
		t.Fatalf("flag over env: MaxConns = %d, %v; want 8", st.MaxConns, err)
	}
	for _, tc := range []struct {
		env  string
		args []string
		want string
	}{
		{env: "0", want: "BINTRAIL_ROUTER_MAX_CONNECTIONS must be between 1 and 1024"},
		{env: "1025", want: "BINTRAIL_ROUTER_MAX_CONNECTIONS must be between 1 and 1024"},
		{env: "-1", want: "BINTRAIL_ROUTER_MAX_CONNECTIONS must be between 1 and 1024"},
		{env: "many", want: `BINTRAIL_ROUTER_MAX_CONNECTIONS="many" is not a whole number`},
		{env: "12.5", want: "is not a whole number"},
		{args: []string{"--max-connections", "0"}, want: "--max-connections must be between 1 and 1024"},
		{args: []string{"--max-connections", "2000"}, want: "--max-connections must be between 1 and 1024"},
	} {
		t.Setenv("BINTRAIL_ROUTER_MAX_CONNECTIONS", tc.env)
		if _, err := maxWaitSettings(t, tc.args...); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("env %q args %v: err = %v; want %q", tc.env, tc.args, err, tc.want)
		}
	}
}

// The startup lines say the limit and the time to log in.
func TestRouterStartupLines_sayTheConnectionLimit(t *testing.T) {
	got := routerStartupLines(routerStartup{Listen: "127.0.0.1:3309", Pool: console.SQLPool{Workers: 2, Threads: 2, Why: "4 cores allow 2"}, MemoryLimit: "4096MiB",
		Cfg: routerFlashbackConfig(routerSettings{MaxConns: 256}), ServersFile: "/x/console-servers.yaml", ServersExists: true})
	want := "Router: at most 256 clients connected at once (--max-connections); one more is refused with error 1040. A client has 30s to log in.\n"
	if !strings.Contains(got, want) {
		t.Errorf("startup lines lack %q:\n%s", want, got)
	}
}
