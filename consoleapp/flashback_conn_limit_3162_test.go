package consoleapp

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/shim"
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

// evictAfter3162 sets how long a connection must have gone without logging
// in before it may be closed to make room, for the length of the test.
func evictAfter3162(t *testing.T, d time.Duration) {
	t.Helper()
	old := flashbackEvictAfter
	flashbackEvictAfter = d
	t.Cleanup(func() { flashbackEvictAfter = old })
}

// greeted3162 connects and reads the handshake, and sends nothing.
func greeted3162(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if p, err := readPacket3162(t, c); err != nil || len(p) == 0 || p[0] == 0xff {
		t.Fatalf("handshake: %v %x", err, p)
	}
	return c
}

// closed3162 reports whether the port closed c, waiting up to d.
func closed3162(c net.Conn, d time.Duration) bool {
	_ = c.SetReadDeadline(time.Now().Add(d))
	var b [1]byte
	_, err := c.Read(b[:])
	var ne net.Error
	return err != nil && !(errors.As(err, &ne) && ne.Timeout())
}

// A full port makes room for a new client by closing the connection that
// has gone longest without logging in, once it has gone longer than the
// threshold; one that has sent nothing goes before one that has.
func TestFlashbackPort_fullPortClosesTheOldestNotLoggedIn(t *testing.T) {
	evictAfter3162(t, 100*time.Millisecond)
	addr := servePortFor3162(t, flashbackConfig{MaxConns: 3})

	// The oldest has sent part of its reply; the two after it nothing.
	partial := greeted3162(t, addr)
	if _, err := partial.Write([]byte{100, 0, 0, 1, 0, 0}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	silentOld := greeted3162(t, addr)
	time.Sleep(20 * time.Millisecond)
	silentNew := greeted3162(t, addr)
	time.Sleep(200 * time.Millisecond) // all three past the threshold

	c, err := mysqlwire.Dial(addr, "no-such-server", "tok", "")
	if err != nil {
		t.Fatalf("a new client on a port full of connections that never logged in: %v", err)
	}
	defer c.Close()
	if !closed3162(silentOld, 5*time.Second) {
		t.Error("the older connection that sent nothing was not the one closed")
	}
	if closed3162(silentNew, 200*time.Millisecond) {
		t.Error("the newer silent connection was closed too")
	}
	if closed3162(partial, 200*time.Millisecond) {
		t.Error("the connection that had sent something was closed before one that had sent nothing")
	}

	// Next: the other silent one. Then the one that sent something.
	c2, err := mysqlwire.Dial(addr, "no-such-server", "tok", "")
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if !closed3162(silentNew, 5*time.Second) || closed3162(partial, 200*time.Millisecond) {
		t.Error("the second arrival did not close the remaining silent connection alone")
	}
	c3, err := mysqlwire.Dial(addr, "no-such-server", "tok", "")
	if err != nil {
		t.Fatal(err)
	}
	defer c3.Close()
	if !closed3162(partial, 5*time.Second) {
		t.Error("the third arrival did not close the last connection that had not logged in")
	}

	// Full of logged-in clients now: nobody is closed, the new one gets 1040.
	if !refused3162(t, addr) {
		t.Fatal("a port full of logged-in clients gave a handshake to one more")
	}
	for _, c := range []*mysqlwire.Conn{c, c2, c3} {
		if _, err := c.Ping(); err != nil {
			t.Fatalf("a logged-in client was closed to make room: %v", err)
		}
	}
}

// A connection that has not logged in for less than the threshold is not
// closed: the new client is refused.
func TestFlashbackPort_aRecentConnectionIsNotClosedToMakeRoom(t *testing.T) {
	addr := servePortFor3162(t, flashbackConfig{MaxConns: 1}) // threshold: 1 second
	recent := greeted3162(t, addr)
	if !refused3162(t, addr) {
		t.Fatal("a second client was taken")
	}
	if closed3162(recent, 200*time.Millisecond) {
		t.Fatal("a connection younger than the threshold was closed to make room")
	}
	// Past the threshold it is.
	time.Sleep(900 * time.Millisecond)
	c := loginSoon3162(t, addr)
	defer c.Close()
	if !closed3162(recent, 5*time.Second) {
		t.Fatal("past the threshold the connection was not closed")
	}
}

// Two clients arrive at once at a full port with one connection to close:
// one of them gets the place, the other error 1040, and the logged-in
// client that was there stays. At the end the count is back to zero.
func TestFlashbackPort_twoArrivalsOneConnectionToClose(t *testing.T) {
	evictAfter3162(t, 50*time.Millisecond)
	addr := servePortFor3162(t, flashbackConfig{MaxConns: 2})
	stays, err := mysqlwire.Dial(addr, "no-such-server", "tok", "")
	if err != nil {
		t.Fatal(err)
	}
	defer stays.Close()
	silent := greeted3162(t, addr)
	time.Sleep(150 * time.Millisecond)

	var wg sync.WaitGroup
	conns := make([]*mysqlwire.Conn, 2)
	errs := make([]error, 2)
	for i := range conns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conns[i], errs[i] = mysqlwire.Dial(addr, "no-such-server", "tok", "")
		}()
	}
	wg.Wait()
	in := 0
	for i, c := range conns {
		if errs[i] != nil {
			if !strings.Contains(errs[i].Error(), "1040") {
				t.Errorf("the arrival that found no room got %v, want error 1040", errs[i])
			}
			continue
		}
		in++
		defer c.Close()
		if _, err := c.Ping(); err != nil {
			t.Errorf("the arrival that got the place is not served: %v", err)
		}
	}
	if in != 1 {
		t.Fatalf("%d of the two arrivals logged in, want exactly 1", in)
	}
	if !closed3162(silent, 5*time.Second) {
		t.Error("the connection that had not logged in was not closed")
	}
	if _, err := stays.Ping(); err != nil {
		t.Fatalf("the logged-in client was closed: %v", err)
	}

	// Back to zero: with everyone gone the port takes 2 again, and no more.
	stays.Close()
	for _, c := range conns {
		if c != nil {
			c.Close()
		}
	}
	a, b := loginSoon3162(t, addr), loginSoon3162(t, addr)
	defer a.Close()
	defer b.Close()
	if !refused3162(t, addr) {
		t.Fatal("the port took more than its limit after the room was made: a place was counted twice")
	}
}

// A connection that finishes its login while it is being closed to make
// room is either served or closed, never both, and holds one place either
// way. With a threshold of zero every connection in its handshake may be
// closed, so two clients racing for a port of one meet at that moment.
func TestFlashbackPort_loginRacingTheClose(t *testing.T) {
	evictAfter3162(t, -1)
	addr := servePortFor3162(t, flashbackConfig{MaxConns: 1})
	for round := range 40 {
		var wg sync.WaitGroup
		conns := make([]*mysqlwire.Conn, 2)
		for i := range conns {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conns[i], _ = mysqlwire.Dial(addr, "no-such-server", "tok", "")
			}()
		}
		wg.Wait()
		served := 0
		for _, c := range conns {
			if c == nil {
				continue
			}
			if _, err := c.Ping(); err == nil {
				served++
			}
		}
		if served > 1 {
			t.Fatalf("round %d: %d clients are served on a port of one", round, served)
		}
		if served == 1 && !refused3162(t, addr) {
			t.Fatalf("round %d: a logged-in client holds the only place and one more was taken", round)
		}
		for _, c := range conns {
			if c != nil {
				c.Close()
			}
		}
		// The place comes back each round: none is lost, none counted twice.
		loginSoon3162(t, addr).Close()
		time.Sleep(10 * time.Millisecond)
	}
}

// The decision itself: of a login that finishes and a close to make room
// on the same connection, exactly one wins, and a connection is picked to
// close at most once.
func TestPreLoginSet_servedOrClosedNeverBoth(t *testing.T) {
	evictAfter3162(t, -1)
	for range 2000 {
		a, b := net.Pipe()
		defer b.Close()
		set := newPreLoginSet()
		p := set.add(a, time.Now())
		var served bool
		var picked [2]*preLoginConn
		var wg sync.WaitGroup
		wg.Add(3)
		go func() { defer wg.Done(); served = p.loggedIn() }()
		for i := range picked {
			go func() { defer wg.Done(); picked[i] = set.evictOne(time.Now()) }()
		}
		wg.Wait()
		closes := 0
		for _, c := range picked {
			if c != nil {
				closes++
			}
		}
		if served && closes != 0 || !served && closes != 1 {
			t.Fatalf("served=%v and picked to close %d times", served, closes)
		}
		if served == p.evicted() {
			t.Fatalf("served=%v evicted=%v", served, p.evicted())
		}
		set.ended(p)
		if len(set.waiting) != 0 {
			t.Fatalf("%d connections still counted as not logged in", len(set.waiting))
		}
	}
}

// Who is picked: nobody under the threshold, nobody who logged in; a silent
// one before one that sent; the oldest first.
func TestPreLoginSet_whoIsClosed(t *testing.T) {
	now := time.Now()
	mk := func(set *preLoginSet, age time.Duration, sent bool) *preLoginConn {
		a, b := net.Pipe()
		t.Cleanup(func() { a.Close(); b.Close() })
		p := set.add(a, now.Add(-age))
		p.sent.Store(sent)
		return p
	}
	set := newPreLoginSet()
	if set.evictOne(now) != nil {
		t.Fatal("picked from an empty port")
	}
	young := mk(set, 500*time.Millisecond, false)
	exact := mk(set, time.Second, false)
	in := mk(set, time.Hour, false)
	if !in.loggedIn() {
		t.Fatal("login refused")
	}
	if got := set.evictOne(now); got != nil {
		t.Fatalf("picked a connection at or under one second, or one that logged in (young=%v exact=%v in=%v)", got == young, got == exact, got == in)
	}
	sentOldest := mk(set, time.Minute, true)
	silentOld := mk(set, 10*time.Second, false)
	silentNewer := mk(set, 5*time.Second, false)
	for i, want := range []*preLoginConn{silentOld, silentNewer, sentOldest, nil} {
		if got := set.evictOne(now); got != want {
			t.Fatalf("pick %d is not the expected connection", i+1)
		}
	}
	if in.evicted() {
		t.Fatal("a logged-in connection was closed")
	}
}

// The moment itself: the decision to close a connection is taken while its
// handshake completes on the wire (its socket still open, so the client is
// told it logged in). It is not served: the port hangs up, and its first
// command gets no answer.
func TestFlashbackPort_closedToMakeRoomIsNotServedAfterItsLogin(t *testing.T) {
	srv := newFlashbackConsole(t, "tok")
	mysrv, err := shim.NewMySQLServerWithTLS("", nil)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	set := newPreLoginSet()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := ln.Accept()
		if err != nil {
			return
		}
		p := set.add(c, time.Now())
		// What evictOne decides, without the close that follows it.
		set.mu.Lock()
		p.state = preLoginEvicted
		delete(set.waiting, p)
		set.mu.Unlock()
		handleFlashbackConn(context.Background(), srv, p, mysrv, flashbackCreds{srv: srv}, shim.NewGate(0), flashbackConfig{}.withDefaults(), slog.Default())
	}()
	c, err := mysqlwire.Dial(ln.Addr().String(), "no-such-server", "tok", "")
	if err == nil {
		defer c.Close()
		if _, err := c.Ping(); err == nil {
			t.Fatal("a connection closed to make room was served after its login finished")
		}
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("its goroutine did not end")
	}
}

// Refusals are logged at once the first time, then at most once per
// interval, with how many were refused since the line before.
func TestRefusalLog_oneLinePerInterval(t *testing.T) {
	r := refusalLog{every: 10 * time.Second}
	t0 := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	if n, say := r.refused(t0); !say || n != 1 {
		t.Fatalf("the first refusal: %d, %v; want a line for 1", n, say)
	}
	for i := 1; i <= 500; i++ {
		if n, say := r.refused(t0.Add(time.Duration(i) * time.Millisecond)); say {
			t.Fatalf("refusal %d of a burst was logged (%d)", i, n)
		}
	}
	if n, say := r.refused(t0.Add(9999 * time.Millisecond)); say {
		t.Fatalf("a line before the interval passed (%d)", n)
	}
	if n, say := r.refused(t0.Add(10 * time.Second)); !say || n != 502 {
		t.Fatalf("after the interval: %d, %v; want a line for the 502 since the first", n, say)
	}
	if n, say := r.refused(t0.Add(10*time.Second + time.Millisecond)); say {
		t.Fatalf("a second line right after (%d)", n)
	}
	if n, say := r.refused(t0.Add(time.Hour)); !say || n != 2 {
		t.Fatalf("an hour later: %d, %v; want a line for 2", n, say)
	}
}
