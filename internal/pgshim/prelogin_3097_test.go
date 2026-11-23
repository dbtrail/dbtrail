package pgshim

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dbtrail/dbtrail/internal/shim"
)

// The port's places before a login (#3097): when the port is full and a new
// connection arrives, the connection that has gone longest without logging in
// loses its place to it, if it has had more than preLoginGrace.

// shortGrace makes a connection that has not logged in lose its place after
// d, for the test.
func shortGrace(t *testing.T, d time.Duration) {
	t.Helper()
	old := preLoginGrace
	preLoginGrace = d
	t.Cleanup(func() { preLoginGrace = old })
}

// loginCode logs a client in and returns it, or the SQLSTATE it was refused
// with.
func loginCode(t *testing.T, addr string) (*pgconn.PgConn, string) {
	t.Helper()
	host, port, _ := net.SplitHostPort(addr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := pgconn.Connect(ctx, fmt.Sprintf("host=%s port=%s user=app password=%s dbname=orders sslmode=disable", host, port, portPassword))
	if err == nil {
		t.Cleanup(func() { _ = c.Close(context.Background()) })
		return c, ""
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return nil, pe.Code
	}
	return nil, err.Error()
}

// silent opens a connection that sends nothing.
func silent(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// closedByPort reports whether the port closed c within d.
func closedByPort(c net.Conn, d time.Duration) bool {
	_ = c.SetReadDeadline(time.Now().Add(d))
	_, err := c.Read(make([]byte, 1))
	return errors.Is(err, io.EOF) || err != nil && strings.Contains(err.Error(), "reset")
}

// A port whose every place is a logged-in client's: nobody is put out, and
// the newcomer is told the port is full, as before.
func TestPreLogin_fullOfClientsPutsNobodyOut(t *testing.T) {
	shortGrace(t, 10*time.Millisecond)
	addr := serveAddrCfg(t, Config{Login: (&portLogin{run: one}).login, MaxConns: 3})
	var held []*pgconn.PgConn
	for i := range 3 {
		c, code := loginCode(t, addr)
		if code != "" {
			t.Fatalf("client %d: %s", i, code)
		}
		held = append(held, c)
	}
	time.Sleep(50 * time.Millisecond)
	for range 3 {
		if _, code := loginCode(t, addr); code != "53300" {
			t.Fatalf("a newcomer to a port full of clients got %q, want 53300", code)
		}
	}
	for i, c := range held {
		if _, err := c.Exec(context.Background(), "SELECT 1").ReadAll(); err != nil {
			t.Errorf("client %d was put out: %v", i, err)
		}
	}
}

// A place held by a connection that sent nothing for longer than the grace
// goes to the newcomer, which logs in; the silent one is closed.
func TestPreLogin_theOneWaitingLongestLosesItsPlace(t *testing.T) {
	shortGrace(t, 100*time.Millisecond)
	addr := serveAddrCfg(t, Config{Login: (&portLogin{run: one}).login, MaxConns: 3})
	first := silent(t, addr)
	time.Sleep(30 * time.Millisecond)
	second := silent(t, addr)
	client, code := loginCode(t, addr)
	if code != "" {
		t.Fatalf("the third place: %s", code)
	}

	// Full, and nobody has waited longer than the grace yet: refused.
	if _, code := loginCode(t, addr); code != "53300" {
		t.Fatalf("before the grace: %q, want 53300", code)
	}
	time.Sleep(150 * time.Millisecond)

	// Now the first silent one, the oldest, loses its place.
	c1, code := loginCode(t, addr)
	if code != "" {
		t.Fatalf("after the grace: %s, want to be let in", code)
	}
	if !closedByPort(first, 2*time.Second) {
		t.Error("the connection that waited longest was not closed")
	}
	if closedByPort(second, 100*time.Millisecond) {
		t.Error("the newer silent connection was closed too")
	}
	// The next newcomer takes the second one's.
	c2, code := loginCode(t, addr)
	if code != "" {
		t.Fatalf("the next newcomer: %s", code)
	}
	if !closedByPort(second, 2*time.Second) {
		t.Error("the second silent connection was not closed")
	}
	// Three clients hold the three places: the port is full again, of
	// clients, and stays so.
	if _, code := loginCode(t, addr); code != "53300" {
		t.Fatalf("a port full of clients: %q, want 53300", code)
	}
	for i, c := range []*pgconn.PgConn{client, c1, c2} {
		if _, err := c.Exec(context.Background(), "SELECT 1").ReadAll(); err != nil {
			t.Errorf("client %d: %v", i, err)
		}
	}
	// A place given back is a place again: one client leaves, one comes.
	_ = c2.Close(context.Background())
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, code := loginCode(t, addr); code == "" {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("after a client left, a newcomer still gets %q", code)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, code := loginCode(t, addr); code != "53300" {
		t.Fatalf("the places were not counted right: a fourth client got %q", code)
	}
}

// Many arrivals at once at a port full of silent connections: each takes one
// place, no place is given twice, and the port never holds more than its
// limit.
func TestPreLogin_arrivalsAtOnce(t *testing.T) {
	// A grace far longer than a login takes, so that no arrival is taken
	// for a connection that says nothing.
	shortGrace(t, 500*time.Millisecond)
	const places = 8
	addr := serveAddrCfg(t, Config{Login: (&portLogin{run: one}).login, MaxConns: places})
	var quiet []net.Conn
	for range places {
		quiet = append(quiet, silent(t, addr))
	}
	time.Sleep(600 * time.Millisecond)
	var wg sync.WaitGroup
	var in, full atomic.Int32
	for range 2 * places {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch _, code := loginCode(t, addr); code {
			case "":
				in.Add(1)
			case "53300":
				full.Add(1)
			default:
				t.Errorf("an arrival got %q", code)
			}
		}()
	}
	wg.Wait()
	if in.Load() != places || full.Load() != places {
		t.Errorf("%d let in and %d told the port is full, want %d and %d", in.Load(), full.Load(), places, places)
	}
	for i, c := range quiet {
		if !closedByPort(c, 2*time.Second) {
			t.Errorf("silent connection %d was not closed", i)
		}
	}
}

// The place itself: taken or logged in, never both, whoever comes first and
// when both come at once.
func TestConnPlace_takenOrLoggedInNeverBoth(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()

	// Logged in first: it is not taken, and gives its place back itself.
	p := newPreLogins()
	pl := p.add(a)
	if !pl.loggedIn() {
		t.Fatal("a waiting connection could not log in")
	}
	if got := p.takeOldest(0); got != nil {
		t.Fatal("a logged-in connection lost its place")
	}
	if !pl.leave() {
		t.Error("a logged-in connection does not give its place back")
	}

	// Taken first: it cannot log in, and does not give back a place that is
	// another's.
	pl = p.add(a)
	if got := p.takeOldest(0); got != pl {
		t.Fatal("the waiting connection was not taken")
	}
	if pl.loggedIn() {
		t.Error("a connection whose place was taken logged in")
	}
	if pl.leave() {
		t.Error("a connection whose place was taken gave a place back")
	}
	if got := p.takeOldest(0); got != nil {
		t.Error("a place was taken twice")
	}

	// Left first: nothing to take.
	pl = p.add(a)
	if !pl.leave() {
		t.Error("a connection that left before logging in does not give its place back")
	}
	if got := p.takeOldest(0); got != nil {
		t.Error("the place of a connection that left was taken")
	}

	// Younger than the grace: kept. The oldest first.
	old, young := p.add(a), p.add(a)
	if got := p.takeOldest(time.Hour); got != nil {
		t.Error("a connection younger than the grace lost its place")
	}
	old.since = time.Now().Add(-2 * time.Second)
	if got := p.takeOldest(time.Second); got != old {
		t.Error("the oldest was not the one taken")
	}
	if got := p.takeOldest(time.Second); got != nil {
		t.Error("the younger one, within its grace, was taken")
	}
	young.leave()

	// Both at once, many times: exactly one of the two wins, and the places
	// given back are as many as were held.
	for range 2000 {
		c1, c2 := net.Pipe()
		pl := p.add(c1)
		var took *connPlace
		var logged bool
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); took = p.takeOldest(0) }()
		go func() { defer wg.Done(); logged = pl.loggedIn() }()
		wg.Wait()
		if (took != nil) == logged {
			t.Fatalf("taken=%v and logged in=%v", took != nil, logged)
		}
		if gives := pl.leave(); gives != logged {
			t.Fatalf("logged in=%v and gives its place back=%v", logged, gives)
		}
		_ = c1.Close()
		_ = c2.Close()
	}
	p.mu.Lock()
	left := p.waiting.Len()
	p.mu.Unlock()
	if left != 0 {
		t.Errorf("%d connections are still listed as not logged in", left)
	}
	// A connection with no place (a port with no limit asks nothing of it).
	var none *connPlace
	if !none.loggedIn() || !none.leave() {
		t.Error("a connection without a place could not log in or leave")
	}
}

// A connection whose proof reached the port keeps its place while the proof
// is checked and its server is opened, however long that takes and however
// full the port is; and one that sent nothing goes before one that spoke.
func TestPreLogin_aProvedConnectionKeepsItsPlace(t *testing.T) {
	shortGrace(t, 20*time.Millisecond)
	entered, release := make(chan struct{}, 4), make(chan struct{})
	slow := func(_ context.Context, user, _ string, password *Password, _ func()) (*Binding, *Error) {
		if !password.Matches([]string{portPassword}) {
			return nil, &Error{Code: "28P01", Message: "no"}
		}
		// Opening its server takes long.
		entered <- struct{}{}
		<-release
		return &Binding{Run: one}, nil
	}
	addr := serveAddrCfg(t, Config{Login: slow, MaxConns: 2})
	type result struct {
		c    *pgconn.PgConn
		code string
	}
	results := make(chan result, 2)
	for range 2 {
		go func() { c, code := loginCode(t, addr); results <- result{c, code} }()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("the logins did not reach their server")
		}
	}
	time.Sleep(100 * time.Millisecond)
	// Full of connections that proved their password and are not in yet:
	// older than the grace, and none loses its place.
	if _, code := loginCode(t, addr); code != "53300" {
		t.Errorf("a newcomer got %q, want 53300: a proved connection lost its place", code)
	}
	close(release)
	for range 2 {
		r := <-results
		if r.code != "" {
			t.Errorf("a connection that had proved its password was put out: %s", r.code)
		} else if _, err := r.c.Exec(context.Background(), "SELECT 1").ReadAll(); err != nil {
			t.Errorf("a connection that had proved its password: %v", err)
		}
	}

	// Among those that have not: the one that sent nothing first, though the
	// one that spoke is older.
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	p := newPreLogins()
	spoke, quiet := p.add(a), p.add(a)
	spoke.spoke.Store(true)
	spoke.since, quiet.since = time.Now().Add(-10*time.Second), time.Now().Add(-5*time.Second)
	if got := p.takeOldest(time.Second); got != quiet {
		t.Error("the connection that sent nothing was not the first to lose its place")
	}
	if got := p.takeOldest(time.Second); got != spoke {
		t.Error("with no silent one left, the oldest that spoke was not taken")
	}
	// A place kept at the proof stays kept at the end of the login.
	kept := p.add(a)
	if !kept.loggedIn() || !kept.loggedIn() {
		t.Error("a place kept twice was lost")
	}
	if p.takeOldest(0) != nil {
		t.Error("a kept place was taken")
	}
}

// tenantLogin logs alice in on a port in the first mode (bintrail-pg
// flashback) and returns her connection, or the SQLSTATE she was refused
// with.
func tenantLogin(t *testing.T, addr string) (*pgconn.PgConn, string) {
	t.Helper()
	host, port, _ := net.SplitHostPort(addr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := pgconn.Connect(ctx, fmt.Sprintf("host=%s port=%s user=alice password=alice-pw dbname=public sslmode=disable", host, port))
	if err == nil {
		t.Cleanup(func() { _ = c.Close(context.Background()) })
		return c, ""
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return nil, pe.Code
	}
	return nil, err.Error()
}

func aliceAuth(t *testing.T) shim.TenantAuth {
	t.Helper()
	auth, err := shim.NewTenantAuth(map[string]string{"alice": "alice-pw"})
	if err != nil {
		t.Fatal(err)
	}
	return auth
}

// placeGivenBack waits for the port's one place to be free again: a tenant
// gets in. The grace is an hour in these tests, so nobody is put out to let
// her in.
func placeGivenBack(t *testing.T, addr, after string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, code := tenantLogin(t, addr)
		if code == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after %s, the place was not given back: a newcomer still gets %q", after, code)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// The first mode (bintrail-pg flashback) keeps the same places: a silent
// connection loses its own to a newcomer, and a tenant that logged in does
// not.
func TestPreLogin_firstMode(t *testing.T) {
	shortGrace(t, 20*time.Millisecond)
	addr := serveAddrCfg(t, Config{Auth: aliceAuth(t), MaxConns: 1})
	quiet := silent(t, addr)
	time.Sleep(100 * time.Millisecond)
	tenant, code := tenantLogin(t, addr)
	if code != "" {
		t.Fatalf("a tenant at a port full of a silent connection: %s, want to be let in", code)
	}
	if !closedByPort(quiet, 2*time.Second) {
		t.Error("the silent connection was not closed")
	}
	time.Sleep(100 * time.Millisecond)
	if _, code := tenantLogin(t, addr); code != "53300" {
		t.Errorf("a newcomer to a port full of a tenant got %q, want 53300", code)
	}
	if closedByPort(tenant.Conn(), 100*time.Millisecond) {
		t.Error("a tenant that had logged in was closed to make room")
	}
}

// A client that leaves in the middle of its login, in the first mode: its
// place is given back.
func TestPreLogin_firstModeClientLeavesDuringTheLogin(t *testing.T) {
	shortGrace(t, time.Hour)
	addr := serveAddrCfg(t, Config{Auth: aliceAuth(t), MaxConns: 1})
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	startup, err := (&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{"user": "alice", "database": "public"}}).Encode(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write(startup); err != nil {
		t.Fatal(err)
	}
	// The port's offer of SCRAM: the login is under way.
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := c.Read(make([]byte, 1)); err != nil {
		t.Fatalf("no offer of a login: %v", err)
	}
	if _, code := tenantLogin(t, addr); code != "53300" {
		t.Fatalf("with the one place held by a login under way, a newcomer got %q, want 53300", code)
	}
	_ = c.Close()
	placeGivenBack(t, addr, "a client left during its login")
}

// hangingIndex is an index server whose every statement waits for its
// context to end.
type hangingIndex struct {
	entered, cancelled chan struct{}
	release            chan struct{}
}

func (d *hangingIndex) Connect(context.Context) (driver.Conn, error) { return hangingConn{d}, nil }
func (d *hangingIndex) Driver() driver.Driver                        { return d }
func (d *hangingIndex) Open(string) (driver.Conn, error)             { return hangingConn{d}, nil }

type hangingConn struct{ d *hangingIndex }

func (c hangingConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not used") }
func (c hangingConn) Close() error                        { return nil }
func (c hangingConn) Begin() (driver.Tx, error)           { return nil, errors.New("not used") }
func (c hangingConn) Ping(context.Context) error          { return nil }

func (c hangingConn) wait(ctx context.Context) error {
	select {
	case c.d.entered <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		select {
		case c.d.cancelled <- struct{}{}:
		default:
		}
		return ctx.Err()
	case <-c.d.release:
		return errors.New("the test ended")
	}
}

func (c hangingConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	return nil, c.wait(ctx)
}

func (c hangingConn) ExecContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
	return nil, c.wait(ctx)
}

func (c hangingConn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	return nil, c.wait(ctx)
}

// A client that hangs up in the middle of a statement, in the first mode:
// the socket is watched while the statement runs (shim.WatchConn), so the
// connection's context ends, the statement stops on the index server, and
// the place is given back. Without the watch the connection would hold its
// place until the statement ended on its own.
func TestPreLogin_firstModeClientHangsUpDuringAStatement(t *testing.T) {
	shortGrace(t, time.Hour)
	idx := &hangingIndex{entered: make(chan struct{}, 1), cancelled: make(chan struct{}, 1), release: make(chan struct{})}
	db := sql.OpenDB(idx)
	t.Cleanup(func() { _ = db.Close() })
	addr := serveAddrCfg(t, Config{Auth: aliceAuth(t), MaxConns: 1, IndexDB: db,
		ShimConfig: shim.Config{NoArchive: true, IndexDBName: "idx"}})
	// Registered after the port, so it runs before the port is stopped: no
	// statement is left waiting.
	t.Cleanup(func() { close(idx.release) })

	c, code := tenantLogin(t, addr)
	if code != "" {
		t.Fatalf("the tenant: %s", code)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = c.Exec(context.Background(), "SELECT * FROM _flashback.users AS OF '2026-01-01 00:00:00' WHERE id = 1").ReadAll()
	}()
	select {
	case <-idx.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the statement did not reach the index server")
	}
	// The client is gone, with its statement still running.
	_ = c.Conn().Close()
	select {
	case <-idx.cancelled:
	case <-time.After(10 * time.Second):
		t.Fatal("the client hung up and its connection's context did not end: the statement is still running")
	}
	<-done
	placeGivenBack(t, addr, "a client hung up during its statement")
}
