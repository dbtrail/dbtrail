package pgshim

import (
	"context"
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
	shortGrace(t, 50*time.Millisecond)
	const places = 8
	addr := serveAddrCfg(t, Config{Login: (&portLogin{run: one}).login, MaxConns: places})
	var quiet []net.Conn
	for range places {
		quiet = append(quiet, silent(t, addr))
	}
	time.Sleep(120 * time.Millisecond)
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

// --- a salt for each login -------------------------------------------------------

func TestScramSaltFor(t *testing.T) {
	long := strings.Repeat("x", 100000)
	names := []string{"app", "App", "APP", "app ", " app", "", "nobody", "app\x00", "ápp", long, long + "y"}
	seen := map[string]string{}
	for _, n := range names {
		salt := scramSaltFor(n)
		if len(salt) != scramSaltLen {
			t.Fatalf("the salt of %.20q is %d bytes", n, len(salt))
		}
		if string(scramSaltFor(n)) != string(salt) {
			t.Errorf("the salt of %.20q changed between two logins", n)
		}
		if other, dup := seen[string(salt)]; dup {
			t.Errorf("%.20q and %.20q have the same salt", n, other)
		}
		seen[string(salt)] = n
	}
}

// deriveCounter counts derivations, and how many ran at once.
type deriveCounter struct {
	total, now, most atomic.Int64
}

func countDerivations(t *testing.T) *deriveCounter {
	t.Helper()
	d := &deriveCounter{}
	old := scramDerive
	scramDerive = func(pw string, salt []byte, n int) []byte {
		d.total.Add(1)
		cur := d.now.Add(1)
		for m := d.most.Load(); cur > m && !d.most.CompareAndSwap(m, cur); m = d.most.Load() {
		}
		defer d.now.Add(-1)
		return old(pw, salt, n)
	}
	t.Cleanup(func() { scramDerive = old })
	return d
}

// wrongLogin tries user with a wrong password and returns the refusal.
func wrongLogin(t *testing.T, addr, user string) string {
	t.Helper()
	host, port, _ := net.SplitHostPort(addr)
	cfg, err := pgconn.ParseConfig(fmt.Sprintf("host=%s port=%s password=wrong dbname=orders sslmode=disable", host, port))
	if err != nil {
		t.Fatal(err)
	}
	cfg.User = user
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err = pgconn.ConnectConfig(ctx, cfg)
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		t.Fatalf("%.20q: %v", user, err)
	}
	return pe.Code
}

// appLogins is a host with the port's password for every username and one
// more for the username app1.
func appLogins(_ context.Context, user, _ string, password *Password, _ func()) (*Binding, *Error) {
	all := []string{"token", portPassword, "app1-own"}
	WarmPasswords(all)
	candidates := all[:2]
	if user == "app1" {
		candidates = all
	}
	if !password.Matches(candidates) {
		return nil, &Error{Code: "28P01", Message: fmt.Sprintf("password authentication failed for user %q", user)}
	}
	return &Binding{Run: one}, nil
}

// The first login under a username derives the same number of keys whether
// the username has a password of its own or is nobody's, and the next ones
// derive none; an empty name, a long one and one in capitals are names like
// any other.
func TestSCRAM_everyNameTheSameWork(t *testing.T) {
	WarmPasswords([]string{"a set no other test uses"})
	d := countDerivations(t)
	addr := serveAddrCfg(t, Config{Login: appLogins})
	for _, user := range []string{"app1", "nobody", "APP1", "x", strings.Repeat("n", 4000), "app1 "} {
		before := d.total.Load()
		if code := wrongLogin(t, addr, user); code != "28P01" {
			t.Fatalf("%.20q: %s", user, code)
		}
		if got := d.total.Load() - before; got != scramDerivations {
			t.Errorf("%.20q: its first login derived %d keys, want %d as for every name", user, got, scramDerivations)
		}
		before = d.total.Load()
		for range 3 {
			if code := wrongLogin(t, addr, user); code != "28P01" {
				t.Fatalf("%.20q: %s", user, code)
			}
		}
		if got := d.total.Load() - before; got != 0 {
			t.Errorf("%.20q: three more logins derived %d keys, want none", user, got)
		}
	}
	// The right password still opens the port, under a name with a password
	// of its own and under one without.
	host, port, _ := net.SplitHostPort(addr)
	for user, pw := range map[string]string{"app1": "app1-own", "anyone": portPassword} {
		c, err := pgconn.Connect(context.Background(), fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=orders sslmode=disable", host, port, user, pw))
		if err != nil {
			t.Fatalf("%s with its password: %v", user, err)
		}
		_ = c.Close(context.Background())
	}
	// A login's own password under another name does not.
	cfg, _ := pgconn.ParseConfig(fmt.Sprintf("host=%s port=%s user=other password=app1-own dbname=orders sslmode=disable", host, port))
	if _, err := pgconn.ConnectConfig(context.Background(), cfg); err == nil {
		t.Error("a login's own password opened the port under another name")
	}

	// A change of any password forgets every key: each name's next login
	// derives again, the same number.
	WarmPasswords([]string{"another set"})
	for _, user := range []string{"nobody", "app1"} {
		before := d.total.Load()
		wrongLogin(t, addr, user)
		// appLogins warms its own set back first, which is a change too.
		if got := d.total.Load() - before; got != scramDerivations {
			t.Errorf("%s after a change of passwords: derived %d keys, want %d", user, got, scramDerivations)
		}
	}
}

// The names whose keys are kept are bounded, and so are the keys of one
// name: logins under ever new names do not grow what the port holds.
func TestSCRAM_namesKeptAreBounded(t *testing.T) {
	WarmPasswords([]string{"a set for the bound"})
	old := scramDerive
	scramDerive = func(pw string, salt []byte, _ int) []byte { return append([]byte(pw+"|"), salt...) }
	t.Cleanup(func() { scramDerive = old; WarmPasswords([]string{"cleared after the bound"}) })
	held := func() (names, order int) {
		scramCache.mu.Lock()
		defer scramCache.mu.Unlock()
		if scramCache.used == nil {
			return len(scramCache.names), 0
		}
		return len(scramCache.names), scramCache.used.Len()
	}
	for i := range scramNamesMax + 500 {
		scramKeys(scramSaltFor(fmt.Sprintf("name-%d", i)), []string{"pw", scramDummy})
	}
	if names, order := held(); names != scramNamesMax || order != scramNamesMax {
		t.Fatalf("%d names kept (%d in order), want %d", names, order, scramNamesMax)
	}
	// The one used last is kept, the first ones are gone.
	var derived atomic.Int64
	scramDerive = func(pw string, salt []byte, _ int) []byte { derived.Add(1); return append([]byte(pw+"|"), salt...) }
	scramKeys(scramSaltFor(fmt.Sprintf("name-%d", scramNamesMax+499)), []string{"pw", scramDummy})
	if derived.Load() != 0 {
		t.Error("the name used last was forgotten")
	}
	scramKeys(scramSaltFor("name-0"), []string{"pw", scramDummy})
	if derived.Load() != scramDerivations {
		t.Errorf("the name used first: %d keys derived, want %d (it was forgotten)", derived.Load(), scramDerivations)
	}
	// A name used again moves to the front: it outlives names used since.
	scramKeys(scramSaltFor("kept"), []string{"pw"})
	for i := range scramNamesMax - 1 {
		scramKeys(scramSaltFor("kept"), []string{"pw"})
		scramKeys(scramSaltFor(fmt.Sprintf("later-%d", i)), []string{"pw"})
	}
	derived.Store(0)
	scramKeys(scramSaltFor("kept"), []string{"pw"})
	if derived.Load() != 0 {
		t.Error("a name used all along was forgotten")
	}
	// One name asked for ever new passwords keeps a bounded number of keys.
	salt := scramSaltFor("many passwords")
	for i := range 20 * scramNameFormsMax {
		scramKeys(salt, []string{fmt.Sprintf("pw-%d", i)})
	}
	var id [scramSaltLen]byte
	copy(id[:], salt)
	scramCache.mu.Lock()
	n := len(scramCache.names[id].keys)
	scramCache.mu.Unlock()
	if n > scramNameFormsMax {
		t.Errorf("one name keeps %d keys, more than %d", n, scramNameFormsMax)
	}
	// The same form asked for twice in one login is derived once, and every
	// form asked for is answered.
	derived.Store(0)
	keys := scramKeys(scramSaltFor("twice"), []string{"a", "a", "b", scramDummy, scramDummy})
	if len(keys) != 3 || keys["a"] == nil || keys["b"] == nil || keys[scramDummy] == nil {
		t.Errorf("keys %v", keys)
	}
	if derived.Load() != scramDerivations {
		t.Errorf("%d derivations for three forms, want %d", derived.Load(), scramDerivations)
	}
}

// At most scramDeriveSlots logins derive at once, however many arrive.
func TestSCRAM_derivationsAtOnceAreBounded(t *testing.T) {
	WarmPasswords([]string{"a set for the slots"})
	d := countDerivations(t)
	addr := serveAddrCfg(t, Config{Login: appLogins})
	var wg sync.WaitGroup
	for i := range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if code := wrongLogin(t, addr, fmt.Sprintf("crowd-%d", i)); code != "28P01" {
				t.Errorf("crowd-%d: %s", i, code)
			}
		}()
	}
	wg.Wait()
	if most := d.most.Load(); most > scramDeriveSlots {
		t.Errorf("%d derivations ran at once, more than %d", most, scramDeriveSlots)
	}
	if got := d.total.Load(); got != 40*scramDerivations {
		t.Errorf("40 new names derived %d keys, want %d", got, 40*scramDerivations)
	}
}

// WarmPasswords forgets the keys when the set changed, and only then; the
// order the passwords are given in is not a change.
func TestWarmPasswords_forgetsOnAChange(t *testing.T) {
	old := scramDerive
	var derived atomic.Int64
	scramDerive = func(pw string, salt []byte, _ int) []byte { derived.Add(1); return append([]byte(pw+"|"), salt...) }
	t.Cleanup(func() { scramDerive = old; WarmPasswords([]string{"cleared after the change test"}) })
	WarmPasswords([]string{"one", "two"})
	salt := scramSaltFor("someone")
	scramKeys(salt, []string{"one"})
	derived.Store(0)
	for _, same := range [][]string{{"one", "two"}, {"two", "one"}} {
		WarmPasswords(same)
		scramKeys(salt, []string{"one"})
	}
	if derived.Load() != 0 {
		t.Errorf("the same set derived %d keys again", derived.Load())
	}
	for _, changed := range [][]string{{"one", "three"}, {"one"}, {"one", "two", ""}, {"onetwo"}, {"on", "etwo"}, nil} {
		WarmPasswords(changed)
		derived.Store(0)
		scramKeys(salt, []string{"one"})
		if derived.Load() != scramDerivations {
			t.Errorf("after the set changed to %q: %d keys derived, want %d", changed, derived.Load(), scramDerivations)
		}
	}
}
