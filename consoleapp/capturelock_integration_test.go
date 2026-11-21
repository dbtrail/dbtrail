//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

func captureLockTestSetup(t *testing.T) (dsn, name string, probe *sql.DB) {
	t.Helper()
	testutil.SkipIfNoMySQL(t)
	_, dbName := testutil.CreateTestDB(t)
	dsn = testutil.IntegrationDSN(dbName)
	name = fmt.Sprintf("bintrail_test_lock_%d", time.Now().UnixNano())
	probe, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { probe.Close() })
	return dsn, name, probe
}

// holderOf returns the connection id holding name, 0 when nobody does.
func holderOf(t *testing.T, probe *sql.DB, name string) int64 {
	t.Helper()
	var id sql.NullInt64
	if err := probe.QueryRow("SELECT IS_USED_LOCK(?)", name).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id.Int64
}

func TestIntegrationCaptureLockWaitsThenTakesOver(t *testing.T) {
	shortCaptureLockTimings(t)
	dsn, name, probe := captureLockTestSetup(t)
	ctx := context.Background()

	first, err := tryCaptureLock(ctx, dsn, name)
	if err != nil || first == nil {
		t.Fatalf("first try: lock=%v err=%v, want held", first, err)
	}
	defer first.Release()

	// Busy: (nil, nil), never an error.
	if l, err := tryCaptureLock(ctx, dsn, name); l != nil || err != nil {
		t.Fatalf("second try while held: lock=%v err=%v, want (nil, nil)", l, err)
	}

	var busyCalls atomic.Int32
	got := make(chan *captureLock, 1)
	go func() {
		l, _ := waitCaptureLock(ctx, dsn, name, func() { busyCalls.Add(1) }, nil)
		got <- l
	}()
	time.Sleep(600 * time.Millisecond) // several polls while busy
	select {
	case <-got:
		t.Fatal("the waiter took a lock someone else holds")
	default:
	}
	if n := busyCalls.Load(); n != 1 {
		t.Errorf("onBusy ran %d times over one busy streak, want 1", n)
	}

	first.Release()
	select {
	case l := <-got:
		if l == nil {
			t.Fatal("waiter returned no lock")
		}
		defer l.Release()
		if holderOf(t, probe, name) == 0 {
			t.Fatal("waiter returned but nobody holds the lock")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the waiter did not take the lock after the holder released it")
	}
}

// A cancelled wait must leave nothing behind that could still win the lock
// later: that is why each ask is GET_LOCK(name, 0).
func TestIntegrationCaptureLockCancelledWaitLeavesNothing(t *testing.T) {
	shortCaptureLockTimings(t)
	dsn, name, probe := captureLockTestSetup(t)

	first, err := tryCaptureLock(context.Background(), dsn, name)
	if err != nil || first == nil {
		t.Fatalf("first try: lock=%v err=%v", first, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := waitCaptureLock(ctx, dsn, name, nil, nil)
		done <- err
	}()
	time.Sleep(300 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled wait returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled wait did not return")
	}
	first.Release()
	time.Sleep(500 * time.Millisecond)
	if id := holderOf(t, probe, name); id != 0 {
		t.Fatalf("after a cancelled wait and the holder's release, connection %d holds the lock", id)
	}
}

// Release frees the lock at once, not when mysqld gets round to tearing the
// session down: the next process should not wait a poll for nothing.
func TestIntegrationCaptureLockReleaseIsImmediate(t *testing.T) {
	dsn, name, probe := captureLockTestSetup(t)
	l, err := tryCaptureLock(context.Background(), dsn, name)
	if err != nil || l == nil {
		t.Fatalf("try: lock=%v err=%v", l, err)
	}
	l.Release()
	l.Release() // twice is safe
	var got int
	if err := probe.QueryRow("SELECT GET_LOCK(?, 0)", name).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Fatal("lock still held right after Release")
	}
	_, _ = probe.Exec("DO RELEASE_LOCK(?)", name)
}

// MySQL ends a session idle past wait_timeout and frees its locks with it;
// measured, a second daemon then captured beside the first. The holder's
// session must carry the raised value.
func TestIntegrationCaptureLockSessionOutlivesWaitTimeout(t *testing.T) {
	dsn, name, probe := captureLockTestSetup(t)
	l, err := tryCaptureLock(context.Background(), dsn, name)
	if err != nil || l == nil {
		t.Fatalf("try: lock=%v err=%v", l, err)
	}
	defer l.Release()
	var v string
	err = probe.QueryRow(`SELECT v.VARIABLE_VALUE
		FROM performance_schema.variables_by_thread v
		JOIN performance_schema.threads t ON t.THREAD_ID = v.THREAD_ID
		WHERE t.PROCESSLIST_ID = IS_USED_LOCK(?) AND v.VARIABLE_NAME = 'wait_timeout'`, name).Scan(&v)
	if err != nil {
		t.Fatal(err)
	}
	if v != fmt.Sprint(captureLockSessionWaitTimeout) {
		t.Fatalf("holder session wait_timeout = %s, want %d", v, captureLockSessionWaitTimeout)
	}
}

// A lock that vanishes (the session killed, the network cut, wait_timeout)
// must be noticed, or the holder keeps writing while another process takes
// over. A dead connection is noticed by the next question, long before the
// limit that a merely slow index gets; and the lock is then free for another
// process.
func TestIntegrationCaptureLockLossIsNoticed(t *testing.T) {
	setCaptureLockTimings(t, 100*time.Millisecond, 100*time.Millisecond, 10*time.Second)
	dsn, name, probe := captureLockTestSetup(t)
	l, err := tryCaptureLock(context.Background(), dsn, name)
	if err != nil || l == nil {
		t.Fatalf("try: lock=%v err=%v", l, err)
	}
	defer l.Release()
	if lostWithin(l, 500*time.Millisecond) {
		t.Fatal("Lost closed while the lock is held")
	}
	id := holderOf(t, probe, name)
	if _, err := probe.Exec(fmt.Sprintf("KILL %d", id)); err != nil {
		t.Fatal(err)
	}
	if !lostWithin(l, 3*time.Second) {
		t.Fatalf("the holder's session was killed and Lost did not close within 3s (the limit for a slow index is %s)", captureLockUnconfirmedLimit)
	}
	next, err := tryCaptureLock(context.Background(), dsn, name)
	if err != nil || next == nil {
		t.Fatalf("after the holder's session died: lock=%v err=%v, want another process to take it", next, err)
	}
	next.Release()
}

// The other way to lose it: the session lives on but no longer owns the
// lock. The check must read the answer, not only notice a broken connection.
func TestIntegrationCaptureLockLossWithLiveSessionIsNoticed(t *testing.T) {
	shortCaptureLockTimings(t)
	dsn, name, _ := captureLockTestSetup(t)
	l, err := tryCaptureLock(context.Background(), dsn, name)
	if err != nil || l == nil {
		t.Fatalf("try: lock=%v err=%v", l, err)
	}
	defer l.Release()
	// Through the holder's own session, which stays open.
	if _, err := l.conn.ExecContext(context.Background(), "DO RELEASE_LOCK(?)", name); err != nil {
		t.Fatal(err)
	}
	select {
	case <-l.Lost():
	case <-time.After(5 * time.Second):
		t.Fatal("the lock was released under a live session and Lost never closed")
	}
}

// stallProxy forwards TCP to the index and can stop forwarding without
// closing anything: an index that is slow, or gone with no reset.
type stallProxy struct {
	ln     net.Listener
	target string

	mu     sync.Mutex
	frozen chan struct{} // nil while forwarding
	conns  []net.Conn
}

func newStallProxy(t *testing.T, target string) *stallProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &stallProxy{ln: ln, target: target}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s, err := net.Dial("tcp", target)
			if err != nil {
				c.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, c, s)
			p.mu.Unlock()
			go p.pipe(c, s)
			go p.pipe(s, c)
		}
	}()
	t.Cleanup(p.Close)
	return p
}

func (p *stallProxy) pipe(dst, src net.Conn) {
	defer dst.Close()
	defer src.Close()
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			p.mu.Lock()
			frozen := p.frozen
			p.mu.Unlock()
			if frozen != nil {
				<-frozen
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *stallProxy) Freeze() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.frozen == nil {
		p.frozen = make(chan struct{})
	}
}

func (p *stallProxy) Thaw() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.frozen != nil {
		close(p.frozen)
		p.frozen = nil
	}
}

// Cut closes every connection through the proxy, both sides.
func (p *stallProxy) Cut() {
	p.mu.Lock()
	conns := p.conns
	p.conns = nil
	p.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
}

func (p *stallProxy) Close() {
	p.ln.Close()
	p.Cut()
	p.Thaw()
}

// throughProxy returns dsn pointed at a stallProxy in front of its server.
func throughProxy(t *testing.T, dsn string) (string, *stallProxy) {
	t.Helper()
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Net != "tcp" {
		t.Skipf("the test index is reached over %q, the proxy needs tcp", cfg.Net)
	}
	p := newStallProxy(t, cfg.Addr)
	cfg.Addr = p.ln.Addr().String()
	return cfg.FormatDSN(), p
}

// Why the heartbeat's question carries no timeout: the driver cancels a
// statement by closing the connection, and the server frees the session's
// lock with it. A timeout on a slow answer gives the lock away.
func TestIntegrationCaptureLockContextTimeoutFreesTheLock(t *testing.T) {
	setCaptureLockTimings(t, 100*time.Millisecond, time.Hour, time.Hour) // no heartbeat in the way
	dsn, name, probe := captureLockTestSetup(t)
	l, err := tryCaptureLock(context.Background(), dsn, name)
	if err != nil || l == nil {
		t.Fatalf("try: lock=%v err=%v", l, err)
	}
	defer l.Release()
	before := holderOf(t, probe, name)
	if before == 0 {
		t.Fatal("nobody holds the lock right after taking it")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	_, err = l.conn.ExecContext(ctx, "DO SLEEP(1)")
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a statement cut by its context returned %v, want context.DeadlineExceeded", err)
	}
	// The same connection is gone for good: nothing can be retried on it.
	var one int
	if err := l.conn.QueryRowContext(context.Background(), "SELECT 1").Scan(&one); err == nil {
		t.Fatal("the connection answered after one of its statements timed out: the driver no longer closes it, and this design's premise changed")
	} else {
		t.Logf("next statement on the same connection: %v", err)
	}
	returned := time.Now()
	for holderOf(t, probe, name) != 0 {
		if time.Since(returned) > 10*time.Second {
			t.Fatalf("connection %d still holds the lock 10s after its statement timed out", before)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("the lock connection %d held was free %s after the timed-out statement returned", before, time.Since(returned).Round(10*time.Millisecond))
}

// An index that answers nothing for most of the limit and then catches up:
// the lock is kept, on the same session, and capture does not restart.
func TestIntegrationCaptureLockSlowIndexKeepsTheLock(t *testing.T) {
	setCaptureLockTimings(t, 100*time.Millisecond, 100*time.Millisecond, 3*time.Second)
	direct, name, probe := captureLockTestSetup(t)
	dsn, proxy := throughProxy(t, direct)
	l, err := tryCaptureLock(context.Background(), dsn, name)
	if err != nil || l == nil {
		t.Fatalf("try: lock=%v err=%v", l, err)
	}
	defer l.Release()
	before := holderOf(t, probe, name)

	for round := range 2 {
		proxy.Freeze()
		if lostWithin(l, 1800*time.Millisecond) { // 18 heartbeats with no answer
			t.Fatalf("round %d: Lost closed while the index was only slow", round)
		}
		proxy.Thaw()
		if lostWithin(l, 700*time.Millisecond) {
			t.Fatalf("round %d: Lost closed after the index caught up", round)
		}
	}
	if after := holderOf(t, probe, name); after != before || after == 0 {
		t.Fatalf("lock holder went from connection %d to %d across a slow index, want the same session", before, after)
	}
}

// An index that stops answering with nothing closed (a host gone, a network
// that drops packets): lost when the limit runs out, and the holder closes
// its side so the server can free the lock for the next process.
func TestIntegrationCaptureLockSilentIndexIsLostAtTheLimit(t *testing.T) {
	setCaptureLockTimings(t, 100*time.Millisecond, 100*time.Millisecond, 1500*time.Millisecond)
	direct, name, probe := captureLockTestSetup(t)
	dsn, proxy := throughProxy(t, direct)
	l, err := tryCaptureLock(context.Background(), dsn, name)
	if err != nil || l == nil {
		t.Fatalf("try: lock=%v err=%v", l, err)
	}
	proxy.Freeze()
	if lostWithin(l, time.Second) {
		t.Fatalf("Lost closed before the %s limit", captureLockUnconfirmedLimit)
	}
	if !lostWithin(l, 3*time.Second) {
		t.Fatal("the index went silent and Lost never closed")
	}
	done := make(chan struct{})
	go func() { l.Release(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Release did not return after a loss by silence")
	}
	// The holder closed its connection; once that reaches the server the
	// lock is free and another process takes it.
	proxy.Thaw()
	deadline := time.Now().Add(10 * time.Second)
	for holderOf(t, probe, name) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the lock is still held 10s after its holder gave it up")
		}
		time.Sleep(50 * time.Millisecond)
	}
	next, err := tryCaptureLock(context.Background(), direct, name)
	if err != nil || next == nil {
		t.Fatalf("after the holder gave up: lock=%v err=%v, want another process to take it", next, err)
	}
	next.Release()
}

// The connection cut under the holder (the index restarted, a proxy dropped
// it): the next question fails and that is a loss at once, with the limit
// for a slow index still far away.
func TestIntegrationCaptureLockCutConnectionIsLostAtOnce(t *testing.T) {
	setCaptureLockTimings(t, 100*time.Millisecond, 100*time.Millisecond, 30*time.Second)
	direct, name, _ := captureLockTestSetup(t)
	dsn, proxy := throughProxy(t, direct)
	l, err := tryCaptureLock(context.Background(), dsn, name)
	if err != nil || l == nil {
		t.Fatalf("try: lock=%v err=%v", l, err)
	}
	defer l.Release()
	if lostWithin(l, 300*time.Millisecond) {
		t.Fatal("Lost closed while the lock is held")
	}
	proxy.Cut()
	if !lostWithin(l, 3*time.Second) {
		t.Fatalf("the connection was cut and Lost did not close within 3s (limit %s)", captureLockUnconfirmedLimit)
	}
}

// A clean stop while the index is not answering is a stop, not a loss, and
// does not wait for the index.
func TestIntegrationCaptureLockReleaseWhileIndexIsSilent(t *testing.T) {
	setCaptureLockTimings(t, 100*time.Millisecond, 100*time.Millisecond, 30*time.Second)
	direct, name, probe := captureLockTestSetup(t)
	dsn, proxy := throughProxy(t, direct)
	l, err := tryCaptureLock(context.Background(), dsn, name)
	if err != nil || l == nil {
		t.Fatalf("try: lock=%v err=%v", l, err)
	}
	proxy.Freeze()
	time.Sleep(400 * time.Millisecond) // a question is in flight by now
	done := make(chan struct{})
	go func() { l.Release(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Release waited for an index that was not answering")
	}
	if lostWithin(l, 100*time.Millisecond) {
		t.Fatal("a clean stop closed Lost")
	}
	proxy.Thaw()
	deadline := time.Now().Add(10 * time.Second)
	for holderOf(t, probe, name) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the lock is still held 10s after Release")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
