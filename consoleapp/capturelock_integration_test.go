//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// shortCaptureLockTimings makes the lock's waits and heartbeat fast enough to
// test, and restores them afterwards.
func shortCaptureLockTimings(t *testing.T) {
	t.Helper()
	poll, beat := captureLockPoll, captureLockHeartbeat
	captureLockPoll, captureLockHeartbeat = 100*time.Millisecond, 100*time.Millisecond
	t.Cleanup(func() { captureLockPoll, captureLockHeartbeat = poll, beat })
}

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
// over.
func TestIntegrationCaptureLockLossIsNoticed(t *testing.T) {
	shortCaptureLockTimings(t)
	dsn, name, probe := captureLockTestSetup(t)
	l, err := tryCaptureLock(context.Background(), dsn, name)
	if err != nil || l == nil {
		t.Fatalf("try: lock=%v err=%v", l, err)
	}
	defer l.Release()
	select {
	case <-l.Lost():
		t.Fatal("Lost closed while the lock is held")
	case <-time.After(500 * time.Millisecond):
	}
	id := holderOf(t, probe, name)
	if _, err := probe.Exec(fmt.Sprintf("KILL %d", id)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-l.Lost():
	case <-time.After(5 * time.Second):
		t.Fatal("the holder's session was killed and Lost never closed")
	}
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
