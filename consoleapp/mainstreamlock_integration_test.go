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

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/streamrun"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// Two daemons with the same index database: the second must not run its
// stream while the first does, and must start it once the first stops.
func TestIntegrationBootStreamWaitsForTheOtherDaemon(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	shortCaptureLockTimings(t)
	_, dbName := testutil.CreateTestDB(t)
	dsn := testutil.IntegrationDSN(dbName)

	var active, maxActive, runs atomic.Int32
	fake := func(ctx context.Context) error {
		runs.Add(1)
		n := active.Add(1)
		for {
			m := maxActive.Load()
			if n <= m || maxActive.CompareAndSwap(m, n) {
				break
			}
		}
		<-ctx.Done()
		active.Add(-1)
		return nil
	}
	ctx1, stop1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- runMainStreamHoldingLock(ctx1, dsn, fake) }()
	waitFor(t, 5*time.Second, "first daemon streaming", func() bool { return runs.Load() == 1 })

	ctx2, stop2 := context.WithCancel(context.Background())
	defer stop2()
	done2 := make(chan error, 1)
	go func() { done2 <- runMainStreamHoldingLock(ctx2, dsn, fake) }()
	time.Sleep(time.Second)
	if runs.Load() != 1 {
		t.Fatal("the second daemon streamed while the first held the lock")
	}

	stop1()
	if err := <-done1; err != nil {
		t.Fatalf("first daemon: %v", err)
	}
	waitFor(t, 10*time.Second, "second daemon taking over", func() bool { return runs.Load() == 2 })
	if m := maxActive.Load(); m != 1 {
		t.Fatalf("%d streams ran at once", m)
	}
	stop2()
	if err := <-done2; err != nil {
		t.Fatalf("second daemon: %v", err)
	}
}

// A daemon stopped while it waits ends cleanly, holding nothing.
func TestIntegrationBootStreamStoppedWhileWaiting(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	shortCaptureLockTimings(t)
	_, dbName := testutil.CreateTestDB(t)
	dsn := testutil.IntegrationDSN(dbName)
	holder, err := tryCaptureLock(context.Background(), dsn, bootStreamLockName(dbName))
	if err != nil || holder == nil {
		t.Fatalf("hold: %v %v", holder, err)
	}
	defer holder.Release()
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	ran := false
	go func() {
		done <- runMainStreamHoldingLock(ctx, dsn, func(context.Context) error { ran = true; return nil })
	}()
	time.Sleep(500 * time.Millisecond)
	stop()
	select {
	case err := <-done:
		if err != nil || ran {
			t.Fatalf("stopped while waiting: err=%v ran=%v, want nil and no stream", err, ran)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a daemon stopped while waiting did not return")
	}
}

// A lost lock stops the stream at once; the daemon takes the lock back and
// streams again, on a new session.
func TestIntegrationBootStreamLockLossStopsTheStream(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	shortCaptureLockTimings(t)
	_, dbName := testutil.CreateTestDB(t)
	dsn := testutil.IntegrationDSN(dbName)
	cfg, _ := mysql.ParseDSN(dsn)
	name := bootStreamLockName(cfg.DBName)
	probe, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()

	var runs atomic.Int32
	causes := make(chan error, 4)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- runMainStreamHoldingLock(ctx, dsn, func(c context.Context) error {
			runs.Add(1)
			<-c.Done()
			causes <- context.Cause(c)
			return nil
		})
	}()
	waitFor(t, 5*time.Second, "streaming", func() bool { return runs.Load() == 1 })
	var holder int64
	if err := probe.QueryRow("SELECT IS_USED_LOCK(?)", name).Scan(&holder); err != nil || holder == 0 {
		t.Fatalf("holder: %d %v", holder, err)
	}
	if _, err := probe.Exec(fmt.Sprintf("KILL %d", holder)); err != nil {
		t.Fatal(err)
	}
	// The stream was told to stop WITHOUT writing what it holds.
	select {
	case cause := <-causes:
		if !errors.Is(cause, streamrun.ErrStopWithoutFlush) {
			t.Fatalf("stream stopped with cause %v, want streamrun.ErrStopWithoutFlush", cause)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the lock was lost and the stream kept running")
	}
	waitFor(t, 20*time.Second, "streaming again after the loss", func() bool { return runs.Load() == 2 })
	var again int64
	if err := probe.QueryRow("SELECT IS_USED_LOCK(?)", name).Scan(&again); err != nil || again == 0 || again == holder {
		t.Fatalf("holder after resuming: %d (before %d) %v", again, holder, err)
	}
	stop()
	<-done
}

func waitFor(t *testing.T, d time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out: %s", what)
}

// The stream's own error (not a lost lock) comes back unchanged, and the
// lock is free afterwards: the daemon's exit path is what it was.
func TestIntegrationBootStreamReturnsTheStreamError(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	shortCaptureLockTimings(t)
	_, dbName := testutil.CreateTestDB(t)
	dsn := testutil.IntegrationDSN(dbName)
	want := errors.New("an un-indexable event")
	if err := runMainStreamHoldingLock(context.Background(), dsn, func(context.Context) error { return want }); err != want {
		t.Fatalf("returned %v, want the stream's own error", err)
	}
	l, err := tryCaptureLock(context.Background(), dsn, bootStreamLockName(dbName))
	if err != nil || l == nil {
		t.Fatalf("lock still held after the stream returned: %v %v", l, err)
	}
	l.Release()
}

// Stopped during the takeover delay: no stream, nil, the lock let go.
func TestIntegrationBootStreamStoppedDuringTakeoverDelay(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	shortCaptureLockTimings(t)
	_, dbName := testutil.CreateTestDB(t)
	dsn := testutil.IntegrationDSN(dbName)
	name := bootStreamLockName(dbName)
	holder, err := tryCaptureLock(context.Background(), dsn, name)
	if err != nil || holder == nil {
		t.Fatalf("hold: %v %v", holder, err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	var ran atomic.Bool
	go func() {
		done <- runMainStreamHoldingLock(ctx, dsn, func(context.Context) error { ran.Store(true); return nil })
	}()
	time.Sleep(300 * time.Millisecond)
	holder.Release()
	time.Sleep(500 * time.Millisecond) // taken by now, inside the delay (about 2 s here)
	stop()
	select {
	case err := <-done:
		if err != nil || ran.Load() {
			t.Fatalf("stopped in the takeover delay: err=%v ran=%v, want nil and no stream", err, ran.Load())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not return")
	}
	time.Sleep(200 * time.Millisecond)
	l, err := tryCaptureLock(context.Background(), dsn, name)
	if err != nil || l == nil {
		t.Fatalf("lock still held after a stop in the takeover delay: %v %v", l, err)
	}
	l.Release()
}
