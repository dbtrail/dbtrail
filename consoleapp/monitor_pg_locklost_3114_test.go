package consoleapp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/streamrun"
)

// runPGJob runs one PostgreSQL job under the supervisor with a lock whose
// heartbeat asks check, and hands over the cause its stream was cancelled with.
func runPGJob(t *testing.T, check func(context.Context) error) (causes <-chan error, started <-chan struct{}) {
	t.Helper()
	shortCaptureLockTimings(t)
	base := lockLostBackoffBase
	lockLostBackoffBase = time.Hour
	t.Cleanup(func() { lockLostBackoffBase = base })
	ctx, cancel := context.WithCancel(context.Background())
	m := newMonitorSupervisor(ctx, "", nil, nil)
	e := console.ServerEntry{ID: "s3114", Name: "shop", SourceDSN: "postgres://u@127.0.0.1:1/db", DSN: "::not a dsn::"}
	jobCtx, jobCancel := context.WithCancel(ctx)
	job := &monitorJob{cancel: jobCancel, done: make(chan struct{}), lock: heartbeatOnly(t, check)}
	m.jobs[e.ID] = job
	got, up := make(chan error, 1), make(chan struct{})
	m.wg.Add(1)
	go m.run(jobCtx, job, e, console.FlavorPostgres, func(c context.Context) error {
		close(up)
		<-c.Done()
		got <- context.Cause(c)
		return context.Cause(c)
	})
	t.Cleanup(func() {
		cancel()
		m.wg.Wait()
	})
	return got, up
}

// A failed answer from the lock's own connection, with the capture itself
// healthy: the PostgreSQL stream stops with the cause that makes it write
// nothing, as a MySQL stream does.
func TestPGStreamGetsTheLockLostCauseWhenOnlyTheLockConnectionFails_3114(t *testing.T) {
	causes, up := runPGJob(t, func(context.Context) error { return errors.New("connection reset") })
	<-up
	select {
	case cause := <-causes:
		if !errors.Is(cause, streamrun.ErrStopWithoutFlush) {
			t.Fatalf("the stream was cancelled with %v, want ErrStopWithoutFlush", cause)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the stream kept running after the lock's connection failed")
	}
}

// A slow answer inside the limit is not a loss: nobody took the lock, and a
// healthy PostgreSQL capture is not stopped for it.
func TestPGStreamKeepsRunningThroughASlowLockAnswer_3114(t *testing.T) {
	causes, up := runPGJob(t, func(ctx context.Context) error {
		select {
		case <-time.After(300 * time.Millisecond): // inside the 1 s limit
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	<-up
	select {
	case cause := <-causes:
		t.Fatalf("a slow answer stopped the stream: %v", cause)
	case <-time.After(2 * time.Second):
	}
}
