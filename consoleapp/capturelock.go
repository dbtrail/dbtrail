package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
)

// captureLock is the MySQL advisory lock that keeps a second DBTrail process
// from capturing a source into an index another process already writes to
// (#2105). Two processes capturing one source both insert every change, and
// the index has no natural key to refuse the second copy.
//
// The rule it enforces: a capture writes only while it holds a LIVE lock.
// Three things make the lock live rather than nominal:
//
//   - It is held on one *sql.Conn, never on a pool. A pool whose connection
//     died opens a new one without a word, and a check run there asks a
//     session that never held the lock.
//   - That session's wait_timeout is raised. MySQL ends a session idle past
//     wait_timeout (8 hours by default) and frees its locks with it: measured,
//     a second process then took the lock and both captured.
//   - A heartbeat asks, every captureLockHeartbeat, whether this session still
//     owns the lock. Any other answer, an error included, is a loss: Lost()
//     closes and the holder must stop writing.
//
// A process that finds the lock taken waits for it (waitCaptureLock) instead
// of failing: a rolling deployment starts the new process while the old one
// still runs, and a failure there was never retried, so capture stopped on
// every deploy until someone pressed Start.
type captureLock struct {
	name string
	db   *sql.DB
	conn *sql.Conn

	lost     chan struct{}
	lostOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
	release  sync.Once
}

// Tunables, variables so tests can shorten them.
var (
	// captureLockPoll is how often a waiting process asks for the lock again.
	// Each ask is GET_LOCK(name, 0): it never queues on the server, so a
	// cancelled wait leaves no session behind that could still win the lock.
	captureLockPoll = 5 * time.Second
	// captureLockHeartbeat is how often the holder checks that it still owns
	// the lock. With captureLockCheckTimeout it bounds how long a holder that
	// lost the lock keeps writing; a process that takes over waits that long
	// first (captureLockTakeoverDelay).
	captureLockHeartbeat = 5 * time.Second
	// captureLockCheckTimeout bounds one heartbeat query, and the release.
	captureLockCheckTimeout = 5 * time.Second
)

// captureLockTakeoverDelay is how long a process that waited for the lock
// holds it before writing: the previous holder may have lost it without
// knowing (a cut connection) and writes until its next heartbeat fails. A
// batch that holder sent just before can still commit after this; the delay
// shrinks that window, it cannot close it.
func captureLockTakeoverDelay() time.Duration {
	return captureLockHeartbeat + captureLockCheckTimeout + time.Second
}

// captureLockSessionWaitTimeout is MySQL's maximum wait_timeout (one year).
// The heartbeat already keeps the session busy; this is the second belt, for
// a heartbeat delayed past a server's short wait_timeout.
const captureLockSessionWaitTimeout = 31536000

// tryCaptureLock makes one attempt. It returns the held lock, or (nil, nil)
// when another session holds it, or an error when the index could not be
// asked.
func tryCaptureLock(ctx context.Context, dsn, name string) (*captureLock, error) {
	db, err := config.Connect(dsn)
	if err != nil {
		return nil, fmt.Errorf("connect for the capture lock: %w", err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("connect for the capture lock: %w", err)
	}
	fail := func(err error) (*captureLock, error) {
		conn.Close()
		db.Close()
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION wait_timeout = %d", captureLockSessionWaitTimeout)); err != nil {
		return fail(fmt.Errorf("raise the capture lock session's wait_timeout: %w", err))
	}
	// NULL is an error inside GET_LOCK (out of memory, the thread killed):
	// not held, and not "someone else has it" either.
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", name).Scan(&got); err != nil {
		return fail(fmt.Errorf("acquire the capture lock %s: %w", name, err))
	}
	if !got.Valid {
		return fail(fmt.Errorf("acquire the capture lock %s: the server returned NULL", name))
	}
	if got.Int64 != 1 {
		return fail(nil)
	}
	l := &captureLock{name: name, db: db, conn: conn,
		lost: make(chan struct{}), stop: make(chan struct{}), done: make(chan struct{})}
	go l.heartbeat()
	return l, nil
}

// waitCaptureLock asks for the lock every captureLockPoll until it is held or
// ctx ends. onBusy runs on the first attempt that finds the lock taken, and
// again only after an attempt that did not; onErr runs for an attempt that
// could not ask the index, which is retried, never final. The returned error
// is ctx's.
func waitCaptureLock(ctx context.Context, dsn, name string, onBusy func(), onErr func(error)) (*captureLock, error) {
	busy := false
	for {
		l, err := tryCaptureLock(ctx, dsn, name)
		switch {
		case l != nil:
			return l, nil
		case ctx.Err() != nil:
			return nil, ctx.Err()
		case err != nil:
			busy = false
			if onErr != nil {
				onErr(err)
			}
		case !busy:
			busy = true
			if onBusy != nil {
				onBusy()
			}
		}
		select {
		case <-time.After(captureLockPoll):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (l *captureLock) heartbeat() {
	defer close(l.done)
	t := time.NewTicker(captureLockHeartbeat)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
		}
		if err := l.check(); err != nil {
			slog.Error("this process lost its capture lock: it stops writing to this index and waits to take the lock back",
				"lock", l.name, "error", err)
			l.lostOnce.Do(func() { close(l.lost) })
			return
		}
	}
}

// check asks whether this session still owns the lock.
func (l *captureLock) check() error {
	ctx, cancel := context.WithTimeout(context.Background(), captureLockCheckTimeout)
	defer cancel()
	var owns sql.NullInt64
	if err := l.conn.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?) = CONNECTION_ID()", l.name).Scan(&owns); err != nil {
		return err
	}
	if !owns.Valid || owns.Int64 != 1 {
		return errors.New("another session holds it, or nobody does")
	}
	return nil
}

// Lost closes when the heartbeat finds the lock no longer this session's.
func (l *captureLock) Lost() <-chan struct{} { return l.lost }

// Release stops the heartbeat, frees the lock and closes the connection.
// Safe to call more than once.
func (l *captureLock) Release() {
	l.release.Do(func() {
		close(l.stop)
		<-l.done
		ctx, cancel := context.WithTimeout(context.Background(), captureLockCheckTimeout)
		// Best effort: closing the session frees the lock too, only later,
		// when mysqld finishes tearing it down. Until then a restart in this
		// process waits on its own lock for a poll or two, which is what the
		// log line explains.
		if _, err := l.conn.ExecContext(ctx, "DO RELEASE_LOCK(?)", l.name); err != nil {
			slog.Warn("could not release the capture lock; it is freed when its connection closes", "lock", l.name, "error", err)
		}
		cancel()
		l.conn.Close()
		l.db.Close()
	})
}
