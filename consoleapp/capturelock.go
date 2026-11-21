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
//     owns the lock. An error, or any answer but "yes", is a loss at once:
//     Lost() closes and the holder must stop writing. A slow answer is not a
//     loss (see heartbeat), until captureLockUnconfirmedLimit has passed
//     without a confirmation.
//
// A process that finds the lock taken waits for it (waitCaptureLock) instead
// of failing: a rolling deployment starts the new process while the old one
// still runs, and a failure there was never retried, so capture stopped on
// every deploy until someone pressed Start.
type captureLock struct {
	name string
	db   *sql.DB
	conn *sql.Conn
	// checkFn asks the session whether it still owns the lock; (*captureLock).check
	// outside unit tests.
	checkFn func(context.Context) error

	lost     chan struct{}
	lostOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
	release  sync.Once
	// taken is when the GET_LOCK that took the lock was sent: where the first
	// captureLockUnconfirmedLimit starts.
	taken time.Time
	// abandoned: the heartbeat cut a check that had not answered, which
	// closes the connection (see heartbeat). Written by the heartbeat before
	// it closes done, read by Release after.
	abandoned bool
}

// Tunables, variables so tests can shorten them.
var (
	// captureLockPoll is how often a waiting process asks for the lock again.
	// Each ask is GET_LOCK(name, 0): it never queues on the server, so a
	// cancelled wait leaves no session behind that could still win the lock.
	captureLockPoll = 5 * time.Second
	// captureLockHeartbeat is how often the holder asks whether it still
	// owns the lock. One question is in flight at a time: the next is asked
	// once the previous one has answered. A second, so that a connection
	// that died is noticed within about that, and so that one slow answer
	// has nearly the whole limit to arrive.
	captureLockHeartbeat = time.Second
	// captureLockUnconfirmedLimit is how long the holder keeps writing
	// without a confirmation, counted from the moment it SENT the last
	// question that came back "yes" (the server answered that one later, so
	// the lock was held no earlier than that). Past it the lock counts as
	// lost, whatever the reason for the silence.
	//
	// Counting from the send means two answers in a row have to fit in it:
	// the second question leaves when the first is answered. An index that
	// takes more than half the limit for every answer still loses the lock;
	// one slow answer among prompt ones has the limit less one heartbeat.
	//
	// It is the 10 seconds the lock has always allowed (a question every 5 s
	// with 5 s to answer) and must not grow on its own: a process that takes
	// the lock over waits captureLockTakeoverDelay for the previous holder to
	// notice, a release from before this limit existed waits 11 seconds
	// (captureLockOldestTakeoverDelay), and a process that finds the lock
	// free on its first attempt does not wait at all, so this is also how
	// long two processes can write side by side after a connection is cut
	// just before the other starts.
	captureLockUnconfirmedLimit = 10 * time.Second
	// captureLockTakeoverMargin is what a process that takes over waits on
	// top of the limit: for the previous holder's timer to fire and its
	// stream to stop.
	captureLockTakeoverMargin = time.Second
	// captureLockSlowWarnEvery is the least time between two warnings about
	// a slow answer that kept the lock.
	captureLockSlowWarnEvery = time.Minute
)

// captureLockOldestTakeoverDelay is the takeover delay of the releases that
// had the lock before captureLockUnconfirmedLimit existed. During an upgrade
// or a rollback one of them can be the process waiting beside this one, so
// the limit has to stay under it.
const captureLockOldestTakeoverDelay = 11 * time.Second

// captureLockReleaseTimeout bounds the RELEASE_LOCK of a clean stop.
const captureLockReleaseTimeout = 5 * time.Second

// captureLockTakeoverDelay is how long a process that waited for the lock
// holds it before writing: the previous holder may have lost it without
// knowing (a cut connection) and writes until captureLockUnconfirmedLimit
// runs out. A batch that holder sent just before can still commit after
// this; the delay shrinks that window, it cannot close it.
func captureLockTakeoverDelay() time.Duration {
	return captureLockUnconfirmedLimit + captureLockTakeoverMargin
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
	// Before it is sent: the server grants the lock later than this, so the
	// heartbeat's first limit starts no later than the lock itself.
	asked := time.Now()
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", name).Scan(&got); err != nil {
		return fail(fmt.Errorf("acquire the capture lock %s: %w", name, err))
	}
	if !got.Valid {
		return fail(fmt.Errorf("acquire the capture lock %s: the server returned NULL", name))
	}
	if got.Int64 != 1 {
		return fail(nil)
	}
	l := newCaptureLock(name, db, conn, asked)
	l.checkFn = l.check
	go l.heartbeat()
	return l, nil
}

func newCaptureLock(name string, db *sql.DB, conn *sql.Conn, taken time.Time) *captureLock {
	return &captureLock{name: name, db: db, conn: conn, taken: taken,
		lost: make(chan struct{}), stop: make(chan struct{}), done: make(chan struct{})}
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

// heartbeat confirms the lock every captureLockHeartbeat and closes Lost when
// it cannot.
//
// A question has no timeout of its own. The driver cancels a statement by
// closing the socket, and MySQL frees a session's locks when its connection
// closes: a timeout on a slow answer would be this process giving up a lock
// it still held, which is what a loaded index used to do to capture several
// times an hour. The lock is a session lock with no expiry, so while the
// connection lives a late "yes" is as good as a prompt one. What bounds the
// wait is captureLockUnconfirmedLimit, across questions, and when it runs out
// the question still in flight is cut, which closes the connection and so
// frees the lock for the process that takes over.
//
// What ends it at once, as before: an error from the question (the
// connection is gone, or the server refused to answer) and an answer that is
// not "yes".
func (l *captureLock) heartbeat() {
	defer close(l.done)
	t := time.NewTicker(captureLockHeartbeat)
	defer t.Stop()
	// Cancelling this is the only thing that interrupts a question: on the
	// way out, never while the lock is still wanted.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lose := func(err error) {
		slog.Error("this process lost its capture lock: it stops writing to this index and waits to take the lock back",
			"lock", l.name, "error", err)
		l.lostOnce.Do(func() { close(l.lost) })
	}
	// confirmed is when the last question answered "yes" was sent; to begin
	// with, when the GET_LOCK that took the lock was.
	confirmed := l.taken
	limit := time.NewTimer(time.Until(confirmed.Add(captureLockUnconfirmedLimit)))
	defer limit.Stop()
	var (
		pending  chan error // nil while no question is in flight
		sent     time.Time
		lastWarn time.Time
	)
	silent := func(now time.Time) error {
		inFlight := "no question was in flight"
		if pending != nil {
			inFlight = fmt.Sprintf("the question cut had been waiting for its answer for %s", now.Sub(sent).Round(time.Millisecond))
		}
		return fmt.Errorf("the last question the index answered was sent %s ago, past the limit of %s; %s",
			now.Sub(confirmed).Round(time.Millisecond), captureLockUnconfirmedLimit, inFlight)
	}
	for {
		select {
		case <-l.stop:
			// An answer that arrived and was not read yet: the question is
			// over and nothing is cut, so Release can free the lock itself.
			// One still unanswered cannot be told from a dead connection
			// without waiting for the index, which a stop must not do: the
			// deferred cancel cuts it and the connection closes.
			select {
			case <-pending:
				pending = nil
			default:
			}
			l.abandoned = pending != nil
			return
		case <-t.C:
			if pending != nil {
				continue
			}
			sent = time.Now()
			answer := make(chan error, 1)
			pending = answer
			go func() { answer <- l.checkFn(ctx) }()
		case err := <-pending:
			now := time.Now()
			if err != nil {
				pending = nil
				lose(err)
				return
			}
			// The timer and this answer can both be ready, and a process
			// that was not scheduled for a while reads them late: the clock
			// decides, not which one select picked.
			if captureLockAnswerTooLate(confirmed, now) {
				err := silent(now)
				pending = nil
				lose(err)
				return
			}
			pending = nil
			if took := now.Sub(sent); took > captureLockUnconfirmedLimit/2 && now.Sub(lastWarn) >= captureLockSlowWarnEvery {
				lastWarn = now
				slog.Warn("the index was slow to confirm the capture lock; the lock is kept, and it is given up if two answers in a row do not fit in the limit",
					"lock", l.name, "answer_took", took.Round(time.Millisecond),
					"left_before_the_limit", confirmed.Add(captureLockUnconfirmedLimit).Sub(now).Round(time.Millisecond),
					"limit", captureLockUnconfirmedLimit)
			}
			confirmed = sent
			limit.Reset(time.Until(confirmed.Add(captureLockUnconfirmedLimit)))
		case <-limit.C:
			l.abandoned = pending != nil
			lose(silent(time.Now()))
			return
		}
	}
}

// captureLockAnswerTooLate reports whether an answer read at now came after
// the limit that started at confirmed.
func captureLockAnswerTooLate(confirmed, now time.Time) bool {
	return now.Sub(confirmed) > captureLockUnconfirmedLimit
}

// check asks whether this session still owns the lock. ctx carries no
// deadline (see heartbeat); cancelling it closes the connection.
func (l *captureLock) check(ctx context.Context) error {
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
		// Best effort: closing the session frees the lock too, only later,
		// when mysqld finishes tearing it down. Until then a restart in this
		// process waits on its own lock for a poll or two, and the waiter's
		// log line says the lock is held by another session.
		switch {
		case l.abandoned:
			// The heartbeat cut a question that had not answered, which
			// closed the connection: there is no session left to ask. A
			// loss already said so; a clean stop says it here.
			if !chanClosed(l.lost) {
				slog.Info("stopped while the index had not answered the capture lock's check: the connection was closed without releasing the lock, and the index frees it when it sees the connection close",
					"lock", l.name)
			}
		case l.conn != nil:
			ctx, cancel := context.WithTimeout(context.Background(), captureLockReleaseTimeout)
			if _, err := l.conn.ExecContext(ctx, "DO RELEASE_LOCK(?)", l.name); err != nil {
				slog.Warn("could not release the capture lock; it is freed when its connection closes", "lock", l.name, "error", err)
			}
			cancel()
		}
		if l.conn == nil {
			return // a unit test's lock, with no session behind it
		}
		l.conn.Close()
		l.db.Close()
	})
}
