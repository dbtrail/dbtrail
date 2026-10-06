package consoleapp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/streamrun"
)

// bootStreamLockName is the capture lock of the source `watch --source-dsn`
// streams, named after the index database it writes to: two processes
// writing one index database insert every change twice, whichever sources
// they read. Hashed, because MySQL refuses lock names over 64 characters and
// a database name alone can be 64; the log names the database beside it.
func bootStreamLockName(indexDB string) string {
	sum := sha256.Sum256([]byte(indexDB))
	return "bintrail_stream_" + hex.EncodeToString(sum[:])[:40]
}

// runMainStreamHoldingLock runs the daemon's main source stream only while
// this process holds the index database's capture lock (#2105). Before it,
// two daemons started with the same --source-dsn and --index-dsn both
// captured: measured, 3,082 duplicate events in 60 seconds, with nothing in
// either log. A daemon that finds the lock taken keeps serving the web
// interface and waits; it starts capturing, from the checkpoint the other
// left, once that one stops. A lost lock (see captureLock) stops the stream
// at once, and the daemon waits to take the lock back.
//
// run is the stream itself (runMainStreamWithWriteDeadlineRetry in watch).
// The returned error is run's; a daemon stopped while waiting returns nil.
func runMainStreamHoldingLock(ctx context.Context, indexDSN string, run func(context.Context) error) error {
	cfg, err := mysql.ParseDSN(indexDSN)
	if err != nil {
		return fmt.Errorf("index DSN: %s", config.ScrubDSNError(err, indexDSN))
	}
	name := bootStreamLockName(cfg.DBName)
	scrub := func(err error) string { return config.ScrubDSNError(err, indexDSN) }
	takeover := false
	for {
		lock, err := tryCaptureLock(ctx, indexDSN, name)
		if lock == nil {
			// One line per change of state: busy, or the index not
			// answering. Nothing on a shutdown that cut the attempt.
			var lastWarn time.Time
			onBusy := func() {
				// Usually another DBTrail process; for a few seconds after a lost
				// lock it can be this process's own previous session, which
				// mysqld has not finished closing.
				slog.Warn("this index database's capture lock is held by another session (another DBTrail process); this one waits and starts capturing when it is let go",
					"index_database", cfg.DBName, "lock", name)
			}
			onErr := func(err error) {
				if ctx.Err() == nil && time.Since(lastWarn) >= time.Minute {
					lastWarn = time.Now()
					slog.Warn("could not ask the index for the capture lock; asking again", "index_database", cfg.DBName, "error", scrub(err))
				}
			}
			// The first attempt's answer is reported by waitCaptureLock's own
			// first attempt, which runs at once.
			lock, err = waitCaptureLock(ctx, indexDSN, name, onBusy, onErr)
			if err != nil {
				return nil // stopped while waiting
			}
			takeover = true
		}
		if takeover {
			// See captureLockTakeoverDelay: the previous holder may have lost
			// the lock without knowing, and writes until its heartbeat fails.
			select {
			case <-time.After(captureLockTakeoverDelay()):
			case <-ctx.Done():
				lock.Release()
				return nil
			}
			slog.Info("took the capture lock; starting capture", "index_database", cfg.DBName, "lock", name)
		}
		// A lost lock stops the stream without writing its pending batch or
		// position (streamrun.ErrStopWithoutFlush): the process that holds
		// the lock now may already be capturing into this index.
		runCtx, cancelRun := context.WithCancelCause(ctx)
		go func() {
			select {
			case <-lock.Lost():
				cancelRun(streamrun.ErrStopWithoutFlush)
			case <-runCtx.Done():
			}
		}()
		err = run(runCtx)
		cancelRun(nil)
		lost := chanClosed(lock.Lost())
		lock.Release()
		if ctx.Err() != nil || !lost {
			return err
		}
		// Lost: stop writing (done) and wait to take the lock back. Whoever
		// holds it now may be capturing, so this is a takeover too.
		// The stream may have failed on its own just as the lock went: say so,
		// or that failure is never written anywhere.
		args := []any{"index_database", cfg.DBName, "lock", name}
		if err != nil && !errors.Is(err, streamrun.ErrStopWithoutFlush) && !errors.Is(err, context.Canceled) {
			args = append(args, "stream_error", scrub(err))
		}
		slog.Error("the main source stream stopped: this process lost its capture lock; waiting to take it back", args...)
		takeover = true
	}
}
