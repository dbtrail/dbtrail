package consoleapp

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/streamrun"
)

// mainStreamFn runs the daemon's main source stream to completion.
// streamrun.One in production; a seam so the restart policy below is testable
// without a live source (the supervised registry sources have the same seam,
// monitorSupervisor.streamFn).
var mainStreamFn = streamrun.One

// runMainStreamWithWriteDeadlineRetry runs `watch --source-dsn`'s stream and,
// on an index write-deadline failure ONLY, restarts it instead of ending the
// daemon (#1482).
//
// The hole this closes: `watch` supervises the registry sources it streams
// (monitorSupervisor.run — crash-loop backoff, circuit breaker) but ran its MAIN
// source as a bare, one-shot call. One slow batch INSERT therefore terminated
// the process, and nothing restarted it; observed in #1482 as 41 minutes of no
// capture after a heavy analytical read starved the index, ended by hand. Worse,
// that exit runs supervisor.Shutdown(), so the main source's failure also tore
// down every supervised source whose own backoff loop would have ridden it out.
// This is the same policy those sources already get, narrowed to the one error
// class where retrying is sound.
//
// Why a RESTART and not a retry of the batch INSERT. The deadline is
// client-side: go-sql-driver cancels by closing the socket (mysqlConn.cancel →
// cleanup, no KILL QUERY), so an INSERT that was merely slow generally runs to
// completion and COMMITS on the server after the client has stopped waiting.
// Re-executing it in place would duplicate every row of that batch, in a
// forensics index, with nothing to clean up after — binlog_events has no natural
// key to dedup on. Re-entering streamrun.One replays from the last durable
// checkpoint and passes through dedup-on-resume
// (deleteEventsSinceCheckpoint / …GTID), which exists precisely to drop rows a
// timed-out write may have left behind. So the restart is not just the cheaper
// fix, it is the only duplicate-safe one. Caveat inherited, not introduced: in
// GTID mode after a gap auto-advance that dedup is deliberately skipped, and the
// documented accepted-duplicate window (#500) applies to a restart here exactly
// as it does to a restart by hand.
//
// Only indexer.ErrWriteDeadline re-arms the loop; every other error returns
// unchanged, exactly as before. That is not a shortcut, it is the point: an
// un-indexable event must still abort loudly rather than spin (#652), and an
// index that is genuinely gone fails One() at connect with a different error, so
// this loop cannot sit on a dead link.
func runMainStreamWithWriteDeadlineRetry(ctx context.Context, cfg streamrun.Config) error {
	var policy crashLoopPolicy
	for {
		started := time.Now()
		err := mainStreamFn(ctx, cfg)
		// ctx.Err() first: on shutdown One's final checkpoint flush can hit the
		// same deadline, and that is a stopping daemon, not a stall to ride out.
		// A transaction cut by a disconnect is normally restarted by One
		// itself; this catches the case where One gave up after its quick
		// restarts, so the daemon rides it out instead of stopping.
		if err == nil || ctx.Err() != nil ||
			(!errors.Is(err, indexer.ErrWriteDeadline) && !errors.Is(err, parser.ErrResentCutTransaction)) {
			return err
		}
		// Circuit breaker. The give-up path is the OLD behaviour, unchanged and
		// undiluted: the same error, with the same three remedies, ends the
		// daemon. Retrying only moves when that happens, never whether.
		delay, looping, giveUp := policy.failed(started, time.Now())
		if giveUp {
			slog.Error("main source stream kept exhausting the index write deadline; giving up and stopping capture",
				"looping_for", looping.Round(time.Minute), "error", err)
			return err
		}
		slog.Warn("main source stream hit the index write deadline; restarting capture from its last checkpoint",
			"delay", delay, "error", err)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			// Stopped mid-backoff. Surface the failure that put us here rather
			// than exiting 0 on a daemon that was not capturing.
			return err
		}
	}
}
