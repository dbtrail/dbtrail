package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

var resetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Tear down a PostgreSQL capture: drop the replication slot and clear the index checkpoint",
	Long: `Cleanly tears down a PostgreSQL capture so you can re-baseline and start fresh.

It performs the two-system teardown that otherwise has to be done by hand:

  1. drops the replication slot on the SOURCE (--query-dsn) — a slot left behind
     keeps pinning WAL and can fill the source disk;
  2. clears the durable checkpoint in the INDEX (--index-dsn): the position and
     counter columns of the stream_state row are cleared (the row is never
     DELETEd), so the next 'bintrail-pg stream' starts from a fresh slot.

Continuity: dropping and recreating a slot inherently skips whatever the old
slot had not yet streamed, so discarding a real checkpoint is durably recorded
as a permanent continuity loss (gap_lost_at/gap_lost_detail in stream_state,
naming the discarded LSN) — 'bintrail status' shows the EVENTS PERMANENTLY LOST
banner and 'status --fail-on-gap' exits non-zero. The loss always remains
recorded: a real-checkpoint reset replaces a prior record's detail with its
own and, being a new loss, withdraws any acknowledgement of the prior one; only
a no-checkpoint reset preserves a prior record verbatim. Run reset with the
stream stopped.

The slot is dropped first: if the run is interrupted between the two steps, the
safe state is "slot gone, checkpoint stale" (the next stream fails loud) rather
than "checkpoint cleared, slot live" (which would silently skip data).

After a lost-slot invalidation the slot may already be gone or unusable; pass
--index-only to clear just the checkpoint without touching the source.

This is destructive and irreversible — pass --force to confirm. The index's
recovery data is NOT affected (recovery is index-only); only capture-resume state
is removed.

Examples:

  bintrail-pg reset --query-dsn "$PG" --index-dsn "$IDX" --slot bintrail_slot --force
  bintrail-pg reset --index-dsn "$IDX" --index-only --force   # slot already gone`,
	RunE: runPGReset,
}

var (
	pgResetQueryDSN  string
	pgResetIndexDSN  string
	pgResetSlot      string
	pgResetForce     bool
	pgResetIndexOnly bool
)

func init() {
	resetCmd.Flags().StringVar(&pgResetQueryDSN, "query-dsn", "", "PostgreSQL ordinary connection string, to drop the slot (required unless --index-only; env BINTRAIL_PG_QUERY_DSN)")
	resetCmd.Flags().StringVar(&pgResetIndexDSN, "index-dsn", "", "DSN for the index MySQL database, to clear the checkpoint (required; env BINTRAIL_INDEX_DSN)")
	resetCmd.Flags().StringVar(&pgResetSlot, "slot", "", "Replication slot to drop (required unless --index-only; env BINTRAIL_PG_SLOT)")
	resetCmd.Flags().BoolVar(&pgResetForce, "force", false, "Confirm this destructive, irreversible teardown")
	resetCmd.Flags().BoolVar(&pgResetIndexOnly, "index-only", false, "Only clear the index checkpoint; do not touch the source slot (use when the slot is already gone or lost)")
	// index-dsn is in cli.EnvBindings, so BindCommandEnv sets it from BINTRAIL_INDEX_DSN;
	// the PG-specific flags use the BINTRAIL_PG_* fallback applied in runPGReset.
	cli.BindCommandEnv(resetCmd)
	rootCmd.AddCommand(resetCmd)
}

func runPGReset(cmd *cobra.Command, args []string) error {
	applyEnvFallback(&pgResetQueryDSN, "BINTRAIL_PG_QUERY_DSN")
	applyEnvFallback(&pgResetSlot, "BINTRAIL_PG_SLOT")

	if pgResetIndexDSN == "" {
		return fmt.Errorf("missing required --index-dsn (or BINTRAIL_INDEX_DSN)")
	}
	if !pgResetIndexOnly {
		var missing []string
		if pgResetQueryDSN == "" {
			missing = append(missing, "--query-dsn (or BINTRAIL_PG_QUERY_DSN)")
		}
		if pgResetSlot == "" {
			missing = append(missing, "--slot (or BINTRAIL_PG_SLOT)")
		}
		if len(missing) > 0 {
			return fmt.Errorf("missing required settings: %s (or pass --index-only to clear just the checkpoint)", strings.Join(missing, ", "))
		}
	}
	if !pgResetForce {
		return errors.New("refusing to run a destructive teardown without --force — it drops the replication slot and clears the index checkpoint (recovery data is unaffected)")
	}

	// Bind the two destructive steps as closures over the live connections, then run
	// the (seam-injected, unit-tested) orchestration. The connections are opened lazily
	// inside the closures so resetPlan owns the ordering: the SOURCE slot is dropped
	// before the INDEX is touched at all.
	dropFn := func(ctx context.Context) (bool, error) {
		conn, err := pgx.Connect(ctx, pgResetQueryDSN)
		if err != nil {
			return false, fmt.Errorf("connect to source: %w", err)
		}
		defer conn.Close(ctx)
		return pgcapture.DropSlot(ctx, conn, pgResetSlot)
	}
	clearFn := func(ctx context.Context) (clearOutcome, error) {
		idx, err := config.Connect(pgResetIndexDSN)
		if err != nil {
			return clearOutcome{}, fmt.Errorf("connect to index: %w", err)
		}
		defer idx.Close()
		return clearCheckpoint(ctx, idx)
	}
	return resetPlan(cmd.Context(), pgResetIndexOnly, pgResetSlot, os.Stdout, dropFn, clearFn)
}

// resetPlan is the orchestration core of `bintrail-pg reset`, injected with the two
// teardown steps so the ordering and partial-failure handling are unit-testable without
// live databases.
//
// Ordering is the safety invariant: the SOURCE slot is dropped FIRST, then the INDEX
// checkpoint is cleared. A failure between them leaves "slot gone, checkpoint stale →
// next stream fails loud", never "checkpoint cleared, slot live → silent skip". If the
// clear fails after the slot was already dropped, the error says so and points at
// --index-only to finish.
func resetPlan(
	ctx context.Context,
	indexOnly bool,
	slot string,
	out io.Writer,
	dropFn func(context.Context) (bool, error),
	clearFn func(context.Context) (clearOutcome, error),
) error {
	slotHandled := false
	if indexOnly {
		fmt.Fprintln(out, "--index-only: leaving the source replication slot untouched.")
	} else {
		dropped, err := dropFn(ctx)
		if err != nil {
			return fmt.Errorf("%w\n(if the slot is active, stop the running `bintrail-pg stream` first, or use --index-only if the slot is already gone)", err)
		}
		slotHandled = true
		if dropped {
			fmt.Fprintf(out, "Dropped replication slot %q on the source.\n", slot)
		} else {
			fmt.Fprintf(out, "Replication slot %q was already absent.\n", slot)
		}
	}

	res, err := clearFn(ctx)
	if err != nil {
		if slotHandled {
			// The source slot is already gone; tell the operator how to finish so they
			// don't have to reason about the half-completed state.
			return fmt.Errorf("clearing the index checkpoint: %w\n(the source slot was already dropped; once the index is reachable, re-run with --index-only to finish)", err)
		}
		return fmt.Errorf("clearing the index checkpoint: %w", err)
	}
	if res.tableMissing {
		// 1146 also fires when --index-dsn points at the wrong database, so don't claim
		// success unconditionally — name the ambiguity.
		fmt.Fprintln(out, "No stream_state table in this index — either it was never streamed, or --index-dsn points at the wrong database. Nothing cleared.")
		fmt.Fprintln(out, "Done.")
		return nil
	}
	fmt.Fprintf(out, "Cleared %d index checkpoint row(s).\n", res.rows)
	if res.lossDetail != "" {
		fmt.Fprintf(out, "Recorded the discarded checkpoint as a permanent continuity loss: %s\n", res.lossDetail)
		fmt.Fprintln(out, "`bintrail status` shows the loss until it is acknowledged; the record survives this reset by design.")
	}
	fmt.Fprintln(out, "Done. Re-seed the baseline, then run `bintrail-pg stream` to start fresh.")
	return nil
}

// clearOutcome reports what clearing the index checkpoint did.
type clearOutcome struct {
	rows         int64  // stream_state rows cleared (0 = the row never existed)
	tableMissing bool   // stream_state table absent (MySQL 1146)
	lossDetail   string // non-empty when a discarded checkpoint was durably stamped as a permanent continuity loss
}

// clearCheckpoint clears the stream_state checkpoint so the next `bintrail-pg stream`
// starts fresh, WITHOUT DELETEing the row (#1082): a blind DELETE would also erase any
// recorded gap_lost_at/gap_lost_detail continuity-loss record, letting a reset silently
// launder a real, still-unacknowledged loss out of `bintrail status --fail-on-gap`.
//
// It loads the existing row first and branches on whether a real checkpoint (a non-zero
// LSN cursor — saveCheckpointPG never writes 0) is being discarded:
//
//   - real checkpoint: dropping and recreating the slot skips whatever the old slot had
//     not yet streamed past that LSN, so the discard is stamped as a permanent loss IN
//     THE SAME STATEMENT that clears the cursor — one atomic write, which satisfies the
//     safety property PR #1080's MySQL --reset enforces by ordering (stamp before
//     advance): the clear can never become durable without the stamp. A prior
//     record's detail is replaced by the reset detail (#1080 jump semantics) and
//     any acknowledgement of it is withdrawn (gap_acked_at = NULL: the discard is
//     a new loss); the loss flag itself is never cleared here.
//   - no checkpoint (row seeded by a lost-slot stamp or a pre-commit health snapshot,
//     or an earlier reset): nothing is discarded — clear only the position/counter
//     columns and leave any prior gap_lost_* record untouched (#1080's no-op path).
//     Residual by design: a crash inside the first checkpoint interval can leave
//     indexed events with pos still 0; a reset then discards the surviving slot's
//     un-streamed WAL without a stamp — softened by "re-seed the baseline" being the
//     documented post-reset contract.
//
// The load and the write run in one transaction with the row locked (FOR UPDATE): the
// branch decision is made on the read, and saveCheckpointPG's upsert landing a FIRST
// checkpoint between a bare read and the write would get cleared UN-stamped — the exact
// laundering this function exists to prevent. The lock serializes against every
// stream_state writer and guarantees the row still exists at write time, which is what
// makes the rows:1 report honest (RowsAffected is no substitute: the driver reports
// CHANGED rows, so an identical same-second re-clear would read as 0).
//
// Either way the cleared row reads as first-run to the stream (loadStreamStatePG
// treats a zero-position row as no checkpoint), so a fresh slot is created on the next
// start. tableMissing=true with a nil error means the stream_state table does not
// exist (MySQL 1146) — a never-streamed index, or an --index-dsn pointing at the wrong
// database.
func clearCheckpoint(ctx context.Context, db *sql.DB) (clearOutcome, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return clearOutcome{}, err
	}
	defer tx.Rollback()

	var flavor string
	var pos uint64
	err = tx.QueryRowContext(ctx,
		"SELECT flavor, binlog_position FROM stream_state WHERE id = 1 FOR UPDATE").
		Scan(&flavor, &pos)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return clearOutcome{}, nil
	case isTableMissingErr(err):
		return clearOutcome{tableMissing: true}, nil
	case err != nil:
		return clearOutcome{}, err
	}
	// Mirror loadStreamStatePG's flavor guard ("postgres" = pgstreamrun's pgFlavor):
	// --index-dsn pointed at a MySQL/MariaDB-source index is the same wrong-database
	// mistake the 1146 message calls out, and clearing a foreign checkpoint here would
	// stamp a MySQL byte offset rendered as a PG LSN — a durable forensic lie on a
	// live stream's index.
	if flavor != "postgres" {
		return clearOutcome{}, fmt.Errorf(
			"index holds a %q checkpoint, not \"postgres\" — refusing to reset a non-PostgreSQL stream's state (is --index-dsn pointing at the right database?)", flavor)
	}

	if pos == 0 {
		if _, err := tx.ExecContext(ctx, `
			UPDATE stream_state SET
				binlog_file     = '',
				binlog_position = 0,
				gtid_set        = NULL,
				events_indexed  = 0,
				last_event_time = NULL,
				last_checkpoint = UTC_TIMESTAMP()
			WHERE id = 1`); err != nil {
			return clearOutcome{}, err
		}
		if err := tx.Commit(); err != nil {
			return clearOutcome{}, err
		}
		return clearOutcome{rows: 1}, nil
	}

	detail := fmt.Sprintf(
		"checkpoint discarded via `bintrail-pg reset`: was LSN %s; the replication slot is (or was already) dropped and will be recreated, so events past that LSN not yet streamed by the old slot are permanently lost",
		pglogrepl.LSN(pos))
	// The stamp withdraws any acknowledgement of an earlier loss in the same
	// statement (gap_acked_at = NULL): this discard is a new loss, and it must
	// not be born acknowledged.
	_, err = tx.ExecContext(ctx, stampDiscardSQL(true), detail)
	if isUnknownColumnErr(err) {
		// An index whose stream_state has no gap_acked_at yet (the stream adds
		// the column when it starts; reset does not migrate). No
		// acknowledgement can be on record there, so the stamp without that
		// assignment means the same thing. A failed statement does not end
		// the transaction, and the row lock is still held.
		_, err = tx.ExecContext(ctx, stampDiscardSQL(false), detail)
	}
	if err != nil {
		return clearOutcome{}, err
	}
	if err := tx.Commit(); err != nil {
		return clearOutcome{}, err
	}
	return clearOutcome{rows: 1, lossDetail: detail}, nil
}

// stampDiscardSQL is the one statement that both records a discarded checkpoint
// as a permanent loss and clears the cursor. withAck adds gap_acked_at = NULL;
// false is only for an index that does not have that column.
func stampDiscardSQL(withAck bool) string {
	ack := ""
	if withAck {
		ack = `
			gap_acked_at    = NULL,`
	}
	return `
		UPDATE stream_state SET
			gap_lost_at     = UTC_TIMESTAMP(),
			gap_lost_detail = ?,` + ack + `
			binlog_file     = '',
			binlog_position = 0,
			gtid_set        = NULL,
			events_indexed  = 0,
			last_event_time = NULL,
			last_checkpoint = UTC_TIMESTAMP()
		WHERE id = 1`
}

// isUnknownColumnErr reports whether err is MySQL error 1054
// (ER_BAD_FIELD_ERROR): the index predates a column this build writes.
func isUnknownColumnErr(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1054
}

// isTableMissingErr reports whether err is MySQL error 1146 (ER_NO_SUCH_TABLE), i.e.
// the target has no stream_state table (never streamed, or wrong database).
func isTableMissingErr(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1146
}
