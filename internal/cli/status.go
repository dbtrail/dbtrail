package cli

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/status"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show index state: indexed files, partition info, and event counts",
	Long: `Displays the current state of the binlog index in three sections:

  - Indexed Files  : which binlog files have been processed and their status
  - Partitions     : all time-range partitions with estimated row counts
  - Summary        : aggregate file and event counts

The Stream section also reports continuity: the cheap "did I lose any events?"
verdict: "no gaps in the captured range" (a contiguity check, not a liveness
one), or a loud "GAP LOST" when an unfillable gap forced an auto-advance with
permanent loss. Pass --fail-on-gap to exit non-zero on that loss, or when
continuity can't be confirmed (fails closed), or when the capture ledger
records ANY dropped events (statement-format DML, column-count mismatches, …;
all the same permanent-loss class), for CI/cron alerting; by default a gap
never changes the exit code.

Freshness is the liveness half continuity is not: "current" (checkpointing and
indexing recent events), "idle" (checkpointing, nothing recent) or "stalled"
(the checkpoint itself is stale; the daemon, not the workload). Offline,
"idle" cannot distinguish a quiet source from one whose capture is far behind;
the daemon's bintrail_stream_index_commit_latency_seconds metric can. Pass
--fail-on-lag <duration> to exit non-zero on a stall, on an unevaluable
verdict, or on a newest event older than the duration.

Partition row counts are estimates read from information_schema (no table scan).

Example:
  bintrail status --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index"`,
	RunE: runStatus,
}

var (
	stIndexDSN    string
	stFormat      string
	stBaselineDir string
	stFailOnGap   bool
	// stAckCaptureSkips is the one WRITE this otherwise read-only command can
	// perform (#1314). It lives on `status` rather than getting its own verb
	// because this is the command that shows the tally: the operator reading
	// the alarm is already holding the DSN, and a separate command they have
	// to discover is a command they will not find. The flag name says the
	// mutation out loud.
	stAckCaptureSkips bool
	stFailOnLag       time.Duration
)

func init() {
	statusCmd.Flags().StringVar(&stIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	statusCmd.Flags().StringVar(&stFormat, "format", "text", "Output format: text or json")
	statusCmd.Flags().StringVar(&stBaselineDir, "baseline-dir", "", "Local directory of baseline Parquet snapshots (optional, shows baseline binlog positions)")
	statusCmd.Flags().DurationVar(&stFailOnLag, "fail-on-lag", 0, "Exit non-zero if capture is not keeping up: a stalled checkpoint, an unevaluable verdict (fails closed), or a newest indexed event older than this duration (e.g. 15m). TRAFFIC-SENSITIVE: on a source with genuinely quiet periods the age check fires with nothing wrong, so pick a threshold above your quiet windows. Unset (0) never changes the exit code")
	statusCmd.Flags().BoolVar(&stAckCaptureSkips, "ack-capture-skips", false, "Record that you have seen the current capture-skip tally, then report as usual. The tally is monotonic and never clears itself, so without this a single skip episode keeps `status` non-clean and --fail-on-gap non-zero forever. Nothing is erased: the counts stay, an acknowledgement timestamp is added, and any skip AFTER this one raises the alarm again")
	statusCmd.Flags().BoolVar(&stFailOnGap, "fail-on-gap", false, "Exit non-zero if the stream lost data (a binlog gap or any recorded capture drops), or its continuity can't be confirmed (fails closed); for CI/cron alerting. By default a gap never changes the exit code")
	_ = statusCmd.MarkFlagRequired("index-dsn")
	BindCommandEnv(statusCmd)
	// Registration on a root command happens via AddReadCommands(root), which
	// each binary's main() calls — see commands.go (#529).
}

func runStatus(cmd *cobra.Command, args []string) error {
	start := time.Now()

	if !cliutil.IsValidOutputFormat(stFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", stFormat)
	}

	cfg, err := mysql.ParseDSN(stIndexDSN)
	if err != nil {
		return fmt.Errorf("invalid --index-dsn: %w", err)
	}
	dbName := cfg.DBName
	if dbName == "" {
		return fmt.Errorf("--index-dsn must include a database name (e.g. user:pass@tcp(host:3306)/binlog_index)")
	}

	slog.Debug("connecting to index database", "database", dbName)
	t := time.Now()
	db, err := config.Connect(stIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()
	slog.Debug("connected", "duration_ms", time.Since(t).Milliseconds())

	// The acknowledgement (#1314) runs BEFORE the report is collected, so the
	// status printed below is the state the operator just created rather than
	// the one they are retiring — otherwise every acknowledgement is followed
	// by a screen that looks like it did nothing, which is the exact confusion
	// this feature exists to end.
	if stAckCaptureSkips {
		// EnsureSchema first: capture_skips_ack post-dates most indexes, and
		// this is a CLI-typed DSN, the one place DDL is allowed to run.
		if err := indexer.EnsureSchema(db); err != nil {
			return indexer.WrapSchemaMigrationErr(err)
		}
		// -1: no stale-render guard. Unlike a console tab, this reads and
		// writes in the same breath, so there is no earlier view to protect.
		ackd, ackErr := status.AcknowledgeCaptureSkips(cmd.Context(), db, -1, time.Now())
		switch {
		case errors.Is(ackErr, status.ErrNothingToAcknowledge):
			// Not an error: an operator who acknowledges a clean ledger got
			// what they wanted. Saying so beats a non-zero exit on a no-op.
			fmt.Fprintln(cmd.OutOrStdout(), "Nothing to acknowledge: no capture skips are recorded for this index.")
		case ackErr != nil:
			return fmt.Errorf("acknowledge capture skips: %w", ackErr)
		default:
			fmt.Fprintf(cmd.OutOrStdout(), "Acknowledged %d skipped event(s) (%s) at %s. The tally is kept; a later skip will alarm again.\n",
				ackd.Total, strings.Join(ackd.Reasons, ", "), ackd.At.Format(status.TSFmt))
		}
	}

	data, err := status.CollectStatus(cmd.Context(), db, dbName)
	if err != nil {
		return err
	}

	// Discover baseline Parquet files if --baseline-dir is provided.
	if stBaselineDir != "" {
		baselines, unreadable, bErr := baseline.DiscoverBaselinesReport(stBaselineDir)
		if bErr == nil {
			bErr = unreadableNewestBaseline(baselines, unreadable)
		}
		if bErr != nil {
			slog.Warn("could not discover baselines", "dir", stBaselineDir, "error", bErr)
			// A configured-but-unreadable dir must not render like "no
			// baselines configured" in JSON — a monitor watching
			// baseline_staleness would read absence as healthy.
			data.BaselinesUnavailable = true
			data.BaselinesErr = bErr
		} else {
			for _, b := range baselines {
				var size int64
				if fi, sErr := os.Stat(b.Path); sErr == nil {
					size = fi.Size()
				} else {
					slog.Warn("could not stat baseline file for size", "path", b.Path, "error", sErr)
				}
				data.Baselines = append(data.Baselines, status.BaselineInfo{
					SnapshotTime: b.SnapshotTime,
					Database:     b.Database,
					Table:        b.Table,
					BinlogFile:   b.BinlogFile,
					BinlogPos:    b.BinlogPos,
					GTIDSet:      b.GTIDSet,
					Path:         b.Path,
					Size:         size,
				})
			}
			// Staleness (#1193): grade every snapshot against the oldest
			// available delta coverage — the live-partition floor (partition
			// existence = coverage) extended backwards by archives. The floor
			// comes from OldestDeltaFromDB, NOT data.Coverage: coverage is
			// best-effort display data that reports an archive_state failure
			// as a flag rather than an error (#816), so a floor built on it
			// still cannot distinguish the cases and would fabricate "broken"
			// on healthy
			// archives whenever that one read fails. A floor error degrades
			// every verdict to unknown.
			floor, fErr := status.OldestDeltaFromDB(cmd.Context(), db, dbName)
			if fErr != nil {
				slog.Warn("could not determine the delta-coverage floor; baseline staleness is unknown", "error", fErr)
			}
			status.AnnotateBaselineStaleness(data.Baselines, floor, time.Now().UTC())
		}
	}

	slog.Info("status complete", "duration_ms", time.Since(start).Milliseconds())

	if stFormat == "json" {
		if err := data.WriteJSON(os.Stdout); err != nil {
			return err
		}
	} else {
		data.Write(os.Stdout)
	}

	// Opt-in alertable exit: --fail-on-gap turns a non-OK continuity verdict into a
	// non-zero exit for CI/cron — AFTER the report is written so the operator still
	// sees the full status. It FAILS CLOSED: a stamped gap, OR an inability to
	// confirm the gap state (no stream row / a swallowed load error → nil stream,
	// or a legacy index missing the gap columns) all alert — the flag exists to
	// catch trouble, so "couldn't check" must not read as "fine". Off by default,
	// so status keeps exiting 0 as before (break-nothing for existing scripts).
	if stFailOnGap {
		cmd.SilenceUsage = true
		switch {
		case data.StreamErr != nil:
			return fmt.Errorf("stream continuity: could not read stream state (%w); failing closed under --fail-on-gap", data.StreamErr)
		case data.Stream == nil:
			return fmt.Errorf("stream continuity: could not confirm gap state (no stream state loaded); failing closed under --fail-on-gap")
		case !data.Stream.GapColumnsPresent:
			return fmt.Errorf("stream continuity: could not confirm gap state (legacy index missing gap-detection columns; migrate the schema); failing closed under --fail-on-gap")
		case data.Stream.GapLostAt.Valid:
			return fmt.Errorf("stream continuity: events permanently lost (gap detected at %s); index is valid only up to the gap, resume requires re-baseline",
				data.Stream.GapLostAt.Time.Format(status.TSFmt))
		}
		// #999: a non-zero IN-SCOPE statement-format DML count is the same loss
		// class as gap_lost — those changes are permanently absent from the
		// index — so it joins the fail-closed contract. A NULL/empty ledger
		// (legacy index / no skip-aware daemon) deliberately does NOT alert
		// here: unlike the gap columns above, capture_skips post-dates this
		// flag, and failing every pre-#1034 deployment's cron would bury the
		// signal in false alarms (the gap-column unknowns already alerted
		// before capture_skips existed, so their fail-closed stance breaks no
		// one). A ledger that IS present but unreadable gets no such pass —
		// a skip-aware daemon wrote it, it may be hiding a loss count, and
		// "couldn't check" must not read as "fine" (the sibling branches'
		// stance).
		// An ACKNOWLEDGED tally (#1314) does not fail the flag. This is a
		// deliberate change to exit semantics, not a rendering one: the tally
		// is monotonic, so before this an operator whose cron went red had
		// exactly two options — hand-edit the column with the daemon stopped
		// (destroying the loss record) or delete --fail-on-gap. An alert
		// nobody can clear is an alert everybody removes, and the next real
		// loss then lands in silence.
		//
		// It is safe because the acknowledgement records a COUNT: one more
		// skipped event lifts the tally above it and every branch below fires
		// again with no operator action. The check sits ahead of all of them
		// because acknowledgement is not per-reason to the operator — they
		// acknowledged the record, and re-failing on a sibling reason they
		// already saw would be the same trap in a smaller box.
		skips, skipsOK := data.Stream.ParseCaptureSkips()
		switch {
		case skipsOK && status.CaptureSkipsAcknowledged(skips, data.Stream.ParseCaptureSkipsAck()):
			// Acknowledged: every branch below is deliberately skipped.
		case skipsOK:
			if st := skips[status.CaptureSkipReasonStatementFormatDML]; st.Count > 0 {
				loc := ""
				if st.LastFile != "" || st.LastStatementType != "" {
					file := st.LastFile
					if file == "" {
						file = "?"
					}
					loc = fmt.Sprintf(" (last: %s at %s:%d, connection id %d)", st.LastStatementType, file, st.LastPos, st.LastConnectionID)
				}
				return fmt.Errorf("capture health: %d statement-format DML event(s) permanently uncaptured%s; set binlog_format=ROW server-wide on the source, then acknowledge this tally with `bintrail status --index-dsn <index> --ack-capture-skips` (it is monotonic and never clears itself; acknowledging erases nothing and a later skip fails this check again); failing closed under --fail-on-gap", st.Count, loc)
			}
			// #1206: the restart path stamps this meta-reason when the
			// previously persisted ledger could not be parsed — a loss tally
			// may have been destroyed, so a now-readable ledger carrying it
			// must not read as "fine".
			if st := skips[status.CaptureSkipReasonUnreadablePreviousLedger]; st.Count > 0 {
				return fmt.Errorf("capture health: a previous capture ledger was unreadable at daemon restart and its tally is lost: permanent loss may be unrecorded; acknowledge it with `bintrail status --index-dsn <index> --ack-capture-skips` once you have acted on the possibility of unrecorded loss; failing closed under --fail-on-gap")
			}
			// #1207: every remaining reason is the same permanent-loss class —
			// an event read from the stream and dropped is absent from the
			// index whatever the reason (the #1034 real-world case: a stale
			// snapshot dropping 100% of rows for days) — so ANY non-zero
			// count fails closed, not just the two named above. The two
			// specific branches stay first for their sharper remediation.
			var dropped int64
			var reasons []string
			for r, st := range skips {
				if st.Count > 0 {
					dropped += st.Count
					reasons = append(reasons, r)
				}
			}
			if dropped > 0 {
				sort.Strings(reasons)
				return fmt.Errorf("capture health: %d event(s) read from the stream and permanently dropped (%s); most often the schema snapshot is stale or corrupt; run `bintrail snapshot` against the source and restart the stream, then acknowledge this tally with `bintrail status --index-dsn <index> --ack-capture-skips` (it is monotonic and never clears itself; acknowledging erases nothing and a later skip fails this check again); failing closed under --fail-on-gap", dropped, strings.Join(reasons, ", "))
			}
		case data.Stream.CaptureSkips.Valid && strings.TrimSpace(data.Stream.CaptureSkips.String) != "":
			return fmt.Errorf("capture health: capture_skips ledger present but unreadable; cannot confirm statement-format DML drops; failing closed under --fail-on-gap")
		}
	}

	// Opt-in alertable exit for FRESHNESS (#1226) — the liveness sibling of
	// --fail-on-gap, and deliberately a separate flag: continuity and liveness
	// are independent failures (a contiguous index can be days stale) and an
	// operator should be able to alert on one without the other.
	//
	// Fails closed on the three verdicts that are not claims — a "couldn't
	// check" that exits 0 is the same cry-wolf inversion --fail-on-gap avoids.
	//
	// The age check is the one an operator can misconfigure into noise: offline,
	// a quiet source and a badly-lagging one are the SAME observation (see
	// status.FreshnessStatus), so this necessarily fires on a genuinely idle
	// source. That is why it is opt-in, takes an explicit duration, and says so
	// in the flag help rather than shipping a default that surprises people.
	if stFailOnLag > 0 {
		cmd.SilenceUsage = true
		now := time.Now()
		verdict := status.FreshnessStatus(data.Stream, data.StreamErr, now, 0, 0)
		if !status.FreshnessEvaluated(verdict) {
			return fmt.Errorf("stream freshness: could not evaluate capture liveness (%s); failing closed under --fail-on-lag", verdict)
		}
		if verdict == status.FreshnessStalled {
			age, _ := status.CheckpointAge(data.Stream, now)
			return fmt.Errorf("stream freshness: capture is STALLED: no checkpoint written for %s; the checkpoint ticker runs even with no traffic, so check the daemon is running",
				age.Round(time.Second))
		}
		age, ok := status.NewestEventAge(data.Stream, now)
		if !ok {
			return fmt.Errorf("stream freshness: no event has been indexed yet, so lag against %s cannot be evaluated; failing closed under --fail-on-lag", stFailOnLag)
		}
		if age > stFailOnLag {
			return fmt.Errorf("stream freshness: newest indexed event is %s old, over the %s threshold (on a source with quiet periods this can be idleness, not lag; bintrail_stream_index_commit_latency_seconds on the daemon distinguishes them)",
				age.Round(time.Second), stFailOnLag)
		}
	}
	return nil
}

// unreadableNewestBaseline refuses to grade a partial baseline walk (#1639):
// a folder that could not be read at or after a table's newest readable
// snapshot may hold a newer copy of that table, and grading the older one
// would report "aging" or "broken" for a table that is current. Staleness is
// graded per table, so the floor is the OLDEST of the per-table newest
// snapshots: a `--tables` snapshot holding one table must not vouch for the
// others. The caller then reports the baselines as unavailable, the verdict a
// wholly unreadable directory already gets. A skipped folder older than every
// table's newest changes nothing.
func unreadableNewestBaseline(baselines []baseline.BaselineInfo, unreadable []time.Time) error {
	newest := make(map[string]time.Time)
	for _, b := range baselines {
		key := b.Database + "." + b.Table
		if b.SnapshotTime.After(newest[key]) {
			newest[key] = b.SnapshotTime
		}
	}
	var floor time.Time
	for _, at := range newest {
		if floor.IsZero() || at.Before(floor) {
			floor = at
		}
	}
	for _, u := range unreadable {
		if !u.Before(floor) {
			return fmt.Errorf("a baseline folder from %s could not be read, and it is not older than the newest readable baseline of every table", u.UTC().Format(time.RFC3339))
		}
	}
	return nil
}
