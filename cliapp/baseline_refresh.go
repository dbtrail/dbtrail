package cliapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

var baselineRefreshCmd = &cobra.Command{
	Use:   "refresh",
	Short: "Produce a new baseline snapshot from the previous one plus indexed deltas",
	Long: `Fold the changes the index already holds onto the newest baseline snapshot and
publish the result as a NEW snapshot — no mydumper run, no connection to the
source database.

The information needed to move a baseline forward is already in bintrail's own
storage: the previous snapshot has the rows, and the index has every change
since. Refreshing keeps time-travel fast (a short delta window instead of
months of replay) and keeps the archive hours a reconstruction depends on from
growing without bound.

Tables default to every table in the newest discoverable snapshot; --tables
narrows that, which is the tool for isolating a table that refuses.

WHAT IT REFUSES, AND WHY IT REFUSES RATHER THAN WARNS

A baseline is picked up automatically by every later reconstruct, so a wrong
one is not a bad output — it is a wrong answer to every future question.

  capture gap   The window spans events the index permanently lost (or an
                index too old to rule that out). --allow-gaps proceeds, and
                the emitted snapshot then carries a permanent marker in its
                metadata saying so — inherited by every snapshot derived from
                it, because the missing events stay missing.
  schema change The table's columns moved since the baseline. The snapshot
                would carry the old CREATE TABLE forward and project rows onto
                the old shape. Only a real re-dump fixes this.
  destructive   A TRUNCATE / DROP / RENAME (or MariaDB's CREATE OR REPLACE) in
      DDL       the window emits no row events, so the fold would resurrect
                rows that no longer exist.

Publication is all-or-nothing: if any table refuses, NOTHING is published and
the exit status is non-zero. A half-refreshed snapshot would be a set of tables
from two different points in time under one anchor.

Every full-table reconstruct limit applies (primary key required, a PK-changing
UPDATE in the window refuses, PostgreSQL sources are out of scope).

Examples:
  # Refresh every table of the newest snapshot, in place
  bintrail baseline refresh --index-dsn "..." --baseline-dir /data/baselines

  # One table, to a point in the past
  bintrail baseline refresh --index-dsn "..." --baseline-dir /data/baselines \
    --tables mydb.orders --at "2026-05-01 12:00:00"

  # Source snapshots on S3: read from there, write locally, then upload
  bintrail baseline refresh --index-dsn "..." \
    --baseline-s3 s3://bucket/baselines/ --output /data/baselines
  bintrail upload --source /data/baselines --destination s3://bucket/baselines/`,
	RunE: runBaselineRefresh,
}

var (
	brIndexDSN     string
	brBaselineDir  string
	brBaselineS3   string
	brOutput       string
	brTables       string
	brAt           string
	brAllowGaps    bool
	brCarryForward bool
	brParallelism  int
	brFetchBatch   int
	brWarnEvents   int64
)

func init() {
	f := baselineRefreshCmd.Flags()
	f.StringVar(&brIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	f.StringVar(&brBaselineDir, "baseline-dir", "", "Local directory of baseline snapshots to refresh from (and, unless --output is set, to write into)")
	f.StringVar(&brBaselineS3, "baseline-s3", "", "S3 URL prefix of baseline snapshots to refresh from (requires --output for the local destination)")
	f.StringVar(&brOutput, "output", "", "Directory to write the new snapshot into (default: --baseline-dir)")
	f.StringVar(&brTables, "tables", "", "Comma-separated schema.table list (default: every table in the newest snapshot)")
	f.StringVar(&brAt, "at", "", "Point-in-time to refresh to (default: now)")
	f.BoolVar(&brAllowGaps, "allow-gaps", false, "Publish even when the window spans a known permanent capture gap; the snapshot is permanently marked as knowingly incomplete")
	f.BoolVar(&brCarryForward, "carry-forward-unchanged", false,
		"When a table had no changes, publish its previous Parquet file instead of rewriting it (hard link "+
			"where possible). Off by default: the rows are identical either way, but it links two snapshots "+
			"to one file, so disk-usage and prune figures then count space they will not reclaim")
	f.IntVar(&brParallelism, "parallelism", 0, "Max tables refreshed concurrently (0 = one per CPU)")
	f.IntVar(&brFetchBatch, "fetch-batch-size", 0, "Event page size for the delta fold (0 = default)")
	f.Int64Var(&brWarnEvents, "warn-event-threshold", 5_000_000, "Warn when a table's delta window exceeds this many events (0 disables)")
	cli.AddDuckDBTuningFlags(baselineRefreshCmd)
	bindCommandEnv(baselineRefreshCmd)

	baselineCmd.AddCommand(baselineRefreshCmd)
}

// refreshOutcome is one table's verdict in the run summary.
type refreshOutcome struct {
	Table   string
	Verdict string // "refreshed", "unchanged", "refused-gap", "refused-ddl", "refused"
	Detail  string
}

func runBaselineRefresh(cmd *cobra.Command, _ []string) error {
	if brIndexDSN == "" {
		return fmt.Errorf("--index-dsn is required")
	}
	if brBaselineDir == "" && brBaselineS3 == "" {
		return fmt.Errorf("one of --baseline-dir or --baseline-s3 is required")
	}
	if brBaselineDir != "" && brBaselineS3 != "" {
		return fmt.Errorf("--baseline-dir and --baseline-s3 are mutually exclusive")
	}
	source := brBaselineDir
	if source == "" {
		source = brBaselineS3
	}
	output := brOutput
	if output == "" {
		output = brBaselineDir
	}
	if output == "" {
		// Reading from S3 with nowhere local to write. Refuse rather than pick a
		// directory for the operator: a snapshot written somewhere they did not
		// name is a snapshot they will not find, and this command's whole value
		// is that the result is discoverable.
		return fmt.Errorf("--output is required with --baseline-s3: the new snapshot is written locally "+
			"(publish it with `bintrail upload --source <output> --destination %s`)", brBaselineS3)
	}
	if brWarnEvents < 0 {
		return fmt.Errorf("--warn-event-threshold must be >= 0 (0 disables)")
	}

	at := time.Now().UTC()
	if brAt != "" {
		parsed, err := cliutil.ParseTime(brAt)
		if err != nil {
			return fmt.Errorf("--at: %w", err)
		}
		if parsed != nil {
			at = *parsed
		}
	}

	tables, err := resolveRefreshTables(cmd.Context(), source)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return fmt.Errorf("no tables to refresh: no baseline snapshot was discovered under %s "+
			"(take one with `bintrail dump` + `bintrail baseline` first)", source)
	}

	tuning, err := cli.DuckDBTuningFromFlags(cmd)
	if err != nil {
		return err
	}

	reports, failures, runErr := reconstruct.ReconstructTablesDetailed(cmd.Context(), reconstruct.FullTableConfig{
		IndexDSN:              brIndexDSN,
		BaselineSrc:           source,
		Tables:                tables,
		At:                    at,
		OutputDir:             output,
		OutputFormat:          reconstruct.OutputFormatParquet,
		AllowGaps:             brAllowGaps,
		CarryForwardUnchanged: brCarryForward,
		Parallelism:           brParallelism,
		FetchBatchSize:        brFetchBatch,
		WarnEventThreshold:    brWarnEvents,
		ArchiveFetcher:        cli.TunedArchiveFetcher(tuning),
		DuckDBTuning:          tuning,
	})

	outcomes := buildRefreshOutcomes(tables, reports, failures)
	writeRefreshSummary(cmd.OutOrStdout(), outcomes, output, at, runErr == nil)

	if runErr != nil {
		// The per-table detail is already in the summary above; keep the returned
		// error to the consequence, which is the part an operator acts on.
		return fmt.Errorf("baseline refresh published nothing: %d of %d table(s) refused",
			len(failures), len(tables))
	}
	return nil
}

// resolveRefreshTables returns the schema.table list to refresh: --tables when
// given, otherwise every table in the NEWEST discoverable snapshot.
//
// Defaulting to the newest snapshot's tables (rather than, say, every table the
// index has ever seen) keeps the refreshed snapshot a strict successor of the
// one it was folded from. A table absent from the source snapshot has nothing to
// fold onto, and inventing an entry for it would publish a snapshot claiming
// coverage it does not have.
func resolveRefreshTables(ctx context.Context, source string) ([]string, error) {
	if brTables != "" {
		var out []string
		for _, entry := range strings.Split(brTables, ",") {
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue
			}
			if !strings.Contains(entry, ".") {
				return nil, fmt.Errorf("--tables entry %q must be schema.table", entry)
			}
			out = append(out, entry)
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("--tables: no entries after trimming")
		}
		return out, nil
	}

	return reconstruct.NewestSnapshotTables(ctx, source)
}

// buildRefreshOutcomes pairs every requested table with its verdict.
//
// The classification reads the sentinels reconstruct exports rather than the
// message text: "the events are gone" and "the table changed shape" have
// completely different remedies, and a summary that blurs them sends the
// operator down the wrong one.
func buildRefreshOutcomes(tables []string, reports []*reconstruct.TableReport, failures []reconstruct.TableFailure) []refreshOutcome {
	failed := make(map[string]error, len(failures))
	for _, f := range failures {
		failed[f.Schema+"."+f.Table] = f.Err
	}
	done := make(map[string]bool, len(reports))
	// Separate from done rather than a second bool on it: a carried-forward
	// table WAS published, so it must not read as skipped, and it was not
	// rewritten, so calling it "refreshed" would hide the thing an operator
	// most wants to see here — which tables are actually costing them a full
	// rewrite each cycle.
	unchanged := make(map[string]bool, len(reports))
	for _, r := range reports {
		k := r.Schema + "." + r.Table
		done[k] = true
		unchanged[k] = r.CarriedForward
	}

	out := make([]refreshOutcome, 0, len(tables))
	for _, t := range tables {
		switch err, bad := failed[t]; {
		case bad && errors.Is(err, reconstruct.ErrCaptureGap):
			out = append(out, refreshOutcome{t, "refused-gap", err.Error()})
		case bad && (errors.Is(err, reconstruct.ErrSchemaChanged) || errors.Is(err, reconstruct.ErrDestructiveDDL)):
			out = append(out, refreshOutcome{t, "refused-ddl", err.Error()})
		case bad:
			out = append(out, refreshOutcome{t, "refused", err.Error()})
		case done[t] && unchanged[t]:
			out = append(out, refreshOutcome{t, "unchanged", "no events in the window; the previous file was published as-is"})
		case done[t]:
			out = append(out, refreshOutcome{t, "refreshed", ""})
		default:
			// Requested, neither reported nor failed: the run was cancelled
			// before this table started. Not "fine" — say so.
			out = append(out, refreshOutcome{t, "skipped", "the run ended before this table was reached"})
		}
	}
	return out
}

func writeRefreshSummary(w io.Writer, outcomes []refreshOutcome, output string, at time.Time, published bool) {
	fmt.Fprintf(w, "baseline refresh to %s\n\n", at.UTC().Format(time.RFC3339))
	width := 0
	for _, o := range outcomes {
		if len(o.Table) > width {
			width = len(o.Table)
		}
	}
	for _, o := range outcomes {
		fmt.Fprintf(w, "  %-*s  %s\n", width, o.Table, o.Verdict)
		if o.Detail != "" {
			fmt.Fprintf(w, "  %-*s  └─ %s\n", width, "", o.Detail)
		}
	}
	fmt.Fprintln(w)
	if published {
		fmt.Fprintf(w, "published %s/%s\n", strings.TrimRight(output, string(os.PathSeparator)),
			strings.ReplaceAll(at.UTC().Format(time.RFC3339), ":", "-"))
		return
	}
	fmt.Fprintln(w, "NOTHING was published: a snapshot mixing refreshed and stale tables under one anchor")
	fmt.Fprintln(w, "would be worse than no refresh. Fix or exclude the tables above (--tables) and retry.")
}
