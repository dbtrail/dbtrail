package cliapp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/icebergexport"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// The export commands live in cliapp, not internal/cli, on purpose: internal/cli
// is linked by the console daemon and bintrail-pg, and the Iceberg library
// must reach neither (cliapp/icebergfree_test.go). cliapp is the root that
// only cmd/bintrail links.

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Write table state to an external table format",
	Long: `Export the current state of indexed tables to a format other tools read.
bintrail's own storage does not change: the archives, baselines and index stay
what they are, and an export reads through them and writes somewhere new.`,
}

var exportIcebergCmd = &cobra.Command{
	Use:   "iceberg",
	Short: "Export table state to Apache Iceberg tables, incrementally",
	Long: `Write each table's current state as an Apache Iceberg table under --warehouse,
one table per <warehouse>/<schema>/<table>/, readable by DuckDB, Spark, Trino
and Athena.

The first run loads the newest baseline snapshot. Every run after that appends
only what changed: the events between the table's own cursor and the run's
binlog cut are folded to the net change per primary key and committed as ONE
Iceberg snapshot (equality deletes for every touched key, plus the rows that
still exist). The table is never rewritten; compaction is the reader's
concern and any engine can do it.

The cursor lives in the table's own properties, in the same commit as the
data, so nothing is written to the index and a run that dies before committing
leaves the previous snapshot in place and resumes from it. To reload a table
from a fresh baseline, remove its directory.

WHAT IT REFUSES

  capture gap     The window spans events the index permanently lost, or hours
                  rotated out without an archive. The table does not advance.
  schema change   The table's columns changed since it was exported (or since
                  its baseline was taken). Remove the table directory to reload
                  it from a fresh baseline.
  destructive DDL A TRUNCATE / DROP / RENAME (or MariaDB's CREATE OR REPLACE) in
                  the window emits no row events, so folding over it would
                  resurrect rows that no longer exist.
  no primary key  Equality deletes name rows by key; a table without one, or
                  with a FLOAT, DOUBLE, TIME, BIT, JSON or spatial key, cannot
                  be exported.
  BIT columns     Stored differently by the baseline and the row events; not
                  reconciled yet.
  skipped events  The capture daemon dropped events for the table inside the
                  window (stream_state.capture_skips), so the index is not
                  complete for it.

Those refusals are per table. A refused table does not advance and is retried
on the next run; the other tables commit. The exit status is non-zero when any
table did not end current.

One refusal is for the whole run, before any table is touched: an index that
holds more than one source in bintrail_servers, or has no such table. Events
cannot be attributed to one source, so two sources with the same schema.table
would interleave in one Iceberg table. A file-mode index with no registered
source is accepted.

This is a one-shot command for the operator's scheduler. It never runs inside
the capture daemon.

Examples:
  # First run: load every table of the newest snapshot, then fold the deltas
  bintrail export iceberg --index-dsn "..." --baseline-dir /data/baselines \
    --warehouse /data/iceberg

  # Hourly, from cron: the same command line appends only what changed
  bintrail export iceberg --index-dsn "..." --baseline-dir /data/baselines \
    --warehouse /data/iceberg --tables shop.orders,shop.customers

  # Read it back (DuckDB 1.4 needs the key columns selected, or a
  # materialized copy, for equality deletes to apply; 1.5 does not)
  duckdb -c "INSTALL iceberg; LOAD iceberg; SELECT id, status FROM iceberg_scan('/data/iceberg/shop/orders') LIMIT 10;"`,
	RunE: runExportIceberg,
}

var (
	eiIndexDSN    string
	eiBaselineDir string
	eiBaselineS3  string
	eiWarehouse   string
	eiTables      string
	eiAt          string
	eiFetchBatch  int
	eiFormat      string
)

func init() {
	f := exportIcebergCmd.Flags()
	f.StringVar(&eiIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	f.StringVar(&eiBaselineDir, "baseline-dir", "", "Local directory of baseline snapshots (one of --baseline-dir/--baseline-s3 is required)")
	f.StringVar(&eiBaselineS3, "baseline-s3", "", "S3 URL prefix of baseline snapshots")
	f.StringVar(&eiWarehouse, "warehouse", "", "Local directory to write the Iceberg tables under (required)")
	f.StringVar(&eiTables, "tables", "", "Comma-separated schema.table list (default: every table in the newest baseline snapshot)")
	f.StringVar(&eiAt, "at", "", "Point-in-time to export up to (default: now)")
	f.IntVar(&eiFetchBatch, "fetch-batch-size", 0, "Event page size for the delta fold (0 = default)")
	f.StringVar(&eiFormat, "format", "text", "Output format: text or json")
	cli.AddDuckDBTuningFlags(exportIcebergCmd)
	bindCommandEnv(exportIcebergCmd)

	exportCmd.AddCommand(exportIcebergCmd)
	rootCmd.AddCommand(exportCmd)
}

func runExportIceberg(cmd *cobra.Command, _ []string) error {
	if eiIndexDSN == "" {
		return fmt.Errorf("--index-dsn is required")
	}
	if eiBaselineDir == "" && eiBaselineS3 == "" {
		return fmt.Errorf("one of --baseline-dir or --baseline-s3 is required")
	}
	if eiBaselineDir != "" && eiBaselineS3 != "" {
		return fmt.Errorf("--baseline-dir and --baseline-s3 are mutually exclusive")
	}
	if eiWarehouse == "" {
		return fmt.Errorf("--warehouse is required")
	}
	if !cliutil.IsValidOutputFormat(eiFormat) {
		return fmt.Errorf("--format must be text or json")
	}
	if eiFetchBatch < 0 {
		return fmt.Errorf("--fetch-batch-size must be >= 0 (0 = default)")
	}
	source := eiBaselineDir
	if source == "" {
		source = eiBaselineS3
	}

	at := time.Now().UTC()
	if eiAt != "" {
		parsed, err := cliutil.ParseTime(eiAt)
		if err != nil {
			return fmt.Errorf("--at: %w", err)
		}
		if parsed != nil {
			at = *parsed
		}
	}

	tables, err := resolveExportTables(cmd, source)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return fmt.Errorf("no tables to export: no baseline snapshot was discovered under %s "+
			"(take one with `bintrail dump` + `bintrail baseline` first)", source)
	}

	tuning, err := cli.DuckDBTuningFromFlags(cmd)
	if err != nil {
		return err
	}

	outcomes, err := icebergexport.Run(cmd.Context(), icebergexport.Config{
		IndexDSN:       eiIndexDSN,
		BaselineSrc:    source,
		Warehouse:      eiWarehouse,
		Tables:         tables,
		At:             at,
		FetchBatchSize: eiFetchBatch,
		ArchiveFetcher: cli.TunedArchiveFetcher(tuning),
		DuckDBTuning:   tuning,
		OnCommit:       func(c icebergexport.Commit) { auditIcebergCommit(cmd.Context(), c) },
	})
	if err != nil {
		return err
	}

	failed := 0
	for _, o := range outcomes {
		if !o.Verdict.OK() {
			failed++
		}
	}
	if eiFormat == "json" {
		err = writeExportJSON(cmd.OutOrStdout(), outcomes, eiWarehouse, at)
	} else {
		err = writeExportSummary(cmd.OutOrStdout(), outcomes, eiWarehouse, at)
	}
	if err != nil {
		return fmt.Errorf("write the export report: %w", err)
	}
	if failed > 0 {
		return fmt.Errorf("iceberg export: %d of %d table(s) did not end current", failed, len(outcomes))
	}
	return nil
}

// resolveExportTables returns --tables when given, otherwise every table in
// the newest discoverable baseline snapshot: the set the first run can seed,
// which keeps later runs a strict continuation of it.
func resolveExportTables(cmd *cobra.Command, source string) ([]string, error) {
	if eiTables != "" {
		var out []string
		for _, entry := range strings.Split(eiTables, ",") {
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
	return reconstruct.NewestSnapshotTables(cmd.Context(), source)
}

// auditIcebergCommit records one audit event per commit, called by the
// export right after that commit is durable and before the run moves on. A
// run killed between two tables has still written the first, and only a
// per-commit emission says so; an unchanged table commits no rows and is not
// an event.
//
// snapshot_id is present only when the commit left the table at an Iceberg
// snapshot. A first load of a zero-row baseline commits the table and its
// cursor and no data file, so there is no snapshot to name; writing "0"
// there would send a reader to iceberg_snapshots() for an id that does not
// exist (#1509).
func auditIcebergCommit(ctx context.Context, c icebergexport.Commit) {
	detail := map[string]string{
		"commit":   string(c.Kind),
		"rows":     strconv.FormatInt(c.Rows, 10),
		"deletes":  strconv.FormatInt(c.Deletes, 10),
		"events":   strconv.FormatInt(c.Events, 10),
		"cursor":   c.Cursor,
		"location": c.Location,
	}
	if c.SnapshotID != nil {
		detail["snapshot_id"] = strconv.FormatInt(*c.SnapshotID, 10)
	}
	ext.Record(ctx, ext.AuditEvent{
		Surface: "cli",
		Action:  "export.iceberg",
		Actor:   ext.ProcessActor(""),
		Schema:  c.Schema,
		Table:   c.Table,
		Detail:  detail,
	})
}

func writeExportSummary(w io.Writer, outcomes []icebergexport.Outcome, warehouse string, at time.Time) error {
	var b strings.Builder
	fmt.Fprintf(&b, "iceberg export to %s at %s\n\n", warehouse, at.UTC().Format(time.RFC3339))
	width := 0
	for _, o := range outcomes {
		if n := len(o.Schema) + 1 + len(o.Table); n > width {
			width = n
		}
	}
	for _, o := range outcomes {
		name := o.Schema + "." + o.Table
		fmt.Fprintf(&b, "  %-*s  %s\n", width, name, o.Verdict)
		if o.Detail != "" {
			fmt.Fprintf(&b, "  %-*s  └─ %s\n", width, "", o.Detail)
		}
	}
	b.WriteString("\n")
	_, err := io.WriteString(w, b.String())
	return err
}

// exportJSON is the --format json shape.
type exportJSON struct {
	Warehouse string            `json:"warehouse"`
	At        string            `json:"at"`
	Tables    []exportTableJSON `json:"tables"`
}

type exportTableJSON struct {
	Schema     string `json:"schema"`
	Table      string `json:"table"`
	Verdict    string `json:"verdict"`
	Detail     string `json:"detail,omitempty"`
	RowsLoaded int64  `json:"rows_loaded"`
	Events     int64  `json:"events"`
	Upserts    int64  `json:"upserts"`
	Deletes    int64  `json:"deletes"`
	SnapshotID *int64 `json:"snapshot_id,omitempty"` // absent when the table has no snapshot (a zero-row load)
	Cursor     string `json:"cursor,omitempty"`
	Location   string `json:"location,omitempty"`
}

func writeExportJSON(w io.Writer, outcomes []icebergexport.Outcome, warehouse string, at time.Time) error {
	out := exportJSON{Warehouse: warehouse, At: at.UTC().Format(time.RFC3339)}
	for _, o := range outcomes {
		out.Tables = append(out.Tables, exportTableJSON{
			Schema: o.Schema, Table: o.Table, Verdict: string(o.Verdict), Detail: o.Detail,
			RowsLoaded: o.RowsLoaded, Events: o.Events, Upserts: o.Upserts, Deletes: o.Deletes,
			SnapshotID: o.SnapshotID, Cursor: o.Cursor, Location: o.Location,
		})
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}
