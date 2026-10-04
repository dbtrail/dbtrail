package main

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
)

var pgBaselineCmd = &cobra.Command{
	Use:   "baseline",
	Short: "Take a Parquet baseline snapshot directly from a live PostgreSQL source",
	Long: `Takes a consistent snapshot of every table the publication streams and writes
one Parquet file per table (the PostgreSQL sibling of 'bintrail baseline' —
no mydumper/pg_dump step; tables are COPYed inside one REPEATABLE READ
transaction directly into Parquet).

The snapshot is anchored to the WAL: the replication slot's own
confirmed_flush_lsn/restart_lsn is read just before the snapshot transaction
opens and embedded in each Parquet file's metadata as the delta-replay floor,
so reconstruct can apply the indexed deltas at or after it (#771 — not
pg_current_wal_lsn(), and not a strictly-after cutoff: the slot floor is
always at or before the snapshot's own live LSN, and any resulting overlap
with rows already in the baseline is harmless because the merge is
last-write-wins over full-row images). The replication slot (--slot) is
ensured to exist BEFORE the snapshot opens — if it does not exist and
--repl-dsn is given, it is created (the same slot 'bintrail-pg stream' will
consume); without --repl-dsn a missing slot is a fatal error, because a
baseline without a slot has no delta stream to anchor.

Values are stored as raw PostgreSQL text, matching the pgoutput rendering the
delta path indexes — no type conversion.

Output structure:
  <output>/<timestamp>/<schema>/<table>.parquet`,
	RunE: runPGBaseline,
}

var (
	pgbQueryDSN     string
	pgbReplDSN      string
	pgbSlot         string
	pgbPublication  string
	pgbOutput       string
	pgbSchemas      string
	pgbTables       string
	pgbCompression  string
	pgbRowGroupSize int
	pgbParallelism  int
	pgbRetry        bool
	pgbUpload       string
	pgbUploadRegion string
	pgbBintrailID   string
)

func init() {
	pgBaselineCmd.Flags().StringVar(&pgbQueryDSN, "query-dsn", "", "PostgreSQL ordinary connection string; snapshot transaction + COPY run on it (required; env BINTRAIL_PG_QUERY_DSN)")
	pgBaselineCmd.Flags().StringVar(&pgbReplDSN, "repl-dsn", "", "PostgreSQL REPLICATION connection string (replication=database); used only to create the slot when absent (env BINTRAIL_PG_REPL_DSN)")
	pgBaselineCmd.Flags().StringVar(&pgbSlot, "slot", "", "Logical replication slot name to anchor against; created if absent when --repl-dsn is given (required; env BINTRAIL_PG_SLOT)")
	pgBaselineCmd.Flags().StringVar(&pgbPublication, "publication", "", "PostgreSQL publication defining the table set (required; env BINTRAIL_PG_PUBLICATION)")
	cli.AddOutputFlag(pgBaselineCmd, &pgbOutput, "", "Parquet output base directory (required)", cli.OutputDirAlias)
	pgBaselineCmd.Flags().StringVar(&pgbSchemas, "schemas", "", "Only snapshot these schemas (comma-separated)")
	pgBaselineCmd.Flags().StringVar(&pgbTables, "tables", "", "Only snapshot these tables (comma-separated, e.g. public.orders)")
	pgBaselineCmd.Flags().StringVar(&pgbCompression, "compression", "zstd", "Parquet compression codec: zstd, snappy, gzip, none")
	pgBaselineCmd.Flags().IntVar(&pgbRowGroupSize, "row-group-size", 500_000, "Rows per Parquet row group")
	pgBaselineCmd.Flags().IntVar(&pgbParallelism, "parallelism", 0, "Concurrent table COPYs, each on its own connection sharing the snapshot (0 = number of CPUs)")
	pgBaselineCmd.Flags().BoolVar(&pgbRetry, "retry", false, "With --upload: skip S3 objects that were already uploaded (local Parquet generation always runs fresh — every run is a new timestamped snapshot)")
	pgBaselineCmd.Flags().StringVar(&pgbUpload, "upload", "", "S3 destination URL to upload Parquet files after generation (e.g. s3://my-bucket/baselines/)")
	pgBaselineCmd.Flags().StringVar(&pgbUploadRegion, "upload-region", "", "AWS region for --upload (default: from AWS_REGION env var or ~/.aws/config)")
	pgBaselineCmd.Flags().StringVar(&pgbBintrailID, "bintrail-id", "", "Server identity UUID to sign the snapshot with. One writer per --output folder or --upload prefix: two installations writing into one location mix their snapshots. Without it the snapshot is unsigned")
	_ = pgBaselineCmd.MarkFlagRequired(cli.OutputFlag)
	// query-dsn/slot/publication are required but validated in RunE, not via
	// MarkFlagRequired: their BINTRAIL_PG_* env fallback (applied below, after
	// cli.BindCommandEnv has loaded the env file) must satisfy them too —
	// the same pattern as stream.go.
	cli.BindCommandEnv(pgBaselineCmd)

	rootCmd.AddCommand(pgBaselineCmd)
}

// pgBaselineConfigFromFlags applies the BINTRAIL_PG_* env fallback, validates
// the required settings, and snapshots the pgb* flag globals into a
// pgbaseline.Config — the pure seam mirroring pgStreamConfigFromFlags.
func pgBaselineConfigFromFlags() (pgbaseline.Config, error) {
	applyEnvFallback(&pgbQueryDSN, "BINTRAIL_PG_QUERY_DSN")
	applyEnvFallback(&pgbReplDSN, "BINTRAIL_PG_REPL_DSN")
	applyEnvFallback(&pgbSlot, "BINTRAIL_PG_SLOT")
	applyEnvFallback(&pgbPublication, "BINTRAIL_PG_PUBLICATION")

	var missing []string
	if pgbQueryDSN == "" {
		missing = append(missing, "--query-dsn (or BINTRAIL_PG_QUERY_DSN)")
	}
	if pgbSlot == "" {
		missing = append(missing, "--slot (or BINTRAIL_PG_SLOT)")
	}
	if pgbPublication == "" {
		missing = append(missing, "--publication (or BINTRAIL_PG_PUBLICATION)")
	}
	if len(missing) > 0 {
		return pgbaseline.Config{}, fmt.Errorf("missing required PostgreSQL settings: %s", strings.Join(missing, ", "))
	}

	return pgbaseline.Config{
		QueryDSN:     pgbQueryDSN,
		ReplDSN:      pgbReplDSN,
		SlotName:     pgbSlot,
		Publication:  pgbPublication,
		Filters:      cliutil.BuildIndexFilters(pgbSchemas, pgbTables),
		OutputDir:    pgbOutput,
		Compression:  pgbCompression,
		RowGroupSize: pgbRowGroupSize,
		Parallelism:  pgbParallelism,
		WriterID:     pgbBintrailID,
	}, nil
}

func runPGBaseline(cmd *cobra.Command, args []string) error {
	cfg, err := pgBaselineConfigFromFlags()
	if err != nil {
		return err
	}

	stats, err := pgbaseline.Run(cmd.Context(), cfg)
	if err != nil {
		return err
	}
	slog.Info("pg baseline complete",
		"tables", stats.TablesProcessed,
		"rows_written", stats.RowsWritten,
		"files_written", stats.FilesWritten,
		"anchor_lsn", stats.AnchorLSN,
		"delta_start_lsn", stats.DeltaStartLSN,
		"snapshot_time", stats.SnapshotTime)

	var uploaded int
	if pgbUpload != "" {
		// internal/baseline's Upload is source-agnostic (it walks the local
		// snapshot layout) — reused unchanged, exactly like 'bintrail baseline'.
		uploaded, err = baseline.Upload(cmd.Context(), pgbOutput, pgbUpload, pgbUploadRegion, pgbRetry)
		warn, fatal := baseline.SplitPointerError(err)
		if fatal != nil {
			return fmt.Errorf("S3 upload: %w", fatal)
		}
		if warn != nil {
			// See bintrail baseline: the snapshot is published (#2052).
			slog.Warn("S3 upload: snapshot published, but the newest-snapshot pointer was not updated; views files "+
				"that follow the newest snapshot read the previous one until the next upload", "error", warn)
		}
		slog.Info("pg baseline S3 upload complete", "files", uploaded, "destination", pgbUpload)
	}

	fmt.Printf("PostgreSQL baseline complete.\n")
	fmt.Printf("  tables     : %d\n", stats.TablesProcessed)
	fmt.Printf("  rows       : %d\n", stats.RowsWritten)
	fmt.Printf("  files      : %d\n", stats.FilesWritten)
	fmt.Printf("  anchor LSN : %d\n", stats.AnchorLSN)
	fmt.Printf("  delta start LSN: %d (embedded metadata; the safe replay floor — see MetaKeyLSN)\n", stats.DeltaStartLSN)
	fmt.Printf("  snapshot   : %s\n", stats.SnapshotTime.Format("2006-01-02T15:04:05Z07:00"))
	if pgbUpload != "" {
		fmt.Printf("  uploaded   : %d files → %s\n", uploaded, pgbUpload)
	}
	return nil
}
