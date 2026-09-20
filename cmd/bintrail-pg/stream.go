package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/rotation"
)

var streamCmd = &cobra.Command{
	Use:   "stream",
	Short: "Index changes from a live PostgreSQL logical-replication stream",
	Long: `Connects to a PostgreSQL server over the logical-replication protocol (pgoutput)
and indexes every row change in real-time into the MySQL binlog_events index.

PostgreSQL requires two connections, which cannot be the same:

  --repl-dsn    a REPLICATION connection (the connection string must include
                replication=database); used for the WAL stream itself. A
                replication connection runs in walsender mode and cannot execute
                ordinary SQL.
  --query-dsn   an ordinary connection; used for primary-key catalog lookups and
                slot/publication validation.

Prerequisites on the source (validated at startup, fails loud otherwise):
  - wal_level = logical
  - every replicated table at REPLICA IDENTITY FULL (so the WAL carries the full
    before-image and de-TOASTed unchanged values — without it recovery silently
    loses columns)
  - a PUBLICATION covering the tables you want indexed (--publication)

The replication slot (--slot) is created on first run if absent and reused on
restart. The durable checkpoint is the last committed LSN, persisted to
stream_state; on restart the stream resumes from it automatically, so re-running
the same command is idempotent. --start-lsn only takes effect on the very first
run (before any checkpoint exists); thereafter the checkpoint always wins.

Re-seeding: once a checkpoint and slot exist, PostgreSQL resumes from the slot's
position — --start-lsn cannot rewind to an earlier point (the older WAL may have
been reclaimed). To force a fresh start, run 'bintrail-pg reset' (drops the slot
and clears the stream_state checkpoint columns), then re-run. Discarding a real
checkpoint that way is durably recorded as a permanent continuity loss — the
recreated slot skips whatever the old one had not yet streamed — and a prior
loss record is never erased by a reset (a real-checkpoint reset replaces its
detail with the reset's own).

Graceful shutdown: send SIGINT or SIGTERM to flush the current batch and write a
final checkpoint before exiting.`,
	RunE: runPGStream,
}

var (
	pgIndexDSN    string
	pgReplDSN     string
	pgQueryDSN    string
	pgSlot        string
	pgPublication string
	pgServerID    uint32
	pgStartLSN    string
	pgSchemas     string
	pgTables      string
	pgBatchSize   int
	pgCheckpoint  int
	pgPartitions  int

	pgRotateRetain    string
	pgRotateInterval  string
	pgRotateAddFuture int
)

func init() {
	streamCmd.Flags().StringVar(&pgIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	streamCmd.Flags().StringVar(&pgReplDSN, "repl-dsn", "", "PostgreSQL REPLICATION connection string, must include replication=database (required; env BINTRAIL_PG_REPL_DSN)")
	streamCmd.Flags().StringVar(&pgQueryDSN, "query-dsn", "", "PostgreSQL ordinary connection string for catalog/PK lookups (required; env BINTRAIL_PG_QUERY_DSN)")
	streamCmd.Flags().StringVar(&pgSlot, "slot", "", "Logical replication slot name; created if absent (required; env BINTRAIL_PG_SLOT)")
	streamCmd.Flags().StringVar(&pgPublication, "publication", "", "PostgreSQL publication name covering the tables to index (required; env BINTRAIL_PG_PUBLICATION)")
	streamCmd.Flags().Uint32Var(&pgServerID, "server-id", 0, "Identifier recorded in stream_state (required, must differ from all other sources)")
	streamCmd.Flags().StringVar(&pgStartLSN, "start-lsn", "", "Explicit start LSN (e.g. 0/1A2B3C4); first run only, ignored once a checkpoint exists (env BINTRAIL_PG_START_LSN)")
	streamCmd.Flags().StringVar(&pgSchemas, "schemas", "", "Only index changes from these schemas (comma-separated)")
	streamCmd.Flags().StringVar(&pgTables, "tables", "", "Only index these tables (comma-separated, e.g. public.orders)")
	streamCmd.Flags().IntVar(&pgBatchSize, "batch-size", 1000, indexer.BatchSizeHelp())
	streamCmd.Flags().DurationVar(&indexer.WriteTimeout, "write-timeout", indexer.DefaultWriteTimeout, "Deadline for each index write (batch INSERT, checkpoint, schema snapshot). A mid-statement network stall surfaces as an error within this window instead of freezing the stream on kernel TCP retransmission (~13-16 min). Raise for very large batches over a slow link")
	streamCmd.Flags().IntVar(&pgCheckpoint, "checkpoint", 5, "Checkpoint interval in seconds")
	streamCmd.Flags().IntVar(&pgPartitions, "partitions", 48, "binlog_events partitions for the one-time index bootstrap")
	// Built-in index rotation (mirrors the core `up`/`watch` daemons). Because
	// bintrail-pg has no `up` command, `stream` is a PostgreSQL install's only
	// long-running daemon and must keep the index bounded itself (#951). Names,
	// defaults, and env vars (BINTRAIL_ROTATE_*, already in cli.EnvBindings) match
	// `up` so operators tune rotation identically across binaries.
	streamCmd.Flags().StringVar(&pgRotateRetain, "rotate-retain", indexer.DefaultRotateRetain, "Built-in rotation: drop index partitions older than this (Nd/Nh; \"off\" disables)")
	streamCmd.Flags().StringVar(&pgRotateInterval, "rotate-interval", "1h", "Built-in rotation: how often to run a rotation cycle")
	streamCmd.Flags().IntVar(&pgRotateAddFuture, "rotate-add-future", 3, "Built-in rotation: keep at least N future hourly partitions ready")
	// index-dsn and server-id live in cli.EnvBindings, so BindCommandEnv both
	// loads the env file (.bintrail.env) AND sets these flags from
	// BINTRAIL_INDEX_DSN/BINTRAIL_SERVER_ID — which marks them Changed, so
	// MarkFlagRequired is satisfied by env too. The PostgreSQL-specific flags
	// are NOT in EnvBindings; their BINTRAIL_PG_* fallback is applied in
	// runPGStream (the env file is already loaded by the time RunE runs).
	_ = streamCmd.MarkFlagRequired("index-dsn")
	_ = streamCmd.MarkFlagRequired("server-id")
	cli.BindCommandEnv(streamCmd)

	rootCmd.AddCommand(streamCmd)
}

// pgStreamConfigFromFlags applies the BINTRAIL_PG_* env fallback, validates the
// PostgreSQL-specific required settings, parses --start-lsn, and snapshots the
// pg* package globals into a pgstreamrun.Config. It is the single pure seam
// (returning an error rather than calling os.Exit) so the wiring is unit-tested
// without a live PostgreSQL — the analog of cmd/bintrail/stream.go's
// streamConfigFromFlags, which is pure because BindCommandEnv handles its env.
func pgStreamConfigFromFlags() (pgstreamrun.Config, error) {
	// BINTRAIL_PG_* fallback for the flags outside cli.EnvBindings. The env file
	// was already loaded into os env by cli.BindCommandEnv (envOnce) at init, so
	// these reads see both the shell environment and .bintrail.env values. A CLI
	// flag (non-empty) always wins over env.
	applyEnvFallback(&pgReplDSN, "BINTRAIL_PG_REPL_DSN")
	applyEnvFallback(&pgQueryDSN, "BINTRAIL_PG_QUERY_DSN")
	applyEnvFallback(&pgSlot, "BINTRAIL_PG_SLOT")
	applyEnvFallback(&pgPublication, "BINTRAIL_PG_PUBLICATION")
	applyEnvFallback(&pgStartLSN, "BINTRAIL_PG_START_LSN")

	// Validate the PG-specific required values here (they cannot use
	// MarkFlagRequired because that ignores the env-only path above).
	var missing []string
	if pgReplDSN == "" {
		missing = append(missing, "--repl-dsn (or BINTRAIL_PG_REPL_DSN)")
	}
	if pgQueryDSN == "" {
		missing = append(missing, "--query-dsn (or BINTRAIL_PG_QUERY_DSN)")
	}
	if pgSlot == "" {
		missing = append(missing, "--slot (or BINTRAIL_PG_SLOT)")
	}
	if pgPublication == "" {
		missing = append(missing, "--publication (or BINTRAIL_PG_PUBLICATION)")
	}
	if len(missing) > 0 {
		return pgstreamrun.Config{}, fmt.Errorf("missing required PostgreSQL connection settings: %s", strings.Join(missing, ", "))
	}

	var startLSN uint64
	if pgStartLSN != "" {
		lsn, err := pglogrepl.ParseLSN(pgStartLSN)
		if err != nil {
			return pgstreamrun.Config{}, fmt.Errorf("invalid --start-lsn %q: %w", pgStartLSN, err)
		}
		startLSN = uint64(lsn)
	}

	return pgstreamrun.Config{
		IndexDSN:    pgIndexDSN,
		ReplDSN:     pgReplDSN,
		QueryDSN:    pgQueryDSN,
		SlotName:    pgSlot,
		Publication: pgPublication,
		ServerID:    pgServerID,
		StartLSN:    startLSN,
		Schemas:     pgSchemas,
		Tables:      pgTables,
		BatchSize:   pgBatchSize,
		Partitions:  pgPartitions,
		Checkpoint:  time.Duration(pgCheckpoint) * time.Second,
	}, nil
}

// runPGStream is the `bintrail-pg stream` entrypoint: it owns the PROCESS
// concerns (signal handling) and delegates the config assembly to
// pgStreamConfigFromFlags and the streaming to pgstreamrun.One, mirroring
// cmd/bintrail/stream.go's split for the MySQL path.
func runPGStream(cmd *cobra.Command, args []string) error {
	cfg, err := pgStreamConfigFromFlags()
	if err != nil {
		return err
	}

	// Parse the built-in rotation settings before opening any connection so a
	// typo fails fast. `explicit` (was --rotate-retain set on the CLI or via
	// BINTRAIL_ROTATE_RETAIN?) drives the upgrade guard: an existing PG-beta
	// index that predates built-in rotation has piled everything into p_future,
	// and when this version first starts rotating at the 30d default the guard is
	// the only thing preventing a surprise drop of deep history — it holds drops
	// (still tops up future partitions) until the operator chooses a retention.
	rotSettings, err := rotation.ParseSettings(pgRotateRetain, pgRotateInterval, pgRotateAddFuture,
		cmd.Flags().Changed("rotate-retain"))
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	// Usage telemetry for a months-lived process: Init's drain runs once, so
	// without this loop a daemon's beacons would never be delivered. Own
	// goroutine — never on the replication path.
	go tel.Client().RunDaemon(ctx, cmd.Name())

	// Graceful shutdown on SIGINT/SIGTERM: cancel the context so pgstreamrun.One
	// flushes its batch and writes a final checkpoint before returning.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		select {
		case sig := <-sigCh:
			slog.Info("received signal — shutting down gracefully", "signal", sig.String())
			cancel()
		case <-ctx.Done():
		}
	}()

	// Built-in index rotation on the daemon lifecycle. Started on the same
	// signal-cancellable ctx as the stream (so SIGINT/SIGTERM stops both) right
	// before the blocking capture call. The loop only touches the MySQL index
	// (source-agnostic), reads its settings once, and never fails the stream —
	// rotation failures are logged and self-heal on the next tick. It is
	// drop-only: a single static target with no per-source S3 archive config,
	// mirroring core `up`; the standalone `bintrail-pg rotate --archive-s3`
	// covers the archive-then-drop path.
	//
	// On a brand-new install the first cycle can briefly race pgstreamrun.One's
	// index bootstrap (table not yet created); that cycle just logs a transient
	// failure and self-heals on the next tick, well within the 48 hours of
	// partitions the bootstrap lays down.
	rotation.StartLoop(ctx,
		func() rotation.Settings { return rotSettings },
		func() []rotation.RotateTarget { return []rotation.RotateTarget{{DSN: cfg.IndexDSN}} })

	return pgstreamrun.One(ctx, cfg)
}

// applyEnvFallback sets *dst from the named env var when *dst is empty (i.e. the
// flag was not given on the command line). A CLI-provided value always wins.
func applyEnvFallback(dst *string, envVar string) {
	if *dst == "" {
		if v := os.Getenv(envVar); v != "" {
			*dst = v
		}
	}
}
