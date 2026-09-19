package cliapp

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/streamdeps"
	"github.com/dbtrail/dbtrail/internal/streamrun"
)

var streamCmd = &cobra.Command{
	Use:   "stream",
	Short: "Index events from a live MySQL replication stream",
	Long: `Connects to a MySQL server as a replica over the replication protocol and
indexes binlog row events in real-time into binlog_events.

Unlike 'bintrail index', this command does not require access to binlog files
on disk and works with managed MySQL (RDS, Aurora, Cloud SQL).

Start position on the first run: by default bintrail auto-discovers the
source's current binlog position via SHOW BINARY LOG STATUS (falling back to
SHOW MASTER STATUS on pre-8.4 MySQL). Pass --start-file/--start-pos or
--start-gtid to override and start from a specific earlier position. On
subsequent runs the saved checkpoint is resumed automatically, even if
--start-file/--start-gtid are still present on the command line. This makes
re-running the same command idempotent.

Use --reset to clear the saved checkpoint and force a new start position:

  bintrail stream --reset --start-file mysql-bin.000500 ...

Without --reset, the checkpoint always wins (idempotent behavior is preserved).
Resetting to any position other than the saved checkpoint — later or earlier;
direction is not inferred — permanently skips every event between the discarded
checkpoint and the new start position, and the skipped range is durably
recorded as lost (surfaced by 'bintrail status' and its --fail-on-gap flag).

Gap detection: on restart, bintrail checks whether the source still has the
binlogs needed to resume from the checkpoint. If binlogs have been purged, it
auto-advances to the earliest available position and logs a warning. Use
--no-gap-fill to refuse to start when an unfillable gap is detected.

Important: configure binlog retention to at least 2 days
(binlog_expire_logs_seconds >= 172800) to give bintrail time to fill gaps.

Graceful shutdown: send SIGINT or SIGTERM to flush the current batch and write
a checkpoint before exiting.`,
	RunE: runStream,
}

var (
	strmIndexDSN              string
	strmSourceDSN             string
	strmFlavor                string
	strmServerID              uint32
	strmStartFile             string
	strmStartPos              uint32
	strmStartGTID             string
	strmBatchSize             int
	strmSchemas               string
	strmTables                string
	strmCheckpoint            int
	strmMetricsAddr           string
	strmMetricsScrapeInterval int
	strmSSLMode               string
	strmSSLCA                 string
	strmSSLCert               string
	strmSSLKey                string
	strmFormat                string
	strmReset                 bool
	strmNoGapFill             bool
	strmGapTimeout            int
)

func init() {
	streamCmd.Flags().StringVar(&strmIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	streamCmd.Flags().StringVar(&strmSourceDSN, "source-dsn", "", "DSN for the source MySQL server (required)")
	streamCmd.Flags().StringVar(&strmFlavor, "source-flavor", "mysql", "Source database flavor: mysql or mariadb (MariaDB source support is alpha)")
	streamCmd.Flags().Uint32Var(&strmServerID, "server-id", 0, "Unique replica server ID (required, must differ from all other servers)")
	streamCmd.Flags().StringVar(&strmStartFile, "start-file", "", "Initial binlog file (mutually exclusive with --start-gtid)")
	streamCmd.Flags().Uint32Var(&strmStartPos, "start-pos", 4, "Initial position within start file")
	streamCmd.Flags().StringVar(&strmStartGTID, "start-gtid", "", "Initial GTID set (mutually exclusive with --start-file)")
	streamCmd.Flags().IntVar(&strmBatchSize, "batch-size", 1000, indexer.BatchSizeHelp())
	streamCmd.Flags().StringVar(&strmSchemas, "schemas", "", "Only index events from these schemas (comma-separated)")
	streamCmd.Flags().StringVar(&strmTables, "tables", "", "Only index these tables (comma-separated, e.g. mydb.orders)")
	streamCmd.Flags().IntVar(&strmCheckpoint, "checkpoint", 10, "Checkpoint interval in seconds")
	streamCmd.Flags().StringVar(&strmMetricsAddr, "metrics-addr", "", "Address to expose Prometheus metrics (e.g. :9090); empty = disabled")
	streamCmd.Flags().IntVar(&strmMetricsScrapeInterval, "metrics-scrape-interval", 60, "How often (seconds) to refresh the bintrail_index_* gauges from a status snapshot")
	streamCmd.Flags().StringVar(&strmSSLMode, "ssl-mode", "preferred", "TLS mode for the source AND index connections: disabled, preferred, required, verify-ca, verify-identity")
	streamCmd.Flags().StringVar(&strmSSLCA, "ssl-ca", "", "Path to CA certificate file for TLS verification (omit to use system CAs)")
	streamCmd.Flags().StringVar(&strmSSLCert, "ssl-cert", "", "Path to client certificate file for mutual TLS")
	streamCmd.Flags().StringVar(&strmSSLKey, "ssl-key", "", "Path to client private key file for mutual TLS")
	streamCmd.Flags().StringVar(&strmFormat, "format", "text", "Output format: text or json")
	streamCmd.Flags().BoolVar(&strmReset, "reset", false, "Clear saved checkpoint before starting (forces use of --start-file/--start-gtid); the skipped range is recorded as permanently lost")
	streamCmd.Flags().BoolVar(&strmNoGapFill, "no-gap-fill", false, "Refuse to start if an unfillable binlog gap is detected (instead of auto-advancing past purged data)")
	streamCmd.Flags().IntVar(&strmGapTimeout, "gap-timeout", 30, "Timeout in seconds for the one-shot gap-detection queries run at startup (SHOW BINARY LOGS plus @@gtid_purged/@@gtid_executed on MySQL or BINLOG_GTID_POS/@@gtid_binlog_pos on MariaDB); raise on managed servers with many binlog files")
	streamCmd.Flags().DurationVar(&indexer.WriteTimeout, "write-timeout", indexer.DefaultWriteTimeout, "Deadline for each index write (batch INSERT, checkpoint, digest lookup). A mid-statement network stall surfaces as an error within this window instead of freezing the stream on kernel TCP retransmission (~13-16 min). Raise for very large batches over a slow link")
	_ = streamCmd.MarkFlagRequired("index-dsn")
	_ = streamCmd.MarkFlagRequired("source-dsn")
	_ = streamCmd.MarkFlagRequired("server-id")
	bindCommandEnv(streamCmd)

	rootCmd.AddCommand(streamCmd)
}

// streamConfigFromFlags snapshots the strm* package globals into a
// streamrun.Config. The globals remain the cobra flag targets (and up.go's
// populateStreamFlags fan-out target); this is the single seam where they
// become values, with the host-supplied Deps attached.
func streamConfigFromFlags() streamrun.Config {
	return streamrun.Config{
		IndexDSN:              strmIndexDSN,
		SourceDSN:             strmSourceDSN,
		Flavor:                strmFlavor,
		ServerID:              strmServerID,
		StartFile:             strmStartFile,
		StartPos:              strmStartPos,
		StartGTID:             strmStartGTID,
		BatchSize:             strmBatchSize,
		Schemas:               strmSchemas,
		Tables:                strmTables,
		Checkpoint:            strmCheckpoint,
		MetricsAddr:           strmMetricsAddr,
		MetricsScrapeInterval: strmMetricsScrapeInterval,
		SSLMode:               strmSSLMode,
		SSLCA:                 strmSSLCA,
		SSLCert:               strmSSLCert,
		SSLKey:                strmSSLKey,
		Format:                strmFormat,
		Reset:                 strmReset,
		NoGapFill:             strmNoGapFill,
		GapTimeout:            strmGapTimeout,
		Deps:                  streamdeps.Default(),
	}
}

// runStream is the `bintrail stream` entrypoint: it owns the PROCESS concerns
// (signal handling) and delegates the actual streaming to streamrun.One with a
// config snapshotted from the flags. The split exists so a supervisor can run
// several streamrun.One instances under its own lifecycle without inheriting
// per-process signal wiring.
func runStream(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	// Usage telemetry for a process that may live for months: Init's drain runs
	// once, at startup, so without this loop a daemon's beacons would spool and
	// age out undelivered. Own goroutine — never on the replication path. Also
	// covers `up`, which delegates here.
	go tel.Client().RunDaemon(ctx, cmd.Name())

	// Graceful shutdown on SIGINT/SIGTERM: cancel the context so streamrun.One
	// flushes its batch and writes a final checkpoint. (Installed before
	// startup rather than after StartSync as it historically was — a signal
	// during the connect/snapshot phase now also exits cleanly.)
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

	// Extension source jobs (ext.RegisterSourceJob) run alongside the stream
	// for its whole lifetime: same contract as rotation under `up` —
	// daemon-scoped secondary work, never fatal to capture, no-op in the stock
	// binary. This is the single wiring point for every daemon that ends up
	// here, `bintrail stream` itself and `bintrail up` (which delegates to
	// runStream after populateStreamFlags has copied its DSNs into the strm*
	// globals). ctx is the signal-bound child installed above, so the jobs
	// stop draining when the stream does.
	ext.RunSourceJobs(ctx, streamSourceJobInfo())

	return streamrun.One(ctx, streamConfigFromFlags())
}

// streamSourceJobInfo describes the stream's capture source for the extension
// source-job seam. Extracted from runStream so the mapping is unit-testable
// without starting a daemon (mirrors consoleapp's mainSourceJobInfo).
func streamSourceJobInfo() ext.SourceJobInfo {
	return ext.SourceJobInfo{
		SourceDSN: strmSourceDSN,
		IndexDSN:  strmIndexDSN,
		Flavor:    strmFlavor,
	}
}
