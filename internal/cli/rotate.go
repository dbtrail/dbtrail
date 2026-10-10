package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/rotation"
)

var rotateCmd = &cobra.Command{
	Use:   "rotate",
	Short: "Drop old partitions and add replacement future ones",
	Long: `Manage the time-range partitions on the binlog_events table.

Old partitions are dropped based on the --retain threshold. For every partition
dropped, one new hourly partition is automatically added for the future so that
the total partition count stays constant. Use --add-future N to maintain a
declarative headroom of at least N future hourly partitions beyond the current
hour (top-up only; already-sufficient headroom is left alone). Use --no-replace
to suppress auto-replacement and only top up toward the --add-future target
(useful when storage is limited).

Examples:
  # Drop partitions older than 7 days (auto-adds 168 future hourly partitions)
  bintrail rotate --index-dsn "..." --retain 7d

  # Drop old partitions and maintain at least 3 future partitions of headroom
  bintrail rotate --index-dsn "..." --retain 7d --add-future 3

  # Only add new future partitions (no drops)
  bintrail rotate --index-dsn "..." --add-future 7

  # Drop without auto-replacing (pure drop, storage-conscious)
  bintrail rotate --index-dsn "..." --retain 7d --no-replace

  # Run as a daemon, rotating every hour
  bintrail rotate --index-dsn "..." --retain 7d --daemon

  # Run as a daemon with a custom interval
  bintrail rotate --index-dsn "..." --retain 7d --daemon --interval 6h`,
	RunE: runRotate,
}

var (
	rotIndexDSN           string
	rotRetain             string
	rotAddFuture          int
	rotNoReplace          bool
	rotArchiveDir         string
	rotArchiveCompression string
	rotBintrailID         string
	rotArchiveS3          string
	rotArchiveS3Region    string
	rotDaemon             bool
	rotInterval           string
	rotFormat             string
	rotRetry              bool
)

func init() {
	rotateCmd.Flags().StringVar(&rotIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	rotateCmd.Flags().StringVar(&rotRetain, "retain", "", "Drop partitions older than this duration (e.g. 7d, 24h)")
	rotateCmd.Flags().IntVar(&rotAddFuture, "add-future", 0, "Maintain at least N future hourly partitions beyond the current hour (declarative target; top-up only)")
	rotateCmd.Flags().BoolVar(&rotNoReplace, "no-replace", false, "Do not auto-add future partitions to replace dropped ones (only top up toward --add-future)")
	rotateCmd.Flags().StringVar(&rotArchiveDir, "archive-dir", "", "Directory to write Parquet archives before dropping partitions")
	rotateCmd.Flags().StringVar(&rotArchiveCompression, "archive-compression", "zstd", "Compression for archive Parquet files (zstd, snappy, gzip, none)")
	rotateCmd.Flags().StringVar(&rotBintrailID, "bintrail-id", "", "Server identity UUID for archive paths (bintrail_id=<uuid>/event_date=<date>/). When --archive-dir is set and this is omitted, the bintrail_id recorded in stream_state is used; an explicit value always wins")
	rotateCmd.Flags().StringVar(&rotArchiveS3, "archive-s3", "", "S3 destination URL to upload Parquet archives after writing (requires --archive-dir; e.g. s3://my-bucket/archives/)")
	rotateCmd.Flags().StringVar(&rotArchiveS3Region, "archive-s3-region", "", "AWS region for --archive-s3 (default: from AWS_REGION env var or ~/.aws/config)")
	rotateCmd.Flags().BoolVar(&rotDaemon, "daemon", false, "Run continuously, repeating rotation on the --interval schedule until SIGINT/SIGTERM")
	rotateCmd.Flags().StringVar(&rotInterval, "interval", "1h", "How often to run rotation in daemon mode (e.g. 1h, 30m)")
	rotateCmd.Flags().StringVar(&rotFormat, "format", "text", "Output format: text or json")
	rotateCmd.Flags().BoolVar(&rotRetry, "retry", false, "Skip archiving partitions whose Parquet file already exists and S3 uploads that already succeeded")
	_ = rotateCmd.MarkFlagRequired("index-dsn")
	BindCommandEnv(rotateCmd)
}

// errNoArchiveBintrailID is the precondition error from resolveArchiveBintrailID
// when no archive bintrail_id can be determined (no CLI flag, no stream_state id,
// no BINTRAIL_ID env). It is a PERMANENT misconfiguration that can never self-heal
// on retry, so daemon mode fails loud on it at startup (via errors.Is) rather than
// spinning silently while the index disk fills.
var errNoArchiveBintrailID = errors.New("--bintrail-id is required when --archive-dir is set: no bintrail_id is recorded in stream_state to fall back to")

// resolveArchiveBintrailID determines the bintrail_id used as the archive
// S3/Hive partition key. Precedence:
//
//  1. An explicitly CLI-typed --bintrail-id — the operator's per-invocation
//     intent. This preserves every existing CLI-typed invocation byte-for-byte.
//  2. The per-server bintrail_id recorded in stream_state (resolved at
//     stream/index time — the synthesized one for MariaDB, the
//     @@server_uuid-derived one for MySQL).
//  3. A BINTRAIL_ID environment value, as a last resort (with a loud warning).
//
// flagValue carries whatever bindCommandEnv left on --bintrail-id, which may be
// CLI-typed OR injected from BINTRAIL_ID — cobra cannot tell them apart, since
// env binding uses Flags().Set (marking the flag Changed just like a CLI value).
// We infer: a flagValue that differs from envValue (os.Getenv("BINTRAIL_ID"))
// was typed on the CLI. This distinction is the whole point — a single GLOBAL
// BINTRAIL_ID must NOT silently become the write key for every server, which
// would collapse multiple servers' archives into one bintrail_id=<id>/ prefix
// (the exact collision this guards against). It returns errNoArchiveBintrailID
// when nothing is available, so an archive run can never write to an empty
// bintrail_id= prefix.
func resolveArchiveBintrailID(ctx context.Context, db *sql.DB, flagValue, envValue string) (string, error) {
	// Tier 1: an explicitly CLI-typed flag (value differs from the env var).
	if flagValue != "" && flagValue != envValue {
		return flagValue, nil
	}

	// Tier 2: the per-server id resolved into stream_state.
	var id sql.NullString
	err := db.QueryRowContext(ctx, "SELECT bintrail_id FROM stream_state WHERE id = 1").Scan(&id)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("read bintrail_id from stream_state: %w", err)
	}
	if id.Valid && id.String != "" {
		// A non-empty flagValue at this point can only be env-derived (a CLI-typed
		// value would have won Tier 1). If it disagrees with the per-server id, the
		// env value is being overridden — surface it rather than silently honoring
		// stream_state, since the flag help promises an explicit value wins.
		if flagValue != "" && flagValue != id.String {
			slog.Warn("--bintrail-id matches BINTRAIL_ID and was treated as environment-derived; using the per-server stream_state bintrail_id instead. To force a value, make --bintrail-id differ from BINTRAIL_ID (or unset BINTRAIL_ID)",
				"requested", flagValue, "using", id.String)
		}
		return id.String, nil
	}

	// Tier 3: a BINTRAIL_ID env value, only when no per-server id exists. Warn —
	// reusing one global id across servers collides their archives in S3.
	if flagValue != "" {
		slog.Warn("archiving under the global BINTRAIL_ID environment value: no per-server bintrail_id is recorded in stream_state. If you capture more than one server, set a per-source BINTRAIL_ID or pass an explicit --bintrail-id, or their archives will collide under one prefix",
			"bintrail_id", flagValue)
		return flagValue, nil
	}
	return "", errNoArchiveBintrailID
}

func runRotate(cmd *cobra.Command, args []string) error {
	if !cliutil.IsValidOutputFormat(rotFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", rotFormat)
	}
	if rotRetain == "" && rotAddFuture == 0 {
		return fmt.Errorf("at least one of --retain or --add-future is required")
	}
	// NOTE: --bintrail-id is no longer required at parse time when archiving. When
	// it is omitted we fall back to the bintrail_id already resolved into
	// stream_state (resolveArchiveBintrailID, inside doRotation, after the DB is
	// open). An explicit --bintrail-id always wins. This needs the DB, so the
	// "neither flag nor resolved id" failure surfaces on the first rotation rather
	// than here.
	if rotArchiveS3 != "" && rotArchiveDir == "" {
		return fmt.Errorf("--archive-s3 requires --archive-dir")
	}
	if rotArchiveDir != "" {
		if err := baseline.ValidateCodec(rotArchiveCompression); err != nil {
			return fmt.Errorf("--archive-compression: %w", err)
		}
	}

	var retainDur time.Duration
	if rotRetain != "" {
		var err error
		retainDur, err = cliutil.ParseRetain(rotRetain)
		if err != nil {
			return fmt.Errorf("--retain: %w", err)
		}
	}

	cfg, err := mysql.ParseDSN(rotIndexDSN)
	if err != nil {
		return fmt.Errorf("invalid --index-dsn: %w", err)
	}
	dbName := cfg.DBName
	if dbName == "" {
		return fmt.Errorf("--index-dsn must include a database name (e.g. user:pass@tcp(host:3306)/binlog_index)")
	}

	if rotDaemon {
		if _, err := time.ParseDuration(rotInterval); err != nil {
			return fmt.Errorf("--interval: %w", err)
		}
	}

	doRotation := func(ctx context.Context) error {
		db, err := config.Connect(rotIndexDSN)
		if err != nil {
			return fmt.Errorf("failed to connect to index database: %w", err)
		}
		defer db.Close()
		if err := indexer.EnsureSchema(db); err != nil {
			return fmt.Errorf("schema migration: %w", err)
		}

		// Resolve the bintrail_id used as the archive S3/Hive partition key. An
		// explicit --bintrail-id wins; otherwise fall back to the id already
		// resolved into stream_state (so MariaDB sources — which have no
		// @@server_uuid and thus get a synthesized id — and MySQL alike namespace
		// their archives correctly without the operator re-typing the id).
		archiveBintrailID := rotBintrailID
		if rotArchiveDir != "" {
			archiveBintrailID, err = resolveArchiveBintrailID(ctx, db, rotBintrailID, os.Getenv("BINTRAIL_ID"))
			if err != nil {
				return err
			}
		}

		res, err := rotation.Perform(ctx, db, dbName, rotation.Options{
			RetainDur:          retainDur,
			RetainRaw:          rotRetain,
			AddFuture:          rotAddFuture,
			NoReplace:          rotNoReplace,
			ArchiveDir:         rotArchiveDir,
			ArchiveCompression: rotArchiveCompression,
			BintrailID:         archiveBintrailID,
			ArchiveS3:          rotArchiveS3,
			ArchiveS3Region:    rotArchiveS3Region,
			Retry:              rotRetry,
			Format:             rotFormat,
			// ProtectUnarchived stays false: the explicit rotate command drops
			// exactly what the operator asked for (the guard is built-in
			// rotation's, set by up's loop).
		})
		if err != nil {
			return err
		}
		if err := replacementsOwed(res, rotNoReplace); err != nil {
			return err
		}
		if rotFormat == "json" {
			return cliutil.OutputJSON(struct {
				PartitionsDropped int `json:"partitions_dropped"`
				PartitionsAdded   int `json:"partitions_added"`
			}{PartitionsDropped: res.Dropped, PartitionsAdded: res.Added})
		}
		return nil
	}

	if !rotDaemon {
		return doRotation(cmd.Context())
	}

	interval, _ := time.ParseDuration(rotInterval) // already validated above
	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("rotate daemon started", "interval", interval)
	if err := doRotation(ctx); err != nil && ctx.Err() == nil {
		// A permanent precondition error (no resolvable archive bintrail_id) will
		// never self-heal on the next tick, so spinning silently would hide a
		// misconfig while the index disk fills. Fail loud at startup. Transient
		// errors (DB blip, a not-yet-ready index DB) stay log-and-continue so the
		// daemon self-heals on the next tick.
		if errors.Is(err, errNoArchiveBintrailID) {
			return fmt.Errorf("rotate --daemon cannot start: %w", err)
		}
		slog.Error("rotation failed", "error", err)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			slog.Info("rotate daemon stopping")
			return nil
		case <-ticker.C:
			// Suppress JSON output in daemon mode — only the initial rotation outputs JSON.
			savedFmt := rotFormat
			rotFormat = "text"
			func() {
				defer func() { rotFormat = savedFmt }()
				if err := doRotation(ctx); err != nil && ctx.Err() == nil {
					slog.Error("rotation failed", "error", err)
				}
			}()
		}
	}
}

// replacementsOwed is the error of a run that dropped partitions and could
// not add their replacements because binlog_events was in use (#2280).
// Replacements are owed only by the run that dropped, so a later plain run
// would leave the window short for good: the run fails and names the
// command that adds them.
func replacementsOwed(res rotation.Result, noReplace bool) error {
	if res.AddSkipped == 0 || res.Dropped == 0 || noReplace {
		return nil
	}
	return fmt.Errorf("dropped %d partition(s) but could not add %d future partition(s): binlog_events was in use; run 'bintrail rotate --add-future %d' to add them",
		res.Dropped, res.AddSkipped, res.AddFutureTarget)
}
