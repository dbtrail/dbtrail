package cliapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/byos"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/serverid"
)

// ddlAutoSnapshot is the file-mode DDL hook's snapshot step. It uses
// metadata.TakeSnapshotExcludingInvalid — the stream DDL hook's degraded
// validation semantics (#1051) — so one no-PK / non-InnoDB table in scope no
// longer fails file-mode DDL handling (#1199), and it warns loudly about any
// exclusions (same contract as the stream hook in internal/streamrun). Every
// OTHER snapshot failure — source unreachable, index write error, nothing
// valid left to capture — still errors.
func ddlAutoSnapshot(sourceDB, indexDB *sql.DB, schemas []string) (metadata.SnapshotStats, error) {
	stats, err := metadata.TakeSnapshotExcludingInvalid(sourceDB, indexDB, schemas)
	if err != nil {
		return stats, err
	}
	if len(stats.ExcludedTables) > 0 {
		// #1051: prominent, so the operator learns these tables are invisible
		// to bintrail from the log rather than from a failed recovery attempt.
		slog.Warn("auto-snapshot EXCLUDED tables that are not InnoDB or lack an explicit primary key — "+
			"their row events are not captured and they are absent from this snapshot; "+
			"add a primary key (and use InnoDB) to capture them",
			"excluded_tables", strings.Join(stats.ExcludedTables, ", "),
			"snapshot_id", stats.SnapshotID)
	}
	return stats, nil
}

var indexCmd = &cobra.Command{
	Use:   "index",
	Short: "Parse binlog files and populate the index",
	Long: `Parses one or more MySQL ROW-format binlog files and writes every row event
into the binlog_events table with full before/after images.

If no schema snapshot exists, one is taken automatically using --source-dsn.
Files already marked 'completed' in index_state are skipped.`,
	RunE: runIndex,
}

var (
	idxIndexDSN        string
	idxSourceDSN       string
	idxBinlogDir       string
	idxFiles           string
	idxAll             bool
	idxBatchSize       int
	idxSchemas         string
	idxTables          string
	idxFormat          string
	idxSkipSourceCheck bool
)

func init() {
	indexCmd.Flags().StringVar(&idxIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	indexCmd.Flags().StringVar(&idxSourceDSN, "source-dsn", "", "DSN for the source MySQL server (required for validation and auto-snapshot)")
	indexCmd.Flags().StringVar(&idxBinlogDir, "binlog-dir", "", "Directory containing binlog files (required)")
	indexCmd.Flags().StringVar(&idxFiles, "files", "", "Comma-separated binlog filenames (e.g. binlog.000042,binlog.000043)")
	indexCmd.Flags().BoolVar(&idxAll, "all", false, "Index all binlog files found in --binlog-dir")
	indexCmd.Flags().IntVar(&idxBatchSize, "batch-size", 1000, indexer.BatchSizeHelp())
	indexCmd.Flags().StringVar(&idxSchemas, "schemas", "", "Only index events from these schemas (comma-separated)")
	indexCmd.Flags().StringVar(&idxTables, "tables", "", "Only index these tables (comma-separated, e.g. mydb.orders,mydb.items)")
	indexCmd.Flags().StringVar(&idxFormat, "format", "text", "Output format: text or json")
	indexCmd.Flags().BoolVar(&idxSkipSourceCheck, "skip-source-validation", false,
		"Index offline binlogs without --source-dsn, skipping the binlog_format/binlog_row_image/FK-cascade pre-flight (the per-row partial-image guard still applies)")
	_ = indexCmd.MarkFlagRequired("index-dsn")
	_ = indexCmd.MarkFlagRequired("binlog-dir")
	bindCommandEnv(indexCmd)

	rootCmd.AddCommand(indexCmd)
}

func runIndex(cmd *cobra.Command, args []string) error {
	if !cliutil.IsValidOutputFormat(idxFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", idxFormat)
	}
	if !idxAll && idxFiles == "" {
		return fmt.Errorf("either --files or --all must be specified")
	}
	// Without --source-dsn the binlog_format/binlog_row_image/FK-cascade
	// pre-flight cannot run; silently skipping it (the old behavior) let a
	// non-FULL source be indexed with NULL-filled partial images. Require an
	// explicit opt-out so offline binlog indexing stays possible while the skip
	// is never accidental (#493). The per-row partial-image guard in
	// internal/parser still fires even under the opt-out.
	if idxSourceDSN == "" && !idxSkipSourceCheck {
		return fmt.Errorf("--source-dsn is required for source validation; pass --skip-source-validation to index offline binlogs without it")
	}

	ctx := cmd.Context()

	// ── 1. Source server: validate binlog_row_image ───────────────────────────────────────
	var sourceDB *sql.DB
	if idxSourceDSN != "" {
		var err error
		sourceDB, err = config.Connect(idxSourceDSN)
		if err != nil {
			return fmt.Errorf("failed to connect to source MySQL: %w", err)
		}
		defer sourceDB.Close()

		if err := metadata.ValidateBinlogFormat(sourceDB); err != nil {
			return err
		}
		fmt.Println("Source: binlog_format=ROW \u2713")

		if err := metadata.ValidateBinlogRowImage(sourceDB); err != nil {
			return err
		}
		fmt.Println("Source: binlog_row_image=FULL \u2713")

		if err := metadata.ValidateNoFKCascades(sourceDB, cliutil.ParseSchemaList(idxSchemas)); err != nil {
			if !errors.Is(err, metadata.ErrFKCascadesFound) {
				return err // genuine query/connection failure: abort as before
			}
			slog.Warn("FK cascade constraints present on source; indexing will proceed, "+
				"but InnoDB executes cascades below the binlog (MySQL Bug #32506) so cascaded "+
				"child-row deletes are NOT captured \u2014 plain `recover` cannot restore them. "+
				"Reconstruct them with `bintrail recover-cascade`.",
				"detail", err.Error())
		} else {
			fmt.Println("Source: no FK cascades \u2713")
		}
	} else {
		slog.Warn("--skip-source-validation set: skipping binlog_format/binlog_row_image/FK-cascade pre-flight — the per-row partial-image guard still applies")
	}

	// ── 2. Index database connection ──────────────────────────────────────────
	indexDB, err := config.Connect(idxIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer indexDB.Close()

	if err := indexer.EnsureSchema(indexDB); err != nil {
		return fmt.Errorf("schema migration: %w", err)
	}

	// ── 3. Resolve server identity ────────────────────────────────────────────
	var bintrailID string
	if sourceDB != nil {
		var idErr error
		bintrailID, idErr = byos.ResolveServerIdentity(ctx, sourceDB, indexDB, idxSourceDSN)
		if idErr != nil {
			if errors.Is(idErr, serverid.ErrConflict) {
				return fmt.Errorf("cannot index: %w", idErr)
			}
			slog.Warn("server identity resolution failed; proceeding without bintrail_id", "error", idErr)
		} else {
			slog.Info("server identity resolved", "bintrail_id", bintrailID)
		}
	}
	// ── 4. Schema snapshot ───────────────────────────────────────────────
	resolver, err := metadata.EnsureResolver(indexDB, sourceDB, cliutil.ParseSchemaList(idxSchemas))
	if err != nil {
		return err
	}
	fmt.Printf("Snapshot: id=%d, tables=%d\n", resolver.SnapshotID(), resolver.TableCount())

	// ── 5. Filters ──────────────────────────────────────────────────────
	filters := cliutil.BuildIndexFilters(idxSchemas, idxTables)

	// ── 5. File list ──────────────────────────────────────────────────────────
	files, err := resolveFiles(idxBinlogDir, idxFiles, idxAll)
	if err != nil {
		return err
	}
	fmt.Printf("Files to process: %d\n\n", len(files))

	// ── 6. Index each file ────────────────────────────────────────────────────────────
	p := parser.New(idxBinlogDir, resolver, filters, nil)
	// Run-scoped skip tally (#1199): validation-excluded tables no longer fail
	// the file, so their skipped events need an aggregate end-of-run signal —
	// file mode has no capture ledger to persist to (empty stream_state IS the
	// no-capture marker), so the tally is summarized below instead.
	runSkips := parser.NewSkipCounters(nil)
	p.SetSkipCounters(runSkips)
	idx := indexer.New(indexDB, idxBatchSize)

	// DDL handler: auto-snapshot when --source-dsn is available; warn-only otherwise.
	// TRUNCATE does not change schema structure, so skip snapshot for it.
	schemas := cliutil.ParseSchemaList(idxSchemas)
	idx.SetOnDDL(func(ev parser.Event) error {
		if ev.DDLType == parser.DDLTruncateTable {
			slog.Info("DDL detected (no snapshot needed)",
				"file", ev.BinlogFile, "pos", ev.EndPos, "ddl_type", ev.DDLType, "query", ev.DDLQuery)
			if err := indexer.InsertSchemaChange(indexDB, ev, nil); err != nil {
				slog.Warn("failed to record schema change", "error", err)
			}
			return nil
		}

		if sourceDB == nil {
			slog.Warn("DDL detected but --source-dsn not provided; run `bintrail snapshot` if schema changed",
				"file", ev.BinlogFile, "pos", ev.EndPos, "ddl_type", ev.DDLType, "query", ev.DDLQuery)
			if err := indexer.InsertSchemaChange(indexDB, ev, nil); err != nil {
				slog.Warn("failed to record schema change", "error", err)
			}
			return nil
		}

		slog.Info("DDL detected — taking auto-snapshot",
			"file", ev.BinlogFile, "pos", ev.EndPos,
			"ddl_type", ev.DDLType, "schema", ev.Schema, "table", ev.Table)

		stats, snapErr := ddlAutoSnapshot(sourceDB, indexDB, schemas)
		var snapID *int
		if snapErr != nil {
			slog.Error("auto-snapshot after DDL failed; subsequent events may use stale schema",
				"error", snapErr, "ddl_type", ev.DDLType, "table", ev.Table)
		} else {
			snapID = &stats.SnapshotID
			newResolver, resolverErr := metadata.NewResolver(indexDB, stats.SnapshotID)
			if resolverErr != nil {
				slog.Warn("failed to load new resolver after DDL snapshot", "error", resolverErr)
			} else {
				p.SwapResolver(newResolver)
				slog.Info("auto-snapshot taken; resolver updated",
					"snapshot_id", stats.SnapshotID,
					"tables", stats.TableCount,
					"columns", stats.ColumnCount)
			}
		}

		if err := indexer.InsertSchemaChange(indexDB, ev, snapID); err != nil {
			slog.Warn("failed to record schema change", "error", err)
		}
		return nil
	})

	var totalEvents int64
	var failedFiles int
	var firstErr error
	for _, filename := range files {
		n, err := indexFile(ctx, p, idx, indexDB, idxBinlogDir, filename, bintrailID)
		totalEvents += n
		if err != nil {
			// Log and continue so --all still processes the remaining files;
			// the failure is surfaced as a non-zero exit after the loop (#652).
			slog.Error("indexing failed", "file", filename, "error", err)
			failedFiles++
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	slog.Info("indexing complete",
		"files_processed", len(files),
		"events_indexed", totalEvents,
		"failed_files", failedFiles)
	if total := runSkips.Total(); total > 0 {
		snap, _ := runSkips.Snapshot()
		slog.Warn("events were read from the binlogs but NOT indexed this run — a restore window over them is incomplete; "+
			"see the per-event WARNs above for each table (validation-excluded tables need a PK + InnoDB and a re-snapshot, or exclude them via --schemas/--tables)",
			"skipped_total", total,
			"skipped_by_reason", snap)
	}

	if idxFormat == "json" {
		if err := cliutil.OutputJSON(struct {
			FilesProcessed int   `json:"files_processed"`
			EventsIndexed  int64 `json:"events_indexed"`
			FailedFiles    int   `json:"failed_files"`
		}{
			FilesProcessed: len(files),
			EventsIndexed:  totalEvents,
			FailedFiles:    failedFiles,
		}); err != nil {
			return err
		}
	} else {
		fmt.Printf("\nTotal events indexed: %d\n", totalEvents)
		if failedFiles > 0 {
			fmt.Printf("WARNING: %d of %d file(s) failed to index; see logs above\n", failedFiles, len(files))
		}
	}

	// Fail loud: a file that could not be fully indexed (e.g. an event the
	// server rejects as over max_allowed_packet) must not exit 0 and read as
	// success to a cron/CI wrapper (#652). The per-file failure is already
	// recorded as status='failed' in index_state. The first failure stays in
	// the chain (%w) so a typed cause — the schema-drift, schema-gap or
	// partial-row-image guard, an index-side server error, an unreadable
	// binlog file (a stat/open failure other than not-found, which the loop
	// skips) — keeps its
	// usage-telemetry class instead of collapsing to "unknown" behind a fresh
	// summary error (#1503).
	if failedFiles > 0 {
		return indexFailureSummary(failedFiles, len(files), firstErr)
	}
	return nil
}

// indexFailureSummary is the non-zero-exit error for a partially failed run.
// Extracted so the one property that matters for usage telemetry — the first
// failure stays in the chain — has a unit test; a %v here would turn every
// typed cause back into "unknown".
func indexFailureSummary(failed, total int, first error) error {
	return fmt.Errorf("indexing finished with %d of %d file(s) failed; first failure: %w", failed, total, first)
}

// indexFile processes a single binlog file with full index_state tracking.
func indexFile(
	ctx context.Context,
	p *parser.Parser,
	idx *indexer.Indexer,
	indexDB *sql.DB,
	binlogDir, filename, bintrailID string,
) (int64, error) {
	// ── a. Skip already-completed files ─────────────────────────────────────────────
	status, err := getFileStatus(indexDB, filename)
	if err != nil {
		return 0, fmt.Errorf("failed to query index_state: %w", err)
	}
	if status == "completed" {
		fmt.Printf("[%s] already indexed \u2014 skipping\n", filename)
		return 0, nil
	}

	// ── b. Check file exists ────────────────────────────────────────────────────
	info, err := os.Stat(filepath.Join(binlogDir, filename))
	if err != nil {
		if os.IsNotExist(err) {
			slog.Warn("binlog file not found \u2014 skipping", "file", filename)
			return 0, nil
		}
		return 0, fmt.Errorf("stat %s: %w", filename, err)
	}
	fileSize := info.Size()

	// ── b. Mark in_progress ───────────────────────────────────────────────────
	if err := upsertFileState(indexDB, filename, "in_progress", fileSize, 0, 0, "", bintrailID); err != nil {
		return 0, fmt.Errorf("failed to mark in_progress: %w", err)
	}
	fmt.Printf("[%s] indexing...\n", filename)

	// ── c. Run parser + indexer concurrently ──────────────────────────────────────────────
	// Use a child context so we can cancel the parser if the indexer fails,
	// avoiding a goroutine leak and the associated channel deadlock.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	events := make(chan parser.Event, 1000)
	parseErrCh := make(chan error, 1) // buffered: goroutine never blocks on send

	go func() {
		defer close(events)
		parseErrCh <- p.ParseFile(ctx, filename, events)
	}()

	count, idxErr := idx.Run(ctx, events)
	if idxErr != nil {
		cancel() // tell the parser goroutine to stop
	}

	parseErr := <-parseErrCh // wait for parser to finish

	// ── e/f. Update index_state ──────────────────────────────────────────────────
	switch {
	case idxErr != nil:
		if stateErr := upsertFileState(indexDB, filename, "failed", fileSize, 0, count, idxErr.Error(), bintrailID); stateErr != nil {
			slog.Warn("failed to record failed state in index_state", "file", filename, "error", stateErr)
		}
		return count, idxErr

	case parseErr != nil && !errors.Is(parseErr, context.Canceled):
		if stateErr := upsertFileState(indexDB, filename, "failed", fileSize, 0, count, parseErr.Error(), bintrailID); stateErr != nil {
			slog.Warn("failed to record failed state in index_state", "file", filename, "error", stateErr)
		}
		return count, parseErr

	default:
		if err := upsertFileState(indexDB, filename, "completed", fileSize, fileSize, count, "", bintrailID); err != nil {
			slog.Warn("failed to mark file completed", "file", filename, "error", err)
		}
		fmt.Printf("[%s] done \u2014 %d events\n", filename, count)
		return count, nil
	}
}

// ─── index_state helpers ────────────────────────────────────────────────────────────────────

// getFileStatus returns the current status from index_state, or "" if no row exists.
func getFileStatus(db *sql.DB, filename string) (string, error) {
	var status string
	err := db.QueryRow("SELECT status FROM index_state WHERE binlog_file = ?", filename).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return status, err
}

// upsertFileState writes or updates an index_state row using INSERT … ON DUPLICATE KEY UPDATE.
// lastPos is the byte offset of the last processed position (0 = unknown/in-progress).
// eventsIndexed is the count of events written so far.
// errMsg is stored for failed status; pass "" otherwise.
// bintrailID is the resolved server identity; pass "" when unknown (stored as NULL).
func upsertFileState(db *sql.DB, filename, status string, fileSize, lastPos, eventsIndexed int64, errMsg, bintrailID string) error {
	var errMsgArg any
	if errMsg != "" {
		errMsgArg = errMsg
	}
	var bintrailIDArg any
	if bintrailID != "" {
		bintrailIDArg = bintrailID
	}

	switch status {
	case "in_progress":
		_, err := db.Exec(`
			INSERT INTO index_state
				(binlog_file, file_size, last_position, events_indexed, status, started_at, completed_at, error_message, bintrail_id)
			VALUES (?, ?, ?, ?, 'in_progress', UTC_TIMESTAMP(), NULL, NULL, ?)
			ON DUPLICATE KEY UPDATE
				file_size      = VALUES(file_size),
				last_position  = VALUES(last_position),
				events_indexed = VALUES(events_indexed),
				status         = 'in_progress',
				started_at     = UTC_TIMESTAMP(),
				completed_at   = NULL,
				error_message  = NULL,
				bintrail_id    = VALUES(bintrail_id)`,
			filename, fileSize, lastPos, eventsIndexed, bintrailIDArg)
		return err

	case "completed":
		// bintrail_id is preserved from the in_progress INSERT; this UPDATE intentionally
		// leaves it unchanged so re-indexing the same file retains the server identity.
		_, err := db.Exec(`
			UPDATE index_state
			SET last_position  = ?,
			    events_indexed = ?,
			    status         = 'completed',
			    completed_at   = UTC_TIMESTAMP(),
			    error_message  = NULL
			WHERE binlog_file = ?`,
			lastPos, eventsIndexed, filename)
		return err

	case "failed":
		// bintrail_id is preserved from the in_progress INSERT; this UPDATE intentionally
		// leaves it unchanged so re-indexing the same file retains the server identity.
		_, err := db.Exec(`
			UPDATE index_state
			SET last_position  = ?,
			    events_indexed = ?,
			    status         = 'failed',
			    completed_at   = UTC_TIMESTAMP(),
			    error_message  = ?
			WHERE binlog_file = ?`,
			lastPos, eventsIndexed, errMsgArg, filename)
		return err
	}
	return fmt.Errorf("upsertFileState: unknown status %q", status)
}

// ─── File discovery ───────────────────────────────────────────────────────────────────────

// binlogFileRe matches standard MySQL binlog filenames: any name ending in six
// or more decimal digits after a dot (e.g. binlog.000042, mysql-bin.000001).
var binlogFileRe = regexp.MustCompile(`\.\d{6,}$`)

// resolveFiles returns the list of binlog filenames to process.
func resolveFiles(binlogDir, filesStr string, all bool) ([]string, error) {
	if all {
		return findBinlogFiles(binlogDir)
	}
	var files []string
	for f := range strings.SplitSeq(filesStr, ",") {
		if f = strings.TrimSpace(f); f != "" {
			files = append(files, f)
		}
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("--files is empty; provide at least one filename")
	}
	return files, nil
}

// findBinlogFiles scans binlogDir and returns filenames that match the standard
// MySQL binlog naming pattern, sorted in ascending order.
func findBinlogFiles(binlogDir string) ([]string, error) {
	entries, err := os.ReadDir(binlogDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read binlog directory %q: %w", binlogDir, err)
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && binlogFileRe.MatchString(e.Name()) {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files) // ascending order = chronological for standard naming
	if len(files) == 0 {
		return nil, fmt.Errorf("no binlog files found in %q", binlogDir)
	}
	return files, nil
}
