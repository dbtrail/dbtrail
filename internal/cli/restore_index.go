package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/storage"
)

var restoreIndexCmd = &cobra.Command{
	Use:   "restore-index",
	Short: "Rebuild a lost index database from the Parquet archive tier",
	Long: `Turns the archive tier back into a working index: the recovery story
for the one stateful component that had none ("who backs up the backup"):

  1. Refuses an index that already holds state (events, a stream position,
     or schema snapshots); restore-index is for a FRESH index: mixing a
     partial restore into surviving state creates positions nothing can
     reason about (a surviving stream_state row would make the restarted
     stream resume a stale position and fake continuity across the hole).
  2. Creates the index schema (the same table set as 'bintrail init';
     pass --encrypt if the lost index was encrypted; parity is NOT
     inferred).
  3. Scans the Hive archive layout (--archive-dir or --archive-s3), then
     re-partitions binlog_events to cover exactly the archived hours plus
     a forward horizon (one ALTER on the empty table, instant), and
     bulk-loads every archived partition back, rebuilding archive_state
     from the scan (it is a rebuildable cache by design).
  4. Restores schema_snapshots and server identity from the index-meta
     sidecar that rotation persists alongside the archives; when present
     and readable.
  5. Reports what was and was NOT recovered, and the next steps. A failed
     file leaves the index PARTIAL: the report says so, and a retry needs
     a fresh (dropped and recreated) database.

Deliberately NOT recovered (and never persisted): stream_state and
index_state; a replication position that survived an index loss is stale,
and resuming from it would fake continuity. Restart the stream cleanly; the
continuity verdict then reports the seam honestly instead of pretending.

Example:
  bintrail restore-index \
    --index-dsn "root:pw@tcp(127.0.0.1:3306)/bintrail_index" \
    --archive-s3 s3://backups/bintrail --region us-east-1`,
	RunE: runRestoreIndex,
}

var (
	riIndexDSN   string
	riArchiveDir string
	riArchiveS3  string
	riRegion     string
	riBatch      int
	riPartitions int
	riEncrypt    bool
	riFormat     string
)

func init() {
	f := restoreIndexCmd.Flags()
	f.StringVar(&riIndexDSN, "index-dsn", "", "DSN of the FRESH index MySQL database to rebuild into (required; refused if it already holds events)")
	f.StringVar(&riArchiveDir, "archive-dir", "", "Local root of the Hive archive layout")
	f.StringVar(&riArchiveS3, "archive-s3", "", "S3 URL of the archive layout (s3://bucket/prefix)")
	f.StringVar(&riRegion, "region", "", "AWS region for --archive-s3")
	f.IntVar(&riBatch, "batch-size", 5000, "Rows per INSERT batch while loading")
	f.IntVar(&riPartitions, "partitions", 48, "Forward partition horizon to create beyond the archived hours (same default as init)")
	f.BoolVar(&riEncrypt, "encrypt", false, "Create binlog_events with InnoDB tablespace encryption (pass it if the lost index was encrypted; parity is not inferred)")
	f.StringVar(&riFormat, "format", "text", "Output format: text or json")
	_ = restoreIndexCmd.MarkFlagRequired("index-dsn")
	BindCommandEnv(restoreIndexCmd)
}

// restoreIndexReport is the honest inventory of a rebuild: what came back,
// what could not, and what the operator must do next.
type restoreIndexReport struct {
	EventsLoaded int64    `json:"events_loaded"`
	FilesLoaded  int      `json:"files_loaded"`
	FailedFiles  []string `json:"failed_files,omitempty"`
	// PartialRows counts rows that ARE in binlog_events from files that then
	// FAILED mid-load (each batch commits independently) — the inventory
	// must not undercount actual index contents, and their presence is what
	// makes a retry need a fresh database.
	PartialRows       int64 `json:"partial_rows_from_failed_files,omitempty"`
	PartitionsCreated int   `json:"partitions_created"`
	ArchiveStateRows  int   `json:"archive_state_rows"`
	// StateRowFailures: the events LOADED but the archive_state row could
	// not be recorded — a rebuildable-cache failure `archive reconcile
	// --repair` fixes, kept apart from FailedFiles so the exit message
	// cannot claim data "failed to load" when it fully did.
	StateRowFailures  []string `json:"archive_state_failures,omitempty"`
	SnapshotsRestored int64    `json:"snapshots_restored"`
	ServersRestored   int64    `json:"servers_restored"`
	SidecarFound      bool     `json:"sidecar_found"`
	// SidecarWarnings surfaces unreadable/undownloadable sidecars in the
	// machine-readable report — a stderr-only warning is invisible to the
	// JSON automation path, and a swallowed one would let the report assert
	// a false cause ("archives predate #1196").
	SidecarWarnings []string `json:"sidecar_warnings,omitempty"`
	// NotRecovered lists state this rebuild cannot bring back — absence of
	// an entry is a recovery claim, so unknowns are listed, never omitted.
	NotRecovered []string `json:"not_recovered"`
	NextSteps    []string `json:"next_steps"`
}

// ExitError is the single exit decision for both output formats. Load
// failures and archive_state failures are named separately — the latter is
// repairable in place and must not read as lost data.
func (r *restoreIndexReport) ExitError() error {
	if len(r.FailedFiles) == 0 && len(r.StateRowFailures) == 0 {
		return nil
	}
	var parts []string
	if len(r.FailedFiles) > 0 {
		msg := fmt.Sprintf("%d archive file(s) failed to load (%s)", len(r.FailedFiles), strings.Join(r.FailedFiles, ", "))
		if r.PartialRows > 0 {
			msg += fmt.Sprintf("; %d partially-loaded row(s) remain; retry needs a fresh database", r.PartialRows)
		}
		parts = append(parts, msg)
	}
	if len(r.StateRowFailures) > 0 {
		parts = append(parts, fmt.Sprintf("%d archive_state row(s) not recorded (events ARE loaded; run `bintrail archive reconcile --repair`): %s",
			len(r.StateRowFailures), strings.Join(r.StateRowFailures, ", ")))
	}
	return fmt.Errorf("restore-index: %s", strings.Join(parts, "; "))
}

func runRestoreIndex(cmd *cobra.Command, args []string) error {
	if riFormat != "text" && riFormat != "json" {
		return fmt.Errorf("invalid --format %q; must be text or json", riFormat)
	}
	if (riArchiveDir == "") == (riArchiveS3 == "") {
		return fmt.Errorf("exactly one of --archive-dir or --archive-s3 is required")
	}
	cfg, err := drivermysql.ParseDSN(riIndexDSN)
	if err != nil {
		return fmt.Errorf("invalid --index-dsn: %w", err)
	}
	dbName := cfg.DBName
	if dbName == "" {
		return fmt.Errorf("--index-dsn must include a database name")
	}
	ctx := cmd.Context()
	db, err := config.Connect(riIndexDSN)
	if err != nil {
		return fmt.Errorf("connect to index: %w", err)
	}
	defer db.Close()

	if err := restoreIndexTargetEmpty(ctx, db, dbName); err != nil {
		return err
	}
	if err := indexer.CreateIndexTables(ctx, db, riPartitions, riEncrypt, nil); err != nil {
		return err
	}
	// EnsureSchema covers the one guard hole CreateIndexTables leaves: a
	// database initialized by an OLDER `bintrail init` (empty of state, so
	// the guard passes) whose CREATE IF NOT EXISTS no-ops against the old
	// definition — without the migration every 18-column INSERT would fail.
	if err := indexer.EnsureSchema(db); err != nil {
		return fmt.Errorf("migrate index schema: %w", err)
	}

	// ── Scan the archive layout ────────────────────────────────────────────
	var files []archive.ScannedFile
	if riArchiveDir != "" {
		files, err = scanLocalArchive(riArchiveDir)
	} else {
		files, err = scanS3Archive(ctx, riArchiveS3, riRegion, false)
	}
	if err != nil {
		return fmt.Errorf("scan archives: %w", err)
	}
	if len(files) == 0 {
		return fmt.Errorf("no archive files found under the given location; nothing to restore")
	}

	// ── Re-partition the empty table to cover the archived hours ──────────
	hours := map[time.Time]bool{}
	ids := map[string]bool{}
	for _, f := range files {
		ids[f.BintrailID] = true
		if d, ok := indexer.PartitionDate(f.PartitionName); ok {
			hours[d] = true
		}
	}
	// MySQL caps a table at 8,192 partitions — ~341 days of hourly archives.
	// Fail actionably up front rather than mid-ALTER with ER 1499.
	if len(hours)+riPartitions+1 > 8192 {
		return fmt.Errorf("the archive tier spans %d hourly partitions; with the +%d horizon that exceeds MySQL's 8192-partition limit; restore a bounded window by pointing --archive-dir/--archive-s3 at a subset, or restore in stages", len(hours), riPartitions)
	}
	partSQL, partCount := buildRestorePartitionSQL(dbName, hours, time.Now().UTC(), riPartitions)
	if _, err := db.ExecContext(ctx, partSQL); err != nil {
		return fmt.Errorf("re-partition binlog_events: %w", err)
	}

	report := &restoreIndexReport{PartitionsCreated: partCount}

	// ── Load every archived partition, rebuilding archive_state as we go ──
	var s3c *s3.Client
	if riArchiveS3 != "" {
		if s3c, err = storage.NewS3Client(ctx, riRegion); err != nil {
			return fmt.Errorf("init S3 client: %w", err)
		}
	}
	for _, f := range files {
		path := f.LocalPath
		if f.Backend == archive.BackendS3 {
			path, err = downloadS3ToTemp(ctx, s3c, f.S3Bucket, f.S3Key)
			if err != nil {
				report.FailedFiles = append(report.FailedFiles, "s3://"+f.S3Bucket+"/"+f.S3Key+": "+err.Error())
				continue
			}
		}
		n, lerr := archive.RestorePartition(ctx, db, path, riBatch)
		if f.Backend == archive.BackendS3 {
			os.Remove(path)
		}
		if lerr != nil {
			// Batches already flushed ARE in binlog_events: count them, name
			// them — an inventory that undercounts actual index contents is
			// the overclaim invariant inverted, and those rows are why a
			// retry needs a fresh database.
			report.PartialRows += n
			report.FailedFiles = append(report.FailedFiles,
				fmt.Sprintf("%s: %v (after %d row(s) already inserted)", f.PartitionName, lerr, n))
			continue
		}
		report.EventsLoaded += n
		report.FilesLoaded++
		if err := recordRestoredArchive(ctx, db, f, n); err != nil {
			report.StateRowFailures = append(report.StateRowFailures, f.PartitionName+": "+err.Error())
			continue
		}
		report.ArchiveStateRows++
		if riFormat != "json" {
			fmt.Printf("loaded %s (%d rows)\n", f.PartitionName, n)
		}
	}

	// ── Sidecar: schema snapshots + server identity ────────────────────────
	var sidecarErr error
	m, sidecarWarnings := newestSidecar(ctx, s3c, ids)
	report.SidecarWarnings = sidecarWarnings
	if m != nil {
		report.SidecarFound = true
		var snaps, servers int64
		snaps, servers, sidecarErr = archive.RestoreMetaSidecar(ctx, db, m)
		report.SnapshotsRestored, report.ServersRestored = snaps, servers
		if sidecarErr != nil {
			report.FailedFiles = append(report.FailedFiles, "index-meta sidecar: "+sidecarErr.Error())
		}
	}

	report.NotRecovered = append(report.NotRecovered,
		"stream_state (replication position; deliberately never persisted: resuming a stale position would fake continuity)",
		"index_state (per-file indexing ledger)")
	// Gated on the RESTORE succeeding, not just the sidecar existing —
	// "absence of an entry is a recovery claim", and a found-but-failed
	// sidecar recovered nothing (the restore is transactional).
	if !report.SidecarFound || sidecarErr != nil {
		reason := "no index-meta sidecar found; archives predate #1196, or the sidecar was unreadable (see sidecar_warnings)"
		if sidecarErr != nil {
			reason = "the sidecar restore failed (see failed_files)"
		}
		report.NotRecovered = append(report.NotRecovered,
			"schema_snapshots + server identity ("+reason+")")
		report.NextSteps = append(report.NextSteps, "run `bintrail snapshot` against the source to re-capture table schemas")
	}
	if len(report.FailedFiles) > 0 {
		report.NextSteps = append(report.NextSteps,
			"this index is PARTIAL (failed files above; already-flushed batches remain loaded); to retry, drop and recreate the database, then re-run restore-index")
	}
	report.NextSteps = append(report.NextSteps,
		"restart the stream (`bintrail stream` / `bintrail-console watch`) from a fresh position; restarting cleanly avoids FAKING continuity across the hole; the missing window shows up as missing restore coverage, not as a gap_lost verdict",
		"run `bintrail archive reconcile --archive-dir/--archive-s3 ...` to double-check archive_state against the layout")

	exitErr := report.ExitError()
	if riFormat == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			return errors.Join(err, exitErr)
		}
	} else {
		writeRestoreIndexText(report)
	}
	cmd.SilenceUsage = true
	return exitErr
}

// restoreIndexTargetEmpty refuses an index that already holds STATE — not
// just events (the drill guard's sibling). A surviving stream_state row is
// the dangerous one: partitions can rotate away leaving binlog_events empty
// while the position survives, and a restarted stream would then RESUME the
// pre-loss position and fake continuity across the hole — the exact thing
// this command's report promises cannot happen. Surviving schema_snapshots
// would collide with the sidecar restore. Absent tables are fine (a truly
// fresh database).
func restoreIndexTargetEmpty(ctx context.Context, db *sql.DB, dbName string) error {
	for _, table := range []string{"binlog_events", "stream_state", "schema_snapshots"} {
		var exists int
		if err := db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM information_schema.TABLES
			WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`, dbName, table).Scan(&exists); err != nil {
			return fmt.Errorf("probe index: %w", err)
		}
		if exists == 0 {
			continue
		}
		var one int
		err := db.QueryRowContext(ctx, "SELECT 1 FROM `"+table+"` LIMIT 1").Scan(&one)
		switch {
		case err == sql.ErrNoRows:
		case err != nil:
			return fmt.Errorf("probe %s: %w", table, err)
		default:
			return fmt.Errorf("the index already holds state (%s is not empty); restore-index only rebuilds a FRESH index; point --index-dsn at a new, empty database (a previous failed restore also leaves state: drop and recreate the database to retry)", table)
		}
	}
	return nil
}

// buildRestorePartitionSQL builds the single ALTER that re-partitions the
// EMPTY binlog_events to exactly the archived hours plus a forward horizon —
// instant on an empty table, and it avoids the fragile arithmetic of
// reorganizing partitions backwards. Returns the statement and the number of
// named partitions (p_future excluded).
func buildRestorePartitionSQL(dbName string, archiveHours map[time.Time]bool, now time.Time, horizon int) (string, int) {
	all := map[time.Time]bool{}
	for h := range archiveHours {
		all[h.UTC().Truncate(time.Hour)] = true
	}
	start := now.Truncate(time.Hour)
	for i := 0; i < horizon; i++ {
		all[start.Add(time.Duration(i)*time.Hour)] = true
	}
	hours := make([]time.Time, 0, len(all))
	for h := range all {
		hours = append(hours, h)
	}
	sort.Slice(hours, func(i, j int) bool { return hours[i].Before(hours[j]) })
	defs := make([]string, 0, len(hours)+1)
	for _, h := range hours {
		defs = append(defs, fmt.Sprintf(
			"    PARTITION p_%s VALUES LESS THAN (TO_SECONDS('%s'))",
			h.Format("2006010215"), h.Add(time.Hour).Format("2006-01-02 15:04:05")))
	}
	defs = append(defs, "    PARTITION p_future VALUES LESS THAN MAXVALUE")
	return fmt.Sprintf("ALTER TABLE `%s`.`binlog_events` PARTITION BY RANGE (TO_SECONDS(event_timestamp)) (\n%s\n)",
		dbName, strings.Join(defs, ",\n")), len(hours)
}

// recordRestoredArchive rebuilds this file's archive_state row (a rebuildable
// cache, #392) — rotation's sibling upsert (NOT the same shape: rotation
// stamps s3_uploaded_at in a separate post-upload UPDATE and records
// min/max_event_ts), plus the s3_uploaded_at stamp the reconcile invariant
// requires: a confirmed S3 object must be stamped or rotate refuses drops
// forever. min/max_event_ts stay NULL permanently — the scan does not read
// row content, and no current command backfills them; the planner falls
// back to the hour label.
func recordRestoredArchive(ctx context.Context, db *sql.DB, f archive.ScannedFile, rows int64) error {
	var localPath, bucket, key, uploadedAt any
	if f.Backend == archive.BackendS3 {
		bucket, key, uploadedAt = f.S3Bucket, f.S3Key, f.LastModified.UTC()
	} else {
		localPath = f.LocalPath
	}
	// column_set (#1535) when the scan read a footer — always on a local scan,
	// never on the S3 one here (it is called with deep=false). NULL where it
	// was not read, which the views generator treats as "cannot group" and
	// falls back to the globbed bind for; recording it costs nothing extra and
	// spares a restored index a `reconcile --repair` before its views are fast
	// again.
	var columnSet any
	if f.ColumnSet != "" {
		columnSet = f.ColumnSet
	}
	_, err := db.ExecContext(ctx, `
		INSERT INTO archive_state
			(partition_name, bintrail_id, local_path, file_size_bytes, row_count, s3_bucket, s3_key, s3_uploaded_at, column_set)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			local_path = COALESCE(VALUES(local_path), local_path),
			s3_bucket = COALESCE(VALUES(s3_bucket), s3_bucket),
			s3_key = COALESCE(VALUES(s3_key), s3_key),
			s3_uploaded_at = COALESCE(VALUES(s3_uploaded_at), s3_uploaded_at),
			column_set = COALESCE(VALUES(column_set), column_set)`,
		f.PartitionName, f.BintrailID, localPath, f.SizeBytes, rows, bucket, key, uploadedAt, columnSet)
	return err
}

func downloadS3ToTemp(ctx context.Context, client *s3.Client, bucket, key string) (string, error) {
	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return "", err
	}
	defer out.Body.Close()
	tmp, err := os.CreateTemp("", "bintrail-restore-*.parquet")
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(tmp, out.Body); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return "", err
	}
	return tmp.Name(), nil
}

// newestSidecar finds the newest index-meta sidecar across the scanned
// bintrail_ids (each source directory carries its own; the sidecar holds a
// FULL table dump, so restoring more than one would duplicate rows). Only a
// genuinely ABSENT sidecar is silent (the routine pre-#1196 case); an
// unreadable or undownloadable one is returned as a warning — swallowing it
// would let the report assert a false cause, and a newer-but-broken sidecar
// silently losing to an older one must at least be visible.
func newestSidecar(ctx context.Context, s3c *s3.Client, ids map[string]bool) (*archive.MetaSidecar, []string) {
	var newest *archive.MetaSidecar
	var warnings []string
	warn := func(format string, a ...any) {
		msg := fmt.Sprintf(format, a...)
		warnings = append(warnings, msg)
		fmt.Fprintln(os.Stderr, "warning: "+msg)
	}
	var bucket, prefix string
	if riArchiveDir == "" {
		var err error
		bucket, prefix, err = storage.ParseS3URL(riArchiveS3)
		if err != nil {
			warn("cannot parse --archive-s3 for sidecar lookup: %v", err)
			return nil, warnings
		}
	}
	for id := range ids {
		var m *archive.MetaSidecar
		if riArchiveDir != "" {
			path := filepath.Join(riArchiveDir, "bintrail_id="+id, archive.MetaSidecarName)
			got, err := archive.ReadMetaSidecar(path)
			if err != nil {
				if !os.IsNotExist(err) {
					warn("unreadable sidecar %s: %v", path, err)
				}
				continue
			}
			m = got
		} else {
			key := strings.TrimSuffix(prefix, "/")
			if key != "" {
				key += "/"
			}
			key += "bintrail_id=" + id + "/" + archive.MetaSidecarName
			path, err := downloadS3ToTemp(ctx, s3c, bucket, key)
			if err != nil {
				// Only NoSuchKey/NotFound is the routine absent case; an
				// AccessDenied/throttle/network error hides a sidecar that
				// may exist.
				var noKey *s3types.NoSuchKey
				var notFound *s3types.NotFound
				if !errors.As(err, &noKey) && !errors.As(err, &notFound) {
					warn("could not download sidecar s3://%s/%s: %v", bucket, key, err)
				}
				continue
			}
			got, rerr := archive.ReadMetaSidecar(path)
			os.Remove(path)
			if rerr != nil {
				warn("unreadable sidecar s3://%s/%s: %v", bucket, key, rerr)
				continue
			}
			m = got
		}
		if newest == nil || m.WrittenAt.After(newest.WrittenAt) {
			newest = m
		}
	}
	return newest, warnings
}

func writeRestoreIndexText(r *restoreIndexReport) {
	fmt.Println("=== restore-index ===")
	fmt.Printf("Events loaded:      %d (%d file(s), %d partition(s) created)\n", r.EventsLoaded, r.FilesLoaded, r.PartitionsCreated)
	fmt.Printf("archive_state rows: %d\n", r.ArchiveStateRows)
	if r.SidecarFound {
		fmt.Printf("Sidecar restored:   %d schema-snapshot row(s), %d server identity row(s)\n", r.SnapshotsRestored, r.ServersRestored)
	}
	for _, f := range r.FailedFiles {
		fmt.Printf("FAILED: %s\n", f)
	}
	fmt.Println("NOT recovered:")
	for _, s := range r.NotRecovered {
		fmt.Printf("  - %s\n", s)
	}
	fmt.Println("Next steps:")
	for _, s := range r.NextSteps {
		fmt.Printf("  - %s\n", s)
	}
}
