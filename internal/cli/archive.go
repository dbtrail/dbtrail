package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// archiveCmd is the parent for archive-maintenance subcommands (the
// `config init` parent+sub precedent).
var archiveCmd = &cobra.Command{
	Use:   "archive",
	Short: "Archive maintenance commands",
}

var archiveReconcileCmd = &cobra.Command{
	Use:   "reconcile",
	Short: "Re-sync archive_state with the Parquet files actually on disk / in S3",
	Long: `Treats archive_state as a rebuildable cache over the self-describing
Hive-partitioned archive layout (#392). Scans the given backends for
bintrail_id=<uuid>/event_date=<d>/event_hour=<h>/*.parquet files, derives the
registry row each file implies, and diffs against archive_state:

  - files without rows   → --repair re-registers them (restores archive
                           auto-discovery and planner coverage after an
                           index rebuild)
  - rows without files   → reported; --prune deletes them (registry rows
                           only; data files are NEVER touched)
  - metadata drift       → --repair updates (file size always; row counts
                           and the archived column set from Parquet footers,
                           which a local scan reads anyway and an S3 scan
                           reads only under --deep)

The archived column set is what lets the DuckDB schema bintrail views writes
read the layout one group per schema instead of opening every file's footer on
every query (#1535). On an S3 archive that means --deep --repair: without
--deep no remote footer is read, so the repair records nothing.

The default is a DRY-RUN that prints the drift and exits non-zero when any
exists; safe to run from cron as a drift monitor. Note the first run after
upgrading to a build that records the column set reports drift on every
partition that predates it — that is the backfill asking to be run, not a new
fault.

Safety rules:
  - a row is only a prune candidate when EVERY backend it references was
    scanned by this invocation and came up empty; a row referencing S3
    during a --archive-dir-only run is reported as unverified, never pruned
  - rows younger than --prune-min-age are never pruned (a concurrent
    rotate may still be mid-write)
  - a scanned backend whose scan finds ZERO layout files provides no
    testimony (a mistyped path looks identical to a real wipe), so its
    rows are reported unverified, never pruned; pass
    --trust-empty-scan=local|s3 NAMING the legitimately emptied backend
    to allow those prunes (the vouch is per-backend so it can never
    disarm the gate for the other, possibly misconfigured one; vouched
    prunes are marked in their reason). Note --repair's column clears
    deliberately keep trusting an empty scan; they are reversible and
    the other backend's file was verified present for that partition
  - repair is backend-scoped: a local-only run never touches S3 columns

Examples:

  # cron drift monitor (read-only; non-zero exit on drift)
  bintrail archive reconcile --index-dsn "$IDX" \
    --archive-dir /var/lib/bintrail/archives --archive-s3 s3://bkt/archives/

  # rebuild the registry after an index loss
  bintrail archive reconcile --index-dsn "$IDX" --archive-s3 s3://bkt/archives/ --repair

  # also drop registrations whose files are gone from BOTH backends
  bintrail archive reconcile --index-dsn "$IDX" \
    --archive-dir /var/lib/bintrail/archives --archive-s3 s3://bkt/archives/ \
    --repair --prune`,
	RunE: runArchiveReconcile,
}

var (
	arcIndexDSN    string
	arcDir         string
	arcS3          string
	arcRegion      string
	arcRepair      bool
	arcPrune       bool
	arcDeep        bool
	arcPruneMinAge time.Duration
	arcFormat      string

	arcTrustEmptyScan string
)

// parseTrustEmptyScan maps --trust-empty-scan's value onto the per-backend
// vouches. The value REQUIRES naming the backend(s): a bare boolean would make
// one vouch cover both backends, so a real S3 wipe plus a silently blind local
// scan (wrong dir, unmounted path) would prune healthy local registrations in
// the same invocation (#1280 review).
func parseTrustEmptyScan(v string) (local, s3 bool, err error) {
	if v == "" {
		return false, false, nil
	}
	for _, part := range strings.Split(v, ",") {
		switch strings.TrimSpace(part) {
		case "local":
			local = true
		case "s3":
			s3 = true
		default:
			return false, false, fmt.Errorf("--trust-empty-scan: unknown backend %q (want local, s3, or local,s3)", part)
		}
	}
	return local, s3, nil
}

// reconcileDiffOptions builds the Diff inputs from the reconcile flags. Split
// from runArchiveReconcile so the flag→field wiring is unit-testable: the
// swap mutation (the local vouch driving the S3 field) compiles, reads
// plausibly, and no engine-level test can see it (#1282 review). It also
// rejects an inert vouch — one naming a backend this invocation does not
// scan — instead of silently ignoring the operator's assertion.
func reconcileDiffOptions(now time.Time) (archive.DiffOptions, error) {
	trustLocal, trustS3, err := parseTrustEmptyScan(arcTrustEmptyScan)
	if err != nil {
		return archive.DiffOptions{}, err
	}
	if trustLocal && arcDir == "" {
		return archive.DiffOptions{}, fmt.Errorf("--trust-empty-scan=local requires --archive-dir (the vouch names a scanned backend)")
	}
	if trustS3 && arcS3 == "" {
		return archive.DiffOptions{}, fmt.Errorf("--trust-empty-scan=s3 requires --archive-s3 (the vouch names a scanned backend)")
	}
	return archive.DiffOptions{
		ScannedLocal:    arcDir != "",
		ScannedS3:       arcS3 != "",
		Deep:            arcDeep,
		PruneMinAge:     arcPruneMinAge,
		Now:             now,
		TrustEmptyLocal: trustLocal,
		TrustEmptyS3:    trustS3,
	}, nil
}

func init() {
	archiveReconcileCmd.Flags().StringVar(&arcIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	archiveReconcileCmd.Flags().StringVar(&arcDir, "archive-dir", "", "Local archive root to scan (the directory given to rotate --archive-dir)")
	archiveReconcileCmd.Flags().StringVar(&arcS3, "archive-s3", "", "S3 archive root to scan (e.g. s3://bucket/prefix/); uses the standard AWS credential chain")
	archiveReconcileCmd.Flags().StringVar(&arcRegion, "region", "", "AWS region (default: from AWS_REGION env var or ~/.aws/config)")
	archiveReconcileCmd.Flags().BoolVar(&arcRepair, "repair", false, "Execute inserts/updates that bring archive_state in line with the scanned files")
	archiveReconcileCmd.Flags().BoolVar(&arcPrune, "prune", false, "Delete registry rows whose every referenced backend was scanned and holds no file (data files are never touched)")
	archiveReconcileCmd.Flags().BoolVar(&arcDeep, "deep", false, "Also verify row counts (reads Parquet footers; one metadata GET per S3 object)")
	archiveReconcileCmd.Flags().DurationVar(&arcPruneMinAge, "prune-min-age", time.Hour, "Never prune rows whose archived_at is younger than this (concurrent-rotate safety margin)")
	archiveReconcileCmd.Flags().StringVar(&arcTrustEmptyScan, "trust-empty-scan", "", "Name a backend (local, s3, or local,s3) whose ZERO-file scan is a legitimate total wipe rather than a mistyped path; allows pruning THAT backend's rows (e.g. after S3 lifecycle expiry of the whole prefix). Per-backend on purpose; never overrides the unscanned-backend rule")
	archiveReconcileCmd.Flags().StringVar(&arcFormat, "format", "text", "Output format: text or json")
	_ = archiveReconcileCmd.MarkFlagRequired("index-dsn")
	BindCommandEnv(archiveReconcileCmd)
	archiveCmd.AddCommand(archiveReconcileCmd)
}

func runArchiveReconcile(cmd *cobra.Command, args []string) error {
	if !cliutil.IsValidOutputFormat(arcFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", arcFormat)
	}
	if arcDir == "" && arcS3 == "" {
		return fmt.Errorf("nothing to scan: pass --archive-dir and/or --archive-s3")
	}
	// Validated BEFORE any scan: a typo in the vouch (or a vouch naming a
	// backend this invocation does not scan) must not cost a full S3 LIST
	// to discover.
	diffOpts, err := reconcileDiffOptions(time.Now().UTC())
	if err != nil {
		return err
	}
	ctx := cmd.Context()

	var files []archive.ScannedFile
	if arcDir != "" {
		local, err := scanLocalArchive(arcDir)
		if err != nil {
			return fmt.Errorf("scan --archive-dir: %w", err)
		}
		files = append(files, local...)
	}
	if arcS3 != "" {
		remote, err := scanS3Archive(ctx, arcS3, arcRegion, arcDeep)
		if err != nil {
			return fmt.Errorf("scan --archive-s3: %w", err)
		}
		files = append(files, remote...)
	}

	db, err := config.Connect(arcIndexDSN)
	if err != nil {
		return fmt.Errorf("connect index database: %w", err)
	}
	defer db.Close()

	// reconcile reads and writes archive_state columns this build knows about,
	// so it migrates THAT TABLE first — the same rule the read paths adopted
	// when query_text/query_hash were added (#699): a SELECT naming a column
	// the index has not been migrated to is a hard 1054, not a degraded read.
	// column_set (#1535) is the current instance: without this, `archive
	// reconcile` against an index whose rotation is older than this binary
	// fails outright, which is exactly the operator most likely to run it.
	//
	// EnsureArchiveStateSchema, NOT EnsureSchema: this command's dry run is
	// documented as a read-only cron drift monitor, and the full migration also
	// adds columns to binlog_events — the largest table in the deployment.
	// Instant on a current MySQL, a rebuild on an older one, and either way not
	// something a monitor should start on its own.
	if err := indexer.EnsureArchiveStateSchema(db); err != nil {
		return fmt.Errorf("migrate archive_state schema: %w", err)
	}

	rows, err := loadArchiveStateRows(ctx, db)
	if err != nil {
		return fmt.Errorf("load archive_state: %w", err)
	}

	report := archive.Diff(files, rows, diffOpts)

	// deepUnverified counts scanned pairs --deep was asked to verify but whose
	// picked row_count came back Invalid — the footer read failed on the
	// backend pickMeta prefers, so the row_count drift check silently skipped
	// them (#469). Computed at the decision layer (internal/archive) so it
	// catches local footer failures AND the dual-backend prefer-local case,
	// and never false-positives when the OTHER backend deep-verified the pair.
	// Surfaced in the report and, on the dry-run path, made to fail the cron
	// monitor.
	deepUnverified := report.DeepUnverified

	executed, execErrs := executeReconcileActions(ctx, db, report.Actions, arcRepair, arcPrune)

	if err := writeReconcileReport(os.Stdout, arcFormat, &report, deepUnverified, executed, execErrs, arcRepair, arcPrune); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	if len(execErrs) > 0 {
		return fmt.Errorf("%d reconcile action(s) failed", len(execErrs))
	}
	if !arcRepair && !arcPrune {
		// Dry-run: non-zero exit on any drift (the cron monitor contract),
		// including objects --deep was asked to verify but couldn't (#469).
		return reconcileDryRunErr(&report, deepUnverified)
	}
	// Execute mode: same #469 contract, distinct drift rule (unaddressed
	// drift, not all drift). The shared helper guarantees deepUnverified is
	// checked on BOTH the dry-run and execute paths — never dropped on one.
	return reconcileExecuteErr(&report, deepUnverified, arcRepair, arcPrune)
}

// scanLocalArchive walks root for Hive-layout parquet files. Footer row
// counts are always read locally — a local footer read is cheap and makes
// repaired rows feed status totals correctly. A failed local footer read
// leaves RowCount Invalid (logged); under --deep that is surfaced by the
// decision-layer deep-unverified count (archive.Diff → Report.DeepUnverified),
// the same path that covers an unreadable S3 footer — see scanS3Archive.
func scanLocalArchive(root string) ([]archive.ScannedFile, error) {
	var out []archive.ScannedFile
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if path == root {
				return walkErr
			}
			slog.Warn("reconcile: skipping unreadable entry", "path", path, "error", walkErr)
			return nil
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".parquet") {
			return nil
		}
		id, part := archive.ParseArchivePath(path)
		if id == "" {
			slog.Debug("reconcile: parquet outside the archive layout, ignoring", "path", path)
			return nil
		}
		info, err := d.Info()
		if err != nil {
			slog.Warn("reconcile: cannot stat file, skipping", "path", path, "error", err)
			return nil
		}
		f := archive.ScannedFile{
			PartitionName: part, BintrailID: id, Backend: archive.BackendLocal,
			LocalPath: path, SizeBytes: info.Size(), LastModified: info.ModTime().UTC(),
		}
		if n, cols, err := localParquetFooter(path, info.Size()); err == nil {
			f.RowCount = sql.NullInt64{Int64: n, Valid: true}
			f.ColumnSet = cols
		} else {
			slog.Warn("reconcile: cannot read parquet footer, row_count and column set left unset", "path", path, "error", err)
		}
		out = append(out, f)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// localParquetFooter reads the row count AND the column set from one local
// footer. One function, one open: the column set is the #1535 backfill, and
// reading it in a second pass would double the file opens a repair costs on an
// archive with thousands of partitions.
func localParquetFooter(path string, size int64) (int64, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()
	pf, err := parquet.OpenFile(f, size)
	if err != nil {
		return 0, "", err
	}
	var names []string
	for _, fld := range pf.Schema().Fields() {
		names = append(names, fld.Name())
	}
	return pf.NumRows(), archive.ColumnSetOf(names), nil
}

// scanS3Archive lists the prefix for Hive-layout parquet objects. Size and
// LastModified come free with the listing; row counts cost one metadata
// read per object and are only fetched under --deep.
//
// A failed --deep footer read leaves RowCount Invalid (logged) — it is NOT
// counted here. The deep-unverified accounting lives at the decision layer
// (archive.Diff → Report.DeepUnverified), keyed on the PICKED row_count, so
// it catches local footer failures and the dual-backend prefer-local case
// too, and never over-counts an object the OTHER backend deep-verified (#469).
func scanS3Archive(ctx context.Context, s3URL, region string, deep bool) ([]archive.ScannedFile, error) {
	bucket, prefix, err := storage.ParseS3URL(s3URL)
	if err != nil {
		return nil, err
	}
	cfg, err := storage.LoadAWSConfig(ctx, region)
	if err != nil {
		return nil, err
	}
	client := storage.NewS3ClientFromConfig(cfg)

	// ONE DuckDB session serves every --deep footer probe of the scan (#807):
	// extension install and credential-chain resolution happen once, not per
	// object (a year of hourly archives = thousands of sessions and IMDS
	// round-trips otherwise), and the chain secret pins the listing's resolved
	// region (#511) so probes on a cross-region bucket don't 301 every object
	// into DeepUnverified. A failed open degrades exactly like a failed probe:
	// row counts stay unset (warned) and the decision layer counts each object
	// deep-unverified (#469) — the scan itself still completes.
	//
	// The chain secret resolves credentials at CREATE time, not per request
	// (duckdbutil.EnableS3CredentialChain's docs explicitly warn against
	// reusing it on a long-lived session under expiring roles). A large scan
	// (thousands of hourly objects, 15-45min) can outlive an IMDS/STS role's
	// credential lifetime, so the secret is re-issued periodically
	// (s3FooterSecretRefreshInterval) instead of being frozen for the whole
	// session — otherwise every remaining probe would 403 once the original
	// credentials expire, degrading into the exact DeepUnverified symptom
	// #807 was filed to fix.
	var footerDB *sql.DB
	var secretIssuedAt time.Time
	if deep {
		fdb, err := openS3FooterSession(ctx, cfg.Region)
		if err != nil {
			slog.Warn("reconcile: cannot open DuckDB session for --deep footer reads; row counts left unset", "error", err)
		} else {
			footerDB = fdb
			secretIssuedAt = time.Now()
			defer footerDB.Close()
		}
	}

	var out []archive.ScannedFile
	var token *string
	for {
		page, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: &bucket, Prefix: &prefix, ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("list s3://%s/%s: %w", bucket, prefix, err)
		}
		for _, obj := range page.Contents {
			if obj.Key == nil || !strings.HasSuffix(*obj.Key, ".parquet") {
				continue
			}
			id, part := archive.ParseArchivePath(*obj.Key)
			if id == "" {
				slog.Debug("reconcile: s3 parquet outside the archive layout, ignoring", "key", *obj.Key)
				continue
			}
			f := archive.ScannedFile{
				PartitionName: part, BintrailID: id, Backend: archive.BackendS3,
				S3Bucket: bucket, S3Key: *obj.Key,
			}
			if obj.Size != nil {
				f.SizeBytes = *obj.Size
			}
			if obj.LastModified != nil {
				f.LastModified = obj.LastModified.UTC()
			}
			if deep && footerDB != nil {
				if dueForS3SecretRefresh(secretIssuedAt, time.Now(), s3FooterSecretRefreshInterval) {
					if err := duckdbutil.EnableS3CredentialChainRegion(ctx, footerDB, cfg.Region); err != nil {
						return nil, err
					}
					secretIssuedAt = time.Now()
				}
				if n, cols, err := duckdbParquetFooter(ctx, footerDB, "s3://"+bucket+"/"+*obj.Key); err == nil {
					f.RowCount = sql.NullInt64{Int64: n, Valid: true}
					f.ColumnSet = cols
				} else {
					slog.Warn("reconcile: cannot read s3 parquet footer, row_count left unset",
						"key", *obj.Key, "error", err)
				}
			}
			out = append(out, f)
		}
		if page.IsTruncated == nil || !*page.IsTruncated {
			break
		}
		token = page.NextContinuationToken
	}
	return out, nil
}

// s3FooterSecretRefreshInterval bounds how long the shared --deep session's
// AWS credential-chain secret is trusted before being re-issued. Chosen well
// under the shortest common STS/IMDS credential lifetime (IMDS role
// credentials ~6h, default STS session minimum 15min) so a rotation mid-scan
// is caught before the original credentials expire — assuming the chain was
// resolved from a session with at least that much life left when the scan
// started; a knob, not a hard guarantee for every possible role config.
var s3FooterSecretRefreshInterval = 10 * time.Minute

// dueForS3SecretRefresh reports whether the shared footer session's
// credential-chain secret, last (re)issued at last, is due for re-issuing at
// now given interval. Split out from the scan loop so the threshold logic is
// testable without a live DuckDB/AWS session.
func dueForS3SecretRefresh(last, now time.Time, interval time.Duration) bool {
	return !now.Before(last.Add(interval))
}

// openS3FooterSession opens the DuckDB session shared by all --deep S3 footer
// probes of one scan: httpfs loaded and the credential-chain secret created
// with the scan's region pinned (duckdbutil.EnableS3CredentialChainRegion,
// #511 — the chain otherwise resolves region from the AWS SDK config, not the
// bucket, and a mismatch 301s every probe). The scan loop re-issues the same
// secret periodically (dueForS3SecretRefresh) as credentials age.
func openS3FooterSession(ctx context.Context, region string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	// Probes run sequentially; a single connection guarantees each one sees
	// the loaded extension and the secret regardless of pool scoping.
	db.SetMaxOpenConns(1)
	if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
		db.Close()
		return nil, fmt.Errorf("load httpfs extension: %w", err)
	}
	if err := duckdbutil.EnableS3CredentialChainRegion(ctx, db, region); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// duckdbParquetFooter reads num_rows and the column set from a Parquet footer
// on the given session (the ReadParquetMetadataAny pattern in
// internal/baseline). path is an s3:// URI on the reconcile path; local paths
// work too (tests).
//
// Two statements, one remote footer: DuckDB caches the metadata it just read
// for parquet_file_metadata, and parquet_schema over the same path answers
// from it. Splitting them keeps each SELECT single-column, which is what the
// scan-into-one-value shape here needs.
//
// parquet_schema returns the FULL schema tree, whose root is the message itself
// and carries a child count; a LEAF carries NULL there, not 0 (verified against
// the pinned DuckDB — filtering on `= 0` selects nothing at all and would
// record every archive as having no columns). The archive layout is flat, so
// "leaf" and "column" are the same set here.
func duckdbParquetFooter(ctx context.Context, db *sql.DB, path string) (int64, string, error) {
	safe := strings.ReplaceAll(path, "'", "''")
	var n int64
	if err := db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT num_rows FROM parquet_file_metadata('%s')", safe)).Scan(&n); err != nil {
		return 0, "", err
	}
	rows, err := db.QueryContext(ctx,
		fmt.Sprintf("SELECT name FROM parquet_schema('%s') WHERE num_children IS NULL", safe))
	if err != nil {
		return 0, "", err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return 0, "", err
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return 0, "", err
	}
	return n, archive.ColumnSetOf(names), nil
}

func loadArchiveStateRows(ctx context.Context, db *sql.DB) ([]archive.StateRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT partition_name, bintrail_id, local_path, file_size_bytes,
		       row_count, s3_bucket, s3_key, s3_uploaded_at, column_set, archived_at
		FROM archive_state
		WHERE bintrail_id IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []archive.StateRow
	for rows.Next() {
		var r archive.StateRow
		if err := rows.Scan(&r.PartitionName, &r.BintrailID, &r.LocalPath, &r.FileSizeBytes,
			&r.RowCount, &r.S3Bucket, &r.S3Key, &r.S3UploadedAt, &r.ColumnSet, &r.ArchivedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// reconcileColumns is the allowlist of archive_state columns an action may
// write — the FieldChange column names come from internal/archive, never
// from user input, but the allowlist makes that contract explicit.
var reconcileColumns = map[string]bool{
	"local_path": true, "file_size_bytes": true, "row_count": true,
	"s3_bucket": true, "s3_key": true, "s3_uploaded_at": true,
	"column_set": true,
}

// executeReconcileActions applies insert/update actions under --repair and
// prune actions under --prune. Returns the number executed and any errors
// (one entry per failed action; execution continues past failures so a
// single bad row doesn't abort a large repair).
func executeReconcileActions(ctx context.Context, db *sql.DB, actions []archive.Action, repair, prune bool) (int, []error) {
	executed := 0
	var errs []error
	for _, a := range actions {
		switch a.Kind {
		case archive.ActionInsert, archive.ActionUpdate:
			if !repair {
				continue
			}
			if err := applyUpsert(ctx, db, a); err != nil {
				errs = append(errs, fmt.Errorf("%s %s/%s: %w", a.Kind, a.PartitionName, a.BintrailID, err))
				continue
			}
			executed++
		case archive.ActionPrune:
			if !prune {
				continue
			}
			if _, err := db.ExecContext(ctx,
				`DELETE FROM archive_state WHERE partition_name = ? AND bintrail_id = ?`,
				a.PartitionName, a.BintrailID); err != nil {
				errs = append(errs, fmt.Errorf("prune %s/%s: %w", a.PartitionName, a.BintrailID, err))
				continue
			}
			executed++
		}
	}
	return executed, errs
}

// applyUpsert writes one action's field changes. Inserts and updates share
// the INSERT … ON DUPLICATE KEY UPDATE shape (rotate's upsert model) so a
// concurrent rotate inserting the same key never races us into a duplicate
// error; only the action's own columns are written (backend-scoped).
func applyUpsert(ctx context.Context, db *sql.DB, a archive.Action) error {
	cols := []string{"partition_name", "bintrail_id"}
	vals := []any{a.PartitionName, a.BintrailID}
	var updates []string
	for _, c := range a.Changes {
		if !reconcileColumns[c.Column] {
			return fmt.Errorf("column %q is not reconcile-writable", c.Column)
		}
		cols = append(cols, c.Column)
		vals = append(vals, c.Value)
		updates = append(updates, fmt.Sprintf("%s = VALUES(%s)", c.Column, c.Column))
	}
	q := fmt.Sprintf(
		"INSERT INTO archive_state (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		strings.Join(cols, ", "),
		strings.TrimSuffix(strings.Repeat("?, ", len(cols)), ", "),
		strings.Join(updates, ", "),
	)
	_, err := db.ExecContext(ctx, q, vals...)
	return err
}

// reconcileDryRunErr is the dry-run cron-monitor exit decision: non-zero on
// any archive_state drift (report.Err()) OR on any scanned pair that --deep
// was asked to verify but whose picked row_count came back Invalid (#469).
// report.Err() itself stays unchanged — the deep-unverified count is a
// distinct signal (Report.DeepUnverified) that can't ride the pure diff
// actions, since a silently-skipped row_count check produces no action.
func reconcileDryRunErr(rep *archive.Report, deepUnverified int) error {
	if err := rep.Err(); err != nil {
		return err
	}
	if deepUnverified > 0 {
		return fmt.Errorf("%d file(s) could not be deep-verified (Parquet footer probe failed)", deepUnverified)
	}
	return nil
}

// reconcileExecuteErr is the execute-mode (--repair/--prune) exit decision,
// the sibling of reconcileDryRunErr. Exit 0 ⟺ no UNADDRESSED drift remains:
// EVERY action this invocation's flags didn't execute counts — --prune
// without --repair must not silently mask insert/update drift the dry-run
// would have flagged (and vice versa). The deepUnverified guard mirrors the
// dry-run path (#469): --deep/--repair/--prune are independent flags, so
// `reconcile --deep --repair` is exercisable, and --repair cannot fix a
// footer it cannot read — a failed deep probe is unaddressed drift that must
// fail the exit code, or a scheduled --deep --repair auto-remediation keyed
// on the exit code reintroduces the green-run-hides-unverifiable-files bug.
// (The drift rule differs from the dry-run's rep.Err(): dry-run fails on ALL
// drift, execute only on what its flags left unaddressed — so this stays a
// sibling, not a single shared helper.)
func reconcileExecuteErr(rep *archive.Report, deepUnverified int, repair, prune bool) error {
	pendingRepairs, pendingPrunes := 0, 0
	if !repair {
		pendingRepairs = rep.Inserts + rep.Updates
	}
	if !prune {
		pendingPrunes = rep.Prunes
	}
	if pendingRepairs+pendingPrunes+rep.SkippedUnverified+rep.SkippedRecent > 0 {
		return fmt.Errorf("drift remains: %d insert/update(s) (need --repair), %d prune candidate(s) (need --prune), %d unverified, %d too recent",
			pendingRepairs, pendingPrunes, rep.SkippedUnverified, rep.SkippedRecent)
	}
	if deepUnverified > 0 {
		return fmt.Errorf("%d file(s) could not be deep-verified (Parquet footer probe failed)", deepUnverified)
	}
	return nil
}

// reconcileReportJSON is the --format json shape.
type reconcileReportJSON struct {
	Actions        []reconcileActionJSON `json:"actions"`
	Inserts        int                   `json:"inserts"`
	Updates        int                   `json:"updates"`
	Prunes         int                   `json:"prune_candidates"`
	Skipped        int                   `json:"skipped"`
	DeepUnverified int                   `json:"deep_unverified"`
	InSync         int                   `json:"in_sync"`
	Executed       int                   `json:"executed"`
	Errors         []string              `json:"errors,omitempty"`
}

type reconcileActionJSON struct {
	Kind       string `json:"kind"`
	Partition  string `json:"partition"`
	BintrailID string `json:"bintrail_id"`
	Reason     string `json:"reason,omitempty"`
}

func writeReconcileReport(w io.Writer, format string, rep *archive.Report, deepUnverified, executed int, execErrs []error, repair, prune bool) error {
	if format == "json" {
		j := reconcileReportJSON{
			Inserts: rep.Inserts, Updates: rep.Updates, Prunes: rep.Prunes,
			Skipped: rep.SkippedUnverified + rep.SkippedRecent, DeepUnverified: deepUnverified,
			InSync: rep.InSync, Executed: executed,
		}
		for _, a := range rep.Actions {
			j.Actions = append(j.Actions, reconcileActionJSON{
				Kind: string(a.Kind), Partition: a.PartitionName, BintrailID: a.BintrailID, Reason: a.Reason,
			})
		}
		for _, e := range execErrs {
			j.Errors = append(j.Errors, e.Error())
		}
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(j)
	}

	mode := "dry-run"
	if repair || prune {
		mode = "execute"
	}
	fmt.Fprintf(w, "archive reconcile (%s): %d in sync, %d to insert, %d to update, %d prune candidate(s), %d skipped\n",
		mode, rep.InSync, rep.Inserts, rep.Updates, rep.Prunes, rep.SkippedUnverified+rep.SkippedRecent)
	for _, a := range rep.Actions {
		fmt.Fprintf(w, "  [%s] %s / %s: %s\n", a.Kind, a.PartitionName, a.BintrailID, a.Reason)
	}
	if deepUnverified > 0 {
		fmt.Fprintf(w, "WARNING: %d file(s) could not be deep-verified (Parquet footer probe failed)\n", deepUnverified)
	}
	if repair || prune {
		fmt.Fprintf(w, "executed: %d action(s)\n", executed)
	}
	for _, e := range execErrs {
		fmt.Fprintf(w, "ERROR: %v\n", e)
	}
	return nil
}
