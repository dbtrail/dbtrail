package rotation

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// Result is one rotation cycle's outcome. Deferred counts partitions past
// retention that this cycle did NOT drop to avoid data loss: the
// ProtectUnarchived guard refusing an unarchived partition, OR (in the archive
// path) an S3 upload that failed or is still pending. The built-in loop sums it
// across targets to drive escalation. The explicit `rotate` command surfaces
// only Dropped/Added, but can still produce Deferred>0 when its --archive-s3
// uploads fail. A named struct, not a positional tuple: three same-typed ints
// invite silent misordering at call sites.
type Result struct {
	Dropped, Added, Deferred int
}

// Options configures one rotation cycle. The fields replace the package-level
// rot* flag globals the engine used to read directly, so the explicit `rotate`
// command and `up`'s built-in rotation loop can both drive Perform with their
// own settings.
type Options struct {
	// RetainDur drops partitions older than this. Zero disables drops (the
	// cycle only tops up future partitions).
	RetainDur time.Duration
	// RetainRaw is the operator-facing retain string (e.g. "7d"), used only in
	// the "no partitions older than %s" message.
	RetainRaw string
	// AddFuture is the declarative future-headroom target.
	AddFuture int
	// NoReplace suppresses one-for-one replacement of dropped partitions.
	NoReplace bool
	// ArchiveDir, when set, archives each partition to Parquet before dropping.
	ArchiveDir         string
	ArchiveCompression string
	BintrailID         string
	// ArchiveS3, when set (requires ArchiveDir), uploads archives to S3.
	ArchiveS3       string
	ArchiveS3Region string
	// Retry skips partitions whose Parquet already exists and S3 uploads that
	// already succeeded.
	Retry bool
	// Format "json" suppresses the per-partition stdout chatter (callers that
	// emit their own JSON, or the built-in loop, pass "json").
	Format string
	// ProtectUnarchived: when the index has any archiving history, the
	// no-archive drop path only drops already-archived partitions — the
	// built-in rotation must not be the first to destroy data an archiving flow
	// would preserve. The explicit rotate command leaves this false.
	ProtectUnarchived bool
	// PruneLocalAfterUpload removes the local staging Parquet once it has been
	// uploaded to S3 and the partition dropped. The unattended built-in loop
	// sets this so a container's staging dir doesn't grow without bound; the
	// read side falls back to S3 when the local copy is gone. Requires
	// ArchiveS3 (a local-only archive IS the durable copy — never pruned). The
	// explicit rotate command leaves this false (operator keeps both copies).
	PruneLocalAfterUpload bool
}

// Perform executes one full rotation cycle against an open DB connection,
// reading every setting from opts so daemon and one-shot modes share identical
// rotation logic.
func Perform(ctx context.Context, db *sql.DB, dbName string, opts Options) (Result, error) {
	retainDur := opts.RetainDur
	start := time.Now()

	// An S3 target without a local staging dir is a misconfiguration: archiving
	// keys off ArchiveDir, so this would silently fall through to the no-archive
	// bulk-drop branch and drop partitions that were never uploaded. Fail loud
	// instead — the field invariant (ArchiveS3 requires ArchiveDir) is enforced
	// here, not just documented on Options.
	if opts.ArchiveS3 != "" && opts.ArchiveDir == "" {
		return Result{}, fmt.Errorf("ArchiveS3 set without ArchiveDir: cannot upload to S3 without a local staging path")
	}

	// ── Load current partition list ─────────────────────────────────────────────
	partitions, err := listPartitions(ctx, db, dbName)
	if err != nil {
		return Result{}, fmt.Errorf("failed to list partitions: %w", err)
	}

	// ── Drop old partitions ───────────────────────────────────────────────────
	var droppedCount, deferredCount int
	if retainDur > 0 {
		cutoff := time.Now().UTC().Add(-retainDur)
		var toDrop []string
		for _, p := range partitions {
			d, ok := indexer.PartitionDate(p.Name)
			if !ok {
				continue // skip p_future and any unrecognised names
			}
			if d.Before(cutoff) {
				toDrop = append(toDrop, p.Name)
			}
		}

		if len(toDrop) == 0 {
			if opts.Format != "json" {
				fmt.Fprintf(os.Stdout, "no partitions older than %s to drop\n", opts.RetainRaw)
			}
		} else {
			// Archive partitions to Parquet before dropping, if requested.
			// Each partition is dropped immediately after archiving to free
			// disk space incrementally and reduce the crash window.
			if opts.ArchiveDir != "" {
				// Set up S3 client once for all uploads (nil when --archive-s3 is not set).
				var s3Client *s3.Client
				var s3Bucket, s3Prefix string
				if opts.ArchiveS3 != "" {
					s3Bucket, s3Prefix, err = storage.ParseS3URL(opts.ArchiveS3)
					if err != nil {
						return Result{}, fmt.Errorf("invalid --archive-s3: %w", err)
					}
					s3Client, err = storage.NewS3Client(ctx, opts.ArchiveS3Region)
					if err != nil {
						return Result{}, fmt.Errorf("init S3 client: %w", err)
					}
				}

				// #1196: persist the durable-state sidecar (schema snapshots +
				// server identity) alongside this source's archives, so a
				// future restore-index can rebuild the state the event files
				// don't carry. Best-effort by contract: a sidecar failure must
				// never fail rotation — the events are the payload.
				sidecarDir := filepath.Join(opts.ArchiveDir, "bintrail_id="+opts.BintrailID)
				if err := archive.WriteMetaSidecar(ctx, db, sidecarDir); err != nil {
					slog.Warn("could not write index-meta sidecar; a future restore-index will be missing schema snapshots/identity", "error", err)
				} else if s3Client != nil {
					sidecarPath := filepath.Join(sidecarDir, archive.MetaSidecarName)
					if key, kerr := storage.BuildS3Key(opts.ArchiveDir, sidecarPath, s3Prefix); kerr != nil {
						slog.Warn("could not build sidecar S3 key", "error", kerr)
					} else if uerr := uploadFileFunc(ctx, s3Client, sidecarPath, s3Bucket, key); uerr != nil {
						slog.Warn("could not upload index-meta sidecar to S3", "error", uerr)
					}
				}

				for _, name := range toDrop {
					outPath, err := HiveArchivePath(opts.ArchiveDir, opts.BintrailID, name)
					if err != nil {
						return Result{}, fmt.Errorf("build archive path for %s: %w", name, err)
					}
					if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
						return Result{}, fmt.Errorf("create archive directory for %s: %w", name, err)
					}
					var n int64
					var minTS, maxTS time.Time
					var columnSet string
					skipped := false
					if opts.Retry && fileExists(outPath) {
						// A file existing at outPath with size>0 is NOT sufficient
						// evidence it is a complete archive: a crash mid-write
						// (kill -9, OOM, reboot) can leave a truncated, no-footer
						// file here even though ArchivePartition now writes via a
						// temp file + atomic rename (issue #802) — this guards
						// leftovers from before that fix, or any other corruption.
						// Only trust the file when archive_state independently
						// recorded the same row count for this partition; a
						// missing/mismatched row (exactly what a mid-write crash
						// leaves, since that INSERT only happens after the write
						// completes) means re-archive rather than silently
						// skip-and-later-upload/drop a corrupt file.
						verified, err := verifiedExistingArchive(ctx, db, outPath, name, opts.BintrailID)
						if err != nil {
							return Result{}, fmt.Errorf("verify existing archive for %s: %w", name, err)
						}
						if verified {
							skipped = true
						} else {
							slog.Warn("existing archive file failed verification (truncated or unrecorded); re-archiving",
								"partition", name, "file", outPath)
						}
					}
					if skipped {
						slog.Info("skipping existing archive (--retry)", "partition", name, "file", outPath)
						if opts.Format != "json" {
							fmt.Fprintf(os.Stdout, "skipped partition %s (already archived) → %s\n", name, outPath)
						}
					} else {
						st, aerr := archive.ArchivePartition(ctx, db, dbName, name, outPath, opts.ArchiveCompression)
						if aerr != nil {
							return Result{}, fmt.Errorf("archive partition %s: %w", name, aerr)
						}
						n, minTS, maxTS, columnSet = st.Rows, st.MinEventTS, st.MaxEventTS, st.Columns
						// A partition whose CONTENT escapes its hour label holds
						// backfilled events (#1037): old rows replayed after a
						// capture stall land in the oldest live RANGE partition.
						// The true range is recorded in archive_state below so
						// time-scoped reads still find these rows; the warning
						// gives the operator the same fact for pre-existing
						// misfiled archives and monitoring.
						if escapesLabelHour(name, minTS, maxTS) {
							slog.Warn("archived partition contains events outside its hour label (backfilled rows); recording true content time range so time-scoped archive reads still find them",
								"partition", name,
								"min_event_ts", minTS.UTC().Format(time.RFC3339),
								"max_event_ts", maxTS.UTC().Format(time.RFC3339))
						}
						if opts.Format != "json" {
							fmt.Fprintf(os.Stdout, "archived partition %s (%d rows) → %s\n", name, n, outPath)
						}
					}

					// Compute S3 key early so we can record the upload intent
					// in archive_state before the actual upload.
					var s3Key string
					if s3Client != nil {
						s3Key, err = storage.BuildS3Key(opts.ArchiveDir, outPath, s3Prefix)
						if err != nil {
							return Result{}, fmt.Errorf("build S3 key for %s: %w", name, err)
						}
					}

					// Record archive in archive_state (skip when retrying —
					// the row already exists with the correct row_count).
					// When S3 is configured, s3_bucket and s3_key are recorded
					// immediately so that future runs (even without --archive-s3)
					// know that an S3 upload is expected before the partition can
					// be dropped.
					if !skipped {
						var fileSize int64
						if fi, statErr := os.Stat(outPath); statErr == nil {
							fileSize = fi.Size()
						}
						var insertBucket, insertKey any
						if s3Client != nil {
							insertBucket = s3Bucket
							insertKey = s3Key
						}
						// min/max_event_ts: the content-derived range of the
						// archived rows (#1037). NULL for an empty partition —
						// the planner then falls back to the hour label.
						var insertMin, insertMax any
						if !minTS.IsZero() {
							insertMin = minTS.UTC()
						}
						if !maxTS.IsZero() {
							insertMax = maxTS.UTC()
						}
						// column_set: the file's own column set, straight from
						// the writer that just produced it (#1535). Recorded
						// here so the views generator can group the layout by
						// schema instead of making DuckDB open every footer at
						// bind time.
						if _, err := db.ExecContext(ctx,
							`INSERT INTO archive_state
								(partition_name, bintrail_id, local_path, file_size_bytes, row_count, s3_bucket, s3_key, min_event_ts, max_event_ts, column_set)
							VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
							ON DUPLICATE KEY UPDATE
								local_path = VALUES(local_path),
								file_size_bytes = VALUES(file_size_bytes),
								row_count = VALUES(row_count),
								s3_bucket = COALESCE(VALUES(s3_bucket), s3_bucket),
								s3_key = COALESCE(VALUES(s3_key), s3_key),
								min_event_ts = VALUES(min_event_ts),
								max_event_ts = VALUES(max_event_ts),
								column_set = VALUES(column_set)`,
							name, opts.BintrailID, outPath, fileSize, n, insertBucket, insertKey, insertMin, insertMax, columnSet,
						); err != nil {
							return Result{}, fmt.Errorf("record archive state for %s: %w", name, err)
						}
					}

					if s3Client != nil {
						skipUpload := false
						if opts.Retry {
							var uploadedAt sql.NullTime
							err := db.QueryRowContext(ctx,
								`SELECT s3_uploaded_at FROM archive_state
								WHERE partition_name = ? AND bintrail_id = ?`,
								name, opts.BintrailID,
							).Scan(&uploadedAt)
							switch {
							case err == nil && uploadedAt.Valid:
								slog.Info("skipping existing S3 upload (--retry)", "partition", name)
								if opts.Format != "json" {
									fmt.Fprintf(os.Stdout, "skipped S3 upload for %s (already uploaded)\n", name)
								}
								skipUpload = true
							case err != nil && !errors.Is(err, sql.ErrNoRows):
								return Result{}, fmt.Errorf("check S3 upload state for %s: %w", name, err)
							}
						}

						if !skipUpload {
							if err := uploadFileFunc(ctx, s3Client, outPath, s3Bucket, s3Key); err != nil {
								// Propagate context cancellation (e.g. SIGINT in daemon mode)
								// instead of logging a misleading S3 warning for every remaining partition.
								if ctx.Err() != nil {
									return Result{}, fmt.Errorf("upload %s to S3: %w", name, err)
								}
								slog.Warn("S3 upload failed; partition will not be dropped",
									"partition", name, "error", err)
								if opts.Format != "json" {
									fmt.Fprintf(os.Stdout, "warning: S3 upload failed for %s: %v\n", name, err)
									fmt.Fprintf(os.Stdout, "  run 'bintrail rotate --retry --archive-s3 ...' to retry\n")
								}
								// Count it deferred so the built-in loop's unhealthy-streak
								// escalation fires: a persistently failing upload keeps the
								// index (and staging dir) growing, the exact condition the
								// streak detector exists to surface above per-cycle warnings.
								deferredCount++
								continue
							}
							if _, err := db.ExecContext(ctx,
								`UPDATE archive_state
									SET s3_bucket = ?, s3_key = ?, s3_uploaded_at = UTC_TIMESTAMP()
								WHERE partition_name = ? AND bintrail_id = ?`,
								s3Bucket, s3Key, name, opts.BintrailID,
							); err != nil {
								return Result{}, fmt.Errorf("update archive state S3 info for %s: %w", name, err)
							}
							slog.Info("uploaded archive to S3", "partition", name, "bucket", s3Bucket, "key", s3Key)
							if opts.Format != "json" {
								fmt.Fprintf(os.Stdout, "uploaded %s → s3://%s/%s\n", name, s3Bucket, s3Key)
							}
						}
					}

					// Safety check: never drop a partition that has a pending S3 upload,
					// even if the current run does not have --archive-s3 configured.
					pending, err := hasPendingS3Upload(ctx, db, name, opts.BintrailID)
					if err != nil {
						return Result{}, fmt.Errorf("check pending S3 upload for %s: %w", name, err)
					}
					if pending {
						slog.Warn("partition archived locally but not yet uploaded to S3; skipping drop",
							"partition", name)
						if opts.Format != "json" {
							fmt.Fprintf(os.Stdout, "skipped drop for %s (pending S3 upload)\n", name)
							fmt.Fprintf(os.Stdout, "  run 'bintrail rotate --retry --archive-s3 ...' to retry\n")
						}
						// A still-pending upload is an undropped partition too — count it
						// so the loop escalates rather than reporting a healthy cycle.
						deferredCount++
						continue
					}

					// ── TOCTOU guard: the partition must be unchanged since it ──
					// was archived. binlog_events is append-only — DROP PARTITION
					// is the only thing that ever removes rows — so any difference
					// between the count captured at archive time
					// (archive_state.row_count == the rows written to the Parquet)
					// and the partition's current live count means rows were
					// INSERTED after the archive SELECT and during the
					// possibly-minutes-long upload. That happens when a backfilled
					// gap is replayed with original binlog timestamps and lands in
					// this old RANGE partition while rotation is archiving it.
					// Dropping now would erase those rows from BOTH the index and
					// the archive. Instead defer the drop and discard the now-stale
					// archive so the next cycle re-archives the full partition; the
					// built-in loop's deferred-streak escalation surfaces a
					// partition that keeps growing.
					archived, err := archivedRowCount(ctx, db, name, opts.BintrailID)
					if err != nil {
						return Result{}, fmt.Errorf("read archived row count for %s: %w", name, err)
					}
					live, err := partitionRowCount(ctx, db, dbName, name)
					if err != nil {
						return Result{}, fmt.Errorf("recount partition %s before drop: %w", name, err)
					}
					if !archivedPartitionUnchanged(archived, live) {
						slog.Warn("partition changed since it was archived; deferring drop and discarding the stale archive for re-archive next cycle",
							"partition", name, "archived_rows", archived.Int64, "live_rows", live)
						if opts.Format != "json" {
							fmt.Fprintf(os.Stdout, "skipped drop for %s (partition changed since archive: %d → %d rows; will re-archive next cycle)\n",
								name, archived.Int64, live)
						}
						// Discard the incomplete staged archive and its
						// archive_state row so (a) --retry re-archives instead of
						// trusting the stale file, and (b) no later drop-only cycle
						// trusts this partition as safely archived.
						if _, derr := db.ExecContext(ctx,
							`DELETE FROM archive_state WHERE partition_name = ? AND bintrail_id = ?`,
							name, opts.BintrailID); derr != nil {
							return Result{}, fmt.Errorf("invalidate stale archive state for %s: %w", name, derr)
						}
						if rerr := os.Remove(outPath); rerr != nil && !os.IsNotExist(rerr) {
							slog.Warn("could not remove stale local archive after deferring drop", "partition", name, "error", rerr)
						}
						deferredCount++
						continue
					}

					// Drop this partition immediately after archiving.
					if err := dropPartitions(ctx, db, dbName, []string{name}); err != nil {
						return Result{}, fmt.Errorf("failed to drop partition %s: %w", name, err)
					}
					droppedCount++
					slog.Info("dropped partition", "partition", name)
					if opts.Format != "json" {
						fmt.Fprintf(os.Stdout, "dropped partition %s\n", name)
					}

					// We only reach the drop once the S3 copy is confirmed (the
					// pending-upload guard above), so removing the local staging
					// Parquet is safe — reads fall back to S3. Best-effort.
					if opts.PruneLocalAfterUpload && opts.ArchiveS3 != "" {
						if err := os.Remove(outPath); err != nil && !os.IsNotExist(err) {
							slog.Warn("could not prune local archive after S3 upload", "partition", name, "error", err)
						}
					}
				}
			} else {
				// No archiving — drop all expired partitions at once.
				// Filter out any partition that has a pending S3 upload from
				// a previous archived rotation run.
				//
				// Under opts.ProtectUnarchived, one more filter applies when the
				// index has archiving history: only already-archived partitions
				// may be dropped; the rest are deferred to the archiving flow.
				// An index with no archive_state rows at all rotates freely —
				// that is the bounded-volume quickstart behavior.
				var protectActive bool
				if opts.ProtectUnarchived {
					anyArchives, err := indexHasArchives(ctx, db)
					if err != nil {
						return Result{}, fmt.Errorf("check archive history: %w", err)
					}
					protectActive = anyArchives
				}
				var safeToDrop []string
				for _, name := range toDrop {
					if protectActive {
						archived, err := partitionArchived(ctx, db, name)
						if err != nil {
							return Result{}, fmt.Errorf("check archive state for %s: %w", name, err)
						}
						if !archived {
							deferredCount++
							slog.Warn("partition past retention but not yet archived; deferring drop to the archiving flow",
								"partition", name)
							if opts.Format != "json" {
								fmt.Fprintf(os.Stdout, "skipped drop for %s (not yet archived)\n", name)
							}
							continue
						}
					}
					pending, err := hasPendingS3Upload(ctx, db, name, opts.BintrailID)
					if err != nil {
						return Result{}, fmt.Errorf("check pending S3 upload for %s: %w", name, err)
					}
					if pending {
						slog.Warn("partition has pending S3 upload from a previous run; skipping drop",
							"partition", name)
						if opts.Format != "json" {
							fmt.Fprintf(os.Stdout, "skipped drop for %s (pending S3 upload)\n", name)
						}
						continue
					}
					safeToDrop = append(safeToDrop, name)
				}
				if len(safeToDrop) > 0 {
					if err := dropPartitions(ctx, db, dbName, safeToDrop); err != nil {
						return Result{}, fmt.Errorf("failed to drop partitions: %w", err)
					}
				}
				for _, name := range safeToDrop {
					// slog (not just stdout): rotation destroys data by design,
					// so the durable log must answer "what did rotation drop" —
					// mirrors the archive path's per-partition Info.
					slog.Info("dropped partition", "partition", name)
					if opts.Format != "json" {
						fmt.Fprintf(os.Stdout, "dropped partition %s\n", name)
					}
				}
				droppedCount = len(safeToDrop)
			}
			// Refresh list so nextPartitionStart sees current state.
			partitions, err = listPartitions(ctx, db, dbName)
			if err != nil {
				return Result{}, fmt.Errorf("failed to refresh partition list: %w", err)
			}
		}
	}

	// ── Warn if p_future already holds data ───────────────────────────────────
	hasFutureData, err := partitionHasData(ctx, db, dbName)
	if err != nil {
		slog.Warn("could not check p_future data", "error", err)
	} else if hasFutureData {
		slog.Warn("p_future partition contains data — events are arriving outside all named partition ranges; consider adding more future partitions with --add-future")
	}

	// ── Add new future partitions ─────────────────────────────────────────────
	// --add-future N is declarative: maintain at least N future hourly
	// partitions beyond the current hour. Top up only if headroom is below
	// target; never shrink. Unless --no-replace, also add one replacement
	// for each partition dropped this cycle.
	nowHour := time.Now().UTC().Truncate(time.Hour)
	futureCount := countFuturePartitions(partitions, nowHour)
	toAdd := computeToAdd(opts.AddFuture, futureCount, droppedCount, opts.NoReplace)
	if toAdd > 0 {
		startDate := nextPartitionStart(partitions)
		if err := addFuturePartitions(ctx, db, dbName, startDate, toAdd); err != nil {
			return Result{}, fmt.Errorf("failed to add future partitions: %w", err)
		}
		for i := range toAdd {
			if opts.Format != "json" {
				fmt.Fprintf(os.Stdout, "added partition %s\n", indexer.PartitionName(startDate.Add(time.Duration(i)*time.Hour)))
			}
		}
	}

	slog.Info("rotation complete",
		"partitions_dropped", droppedCount,
		"partitions_added", toAdd,
		"partitions_deferred", deferredCount,
		"duration_ms", time.Since(start).Milliseconds())

	return Result{Dropped: droppedCount, Added: toAdd, Deferred: deferredCount}, nil
}

// uploadFileFunc is the function used to upload a file to S3. It defaults to
// uploadFile and can be overridden in tests to simulate S3 failures.
var uploadFileFunc = storage.UploadFile

// ─── Helpers ─────────────────────────────────────────────────────────────────

// hasPendingS3Upload reports whether archive_state records a non-empty S3
// destination (s3_bucket) for the given partition that has not yet been uploaded
// (s3_uploaded_at IS NULL). When bintrailID is empty, it checks across all
// bintrail_ids for that partition. Returns false if no archive_state row exists
// or if the row has no S3 bucket (NULL or empty).
func hasPendingS3Upload(ctx context.Context, db *sql.DB, partition, bintrailID string) (bool, error) {
	var pending bool
	var err error
	if bintrailID != "" {
		err = db.QueryRowContext(ctx,
			`SELECT COUNT(*) > 0 FROM archive_state
			WHERE partition_name = ? AND bintrail_id = ?
			  AND s3_bucket IS NOT NULL AND s3_bucket != ''
			  AND s3_uploaded_at IS NULL`,
			partition, bintrailID,
		).Scan(&pending)
	} else {
		err = db.QueryRowContext(ctx,
			`SELECT COUNT(*) > 0 FROM archive_state
			WHERE partition_name = ?
			  AND s3_bucket IS NOT NULL AND s3_bucket != ''
			  AND s3_uploaded_at IS NULL`,
			partition,
		).Scan(&pending)
	}
	if err != nil {
		return false, err
	}
	return pending, nil
}

// indexHasArchives reports whether archive_state contains any rows at all —
// i.e. whether anything has ever archived partitions of this index.
func indexHasArchives(ctx context.Context, db *sql.DB) (bool, error) {
	var has bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM archive_state)`).Scan(&has)
	return has, err
}

// partitionArchived reports whether archive_state records a local archive for
// the partition under any bintrail_id. Completed-S3 status is checked
// separately by hasPendingS3Upload.
func partitionArchived(ctx context.Context, db *sql.DB, partition string) (bool, error) {
	var has bool
	err := db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM archive_state WHERE partition_name = ?)`,
		partition).Scan(&has)
	return has, err
}

// archivedRowCount returns the row count archive_state recorded for a
// partition — the number of rows written to its Parquet archive at the moment
// it was taken. A missing archive_state row yields an invalid NullInt64, which
// archivedPartitionUnchanged treats as "cannot verify" (never safe to drop).
func archivedRowCount(ctx context.Context, db *sql.DB, partition, bintrailID string) (sql.NullInt64, error) {
	var rc sql.NullInt64
	err := db.QueryRowContext(ctx,
		`SELECT row_count FROM archive_state WHERE partition_name = ? AND bintrail_id = ?`,
		partition, bintrailID).Scan(&rc)
	if errors.Is(err, sql.ErrNoRows) {
		return sql.NullInt64{}, nil
	}
	return rc, err
}

// partitionRowCount returns the live row count of a single binlog_events
// partition. Used by the archive-then-drop path to re-check, right before
// DROP PARTITION, that no rows landed in the partition after it was archived
// (the archive→drop TOCTOU, issue #779).
func partitionRowCount(ctx context.Context, db *sql.DB, dbName, partition string) (int64, error) {
	q := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`binlog_events` PARTITION (`%s`)", dbName, partition)
	var c int64
	err := db.QueryRowContext(ctx, q).Scan(&c)
	return c, err
}

// archivedPartitionUnchanged reports whether a partition is safe to drop after
// archiving: only when its archived snapshot row count is known AND still
// equals the partition's current live row count. binlog_events is append-only,
// so any difference means rows were inserted after the archive SELECT and would
// be lost by the drop; an unknown archived count is never safe.
func archivedPartitionUnchanged(archived sql.NullInt64, live int64) bool {
	return archived.Valid && archived.Int64 == live
}

// escapesLabelHour reports whether an archived partition's content-derived
// event_timestamp range [minTS, maxTS] extends outside the hour its name
// labels (#1037). False when the range is unknown (zero — empty partition) or
// the name is not an hourly partition.
func escapesLabelHour(partitionName string, minTS, maxTS time.Time) bool {
	label, ok := indexer.PartitionDate(partitionName)
	if !ok || minTS.IsZero() || maxTS.IsZero() {
		return false
	}
	labelStart := label.UTC()
	labelEnd := labelStart.Add(time.Hour)
	return minTS.UTC().Before(labelStart) || !maxTS.UTC().Before(labelEnd)
}

// fileExists reports whether a file exists and has a size greater than zero.
// Existence alone does NOT mean the file is a complete archive — see
// verifiedExistingArchive, which callers deciding whether to trust an
// on-disk file (--retry skip, or before a DROP PARTITION) must use instead.
func fileExists(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.Size() > 0
}

// verifiedExistingArchive reports whether the Parquet file at outPath can be
// trusted as a complete, previously-finished archive of partition — i.e.
// whether --retry may safely skip re-archiving it. It is trusted only when
// BOTH: (1) archive_state records a row count for this partition (a
// completed prior run inserts that row only after the write finishes — a
// crash mid-write never reaches it), and (2) the file's own Parquet footer
// opens cleanly and reports that same row count (issue #802: a truncated
// file has no valid footer, even though its size is greater than zero). A
// missing archive_state row or a footer/row-count mismatch both mean the
// file cannot be trusted, so this returns false and the caller re-archives.
func verifiedExistingArchive(ctx context.Context, db *sql.DB, outPath, partition, bintrailID string) (bool, error) {
	var recordedRows sql.NullInt64
	err := db.QueryRowContext(ctx,
		`SELECT row_count FROM archive_state WHERE partition_name = ? AND bintrail_id = ?`,
		partition, bintrailID,
	).Scan(&recordedRows)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	}
	if !recordedRows.Valid {
		return false, nil
	}
	fileRows, err := archive.ValidateArchiveFile(outPath)
	if err != nil {
		return false, nil // truncated/invalid footer: not trusted, not a hard error
	}
	return fileRows == recordedRows.Int64, nil
}

// HiveArchivePath returns the Hive-partitioned path for a binlog_events partition
// archive. The layout is:
//
//	<archiveDir>/bintrail_id=<uuid>/event_date=<YYYY-MM-DD>/event_hour=<HH>/events.parquet
//
// Each hourly partition maps to exactly one file. The event_hour= directory level
// enables DuckDB Hive partition pruning on hour-scoped queries.
func HiveArchivePath(archiveDir, bintrailID, partitionName string) (string, error) {
	d, ok := indexer.PartitionDate(partitionName)
	if !ok {
		return "", fmt.Errorf("cannot parse partition date from %q", partitionName)
	}
	return filepath.Join(
		archiveDir,
		"bintrail_id="+bintrailID,
		"event_date="+d.UTC().Format("2006-01-02"),
		fmt.Sprintf("event_hour=%02d", d.UTC().Hour()),
		"events.parquet",
	), nil
}

// partitionInfo holds metadata for a single table partition.
type partitionInfo struct {
	Name        string
	Description string // LESS THAN value or "MAXVALUE"
	Ordinal     int
}

// listPartitions returns all partitions for binlog_events ordered by ordinal position.
func listPartitions(ctx context.Context, db *sql.DB, dbName string) ([]partitionInfo, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT PARTITION_NAME, IFNULL(PARTITION_DESCRIPTION, ''), PARTITION_ORDINAL_POSITION
		FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events'
		ORDER BY PARTITION_ORDINAL_POSITION`,
		dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var partitions []partitionInfo
	for rows.Next() {
		var p partitionInfo
		if err := rows.Scan(&p.Name, &p.Description, &p.Ordinal); err != nil {
			return nil, err
		}
		partitions = append(partitions, p)
	}
	return partitions, rows.Err()
}

// dropPartitions drops one or more named partitions in a single ALTER TABLE statement.
func dropPartitions(ctx context.Context, db *sql.DB, dbName string, names []string) error {
	q := fmt.Sprintf("ALTER TABLE `%s`.`binlog_events` DROP PARTITION %s",
		dbName, strings.Join(names, ", "))
	_, err := db.ExecContext(ctx, q)
	return err
}

// partitionHasData reports whether the p_future catch-all partition holds any rows.
// Uses SELECT 1 ... LIMIT 1 rather than COUNT(*) for efficiency on large tables.
func partitionHasData(ctx context.Context, db *sql.DB, dbName string) (bool, error) {
	q := fmt.Sprintf("SELECT 1 FROM `%s`.`binlog_events` PARTITION (p_future) LIMIT 1", dbName)
	var dummy int
	err := db.QueryRowContext(ctx, q).Scan(&dummy)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// computeToAdd returns how many new future partitions to add in a rotation
// cycle given the declarative --add-future target, the current future-headroom
// count, the number of partitions dropped this cycle, and --no-replace.
//
// Semantics:
//   - Top up toward `target` if `futureCount < target`; never shrink.
//   - Unless `noReplace`, add one replacement per dropped partition on top of
//     the top-up so a drop-and-add rotation keeps total count flat.
func computeToAdd(target, futureCount, dropped int, noReplace bool) int {
	toAdd := 0
	if target > futureCount {
		toAdd = target - futureCount
	}
	if !noReplace {
		toAdd += dropped
	}
	return toAdd
}

// countFuturePartitions returns how many named hourly partitions start
// strictly after the given reference hour. p_future and unrecognised names
// are ignored.
func countFuturePartitions(partitions []partitionInfo, ref time.Time) int {
	n := 0
	for _, p := range partitions {
		d, ok := indexer.PartitionDate(p.Name)
		if !ok {
			continue
		}
		if d.After(ref) {
			n++
		}
	}
	return n
}

// nextPartitionStart returns the hour for the first new partition to add.
// It finds the latest p_YYYYMMDDHH partition and returns the following hour.
// Falls back to the current hour (UTC) if no named hourly partitions exist.
func nextPartitionStart(partitions []partitionInfo) time.Time {
	var maxDate time.Time
	for _, p := range partitions {
		d, ok := indexer.PartitionDate(p.Name)
		if !ok {
			continue
		}
		if d.After(maxDate) {
			maxDate = d
		}
	}
	if maxDate.IsZero() {
		return time.Now().UTC().Truncate(time.Hour)
	}
	return maxDate.Add(time.Hour)
}

// addFuturePartitions reorganizes p_future to insert n new hourly partitions
// beginning at startDate, then appends a new p_future MAXVALUE catch-all.
func addFuturePartitions(ctx context.Context, db *sql.DB, dbName string, startDate time.Time, n int) error {
	parts := make([]string, 0, n+1)
	for i := range n {
		d := startDate.Add(time.Duration(i) * time.Hour)
		nextHour := d.Add(time.Hour)
		parts = append(parts, fmt.Sprintf(
			"PARTITION %s VALUES LESS THAN (TO_SECONDS('%s'))",
			indexer.PartitionName(d),
			nextHour.UTC().Format("2006-01-02 15:04:05"),
		))
	}
	parts = append(parts, "PARTITION p_future VALUES LESS THAN MAXVALUE")

	q := fmt.Sprintf(
		"ALTER TABLE `%s`.`binlog_events` REORGANIZE PARTITION p_future INTO (\n\t%s\n)",
		dbName,
		strings.Join(parts, ",\n\t"),
	)
	_, err := db.ExecContext(ctx, q)
	return err
}
