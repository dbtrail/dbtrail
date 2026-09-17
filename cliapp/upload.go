package cliapp

import (
	"fmt"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/storage"
)

var uploadCmd = &cobra.Command{
	Use:   "upload",
	Short: "Upload local Parquet files to S3",
	Long: `Uploads Parquet files from a local directory to an S3 destination,
preserving the directory structure as S3 key prefixes.

This command is useful when Parquet files were created by 'baseline' or
'rotate --archive-dir' without the S3 upload flags (e.g. because the network
was down or credentials weren't configured at the time).

When --index-dsn is provided, the command also updates the archive_state table
with S3 metadata for files whose paths match the Hive-partitioned archive
layout (bintrail_id=<uuid>/event_date=<date>/event_hour=<hour>/events.parquet).

Examples:
  # Upload archive Parquet files created by a previous rotate
  bintrail upload \
    --source /var/lib/bintrail/archives/ \
    --destination s3://my-bucket/archives/

  # Upload baseline Parquet files, skipping already-uploaded ones
  bintrail upload \
    --source /var/lib/bintrail/baselines/2026-03-01T00-00-00Z/ \
    --destination s3://my-bucket/baselines/ \
    --retry

  # Upload and record in archive_state (for rotate archives)
  bintrail upload \
    --source /var/lib/bintrail/archives/ \
    --destination s3://my-bucket/archives/ \
    --index-dsn 'user:pass@tcp(localhost:3306)/binlog_index'`,
	RunE: runUpload,
}

var (
	uplSource      string
	uplDestination string
	uplRegion      string
	uplIndexDSN    string
	uplFormat      string
	uplRetry       bool
)

func init() {
	uploadCmd.Flags().StringVar(&uplSource, "source", "", "Local directory containing Parquet files to upload (required)")
	uploadCmd.Flags().StringVar(&uplDestination, "destination", "", "S3 destination URL (e.g. s3://my-bucket/archives/) (required)")
	uploadCmd.Flags().StringVar(&uplRegion, "region", "", "AWS region (default: from AWS_REGION env var or ~/.aws/config)")
	uploadCmd.Flags().StringVar(&uplIndexDSN, "index-dsn", "", "Update archive_state table with S3 metadata after upload")
	uploadCmd.Flags().StringVar(&uplFormat, "format", "text", "Output format: text or json")
	uploadCmd.Flags().BoolVar(&uplRetry, "retry", false, "Skip files that already exist in S3 (via HeadObject check)")
	_ = uploadCmd.MarkFlagRequired("source")
	_ = uploadCmd.MarkFlagRequired("destination")
	bindCommandEnv(uploadCmd)

	rootCmd.AddCommand(uploadCmd)
}

func runUpload(cmd *cobra.Command, args []string) error {
	if !cliutil.IsValidOutputFormat(uplFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", uplFormat)
	}

	bucket, prefix, err := storage.ParseS3URL(uplDestination)
	if err != nil {
		return fmt.Errorf("invalid --destination: %w", err)
	}

	// Verify source directory exists.
	info, err := os.Stat(uplSource)
	if err != nil {
		return fmt.Errorf("--source: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("--source %q is not a directory", uplSource)
	}

	ctx := cmd.Context()

	client, err := storage.NewS3Client(ctx, uplRegion)
	if err != nil {
		return err
	}

	start := time.Now()
	var uploaded, skipped int

	// Collect uploaded file info for optional archive_state updates.
	type uploadedFile struct {
		s3Key      string
		bintrailID string
		partName   string
	}
	var dbUpdates []uploadedFile

	err = filepath.WalkDir(uplSource, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return walkErr
		}
		if !uploadable(path) {
			return nil
		}

		key, err := storage.BuildS3Key(uplSource, path, prefix)
		if err != nil {
			return err
		}

		if uplRetry {
			exists, err := storage.S3ObjectExists(ctx, client, bucket, key)
			if err != nil {
				return err
			}
			if exists {
				slog.Info("skipping existing S3 object (--retry)", "bucket", bucket, "key", key)
				skipped++
				return nil
			}
		}

		if err := storage.UploadFile(ctx, client, path, bucket, key); err != nil {
			return err
		}
		slog.Debug("uploaded", "file", path, "bucket", bucket, "key", key)
		uploaded++

		if uplFormat != "json" {
			fmt.Fprintf(os.Stdout, "uploaded %s → s3://%s/%s\n", path, bucket, key)
		}

		if uplIndexDSN != "" {
			bintrailID, partName := archive.ParseArchivePath(path)
			if bintrailID != "" && partName != "" {
				dbUpdates = append(dbUpdates, uploadedFile{
					s3Key:      key,
					bintrailID: bintrailID,
					partName:   partName,
				})
			}
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("upload: %w", err)
	}

	// Update archive_state if --index-dsn was provided and we have matching files.
	var dbUpdated int
	if uplIndexDSN != "" && len(dbUpdates) > 0 {
		db, err := config.Connect(uplIndexDSN)
		if err != nil {
			return fmt.Errorf("connect to index DB: %w", err)
		}
		defer db.Close()

		for _, u := range dbUpdates {
			res, err := db.ExecContext(ctx,
				`UPDATE archive_state
					SET s3_bucket = ?, s3_key = ?, s3_uploaded_at = UTC_TIMESTAMP()
				WHERE partition_name = ? AND bintrail_id = ?`,
				bucket, u.s3Key, u.partName, u.bintrailID,
			)
			if err != nil {
				return fmt.Errorf("update archive_state for %s: %w", u.partName, err)
			}
			n, _ := res.RowsAffected()
			if n > 0 {
				dbUpdated++
			} else {
				slog.Warn("archive_state row not found, skipping DB update",
					"partition", u.partName, "bintrail_id", u.bintrailID)
			}
		}
		slog.Info("archive_state updated", "rows", dbUpdated)
	}

	duration := time.Since(start)
	slog.Info("upload complete",
		"uploaded", uploaded,
		"skipped", skipped,
		"archive_state_updated", dbUpdated,
		"duration_ms", duration.Milliseconds())

	if uplFormat == "json" {
		result := struct {
			Uploaded    int    `json:"uploaded"`
			Skipped     int    `json:"skipped"`
			Destination string `json:"destination"`
			DBUpdated   int    `json:"archive_state_updated,omitempty"`
			DurationMs  int64  `json:"duration_ms"`
		}{
			Uploaded:    uploaded,
			Skipped:     skipped,
			Destination: uplDestination,
			DBUpdated:   dbUpdated,
			DurationMs:  duration.Milliseconds(),
		}
		return cliutil.OutputJSON(result)
	}

	fmt.Printf("Upload complete.\n")
	fmt.Printf("  uploaded  : %d files → %s\n", uploaded, uplDestination)
	if skipped > 0 {
		fmt.Printf("  skipped   : %d files (already in S3)\n", skipped)
	}
	if dbUpdated > 0 {
		fmt.Printf("  db updated: %d archive_state rows\n", dbUpdated)
	}

	return nil
}

// uploadable reports whether `bintrail upload` sends a file: Parquet, and the
// two files of a table delta (#1638), which are Parquet under another suffix.
//
// The delta travels WITH its table, and that is not a nicety. A table file
// uploaded without it is not a smaller backup, it is a wrong one: the file is
// the table as it was when its chain of deltas started, under a directory that
// says otherwise, and a reader bounds its event window by that directory.
func uploadable(path string) bool {
	lower := strings.ToLower(path)
	return strings.HasSuffix(lower, ".parquet") ||
		strings.HasSuffix(lower, baseline.TableDeltaPosdelSuffix) ||
		strings.HasSuffix(lower, baseline.TableDeltaUpsertsSuffix)
}
