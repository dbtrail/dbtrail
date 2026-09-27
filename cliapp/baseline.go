package cliapp

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/cliutil"
)

var baselineCmd = &cobra.Command{
	Use:   "baseline",
	Short: "Convert mydumper output to Parquet baseline snapshots",
	Long: `Reads mydumper per-table dump files (both SQL INSERT and TSV formats) and
converts them into Parquet files, one per table. The output preserves full
column typing and is suitable for audit reconstruction when combined with
binlog change events indexed by 'bintrail index'.

No database connection is required — this command operates purely on files.

Output structure:
  <output>/<timestamp>/<database>/<table>.parquet`,
	RunE: runBaseline,
}

var (
	bslInput          string
	bslOutput         string
	bslTimestamp      string
	bslTables         string
	bslCompression    string
	bslRowGroupSize   int
	bslUpload         string
	bslUploadRegion   string
	bslBaselineRetain string
	bslFormat         string
	bslRetry          bool
	bslEncrypt        bool
	bslEncryptKey     string
	bslBintrailID     string
)

func init() {
	baselineCmd.Flags().StringVar(&bslInput, "input", "", "mydumper output directory (required)")
	baselineCmd.Flags().StringVar(&bslOutput, "output", "", "Parquet output base directory (required)")
	baselineCmd.Flags().StringVar(&bslTimestamp, "timestamp", "", "Snapshot timestamp override (ISO 8601; default: from mydumper metadata)")
	baselineCmd.Flags().StringVar(&bslTables, "tables", "", "Comma-separated db.table filter (e.g. mydb.orders,mydb.items; default: all)")
	baselineCmd.Flags().StringVar(&bslCompression, "compression", "zstd", "Parquet compression codec: zstd, snappy, gzip, none")
	baselineCmd.Flags().IntVar(&bslRowGroupSize, "row-group-size", 500_000, "Rows per Parquet row group")
	baselineCmd.Flags().StringVar(&bslUpload, "upload", "", "S3 destination URL to upload Parquet files after generation (e.g. s3://my-bucket/baselines/). One writer per prefix")
	baselineCmd.Flags().StringVar(&bslUploadRegion, "upload-region", "", "AWS region for --upload (default: from AWS_REGION env var or ~/.aws/config)")
	baselineCmd.Flags().StringVar(&bslBaselineRetain, "baseline-retain", "", "Prune local snapshots older than this (Nd/Nh) once a durable S3 copy exists (requires --upload; never deletes the only copy or the newest snapshot per table)")
	baselineCmd.Flags().StringVar(&bslFormat, "format", "text", "Output format: text or json")
	baselineCmd.Flags().BoolVar(&bslRetry, "retry", false, "Skip tables whose output Parquet file already exists and S3 objects that were already uploaded")
	baselineCmd.Flags().BoolVar(&bslEncrypt, "encrypt", false, "Decrypt encrypted dump files before processing, verifying each file's .enc.hmac integrity sidecar first (requires openssl on $PATH)")
	baselineCmd.Flags().StringVar(&bslEncryptKey, "encrypt-key", "", "Path to encryption key file (default: ~/.config/bintrail/dump.key)")
	baselineCmd.Flags().StringVar(&bslBintrailID, "bintrail-id", "", "Server identity UUID to sign the snapshot with. One writer per --output folder or --upload prefix: two installations writing into one location mix their snapshots. Without it the snapshot is unsigned")
	_ = baselineCmd.MarkFlagRequired("input")
	_ = baselineCmd.MarkFlagRequired("output")
	bindCommandEnv(baselineCmd)

	rootCmd.AddCommand(baselineCmd)
}

func runBaseline(cmd *cobra.Command, args []string) error {
	if !cliutil.IsValidOutputFormat(bslFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", bslFormat)
	}
	if err := baseline.ValidateCodec(bslCompression); err != nil {
		return fmt.Errorf("--compression: %w", err)
	}

	// Decrypt encrypted dump files if --encrypt is set.
	if bslEncrypt {
		keyPath, err := resolveEncryptKey(bslEncryptKey)
		if err != nil {
			return err
		}
		cleanup, err := decryptDumpFiles(bslInput, keyPath)
		if err != nil {
			return fmt.Errorf("decrypt dump files: %w", err)
		}
		defer cleanup()
	}

	var ts time.Time
	if bslTimestamp != "" {
		var err error
		ts, err = time.Parse(time.RFC3339, bslTimestamp)
		if err != nil {
			// Try without timezone suffix
			ts, err = time.ParseInLocation("2006-01-02T15:04:05", bslTimestamp, time.UTC)
			if err != nil {
				ts, err = time.ParseInLocation("2006-01-02 15:04:05", bslTimestamp, time.UTC)
				if err != nil {
					return fmt.Errorf("--timestamp %q: expected ISO 8601 format (e.g. 2025-02-28T00:00:00Z)", bslTimestamp)
				}
			}
		}
	}

	cfg := baseline.Config{
		InputDir:     bslInput,
		OutputDir:    bslOutput,
		Timestamp:    ts,
		Tables:       parseTableFilter(bslTables),
		Compression:  bslCompression,
		RowGroupSize: bslRowGroupSize,
		Retry:        bslRetry,
		WriterID:     bslBintrailID,
	}

	stats, err := baseline.Run(cmd.Context(), cfg)
	if err != nil {
		return err
	}

	slog.Info("baseline complete",
		"tables", stats.TablesProcessed,
		"rows_written", stats.RowsWritten,
		"files_written", stats.FilesWritten)

	var uploaded int
	if bslUpload != "" {
		var err error
		uploaded, err = baseline.Upload(cmd.Context(), bslOutput, bslUpload, bslUploadRegion, bslRetry)
		if err != nil {
			return fmt.Errorf("S3 upload: %w", err)
		}
		if bslFormat != "json" {
			fmt.Printf("  uploaded  : %d files → %s\n", uploaded, bslUpload)
		}
		slog.Info("baseline S3 upload complete", "files", uploaded, "destination", bslUpload)
	}

	var prunedSnapshots int
	var reclaimedBytes int64
	retain, doPrune, err := resolveBaselineRetain(bslBaselineRetain, bslUpload)
	if err != nil {
		return err
	}
	if doPrune {
		res, err := baseline.PruneLocal(cmd.Context(), baseline.PruneOptions{
			LocalDir: bslOutput,
			S3URL:    bslUpload,
			S3Region: bslUploadRegion,
			Retain:   retain,
		})
		if err != nil {
			return fmt.Errorf("baseline prune: %w", err)
		}
		prunedSnapshots = len(res.Pruned)
		reclaimedBytes = res.ReclaimedBytes
		if prunedSnapshots > 0 {
			slog.Info("baseline prune complete", "pruned", prunedSnapshots, "reclaimed_bytes", reclaimedBytes)
			if bslFormat != "json" {
				fmt.Printf("  pruned    : %d local snapshots (%d bytes reclaimed)\n", prunedSnapshots, reclaimedBytes)
			}
		}
	}

	if bslFormat == "json" {
		result := struct {
			Tables       int    `json:"tables"`
			RowsWritten  int64  `json:"rows_written"`
			FilesWritten int    `json:"files_written"`
			Uploaded     int    `json:"uploaded,omitempty"`
			UploadDest   string `json:"upload_destination,omitempty"`
			Pruned       int    `json:"pruned_snapshots,omitempty"`
			Reclaimed    int64  `json:"reclaimed_bytes,omitempty"`
		}{
			Tables:       stats.TablesProcessed,
			RowsWritten:  stats.RowsWritten,
			FilesWritten: stats.FilesWritten,
			Pruned:       prunedSnapshots,
			Reclaimed:    reclaimedBytes,
		}
		if bslUpload != "" {
			result.Uploaded = uploaded
			result.UploadDest = bslUpload
		}
		return cliutil.OutputJSON(result)
	}

	fmt.Printf("Baseline complete.\n")
	fmt.Printf("  tables    : %d\n", stats.TablesProcessed)
	fmt.Printf("  rows      : %d\n", stats.RowsWritten)
	fmt.Printf("  files     : %d\n", stats.FilesWritten)
	return nil
}

// resolveBaselineRetain decides what --baseline-retain should do given --upload.
// It returns the parsed retention window and whether to actually prune:
//   - retainRaw empty            → no pruning (feature off).
//   - retainRaw malformed        → error (fail fast on a typo).
//   - retain set, upload empty   → loud no-op (doPrune false): a durable S3 copy
//     is the precondition for ever deleting a local snapshot (#616); without one
//     the local copy is the only copy and is never deleted.
//   - retain set, upload set     → prune.
//
// Extracted from runBaseline so the three outcomes are unit-testable without the
// dump→convert pipeline (mirrors the daemon's startBaselinePruneLoop gating).
func resolveBaselineRetain(retainRaw, upload string) (retain time.Duration, doPrune bool, err error) {
	if retainRaw == "" {
		return 0, false, nil
	}
	retain, err = cliutil.ParseRetain(retainRaw)
	if err != nil {
		return 0, false, fmt.Errorf("--baseline-retain: %w", err)
	}
	if upload == "" {
		slog.Warn("--baseline-retain is set but --upload is not; refusing to prune local snapshots (they are the only copy)")
		return retain, false, nil
	}
	return retain, true, nil
}

// decryptDumpFiles walks inputDir and decrypts every .enc file using openssl,
// writing the decrypted output alongside with the .enc extension stripped.
// Returns a cleanup function that removes the decrypted files.
//
// Before decrypting each file it verifies the HMAC-SHA256 sidecar written by
// `bintrail dump --encrypt` (#960): CBC gives no authentication, so a tampered
// or bit-rotted .enc would otherwise decrypt into garbled SQL that flows
// silently into the baseline. A sidecar mismatch is a hard error and the file
// is never handed to openssl; a MISSING sidecar (legacy pre-sidecar dump) only
// warns, so existing dumps keep working. The `.enc.hmac` sidecars themselves
// do not end in ".enc" and are skipped by the walk.
func decryptDumpFiles(inputDir, keyPath string) (func(), error) {
	absKey, err := filepath.Abs(keyPath)
	if err != nil {
		return nil, fmt.Errorf("resolve key path: %w", err)
	}

	// Raw key bytes double as the HMAC key. Read in-process — never on argv.
	hmacKey, err := readHMACKey(absKey)
	if err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(inputDir)
	if err != nil {
		return nil, fmt.Errorf("read input directory: %w", err)
	}

	var decrypted []string
	cleanup := func() {
		for _, f := range decrypted {
			os.Remove(f)
		}
	}

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".enc") {
			continue
		}
		encPath := filepath.Join(inputDir, e.Name())
		outPath := strings.TrimSuffix(encPath, ".enc")

		hadSidecar, verr := verifyEncFileHMAC(encPath, hmacKey)
		if verr != nil {
			cleanup()
			return nil, verr
		}
		if !hadSidecar {
			slog.Warn("no .hmac sidecar for encrypted dump file — legacy unauthenticated dump, integrity cannot be verified",
				"file", e.Name())
		}

		cmd := exec.Command("openssl", "enc", "-d", "-aes-256-cbc", "-pbkdf2",
			"-pass", "file:"+absKey, "-in", encPath, "-out", outPath)
		if output, err := cmd.CombinedOutput(); err != nil {
			cleanup()
			return nil, fmt.Errorf("decrypt %s: %w\n%s", e.Name(), err, output)
		}
		decrypted = append(decrypted, outPath)
		slog.Debug("decrypted", "file", e.Name())
	}

	if len(decrypted) == 0 {
		slog.Warn("no .enc files found in input directory; is the dump encrypted?", "dir", inputDir)
	}

	return cleanup, nil
}

// parseTableFilter splits a comma-separated "db.table" list.
func parseTableFilter(s string) []string {
	if s == "" {
		return nil
	}
	var result []string
	for part := range strings.SplitSeq(s, ",") {
		if t := strings.TrimSpace(part); t != "" {
			result = append(result, t)
		}
	}
	return result
}
