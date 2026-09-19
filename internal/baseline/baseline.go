// Package baseline converts mydumper output into Parquet files, enabling full
// audit reconstruction when combined with binlog change events.
package baseline

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/consistency"
)

// Version is embedded in Parquet file metadata.
const Version = "0.1.0"

// Config holds all parameters for a baseline conversion run.
type Config struct {
	InputDir     string
	OutputDir    string
	Timestamp    time.Time // zero = read from mydumper metadata
	Tables       []string  // "db.table" filter; nil = all
	Compression  string    // "zstd", "snappy", "gzip", "none"
	RowGroupSize int       // rows per row group
	Retry        bool      // skip tables whose output Parquet file already exists
	// TableDeltas: also write an EMPTY table delta beside every table (#1638),
	// so a full backup taken by a daemon running with table deltas has the
	// layout of the refreshes around it. See WriteEmptyTableDeltas.
	TableDeltas bool
}

// Stats describes the outcome of a baseline run.
type Stats struct {
	TablesProcessed int
	RowsWritten     int64
	FilesWritten    int
}

// Run converts a mydumper output directory into Parquet files.
func Run(ctx context.Context, cfg Config) (Stats, error) {
	// A dump `bintrail dump` refused stays on disk when there was no previous
	// dump to restore (#1744), so its refusal has to be honored here too.
	if reason, refused := ReadRefusedDumpMarker(cfg.InputDir); refused {
		return Stats{}, fmt.Errorf("%s holds a dump that bintrail dump refused, so it is not converted: %s", cfg.InputDir, reason)
	}

	// Resolve timestamp and binlog position from mydumper metadata.
	var (
		meta        DumpMetadata
		metaReadErr error // only the Timestamp-override path continues past one
	)
	ts := cfg.Timestamp
	if ts.IsZero() {
		var err error
		meta, err = ParseMetadata(cfg.InputDir)
		if err != nil {
			return Stats{}, fmt.Errorf("parse mydumper metadata: %w", err)
		}
		ts = meta.StartedAt
	} else {
		// Best-effort: try to get binlog position even with timestamp override.
		meta, metaReadErr = ParseMetadata(cfg.InputDir)
	}
	// Read, but no position (#1744): mydumper exits 0 when binary logging is
	// off, when the dump user cannot read the position, and for a build older
	// than 0.16.3 against MySQL 8.4. The conversion still runs (a dump made by
	// hand for another purpose is not this code's to refuse), but it must not
	// be silent: an update or restore from this baseline falls back to
	// timestamps.
	// Unreadable metadata lands here too, with an explicit Timestamp: it also
	// produces a baseline with no position, and used to say so at Info while
	// the read-but-empty case warned — the louder line belongs to both, since
	// the consequence is identical.
	if meta.BinlogFile == "" {
		attrs := []any{"input_dir", cfg.InputDir}
		if metaReadErr != nil {
			attrs = append(attrs, "error", metaReadErr)
		}
		slog.Warn("this dump records no binlog position, so the baseline will carry none and an update or restore "+
			"from it falls back to timestamps; check that binary logging is on, that the dump user has REPLICATION CLIENT, "+
			"and that mydumper is 0.16.3 or newer on MySQL 8.4 and later",
			attrs...)
	}

	// Discover tables.
	tables, err := DiscoverTables(cfg.InputDir)
	if err != nil {
		return Stats{}, fmt.Errorf("discover tables: %w", err)
	}
	if len(tables) == 0 {
		// A metadata-only dump is easy to produce with mydumper itself exiting
		// 0 (a --regex that matches nothing, a dump user lacking SELECT on the
		// requested schemas). Returning success here converted that into a
		// missing baseline that surfaces weeks later as ErrNoBaseline — or as
		// Time-travel silently reconstructing from an older snapshot (#461).
		return Stats{}, fmt.Errorf("no tables found in %s — the dump contains no table data; check the dump's schema filter and the dump user's SELECT privileges", cfg.InputDir)
	}

	// Apply table filter.
	if len(cfg.Tables) > 0 {
		discovered := len(tables)
		tables = filterTables(tables, cfg.Tables)
		if len(tables) == 0 {
			return Stats{}, fmt.Errorf("--tables filter %v matched none of the %d table(s) in the dump", cfg.Tables, discovered)
		}
	}

	// Timestamp string for directory name and metadata (colons → dashes for
	// filesystem compatibility).
	tsStr := ts.UTC().Format(time.RFC3339)
	tsDir := strings.ReplaceAll(tsStr, ":", "-")

	rowGroupSize := cfg.RowGroupSize
	if rowGroupSize <= 0 {
		rowGroupSize = 500_000
	}
	compression := cfg.Compression
	if compression == "" {
		compression = "zstd"
	}

	// Crash-safety (#467): create the snapshot directory and flag it _INCOMPLETE
	// BEFORE launching the workers. The graceful failure paths below leave this
	// marker in place; a successful run replaces it with _SUCCESS (which removes
	// _INCOMPLETE). Writing the marker only after wg.Wait() (the original code)
	// left a window where an UNCATCHABLE kill (OOM / SIGKILL / power loss)
	// mid-conversion produced a markerless partial snapshot that SnapshotComplete
	// trusts as complete-by-default and discovery serves as the newest baseline —
	// the exact #467 silent loss this marker exists to close. (It also fixes a
	// latent bug: a context cancelled before any table converted never created
	// snapDir, so the old post-wait markIncomplete wrote into a non-existent
	// directory and silently failed.) The per-table output dirs created lazily by
	// the writers nest under snapDir.
	snapDir := filepath.Join(cfg.OutputDir, tsDir)
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		return Stats{}, fmt.Errorf("create snapshot directory %s: %w", snapDir, err)
	}
	// The marker write is FATAL: marker-absent directories are
	// complete-by-default (legacy compat), so proceeding without _INCOMPLETE
	// and then dying uncatchably mid-conversion would leave a markerless
	// partial snapshot that discovery serves as complete — the very #467 hole
	// the marker closes. A run whose crash-safety net failed to deploy (e.g.
	// ENOSPC) has nothing to salvage; abort before any table converts.
	if err := WriteIncompleteMarker(snapDir); err != nil {
		return Stats{}, fmt.Errorf("could not write incomplete-snapshot marker in %s (refusing to convert without the crash-safety marker): %w", snapDir, err)
	}

	// Process tables in parallel with bounded concurrency.
	concurrency := runtime.NumCPU()
	if concurrency < 1 {
		concurrency = 1
	}
	sem := make(chan struct{}, concurrency)

	var (
		mu    sync.Mutex
		stats Stats
		errs  []error
	)

	var wg sync.WaitGroup
	for _, tf := range tables {
		tf := tf
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			if ctx.Err() != nil {
				return
			}

			outPath := filepath.Join(cfg.OutputDir, tsDir, tf.Database, tf.Table+".parquet")

			if cfg.Retry {
				if fi, err := os.Stat(outPath); err == nil && fi.Size() > 0 {
					// Size>0 alone is NOT proof the file is a usable Parquet
					// (#798): an OOM/SIGKILL mid-processTable can leave a
					// nonzero-but-truncated file with no footer. ReadParquetMetadata
					// parses the footer, so its error is the exact signal that this
					// "existing" file is corrupt — re-convert rather than bless the
					// truncated bytes (WriteManifest would otherwise CRC them and
					// publish _SUCCESS, exploding only at restore time). Only skip
					// when the file both exists AND parses as valid Parquet.
					existing, mErr := ReadParquetMetadata(outPath)
					if mErr != nil {
						slog.Warn("existing baseline file did not parse as valid Parquet (truncated/corrupt) — re-converting instead of skipping (--retry)",
							"db", tf.Database, "table", tf.Table, "file", outPath, "error", mErr)
						// fall through to re-convert
					} else {
						// A retry-skipped file keeps whatever digest it already has.
						// A file written before #633 (or by an older binary across a
						// mid-baseline upgrade) has none, and re-running --retry will
						// not heal it — surface that so the operator knows the
						// snapshot won't be fully verifiable without a fresh run.
						if existing.ContentDigest == "" {
							slog.Warn("skipped existing file has no content digest; this table won't be verifiable without a fresh baseline",
								"db", tf.Database, "table", tf.Table, "file", outPath)
						}
						slog.Info("skipping existing file (--retry)",
							"db", tf.Database, "table", tf.Table, "file", outPath)
						mu.Lock()
						stats.TablesProcessed++
						stats.FilesWritten++
						mu.Unlock()
						return
					}
				}
			}

			md := map[string]string{
				MetaKeySnapshotTimestamp:    tsStr,
				"bintrail.source_database":  tf.Database,
				"bintrail.source_table":     tf.Table,
				MetaKeyMydumperFormat:       tf.Format,
				"bintrail.bintrail_version": Version,
				// #1545: say so, rather than leaving every reader to infer a
				// dump from the ABSENCE of the fold's keys.
				MetaKeySnapshotProducer: ProducerDump,
			}
			if meta.BinlogFile != "" {
				md[MetaKeyBinlogFile] = meta.BinlogFile
			}
			if meta.BinlogPos != 0 {
				md[MetaKeyBinlogPos] = strconv.FormatInt(meta.BinlogPos, 10)
			}
			if meta.GTIDSet != "" {
				md[MetaKeyGTIDSet] = meta.GTIDSet
			}
			// Embed the raw mydumper <db>.<table>-schema.sql bytes so that
			// full-table reconstruct (#187) can emit a faithful schema file
			// without re-synthesising from Parquet column types. Non-fatal:
			// an older baseline or a schema-file read error just leaves the
			// key absent and full-table reconstruct will abort with a clear
			// "re-run bintrail baseline" message.
			if rawSchema, schemaErr := os.ReadFile(tf.SchemaFile); schemaErr != nil {
				slog.Warn("could not read schema file for CREATE TABLE embed",
					"db", tf.Database, "table", tf.Table,
					"path", tf.SchemaFile, "error", schemaErr)
			} else {
				md[MetaKeyCreateTableSQL] = string(rawSchema)
			}
			writerCfg := WriterConfig{
				Compression:  compression,
				RowGroupSize: rowGroupSize,
				Metadata:     md,
			}

			n, err := processTable(ctx, tf, outPath, writerCfg)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				slog.Error("failed to process table",
					"db", tf.Database, "table", tf.Table, "error", err)
				errs = append(errs, fmt.Errorf("%s.%s: %w", tf.Database, tf.Table, err))
				return
			}
			stats.TablesProcessed++
			stats.RowsWritten += n
			stats.FilesWritten++
			slog.Info("table complete",
				"db", tf.Database, "table", tf.Table,
				"rows", n, "file", outPath)
		}()
	}
	wg.Wait()

	// snapDir was created and flagged _INCOMPLETE before the workers launched
	// (see above), so every early return below leaves the snapshot positively
	// marked incomplete without re-writing the marker. Only full success
	// replaces it with _SUCCESS. A cancelled run skipped tables without
	// recording errors (workers return on ctx.Err()), so it too must stay
	// _INCOMPLETE rather than publish a partial snapshot as complete.
	if err := ctx.Err(); err != nil {
		return stats, err
	}
	if len(errs) > 0 {
		if len(errs) > 1 {
			return stats, fmt.Errorf("%d of %d tables failed (others logged); first: %w", len(errs), len(tables), errs[0])
		}
		return stats, errs[0]
	}
	// At-rest integrity manifest (#636): CRC-32C every Parquet file before the
	// _SUCCESS marker, so a complete snapshot ALWAYS carries its checksums. Fatal,
	// like the _SUCCESS write below: a complete-but-manifestless snapshot is an
	// undetectable downgrade — at read time it is indistinguishable from a legacy
	// (pre-#636) snapshot, so later corruption of its data would go unnoticed.
	// Re-run rather than publish one. (A read-time rotted manifest is a different
	// case — handled gracefully in ValidateLocalFile, not here.)
	// Before the manifest, which covers the pair like any other file.
	if cfg.TableDeltas {
		if err := WriteEmptyTableDeltas(snapDir); err != nil {
			return stats, err
		}
	}
	if err := baselineintegrity.WriteManifest(snapDir); err != nil {
		return stats, fmt.Errorf("snapshot complete but could not write integrity manifest: %w", err)
	}
	if err := WriteSuccessMarker(snapDir); err != nil {
		// The snapshot is complete on disk but unmarked; without the _SUCCESS
		// marker (and absent _INCOMPLETE) discovery still treats it as
		// complete-by-default, so this is a degraded-observability failure, not
		// a data one. Fail loud so the operator can re-run.
		return stats, fmt.Errorf("snapshot complete but could not write %s marker: %w", SuccessMarker, err)
	}
	return stats, nil
}

// processTable converts a single table's mydumper files to Parquet.
// Returns the number of rows written.
func processTable(ctx context.Context, tf TableFiles, outPath string, cfg WriterConfig) (int64, error) {
	// Parse schema.
	cols, err := ParseSchema(tf.SchemaFile)
	if err != nil {
		return 0, fmt.Errorf("parse schema: %w", err)
	}

	// Create writer.
	w, err := NewWriter(outPath, cols, cfg)
	if err != nil {
		return 0, fmt.Errorf("create writer: %w", err)
	}
	// Close file on error — on success we close below and return any error.
	var closed bool
	defer func() {
		if !closed {
			w.Close() //nolint
			if err := os.Remove(outPath); err != nil && !os.IsNotExist(err) {
				slog.Warn("failed to remove partial file", "path", outPath, "error", err)
			}
		}
	}()

	// Fingerprint the rows as they stream past, in MySQL column order. The
	// digest is byte-identical to a live consistency.ConsistentTableChecksum of
	// the same rows (#633), so the verify capstone (#634) can compare a baseline
	// against its source. Tapping the parser values (MySQL's text rendering, the
	// form ConsistentTableChecksum reads via the text protocol) costs one extra
	// hash per row and needs no second pass over the dump.
	//
	// Scope: this certifies that the dump captured the SAME ROWS as the source,
	// not that WriteRow encoded them faithfully into Parquet. WriteRow applies
	// value transforms the tap deliberately does NOT mirror — notably a MySQL
	// zero-date is stored as Parquet NULL while the tap (and the live checksum's
	// CAST(... AS CHAR)) both hash the "0000-00-00 00:00:00" string. Mirroring
	// WriteRow here would instead produce a false MISMATCH on every zero-date
	// table; the source-fidelity framing is the correct one. Parquet-encoding
	// fidelity (and the zero-date NULLing) is the verify capstone's concern.
	hasher := consistency.NewHasher()
	var rowCount int64
	rowFn := func(values []string, nulls []bool) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// A row's value count must match the schema's column count exactly.
		// WriteRow tolerates a short row by NULL-padding the missing tail
		// (mysqlIdx >= len(nulls)) — a defensive bounds check, not a supported
		// layout — so a genuine mismatch (e.g. ParseSchema's generated-column
		// detection missing an unusual DDL shape) would otherwise shift or
		// silently NULL a real column instead of failing loud (issue #767).
		if len(values) != len(cols) {
			return fmt.Errorf("row has %d values but schema %s has %d columns "+
				"(generated columns are excluded from both) — dump/schema mismatch for %s.%s",
				len(values), tf.SchemaFile, len(cols), tf.Database, tf.Table)
		}
		if err := w.WriteRow(values, nulls); err != nil {
			return err
		}
		hasher.AddStrings(values, nulls)
		rowCount++
		return nil
	}

	for _, dataFile := range tf.DataFiles {
		if ctx.Err() != nil {
			return rowCount, ctx.Err()
		}
		switch tf.Format {
		case "tab":
			if err := ReadTabFile(dataFile, len(cols), rowFn); err != nil {
				return rowCount, fmt.Errorf("read tab file %s: %w", dataFile, err)
			}
		case "sql":
			if err := ReadSQLFile(dataFile, rowFn); err != nil {
				return rowCount, fmt.Errorf("read sql file %s: %w", dataFile, err)
			}
		default:
			return rowCount, fmt.Errorf("unknown format %q", tf.Format)
		}
	}

	// Persist the content fingerprint and row count into the Parquet footer
	// before closing. SetMetadata upserts, so these win even if the same keys
	// were seeded at writer construction.
	w.SetMetadata(MetaKeyContentDigest, hasher.Digest())
	w.SetMetadata(MetaKeyRowCount, strconv.FormatInt(rowCount, 10))

	closed = true
	if err := w.Close(); err != nil {
		os.Remove(outPath)
		return rowCount, fmt.Errorf("close writer: %w", err)
	}
	return rowCount, nil
}

// filterTables returns only tables that match the "db.table" filter list.
func filterTables(tables []TableFiles, filter []string) []TableFiles {
	set := make(map[string]bool, len(filter))
	for _, f := range filter {
		set[strings.ToLower(f)] = true
	}
	var result []TableFiles
	for _, tf := range tables {
		key := strings.ToLower(tf.Database + "." + tf.Table)
		if set[key] {
			result = append(result, tf)
		}
	}
	return result
}
