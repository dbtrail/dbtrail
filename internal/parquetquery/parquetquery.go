// Package parquetquery implements a DuckDB-backed query engine for Parquet archive files.
// It reads Parquet files written by bintrail rotate --archive-dir (local) or stored in S3,
// and returns results in the same format as internal/query.
package parquetquery

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/big"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/duckdb/duckdb-go/v2"
	"golang.org/x/sync/errgroup"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// Fetch queries Parquet archive files (local or S3) using DuckDB and returns matching events.
// source is either a local directory path or an S3 URL prefix (s3://bucket/prefix/).
// Archives follow the Hive-partitioned layout written by bintrail rotate
// (event_date=YYYY-MM-DD/event_hour=HH/events.parquet).
//
// It applies bintrail's conservative, container-safe DuckDB budget
// (duckdbutil.DefaultTuning). Long-lived/shared callers (shim, console, agent)
// use this. The offline CLI commands that can afford more resources call
// FetchWithTuning to lift the cap (#510).
func Fetch(ctx context.Context, opts query.Options, source string) ([]query.ResultRow, error) {
	// Mirror internal/query's engine-side check so the keyset cursor is rejected
	// with DESC wherever the predicate can be emitted, not just on the paged
	// path that sets it today (#1097).
	if opts.AfterEvent != nil && query.OrderDirection(opts.Order) == "DESC" {
		return nil, errors.New("parquetquery: AfterEvent is a forward keyset cursor and cannot be combined with Order=DESC")
	}
	if opts.BeforeEvent != nil && query.OrderDirection(opts.Order) == "ASC" {
		return nil, errors.New("parquetquery: BeforeEvent is a backward keyset cursor and cannot be combined with Order=ASC")
	}
	return FetchWithTuning(ctx, opts, source, duckdbutil.DefaultTuning())
}

// FetchWithTuning is Fetch with an explicit DuckDB resource budget. See Tuning:
// the conservative DefaultTuning (threads=2, memory_limit=4GB) is for small
// containers; duckdbutil.Ultrafast lets DuckDB self-tune to the host.
func FetchWithTuning(ctx context.Context, opts query.Options, source string, tuning duckdbutil.Tuning) ([]query.ResultRow, error) {
	// Mirror internal/query's engine-side range check (#1440): the predicate
	// below is emitted from a cast the schema snapshot chose, and a range
	// that reaches this engine unresolved must be refused, never guessed.
	// Here rather than in Fetch because the tuned CLI path enters directly.
	if err := opts.ValidatePKRange(); err != nil {
		return nil, fmt.Errorf("parquetquery: %w", err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	// Use the OS temp directory for DuckDB scratch files. By default DuckDB
	// creates a .tmp directory in the CWD, which fails in containers where
	// the working directory (often /) is read-only. Set unconditionally: it is
	// the spill backstop, not a memory-for-speed knob, so it stays on even
	// under ultrafast — exceeding the memory budget spills here instead of
	// inviting the OOM-killer. Shared with every other Tuning.Apply call site
	// (internal/duckdbutil.SetTempDirectory) so there is one definition, not a
	// duplicated copy.
	duckdbutil.SetTempDirectory(ctx, db)

	// preserve_insertion_order is safe to disable because our queries have
	// explicit ORDER BY; disabling it lets DuckDB stream without buffering the
	// whole result set to reorder. It saves memory AND is faster, so it stays
	// on unconditionally — it is not part of the tunable trade-off.
	if _, err := db.ExecContext(ctx, "SET preserve_insertion_order = false"); err != nil {
		slog.Warn("could not configure DuckDB", "statement", "SET preserve_insertion_order = false", "error", err)
	}

	// Constrain DuckDB thread count and memory. DuckDB requires ~125MB per
	// thread; the conservative default (2 threads, 4GB) keeps small containers
	// alive, while ultrafast leaves both unset so DuckDB self-tunes to the host.
	tuning.Apply(ctx, db)

	// For S3, download each file locally via the AWS SDK and query with
	// DuckDB from disk. This avoids DuckDB's httpfs extension which holds
	// entire S3 files in memory (outside memory_limit tracking), causing
	// OOM kills in containers. Local reads use OS page cache / mmap.
	if strings.HasPrefix(source, "s3://") {
		// sinceHint, not opts.Since directly: when SincePos is set it widens the
		// lower bound by a safety margin so file/date scoping can never prune
		// away the very file a position-anchored fetch needs (see
		// sinceLowerBoundHint).
		sinceHint := sinceLowerBoundHint(opts)
		// untilHint, not opts.Until directly: a newest-first keyset cursor
		// lowers the top of the window with every page (see
		// untilUpperBoundHint). It is passed to classifyEmptyS3Listing below
		// as well, because that classification asks what filterFilesByTimeRange
		// could have discarded — so it must see the same bounds the filter used.
		untilHint := untilUpperBoundHint(opts)
		files, maxSize, region, s3Client, err := listS3ParquetScoped(ctx, source, sinceHint, untilHint, opts.ExtraArchiveHours)
		if err != nil {
			return nil, fmt.Errorf("list S3 archive files: %w", err)
		}
		// Pre-filter files by Hive partition values (event_date/event_hour)
		// to avoid downloading parquet files outside the requested time range.
		// ExtraArchiveHours (#1037) exempts misfiled files — archives whose
		// label hour lies outside the range but whose content overlaps it.
		files = scopeArchiveFiles(files, opts)
		// Sort chronologically so we can terminate early when --limit is satisfied.
		files = sortFilesByHour(files)
		slog.Debug("files after time-range pruning", "count", len(files))
		if len(files) == 0 {
			return classifyEmptyS3Listing(ctx, s3Client, source, sinceHint, untilHint)
		}

		// Ultrafast: read the S3 files directly via DuckDB httpfs in one
		// parallel multi-file scan, skipping the download-to-disk pipeline.
		// maxSize is pre-filter (a conservative over-estimate for the RAM warn).
		if tuning.S3Direct {
			return fetchS3Direct(ctx, db, files, region, maxSize, opts)
		}

		dl := newS3Downloader(s3Client)

		// Pipeline: prefetch up to maxInFlightDownloads files in parallel
		// while DuckDB queries the current one. Queries remain strictly
		// sequential — only one DuckDB query is active at a time, so peak
		// RAM (DuckDB's per-query working set) is unchanged from a serial
		// implementation. Peak temp files on disk at any instant:
		// maxInFlightDownloads + 1 (one being queried, the rest buffered
		// as completed prefetches).
		const maxInFlightDownloads = 2
		slots := make([]chan dlResult, len(files))
		for i := range slots {
			slots[i] = make(chan dlResult, 1)
		}
		dlCtx, cancelDl := context.WithCancel(ctx)
		defer cancelDl()
		go prefetchAll(dlCtx, files, slots, maxInFlightDownloads, dl.download)

		var results []query.ResultRow
		for i := range files {
			dr, ok := <-slots[i]
			if !ok {
				// Slot closed without a value (download canceled); stop.
				break
			}
			if dr.err != nil {
				cancelDl()
				drainSlots(slots[i+1:])
				return nil, fmt.Errorf("download archive file %s: %w", dr.src, dr.err)
			}

			fileResults, qErr := queryLocalFile(ctx, db, dr.path, dr.src, opts)
			removeTempFile(dr.path)
			if qErr != nil {
				cancelDl()
				drainSlots(slots[i+1:])
				return nil, qErr
			}
			results = append(results, fileResults...)
			slog.Debug("queried archive file", "file", dr.src, "rows", len(fileResults))

			// Per-file early termination: stop as soon as no remaining file
			// can produce a row earlier than what we already have.
			if opts.Limit > 0 && len(results) >= opts.Limit && i+1 < len(files) {
				if canTerminateEarly(results, files[i+1:], opts.Limit, opts.Order, opts.ExtraArchiveHours) {
					slog.Debug("early termination: collected enough results",
						"collected", len(results), "limit", opts.Limit,
						"remaining_files", len(files)-i-1)
					cancelDl()
					drainSlots(slots[i+1:])
					break
				}
			}
		}
		return results, nil
	}

	glob := buildGlob(source)
	cols, colErr := parquetColumns(ctx, db, glob)
	if colErr != nil {
		return nil, fmt.Errorf("read parquet schema: %w", colErr)
	}
	reportDigestCoverage(ctx, db, "'"+strings.ReplaceAll(glob, "'", "''")+"'", source, opts)
	q, args := buildQueryForFile(glob, opts, cols)
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("parquet query: %w", err)
	}
	defer rows.Close()
	return scanRows(rows)
}

// fetchS3Direct reads the S3 archive files directly through DuckDB's httpfs
// extension as a single parallel multi-file scan, instead of the default
// download-to-disk pipeline. This is the ultrafast S3 lever (tuning.S3Direct),
// mirroring the local-glob path above (probe columns → one query) but over
// s3:// paths.
//
// httpfs holds each scanned file in memory OUTSIDE DuckDB's memory_limit, so
// peak RAM ≈ largestFile × DuckDB threads. The caller gates this behind the
// explicit --ultrafast opt-in; we warn with the estimate so the operator can
// judge it against free RAM (lowering --duckdb-threads bounds the peak). The
// Go-side early termination and per-file streaming are given up here — DuckDB
// applies the global ORDER BY + LIMIT (top-N) natively across all files.
func fetchS3Direct(ctx context.Context, db *sql.DB, files []string, region string, maxFileSize int64, opts query.Options) ([]query.ResultRow, error) {
	if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
		return nil, fmt.Errorf("load DuckDB httpfs for S3-direct read: %w", err)
	}
	// Pin the bucket's region so httpfs does not 301/PermanentRedirect on a
	// cross-region bucket. The AWS-SDK download path gets this from
	// GetBucketLocation; httpfs only knows what the session is told.
	if region != "" {
		if _, err := db.ExecContext(ctx, "SET s3_region='"+strings.ReplaceAll(region, "'", "''")+"'"); err != nil {
			slog.Warn("could not set DuckDB s3_region for S3-direct read", "region", region, "error", err)
		}
	}
	// Pin the region IN the secret (not just the session SET above): DuckDB's
	// secrets manager can take precedence over SET s3_region, and a
	// credential_chain secret otherwise resolves region from the AWS SDK config,
	// not the bucket. Both together cover every precedence model — and the SET
	// still applies when the aws extension is unavailable and no secret exists.
	if err := duckdbutil.EnableS3CredentialChainRegion(ctx, db, region); err != nil {
		return nil, err
	}

	threads := duckDBThreadCount(ctx, db)
	warnAttrs := []any{"files", len(files), "duckdb_threads", threads}
	if maxFileSize > 0 {
		warnAttrs = append(warnAttrs, "largest_file_bytes", maxFileSize, "peak_ram_estimate_bytes", maxFileSize*int64(threads))
	} else {
		// S3 omitted object sizes for every file — print "unknown" rather than a
		// falsely reassuring 0 on the one path whose warning exists precisely to
		// make the operator weigh RAM before an OOM.
		warnAttrs = append(warnAttrs, "largest_file_bytes", "unknown (S3 omitted object sizes)")
	}
	slog.Warn("ultrafast S3-direct: reading S3 archives via DuckDB httpfs, held in memory OUTSIDE memory_limit — ensure free RAM exceeds the peak estimate; lower --duckdb-threads to bound it", warnAttrs...)

	return queryFileList(ctx, db, files, opts)
}

// queryFileList runs the single multi-file parquet_scan that backs the
// S3-direct path: probe the unioned columns, build one query over the whole
// list (DuckDB parallelizes the scan and applies the global ORDER BY + LIMIT),
// and scan the rows. Split out of fetchS3Direct so it can be tested over local
// parquet without the httpfs install/region setup — the file paths can be
// local or s3://; only the transport differs (#511).
func queryFileList(ctx context.Context, db *sql.DB, files []string, opts query.Options) ([]query.ResultRow, error) {
	cols, err := parquetColumnsFromFiles(ctx, db, files)
	if err != nil {
		return nil, fmt.Errorf("read parquet schema: %w", err)
	}
	reportDigestCoverage(ctx, db, fileArrayLiteral(files), "s3-direct", opts)
	q, args := buildQueryFromFiles(files, opts, cols)
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("multi-file parquet query: %w", err)
	}
	defer rows.Close()
	return scanRows(rows)
}

// duckDBThreadCount reports the session's effective DuckDB thread count for the
// httpfs peak-RAM warning. Falls back to NumCPU only if the setting genuinely
// can't be read (query/scan error); when threads is merely unset under
// ultrafast, current_setting returns the resolved one-per-core value directly.
func duckDBThreadCount(ctx context.Context, db *sql.DB) int {
	var n int
	if err := db.QueryRowContext(ctx, "SELECT current_setting('threads')").Scan(&n); err != nil || n <= 0 {
		return runtime.NumCPU()
	}
	return n
}

// parquetColumnsFromFiles probes the unioned column set across an explicit list
// of parquet files (local or s3://), so buildQueryFromFiles can substitute a
// typed NULL for columns absent from every file (e.g. connection_id in archives
// written before that column existed).
func parquetColumnsFromFiles(ctx context.Context, db *sql.DB, files []string) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, "SELECT * FROM parquet_scan("+fileArrayLiteral(files)+", hive_partitioning=true, union_by_name=true) LIMIT 0")
	if err != nil {
		return nil, err
	}
	names, err := rows.Columns()
	rows.Close()
	if err != nil {
		return nil, err
	}
	cols := make(map[string]bool, len(names))
	for _, n := range names {
		cols[n] = true
	}
	return cols, nil
}

// s3ClientForBucket builds the client the archive listing and downloads use,
// and the region it signs with. A bucket with its own store (#1575) goes
// there, with the store's endpoint, style and region, and no detection call
// is made against the ambient endpoint (for a MinIO bucket that is AWS,
// where a same-named bucket may be someone else's). Any other bucket keeps
// the detected region on the ambient configuration.
func s3ClientForBucket(ctx context.Context, bucket string) (*s3.Client, string, error) {
	if _, routed := storage.BucketStoreFor(bucket); routed {
		client, err := storage.NewS3ClientForBucket(ctx, bucket, "")
		if err != nil {
			return nil, "", err
		}
		return client, client.Options().Region, nil
	}
	cfg, err := storage.LoadAWSConfig(ctx, "")
	if err != nil {
		return nil, "", err
	}
	// The read path ignores the detected flag on purpose: it is about to
	// read, so a wrong guess fails here and loudly.
	bucketRegion, _ := storage.DetectBucketRegion(ctx, cfg, bucket)
	client := storage.NewS3ClientFromConfig(cfg, func(o *s3.Options) {
		o.Region = bucketRegion
	})
	return client, bucketRegion, nil
}

// listS3ParquetScoped lists .parquet files under an S3 prefix, optionally scoping
// to date-specific prefixes when since/until are provided and span ≤31 days.
// This avoids listing all files in the archive when only a narrow time range is needed.
// It returns the S3 client configured for the bucket's region so callers can reuse
// it for downloads without loading the AWS config again.
func listS3ParquetScoped(ctx context.Context, source string, since, until *time.Time, extraHours []time.Time) (files []string, maxSize int64, region string, client *s3.Client, err error) {
	bucket, prefix, err := parseS3Source(source)
	if err != nil {
		return nil, 0, "", nil, err
	}

	client, bucketRegion, err := s3ClientForBucket(ctx, bucket)
	if err != nil {
		return nil, 0, "", nil, err
	}

	// Generate date-scoped prefixes when time range is narrow enough.
	// This avoids listing thousands of irrelevant files for large archives.
	datePrefixes := generateDatePrefixes(prefix, since, until, extraHours)
	if datePrefixes == nil {
		// Wide range or no time bounds — list everything under the base prefix.
		datePrefixes = []string{prefix}
	}
	slog.Debug("S3 listing prefixes", "count", len(datePrefixes))

	// List objects under each date prefix concurrently.
	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(4)
	for _, dp := range datePrefixes {
		g.Go(func() error {
			paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
				Bucket: &bucket,
				Prefix: &dp,
			})
			for paginator.HasMorePages() {
				page, pageErr := paginator.NextPage(gctx)
				if pageErr != nil {
					return fmt.Errorf("list S3 objects under s3://%s/%s: %w", bucket, dp, pageErr)
				}
				mu.Lock()
				for _, obj := range page.Contents {
					if obj.Key != nil && strings.HasSuffix(*obj.Key, ".parquet") {
						files = append(files, fmt.Sprintf("s3://%s/%s", bucket, *obj.Key))
						// Track the largest object so the httpfs-direct path can
						// warn on its peak-RAM estimate (largest_file × threads).
						if obj.Size != nil && *obj.Size > maxSize {
							maxSize = *obj.Size
						}
					}
				}
				mu.Unlock()
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, 0, "", nil, err
	}

	slog.Debug("listed S3 archive files", "source", source, "count", len(files))
	return files, maxSize, bucketRegion, client, nil
}

// sinceLowerBoundHint computes the coarse, deliberately over-inclusive
// lower-bound hint used for archive file/date scoping when a caller pairs
// Since with an exact SincePos binlog-coordinate anchor (see
// query.Options.SincePos, #797). Mirrors internal/query's buildQuery: truncate
// to the hour, then back off ONE MORE full hour — archived events partition by
// event_timestamp (statement EXECUTION time), not binlog position, so a
// transaction that started before the anchor's hour but committed (and so
// gained its binlog position) after it can be filed under an earlier
// date/hour than the anchor's own. Returns opts.Since unchanged when SincePos
// is nil (nothing to widen for) or Since itself is nil.
// A keyset cursor (opts.AfterEvent, #1097) tightens the hint: every row still
// to be returned sorts at-or-after the cursor, so its event_timestamp is >= the
// cursor's, and no file whose hour ends before the cursor's hour can hold one.
// This is what keeps paginated archive reads near-linear instead of quadratic —
// without it every page would re-list and re-download the whole window's files
// and rely on the row-level filter to discard them, once per page.
func sinceLowerBoundHint(opts query.Options) *time.Time {
	hint := opts.Since
	if opts.Since != nil && opts.SincePos != nil {
		t := opts.Since.Truncate(time.Hour).Add(-time.Hour)
		hint = &t
	}
	if opts.AfterEvent != nil {
		// Floor to the hour: archive files are Hive-partitioned by
		// event_date/event_hour, so an hour-granular bound is the finest one
		// file scoping can act on, and flooring can only over-include.
		cur := opts.AfterEvent.Timestamp.Truncate(time.Hour)
		if hint == nil || cur.After(*hint) {
			hint = &cur
		}
	}
	return hint
}

// scopeArchiveFiles prunes a listed archive file set to the query's EFFECTIVE
// time bounds — the keyset-cursor-tightened ones, not the raw Since/Until.
//
// It exists as a named function rather than an inline call so the composition
// is reachable from a test. Passing opts.Since/opts.Until here instead of the
// hints is invisible to every correctness test (the row-level predicate still
// makes the exact cut, so results are identical) and merely re-downloads the
// whole window's files once per page — a regression that would only ever show
// up as an S3 bill. TestScopeArchiveFiles_prunesAboveTheCursor is what makes
// it fail loudly instead.
func scopeArchiveFiles(files []string, opts query.Options) []string {
	return filterFilesByTimeRange(files, sinceLowerBoundHint(opts), untilUpperBoundHint(opts), opts.ExtraArchiveHours)
}

// untilUpperBoundHint is sinceLowerBoundHint's mirror for newest-first paging
// (#1297): a Options.BeforeEvent cursor tightens the archive file/date scoping
// upper bound, because every row still to be returned sorts at-or-before the
// cursor and so cannot live in a file whose hour STARTS after the cursor's.
//
// It exists for the same reason the AfterEvent half of sinceLowerBoundHint
// does: without it every newest-first page would re-list and re-download the
// whole window's files and lean on the row-level filter to throw them away,
// once per page — quadratic S3 traffic behind a Next button.
//
// The cursor's hour is CEILED (truncate, then add an hour) rather than
// floored: the cursor's own hour still holds the events immediately below the
// page break, and Hive scoping is hour-granular, so flooring would prune the
// very file the next page must read. Over-including one hour is free; the
// row-level predicate makes the exact cut.
//
// Returns opts.Until unchanged when no cursor is set.
func untilUpperBoundHint(opts query.Options) *time.Time {
	hint := opts.Until
	if opts.BeforeEvent != nil {
		cur := opts.BeforeEvent.Timestamp.Truncate(time.Hour).Add(time.Hour)
		if hint == nil || cur.Before(*hint) {
			hint = &cur
		}
	}
	return hint
}

// maxScopedDays is the maximum number of days for prefix-scoped S3 listing.
// Beyond this, we fall back to listing the full prefix.
const maxScopedDays = 31

// generateDatePrefixes returns date-scoped S3 prefixes for Hive-partitioned
// archives (event_date=YYYY-MM-DD/). Returns nil when the range is too wide,
// no time bounds are provided, or since is nil, signaling the caller to list
// the full prefix. Assumes event_date= directories are directly under
// basePrefix (the layout written by bintrail rotate --archive-dir).
//
// since is required to scope a start day: with since==nil (until-only, or no
// bounds at all) there is no lower bound to anchor a start day on, so this
// used to invent one (now - maxScopedDays). That silently dropped archived
// data older than that invented window when until fell within it, and
// collapsed to a single bogus day when until was older still (#774). Listing
// everything and letting until alone filter downstream (filterFilesByTimeRange)
// is correct in both cases, just less scoped.
//
// Until-only queries are NOT rare: no CLI command marks --since required
// (query/recover/reconstruct/verify all accept --until alone), the MCP tool
// schema marks Since optional (omitempty), the console's request builders
// impose no such requirement, and #774's own reproduction is exactly this
// shape (`bintrail query --until ...` with no --since, and the agent's
// HandleRecover, which is TimeEnd-only). The real tradeoff: this trades some
// S3 listing performance on that reachable path for correctness — no
// listing scope narrower than "everything" is safe without a lower bound.
// extraHours (#1037) are misfiled-archive hour labels whose files must be
// findable even though those labels fall outside [since, until]: their DATE
// prefixes are appended to the scoped listing so the files get listed at all.
// They never shrink the listing — with scoping disabled (nil return)
// everything is listed and no extra prefixes are needed.
func generateDatePrefixes(basePrefix string, since, until *time.Time, extraHours []time.Time) []string {
	if since == nil {
		return nil
	}
	start := since.Truncate(24 * time.Hour)

	// Use the end-of-day (truncate to day) for the until bound.
	// When until is nil, include today fully.
	end := time.Now().UTC().Truncate(24 * time.Hour)
	if until != nil {
		end = until.Truncate(24 * time.Hour)
	}

	days := int(end.Sub(start).Hours()/24) + 1
	if days <= 0 {
		days = 1
	}
	if days > maxScopedDays {
		return nil
	}

	seen := make(map[string]bool, days+len(extraHours))
	prefixes := make([]string, 0, days+len(extraHours))
	add := func(day string) {
		if !seen[day] {
			seen[day] = true
			prefixes = append(prefixes, basePrefix+"event_date="+day+"/")
		}
	}
	for d := range days {
		add(start.AddDate(0, 0, d).Format("2006-01-02"))
	}
	for _, h := range extraHours {
		add(h.UTC().Format("2006-01-02"))
	}
	return prefixes
}

// classifyEmptyS3Listing decides what a zero-file S3 listing means (#383).
// "No files" is ambiguous whenever ANY time bound is in play, so a healthy
// source with a legitimately empty range (fine — empty result) must be told
// apart from a REGISTERED source whose objects vanished after archive_state
// was written (stale registration — must fail loud: the planner already
// counted these hours as covered). Disambiguates with one unscoped probe of
// the base prefix.
//
// The probe-vs-fail-fast decision is NOT based on whether generateDatePrefixes
// scoped the S3 LIST call itself (that only controls how many objects get
// listed, an optimization) — it is based on whether filterFilesByTimeRange
// could have discarded real files after listing. filterFilesByTimeRange is a
// true no-op only when BOTH since and until are nil (see its own since==nil
// check): in every other case — including since==nil with a narrow/old
// until, and the since!=nil/range>maxScopedDays case — the S3 listing may
// have been unscoped (full-prefix) yet still have contained real files that
// the downstream time filter removed. Skipping the probe there would
// misclassify a query whose window simply predates (or excludes) all
// archived data — a healthy empty result — as SourceEmptyError. Only when
// there is no time bound at all does "zero files listed" already equal
// "zero files after filtering", making the probe redundant.
//
// Extracted from Fetch's S3 branch so the decision table is unit-testable
// with a faked s3BaseHasParquet (the function itself never touches the
// client — it only forwards it to the probe).
func classifyEmptyS3Listing(ctx context.Context, client *s3.Client, source string, since, until *time.Time) ([]query.ResultRow, error) {
	bucket, prefix, err := parseS3Source(source)
	if err != nil {
		return nil, fmt.Errorf("parse S3 archive source: %w", err)
	}
	if since != nil || until != nil {
		has, probeErr := s3BaseHasParquet(ctx, client, bucket, prefix)
		if probeErr != nil {
			return nil, fmt.Errorf("probe S3 archive source %s: %w", source, probeErr)
		}
		if has {
			slog.Warn("no .parquet files in the queried date range (source itself is healthy)", "source", source)
			return nil, nil
		}
	}
	return nil, &query.SourceEmptyError{Source: source}
}

// s3BaseHasParquet reports whether ANY .parquet object exists under
// bucket/prefix, regardless of date. It is the stale-registration probe
// (#383): a registered source whose date-scoped listing came back empty is
// only healthy if the base prefix still holds parquet somewhere. It
// paginates past non-parquet keys — a MaxKeys=1 shortcut would false-report
// "empty" when the lexicographically-first key is a _SUCCESS marker, a
// cosign .sig, or an inventory manifest. Package var so tests can inject a
// fake (the downloadFn precedent — the real client needs live S3).
var s3BaseHasParquet = func(ctx context.Context, client *s3.Client, bucket, prefix string) (bool, error) {
	var token *string
	for {
		out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &bucket,
			Prefix:            &prefix,
			ContinuationToken: token,
		})
		if err != nil {
			return false, err
		}
		for _, obj := range out.Contents {
			if obj.Key != nil && strings.HasSuffix(*obj.Key, ".parquet") {
				return true, nil
			}
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			return false, nil
		}
		token = out.NextContinuationToken
	}
}

// s3Downloader holds a reusable S3 client to avoid re-creating AWS config
// on every file download.
type s3Downloader struct {
	client *s3.Client
}

func newS3Downloader(client *s3.Client) *s3Downloader {
	return &s3Downloader{client: client}
}

// download fetches an S3 parquet file to a temporary local file.
// The caller must os.Remove the file when done.
func (d *s3Downloader) download(ctx context.Context, s3URL string) (string, error) {
	rest := strings.TrimPrefix(s3URL, "s3://")
	bucket, key, _ := strings.Cut(rest, "/")
	if bucket == "" || key == "" {
		return "", fmt.Errorf("invalid S3 URL %q", s3URL)
	}

	resp, err := d.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return "", fmt.Errorf("download s3://%s/%s: %w", bucket, key, err)
	}
	defer resp.Body.Close()

	tmp, err := os.CreateTemp("", "bintrail-*.parquet")
	if err != nil {
		return "", fmt.Errorf("create temp file: %w", err)
	}
	if _, err := io.Copy(tmp, resp.Body); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", fmt.Errorf("write temp file: %w", err)
	}
	tmp.Close()
	var size int64
	if resp.ContentLength != nil {
		size = *resp.ContentLength
	}
	slog.Debug("downloaded archive file", "s3", s3URL, "local", tmp.Name(), "bytes", size)
	return tmp.Name(), nil
}

// dlResult carries the outcome of a single S3 file download to the consumer.
// path is the temp file path on disk (caller deletes); empty when err is set.
type dlResult struct {
	path string
	src  string
	err  error
}

// downloadFn fetches a remote file to a local temp path. The implementation
// returns ("", err) on failure or (path, nil) on success. Abstracted as a
// function (rather than a method on *s3Downloader) so tests can inject fakes.
type downloadFn func(ctx context.Context, src string) (string, error)

// prefetchAll downloads files into their slots with bounded parallelism.
// Each slot is closed exactly once — by the per-file goroutine if it ran,
// by prefetchAll directly if cancellation prevented launch, or by the
// goroutine's defer when cancellation arrives after download completes —
// so the consumer's <-slots[i] always unblocks.
func prefetchAll(ctx context.Context, files []string, slots []chan dlResult, maxInFlight int, download downloadFn) {
	sem := make(chan struct{}, maxInFlight)
	var wg sync.WaitGroup
	for i, f := range files {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			// Close slots we never launched so the consumer doesn't block.
			for k := i; k < len(slots); k++ {
				close(slots[k])
			}
			wg.Wait()
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			defer close(slots[i])
			path, err := download(ctx, f)
			if ctx.Err() != nil {
				// Consumer is gone; clean up our temp file rather than send.
				// Any error here is downstream of cancellation — a real S3
				// download error (closed connection, request canceled by the
				// transport, etc.) often does NOT wrap context.Canceled, so
				// classifying by error type produces false positives. Real
				// persistent S3 problems will resurface on the consumer's
				// next non-canceled query via dr.err.
				if err != nil {
					slog.Debug("download discarded after cancel", "src", f, "error", err)
				}
				removeTempFile(path)
				return
			}
			slots[i] <- dlResult{path: path, src: f, err: err}
		}()
	}
	wg.Wait()
}

// drainSlots consumes any pending prefetched results and removes their temp
// files. Called after the consumer stops reading (early termination or error)
// so we don't leak files prefetched before cancellation took effect.
func drainSlots(slots []chan dlResult) {
	for _, ch := range slots {
		if dr, ok := <-ch; ok {
			removeTempFile(dr.path)
		}
	}
}

// removeTempFile deletes a single temp file path, ignoring missing-file errors.
func removeTempFile(path string) {
	if path == "" {
		return
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		slog.Warn("failed to remove temp file", "path", path, "error", err)
	}
}

// queryLocalFile runs a single DuckDB query against a downloaded parquet file
// and returns the matching rows. The caller deletes the file after this returns.
func queryLocalFile(ctx context.Context, db *sql.DB, path, srcURL string, opts query.Options) ([]query.ResultRow, error) {
	cols, colErr := parquetColumns(ctx, db, path)
	if colErr != nil {
		return nil, fmt.Errorf("read parquet schema %s: %w", srcURL, colErr)
	}
	reportDigestCoverage(ctx, db, "'"+strings.ReplaceAll(path, "'", "''")+"'", srcURL, opts)
	q, args := buildQueryForFile(path, opts, cols)
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("parquet query %s: %w", srcURL, err)
	}
	defer rows.Close()
	return scanRows(rows)
}

// sortFilesByHour sorts S3 file paths chronologically by their Hive partition
// values (event_date/event_hour). Files with unparseable paths are placed at the end.
func sortFilesByHour(files []string) []string {
	sorted := make([]string, len(files))
	copy(sorted, files)
	slices.SortFunc(sorted, func(a, b string) int {
		ta, aOK := parseFileHour(a)
		tb, bOK := parseFileHour(b)
		if !aOK && !bOK {
			return cmp.Compare(a, b)
		}
		if !aOK {
			return 1 // unparseable goes to end
		}
		if !bOK {
			return -1
		}
		return ta.Compare(tb)
	})
	return sorted
}

// canTerminateEarly returns true when we've collected enough results for the
// limit and all remaining files are from later hours. Since files are processed
// in chronological (ASCENDING) order regardless of the requested output order
// (sortFilesByHour always sorts oldest-first), this heuristic is only valid
// when results are being accumulated in ascending time order: the limit-th
// earliest result becomes a cutoff, and any remaining file starting after
// that cutoff cannot contribute an earlier row.
//
// Under Order=DESC the caller wants the NEWEST rows, but iteration still
// starts at the OLDEST hour — an ASC-shaped cutoff computed from the first
// files read would sit inside the oldest hour and wrongly signal "done"
// before the newest hours (the ones that actually matter for DESC) are ever
// read (#773). There is no cheap symmetric cutoff here because file order
// and the desired row order are inverted, so DESC simply never terminates
// early — every candidate file is read and the final global sort+limit
// (query.MergeAndTrim) picks the true newest rows. Correctness over speed.
// extraHours (#1037) disables early termination entirely: a misfiled archive
// file sorts by its LABEL hour (late) yet can hold the window's EARLIEST rows,
// so a label-ordered cutoff would wrongly stop before reading it. Misfiled
// archives only exist after a backfill, so the cost is confined to that case.
func canTerminateEarly(results []query.ResultRow, remainingFiles []string, limit int, order string, extraHours []time.Time) bool {
	if len(extraHours) > 0 {
		return false
	}
	if query.OrderDirection(order) == "DESC" {
		return false
	}
	if len(results) < limit || len(remainingFiles) == 0 {
		return false
	}
	// Filter out drift rows with zero timestamp (dbtrail/bintrail#318).
	// They sort to year 0001 and would otherwise pin cutoff to the past,
	// making every remaining file's hour appear after it → early
	// termination silently drops all later real data.
	sorted := make([]query.ResultRow, 0, len(results))
	for _, r := range results {
		if !r.EventTimestamp.IsZero() {
			sorted = append(sorted, r)
		}
	}
	if len(sorted) < limit {
		// After filtering, we don't have enough real-timestamp rows to
		// ground a cutoff — keep reading.
		return false
	}
	slices.SortFunc(sorted, func(a, b query.ResultRow) int {
		if c := a.EventTimestamp.Compare(b.EventTimestamp); c != 0 {
			return c
		}
		return cmp.Compare(a.EventID, b.EventID)
	})
	cutoff := sorted[limit-1].EventTimestamp

	// If the next remaining file's hour starts after the cutoff, we can stop.
	nextHour, ok := parseFileHour(remainingFiles[0])
	if !ok {
		return false // can't determine, keep going
	}
	return nextHour.After(cutoff)
}

// parseS3Source extracts the bucket and prefix from an S3 URL.
// The prefix always ends with "/" for listing purposes.
func parseS3Source(source string) (bucket, prefix string, err error) {
	rest := strings.TrimPrefix(source, "s3://")
	bucket, prefix, _ = strings.Cut(rest, "/")
	if bucket == "" {
		return "", "", fmt.Errorf("empty bucket in S3 source %q", source)
	}
	// Ensure prefix ends with "/" so ListObjectsV2 scopes correctly.
	prefix = strings.TrimSuffix(prefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	return bucket, prefix, nil
}

// fileArrayLiteral renders a file list as a DuckDB array literal
// (['s3://...', '/tmp/...']) with single quotes escaped, for parquet_scan over
// an explicit list instead of a glob. A glob is avoided for S3 because DuckDB's
// glob expansion breaks on Hive partition keys (= signs) in the path.
func fileArrayLiteral(files []string) string {
	escaped := make([]string, len(files))
	for i, f := range files {
		escaped[i] = "'" + strings.ReplaceAll(f, "'", "''") + "'"
	}
	return "[" + strings.Join(escaped, ", ") + "]"
}

// buildQueryFromFiles constructs a DuckDB SQL query using an explicit list of
// file paths instead of a glob pattern. cols is the unioned column set across
// those files (from parquetColumnsFromFiles): a typed NULL is substituted for
// connection_id when it is absent from every file, matching buildQueryForFile
// so archives written before that column read back correctly.
func buildQueryFromFiles(files []string, opts query.Options, cols map[string]bool) (string, []any) {
	where, args := buildFilters(opts, cols)

	q := "SELECT event_id, binlog_file, start_pos, end_pos, event_timestamp," +
		" gtid, " + optionalCol(cols, "connection_id", "INT32") + ", schema_name, table_name, event_type, pk_values," +
		" changed_columns, row_before, row_after, schema_version," +
		" " + optionalCol(cols, "query_text", "VARCHAR") + ", " + optionalCol(cols, "query_hash", "VARCHAR") +
		", " + optionalCol(cols, "commit_ts_us", "BIGINT") +
		" FROM parquet_scan(" + fileArrayLiteral(files) + ", hive_partitioning=true, union_by_name=true)"
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	if qual, qArgs := limitPerPKClause(opts); qual != "" {
		q += qual
		args = append(args, qArgs...)
	}
	dir := query.OrderDirection(opts.Order)
	q += " ORDER BY event_timestamp " + dir + ", event_id " + dir
	if opts.Limit > 0 {
		q += " LIMIT ?"
		args = append(args, opts.Limit)
	}

	return q, args
}

// buildGlob converts a local directory path to a glob pattern that selects all
// Parquet archive files under that location. Only used for local paths — S3
// sources use listS3Parquet + buildQueryFromFiles instead.
func buildGlob(source string) string {
	s := strings.TrimSuffix(source, "/")
	if strings.HasSuffix(s, ".parquet") {
		return source
	}
	return s + "/**/*.parquet"
}

// buildUnsortedQuery constructs a DuckDB query for a single local parquet file
// without ORDER BY. Used for per-file S3 queries where the caller handles
// sorting after collecting all results. Skipping ORDER BY lets DuckDB stream
// rows without buffering the full result set, dramatically reducing memory.
func buildUnsortedQuery(path string, opts query.Options) (string, []any) {
	where, args := buildFilters(opts, nil)
	safePath := strings.ReplaceAll(path, "'", "''")

	q := "SELECT event_id, binlog_file, start_pos, end_pos, event_timestamp," +
		" gtid, connection_id, schema_name, table_name, event_type, pk_values," +
		" changed_columns, row_before, row_after, schema_version, query_text, query_hash, commit_ts_us" +
		" FROM parquet_scan('" + safePath + "', union_by_name=true)"
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	if qual, qArgs := limitPerPKClause(opts); qual != "" {
		q += qual
		args = append(args, qArgs...)
	}

	return q, args
}

// buildQuery constructs a DuckDB SQL query from a glob pattern (local paths only).
// The glob is embedded directly in the SQL because DuckDB table functions do not
// support bind parameters for the file path argument.
func buildQuery(glob string, opts query.Options) (string, []any) {
	where, args := buildFilters(opts, nil)

	// Escape single quotes in the glob path to prevent SQL injection.
	safeGlob := strings.ReplaceAll(glob, "'", "''")

	q := "SELECT event_id, binlog_file, start_pos, end_pos, event_timestamp," +
		" gtid, connection_id, schema_name, table_name, event_type, pk_values," +
		" changed_columns, row_before, row_after, schema_version, query_text, query_hash, commit_ts_us" +
		" FROM parquet_scan('" + safeGlob + "', hive_partitioning=true, union_by_name=true)"
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	if qual, qArgs := limitPerPKClause(opts); qual != "" {
		q += qual
		args = append(args, qArgs...)
	}
	dir := query.OrderDirection(opts.Order)
	q += " ORDER BY event_timestamp " + dir + ", event_id " + dir
	if opts.Limit > 0 {
		q += " LIMIT ?"
		args = append(args, opts.Limit)
	}

	return q, args
}

// limitPerPKClause returns the DuckDB QUALIFY fragment that caps the result
// to the latest LimitPerPK events per pk_values, plus the bind argument.
// Returns ("", nil) when LimitPerPK is unset. Inner ORDER BY DESC mirrors
// the MySQL ROW_NUMBER ordering in internal/query so both engines pick the
// same events for a given filter.
func limitPerPKClause(opts query.Options) (string, []any) {
	if opts.LimitPerPK <= 0 {
		return "", nil
	}
	return " QUALIFY ROW_NUMBER() OVER (PARTITION BY pk_values" +
		" ORDER BY event_timestamp DESC, event_id DESC) <= ?", []any{opts.LimitPerPK}
}

// posArg renders a binlog position for use as a DuckDB bind argument. The
// duckdb driver rejects a uint64 whose high bit is set ("uint64 values with
// high bit set are not supported"), and a >2^63 position is exactly the
// #986/#1117 underflow shape this filter must be able to anchor on (#1218) —
// so positions above int64 bind as *big.Int (HUGEINT), which DuckDB compares
// exactly against BIGINT, UBIGINT, and HUGEINT position columns alike. The
// common below-2^63 case keeps the plain uint64 fast path.
func posArg(pos uint64) any {
	if pos > math.MaxInt64 {
		return new(big.Int).SetUint64(pos)
	}
	return pos
}

// buildFilters extracts WHERE clause fragments and bind args from query options.
func buildFilters(opts query.Options, cols map[string]bool) ([]string, []any) {
	var where []string
	var args []any

	if opts.Schema != "" {
		where = append(where, "schema_name = ?")
		args = append(args, opts.Schema)
	}
	if opts.Table != "" {
		where = append(where, "table_name = ?")
		args = append(args, opts.Table)
	}
	if opts.PKValues != "" {
		where = append(where, "pk_values = ?")
		args = append(args, opts.PKValues)
	} else if len(opts.PKValuesIn) > 0 {
		placeholders := make([]string, len(opts.PKValuesIn))
		for i, v := range opts.PKValuesIn {
			placeholders[i] = "?"
			args = append(args, v)
		}
		where = append(where, "pk_values IN ("+strings.Join(placeholders, ",")+")")
	}
	if opts.PKRange != nil {
		// The archive mirror of internal/query's range predicate (#1440):
		// TRY_CAST over the Parquet pk_values column, bounds inlined. See
		// query.PKRange.DuckDBPredicates for why TRY_CAST and why inlined.
		where = append(where, opts.PKRange.DuckDBPredicates()...)
	}
	if opts.EventType != nil {
		where = append(where, "event_type = ?")
		args = append(args, int32(*opts.EventType))
	}
	if opts.GTID != "" {
		where = append(where, "gtid = ?")
		args = append(args, opts.GTID)
	}
	if opts.SincePos != nil {
		// Coarse, deliberately over-inclusive lower-bound filter only (mirrors
		// internal/query's buildQuery) — never the exact cut; see SincePos and
		// sinceLowerBoundHint. The exact correctness gate is the position
		// comparison below.
		if hint := sinceLowerBoundHint(opts); hint != nil {
			where = append(where, "event_timestamp >= ?")
			args = append(args, *hint)
		}
		// Exact binlog lower bound, mirroring UntilPos's rollover-safe file
		// ordering (#840) below, inverted for a lower bound.
		where = append(where, "(length(binlog_file) > length(?)"+
			" OR (length(binlog_file) = length(?) AND binlog_file > ?)"+
			" OR (binlog_file = ? AND start_pos >= ?))")
		args = append(args, opts.SincePos.File, opts.SincePos.File, opts.SincePos.File, opts.SincePos.File, posArg(opts.SincePos.Pos))
	} else if opts.Since != nil {
		where = append(where, "event_timestamp >= ?")
		args = append(args, *opts.Since)
	}
	if opts.Until != nil {
		where = append(where, "event_timestamp <= ?")
		args = append(args, *opts.Until)
	}
	if opts.UntilPos != nil {
		// Exact binlog upper bound, mirroring the live-MySQL path (query.go):
		// events whose end position is at-or-before the anchor. File order is
		// length-then-lexicographic so the .999999 → .1000000 suffix rollover
		// doesn't invert the cut (#840); DuckDB's length() counts characters,
		// matching MySQL's CHAR_LENGTH.
		where = append(where, "(length(binlog_file) < length(?)"+
			" OR (length(binlog_file) = length(?) AND binlog_file < ?)"+
			" OR (binlog_file = ? AND end_pos <= ?))")
		args = append(args, opts.UntilPos.File, opts.UntilPos.File, opts.UntilPos.File, opts.UntilPos.File, posArg(opts.UntilPos.Pos))
	}
	if opts.EventAnchor != nil {
		// The archive mirror of buildQuery's anchor block (#1411). No coarse
		// hint is emitted: file/date scoping on this side is driven by
		// Since/Until, and DuckDB prunes Parquet row groups from the equality
		// below via their event_timestamp statistics. Carrying the timestamp
		// alongside the unique event_id keeps a stale anchor returning nothing
		// rather than a different row.
		where = append(where, "event_timestamp = ? AND event_id = ?")
		args = append(args, opts.EventAnchor.Timestamp, opts.EventAnchor.EventID)
	}
	if opts.AfterEvent != nil {
		// Keyset cut on the composite sort key, mirroring internal/query's
		// buildQuery (#1097). No separate coarse hint is emitted here: the
		// archive side's equivalent of MySQL partition pruning is file/date
		// scoping, which sinceLowerBoundHint already advances with the cursor
		// before this filter is ever built. DuckDB additionally prunes Parquet
		// row groups from this predicate via their event_timestamp statistics.
		where = append(where, "(event_timestamp > ? OR (event_timestamp = ? AND event_id > ?))")
		args = append(args, opts.AfterEvent.Timestamp, opts.AfterEvent.Timestamp, opts.AfterEvent.EventID)
	}
	if opts.BeforeEvent != nil {
		// The newest-first mirror (#1297). As with AfterEvent no coarse hint is
		// emitted here — untilUpperBoundHint has already tightened file/date
		// scoping before this filter is built — and DuckDB prunes row groups
		// from this predicate via their event_timestamp statistics.
		where = append(where, "(event_timestamp < ? OR (event_timestamp = ? AND event_id < ?))")
		args = append(args, opts.BeforeEvent.Timestamp, opts.BeforeEvent.Timestamp, opts.BeforeEvent.EventID)
	}
	if opts.ChangedColumn != "" {
		needle, _ := json.Marshal(opts.ChangedColumn)
		where = append(where, "json_contains(changed_columns, ?)")
		args = append(args, string(needle))
	}
	if opts.QueryHash != "" {
		if cols != nil && !cols["query_hash"] {
			// The column exists in NONE of the scanned files, and parquet_scan
			// errors on a predicate over a column it cannot resolve — the same
			// trap optionalCol handles for the SELECT list, which is why this
			// cannot be left to the projection. Such a file provably holds no
			// event carrying any digest, so contributing nothing is the correct
			// answer, not a dropped filter.
			//
			// Reporting deliberately does NOT live here. cols is a UNION over
			// the scanned set on two of the three entry paths, so this branch
			// says nothing about the case that actually matters — a set MIXED
			// across the #699 upgrade, where the predicate IS emitted and the
			// older files quietly pad to NULL. reportDigestCoverage is where
			// that distinction is visible; see it for what the operator is told.
			where = append(where, "1=0")
		} else {
			// Lowercased because DuckDB compares strings case-sensitively while
			// MySQL's default collation does not: without this the same filter
			// would return live rows and drop their archived counterparts,
			// which reads as "the statement stopped touching rows" at exactly
			// the rotation boundary.
			where = append(where, "query_hash = ?")
			args = append(args, strings.ToLower(opts.QueryHash))
		}
	}
	for _, ce := range opts.ColumnEq {
		// DuckDB does not bind JSON paths either, so the column name is
		// interpolated; re-validate via the shared allowlist for the same
		// defense-in-depth reason as internal/query.buildQuery. DuckDB's
		// json_type returns uppercase 'NULL' for JSON null, matching MySQL's
		// JSON_TYPE — so the IsNull branch shape is identical on both engines.
		if !query.IsSafeColumnName(ce.Column) {
			slog.Error("parquetquery.buildFilters: rejected unsafe column name in ColumnEq filter; emitting no-match clause",
				"column", ce.Column)
			where = append(where, "1=0")
			continue
		}
		path := "$." + ce.Column
		if ce.IsNull {
			where = append(where, fmt.Sprintf(
				"(json_type(json_extract(row_after, '%s')) = 'NULL' "+
					"OR json_type(json_extract(row_before, '%s')) = 'NULL')",
				path, path))
			continue
		}
		where = append(where, fmt.Sprintf(
			"(json_extract_string(row_after, '%s') = ? "+
				"OR json_extract_string(row_before, '%s') = ?)",
			path, path))
		args = append(args, ce.Value, ce.Value)
	}

	return where, args
}

// parquetColumns returns the set of column names present in a parquet source.
// Works with both single files (S3 temp downloads) and glob patterns (local
// archives). For globs, union_by_name merges schemas across all matching
// files so the result reflects the full column superset.
func parquetColumns(ctx context.Context, db *sql.DB, path string) (map[string]bool, error) {
	safePath := strings.ReplaceAll(path, "'", "''")
	rows, err := db.QueryContext(ctx, "SELECT * FROM parquet_scan('"+safePath+"', hive_partitioning=true, union_by_name=true) LIMIT 0")
	if err != nil {
		return nil, err
	}
	names, err := rows.Columns()
	rows.Close()
	if err != nil {
		return nil, err
	}
	cols := make(map[string]bool, len(names))
	for _, n := range names {
		cols[n] = true
	}
	return cols, nil
}

// digestCoverageWarning renders what an operator must be told when only part of
// the scanned archive set can carry a statement digest. Pure so the wording and
// the boundaries are testable without DuckDB.
//
// Both non-empty verdicts describe the SAME row-level outcome — those files
// contribute nothing — and that outcome is correct. What is not acceptable is
// it happening silently: a narrower answer that looks identical to "the
// statement touched nothing" is the failure this whole filter is written
// against. Returns "" when every scanned file can answer, which is the steady
// state once every archive postdates the upgrade.
func digestCoverageWarning(withDigest, total int) string {
	switch {
	case total == 0 || withDigest >= total:
		return ""
	case withDigest == 0:
		return fmt.Sprintf("no archive file in this source has a statement-digest column (%d file(s), all written before statement capture); this source contributes no rows to a --query-hash answer", total)
	default:
		return fmt.Sprintf("%d of %d archive file(s) in this source predate statement capture and contribute no rows to a --query-hash answer", total-withDigest, total)
	}
}

// reportDigestCoverage probes how many of the scanned parquet files carry
// query_hash and warns when some or all of them cannot.
//
// scanTarget is the parquet_schema() argument the caller would pass to
// parquet_scan (a quoted glob, or an array literal from fileArrayLiteral);
// source names it in the warning, because a query can span several registered
// archive sources and "some files are old" is useless without knowing which.
//
// Best-effort by construction: it reads footers only, it runs ONLY under a
// digest filter, and a probe failure downgrades to a warning about the probe
// rather than failing a query whose rows are already correct.
func reportDigestCoverage(ctx context.Context, db *sql.DB, scanTarget, source string, opts query.Options) {
	if opts.QueryHash == "" {
		return
	}
	var total, withDigest int
	err := db.QueryRowContext(ctx,
		"SELECT count(DISTINCT file_name), count(DISTINCT CASE WHEN name = 'query_hash' THEN file_name END) FROM parquet_schema("+scanTarget+")").
		Scan(&total, &withDigest)
	if err != nil {
		slog.Warn("could not determine statement-digest coverage of this archive source; a narrower answer would go unreported",
			"source", source, "error", err)
		return
	}
	if w := digestCoverageWarning(withDigest, total); w != "" {
		slog.Warn("parquetquery: "+w, "source", source, "query_hash", opts.QueryHash)
	}
}

// optionalCol returns the bare column name when the scanned parquet source has
// it, or a typed-NULL alias when absent. This handles backward compatibility
// when reading archive files written before a schema-adding release (e.g.
// pre-v0.4.4 files lack connection_id; pre-#699 files lack query_text and
// query_hash) — parquet_scan with an explicit column list errors when the
// column exists in NONE of the scanned files, even under union_by_name.
func optionalCol(cols map[string]bool, name, sqlType string) string {
	if cols[name] {
		return name
	}
	return "NULL::" + sqlType + " AS " + name
}

// buildQueryForFile constructs a DuckDB query for a single parquet file,
// substituting typed NULLs for optional columns not present in the file.
func buildQueryForFile(path string, opts query.Options, cols map[string]bool) (string, []any) {
	where, args := buildFilters(opts, cols)
	safePath := strings.ReplaceAll(path, "'", "''")

	q := "SELECT event_id, binlog_file, start_pos, end_pos, event_timestamp," +
		" gtid, " + optionalCol(cols, "connection_id", "INT32") + ", schema_name, table_name, event_type, pk_values," +
		" changed_columns, row_before, row_after, schema_version," +
		" " + optionalCol(cols, "query_text", "VARCHAR") + ", " + optionalCol(cols, "query_hash", "VARCHAR") +
		", " + optionalCol(cols, "commit_ts_us", "BIGINT") +
		" FROM parquet_scan('" + safePath + "', hive_partitioning=true, union_by_name=true)"
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	if qual, qArgs := limitPerPKClause(opts); qual != "" {
		q += qual
		args = append(args, qArgs...)
	}
	dir := query.OrderDirection(opts.Order)
	q += " ORDER BY event_timestamp " + dir + ", event_id " + dir
	if opts.Limit > 0 {
		q += " LIMIT ?"
		args = append(args, opts.Limit)
	}
	return q, args
}

// filterFilesByTimeRange prunes the file list based on Hive partition values
// (event_date=YYYY-MM-DD/event_hour=HH) extracted from the paths. Files whose
// hour does not overlap with [since, until] are excluded. Files without
// parseable partition values are kept (safe fallback). Files whose hour label
// appears in extraHours are always kept (#1037): those are misfiled archives
// whose CONTENT overlaps the range even though the label does not — the
// row-level time filters downstream still bound what they contribute.
func filterFilesByTimeRange(files []string, since, until *time.Time, extraHours []time.Time) []string {
	if since == nil && until == nil {
		return files
	}
	extra := make(map[time.Time]bool, len(extraHours))
	for _, h := range extraHours {
		extra[h.UTC().Truncate(time.Hour)] = true
	}
	var filtered []string
	for _, f := range files {
		hourStart, ok := parseFileHour(f)
		if !ok {
			filtered = append(filtered, f) // can't determine; include to be safe
			continue
		}
		if extra[hourStart] {
			filtered = append(filtered, f) // misfiled archive: content overlaps
			continue
		}
		hourEnd := hourStart.Add(time.Hour)
		if since != nil && hourEnd.Before(*since) {
			continue // entire hour is before the since cutoff
		}
		if until != nil && hourStart.After(*until) {
			continue // entire hour is after the until cutoff
		}
		filtered = append(filtered, f)
	}
	return filtered
}

// parseFileHour extracts the hour start time from Hive partition path segments
// (event_date=YYYY-MM-DD/event_hour=HH). Returns zero time and false if the
// path does not contain both segments.
func parseFileHour(path string) (time.Time, bool) {
	var dateStr, hourStr string
	for _, seg := range strings.Split(path, "/") {
		if strings.HasPrefix(seg, "event_date=") {
			dateStr = strings.TrimPrefix(seg, "event_date=")
		} else if strings.HasPrefix(seg, "event_hour=") {
			hourStr = strings.TrimPrefix(seg, "event_hour=")
		}
	}
	if dateStr == "" || hourStr == "" {
		return time.Time{}, false
	}
	t, err := time.Parse("2006-01-02 15", dateStr+" "+hourStr)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// scanRows converts DuckDB result rows into []query.ResultRow.
func scanRows(rows *sql.Rows) ([]query.ResultRow, error) {
	var results []query.ResultRow
	for rows.Next() {
		// Every NOT NULL column is scanned defensively. The Parquet
		// writer in internal/archive now correctly preserves NULL for
		// every column its Scan saw as NULL, so the consumer side must
		// handle the same set. See dbtrail/bintrail#318. event_id stays
		// a bare int64 since AUTO_INCREMENT cannot be NULL.
		// start_pos/end_pos scan through sql.Null[uint64], not sql.NullInt64
		// (#1218): a >2^63 position (the #986/#1117 MariaDB underflow shape
		// written by pre-#1180 builds) does not fit the int64 path. The
		// driver hands back three shapes, one per archive generation mix —
		// int64 from a signed pre-#1218 file, uint64 from a current unsigned
		// file, and *big.Int when one union_by_name scan covers BOTH
		// generations (DuckDB promotes BIGINT ∪ UBIGINT to HUGEINT).
		// convertAssign takes all three losslessly into uint64 (*big.Int via
		// its exact decimal-string fallback); the int64 destination was the
		// one that failed, on two of the three. Pinned against real files by
		// TestFetch_mixedSignedUnsignedPositionArchives and siblings.
		var (
			eventID        int64
			binlogFile     sql.NullString
			startPos       sql.Null[uint64]
			endPos         sql.Null[uint64]
			eventTimestamp sql.NullTime
			gtid           sql.NullString
			connID         sql.NullInt64
			schemaName     sql.NullString
			tableName      sql.NullString
			eventType      sql.NullInt32
			pkValues       sql.NullString
			changedCols    sql.NullString
			rowBefore      sql.NullString
			rowAfter       sql.NullString
			schemaVersion  sql.NullInt32
			queryText      sql.NullString
			queryHash      sql.NullString
			commitTsUS     sql.NullInt64
		)
		if err := rows.Scan(
			&eventID, &binlogFile, &startPos, &endPos, &eventTimestamp,
			&gtid, &connID, &schemaName, &tableName, &eventType, &pkValues,
			&changedCols, &rowBefore, &rowAfter, &schemaVersion, &queryText, &queryHash,
			&commitTsUS,
		); err != nil {
			return nil, fmt.Errorf("scan parquet result: %w", err)
		}

		r := query.ResultRow{
			EventID:        uint64(eventID),
			BinlogFile:     binlogFile.String,
			StartPos:       startPos.V,
			EndPos:         endPos.V,
			EventTimestamp: eventTimestamp.Time,
			SchemaName:     schemaName.String,
			TableName:      tableName.String,
			EventType:      event.EventType(eventType.Int32),
			PKValues:       pkValues.String,
			SchemaVersion:  uint32(schemaVersion.Int32),
		}
		if gtid.Valid {
			r.GTID = &gtid.String
		}
		if connID.Valid {
			v := uint32(connID.Int64)
			r.ConnectionID = &v
		}
		if queryText.Valid {
			r.QueryText = &queryText.String
		}
		if queryHash.Valid {
			r.QueryHash = &queryHash.String
		}
		// Archives written before #18 have no commit_ts_us at all: optionalCol
		// substitutes a typed NULL, which lands here as invalid and leaves the
		// pointer nil — the same "only the one-second timestamp is known"
		// signal a live row from a MariaDB source carries.
		if commitTsUS.Valid && commitTsUS.Int64 > 0 {
			v := uint64(commitTsUS.Int64)
			r.CommitTsUS = &v
		}
		if changedCols.Valid && changedCols.String != "" {
			_ = json.Unmarshal([]byte(changedCols.String), &r.ChangedColumns)
		}
		if rowBefore.Valid && rowBefore.String != "" {
			r.RowBefore = query.UnmarshalRowImage([]byte(rowBefore.String))
		}
		if rowAfter.Valid && rowAfter.String != "" {
			r.RowAfter = query.UnmarshalRowImage([]byte(rowAfter.String))
		}
		results = append(results, r)
	}
	return results, rows.Err()
}
