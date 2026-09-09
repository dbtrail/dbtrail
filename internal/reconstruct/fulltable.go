package reconstruct

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/duckdb/duckdb-go/v2" // DuckDB driver for parquet_scan baseline streaming
	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
)

// FullTableConfig drives ReconstructTables — the full-table merge-on-read
// entry point for #187. One Config instance covers one run; each table in
// Tables is reconstructed concurrently with at most Parallelism goroutines
// running at a time (a goroutine is spawned for every table, but a buffered-
// channel semaphore caps the number that actually do work).
//
// Supported primary-key types (#212 + #214):
//
//   - integer: int, smallint, tinyint, mediumint, bigint (+ unsigned)
//   - string: char, varchar, text, tinytext, mediumtext, longtext
//   - enum, set
//   - datetime, timestamp — canonicalized from DuckDB time.Time to the
//     go-mysql string format the indexer stores
//   - date — canonicalized to "2006-01-02"
//   - year
//   - decimal, numeric — pass-through string (go-mysql delivers a
//     pre-formatted string when useDecimal is false, which is the
//     bintrail default; baseline stores DECIMAL as parquet.String, so
//     both sides agree byte-for-byte)
//   - binary, varbinary, blob family — bytes; BINARY(n) padding trimmed (#1155)
//
// PK columns with any other type (FLOAT, DOUBLE, TIME, BIT, JSON, spatial
// types) are rejected at ReconstructTable entry with a hard error;
// supportedPKType in pk_canonicalize.go is the authoritative list. Adding a
// type means verifying its round-trip between event.BuildPKValues and the
// baseline Parquet end to end, so each is deferred behind its own follow-up
// issue.
//
// UPDATE events that mutate the primary key itself cannot be folded safely:
// the change map is keyed by the before-image PK, so a changed PK would emit
// the after-image row under the OLD key and resurrect a row a later DELETE
// removed, duplicate one another event already targets, or (when its old key is
// reused) silently drop the moved row (#782). Rather than ship a silently wrong
// dump, every full-table entry point detects such an UPDATE up front and
// REFUSES the run with an actionable error. The authoritative check
// (pkChangingUpdateInEvents) scans the RAW pre-collapse event slice — the map
// alone would miss a PK-changing UPDATE whose old key a later event reused
// (`UPDATE 1->2; INSERT 1`) — so the paths that fetch the full window (this dump
// path and the binlog-only fallback) catch every case; pkChangingUpdate is a
// cheap map backstop. Re-snapshot the baseline at or after the PK change and
// reconstruct from there.
// Output formats FullTableConfig.OutputFormat accepts.
const (
	// OutputFormatMydumper writes a mydumper-compatible SQL dump directory —
	// the original full-table output (#187), and what the zero value resolves to.
	OutputFormatMydumper = "mydumper"
	// OutputFormatParquet writes a baseline snapshot in Parquet (#1169): the same
	// artifact `bintrail baseline` produces from a mydumper dump, so the output of
	// a reconstruct can itself anchor the next one.
	OutputFormatParquet = "parquet"
)

// SnapshotDirName renders a snapshot's directory name from its target instant,
// byte-identical to what baseline.Run produces (RFC3339 UTC with ':' replaced
// by '-' for filesystem portability). The name IS the snapshot's timestamp as
// far as FindBaseline is concerned, so this formatting is a compatibility
// contract, not a display choice. Exported for the console's backup detail
// and download surfaces, which resolve one snapshot's directory from its time.
func SnapshotDirName(at time.Time) string {
	return strings.ReplaceAll(at.UTC().Format(time.RFC3339), ":", "-")
}

type FullTableConfig struct {
	IndexDSN    string    // DSN for the bintrail index database
	BaselineSrc string    // local directory or s3:// URL of baselines
	Tables      []string  // "db.table" entries
	At          time.Time // target point-in-time
	OutputDir   string    // output root: the mydumper dump directory, or the baselines root under OutputFormatParquet
	ChunkSize   int64     // per-chunk SQL file size (0 → 256 MiB)

	// OutputFormat selects the artifact the run produces. The zero value means
	// "not specified" and resolves to OutputFormatMydumper, matching this
	// struct's existing convention for ArchiveFetcher/WarnEventThreshold.
	//
	// Under OutputFormatParquet the run writes a discoverable BASELINE SNAPSHOT
	// (#1169) instead of a SQL dump, and OutputDir is read as the baselines root
	// — the same path `bintrail baseline --output` takes — not as the directory
	// the files land in directly. See emitParquetSnapshot for the layout and the
	// anchoring contract.
	OutputFormat string

	// snapshotDir and cut are per-run state ReconstructTables resolves once and
	// hands to each ReconstructTable goroutine. Unexported because they are
	// derived, not configured: setting them from outside would let a caller
	// anchor a snapshot at a coordinate unrelated to the events it folded.
	//
	// snapshotDir is <OutputDir>/<at, RFC3339 with ':' → '-'>, the directory
	// FindBaseline discovers. cut is the binlog coordinate the snapshot is
	// anchored at (nil when the index holds no events); see ResolveSnapshotCut.
	snapshotDir string
	cut         *query.BinlogPos
	Parallelism int  // max concurrent tables (0 → runtime.NumCPU())
	AllowGaps   bool // false = strict abort on gaps (default for reconstruct)

	// CarryForwardUnchanged publishes a table that had NO events in the window
	// by carrying its previous Parquet file forward (a hard link where the
	// filesystem allows one) instead of folding an empty change map over it and
	// re-emitting the same rows. OutputFormatParquet only.
	//
	// OPT-IN, and the zero value is the conservative one. The rows are the same
	// either way; what changes is the representation on disk. See
	// carryForwardEligible for the trade-offs an operator is agreeing to.
	CarryForwardUnchanged bool

	// WarnEventThreshold logs a loud warning when a table's fetched event count
	// exceeds it. The event window itself is PAGED since #1097, so the resident
	// cost this warns about is the change map: one entry per touched PK, which
	// paging does not bound (#1107). The event count stays the trigger because
	// it is what the fetch knows (#654).
	//
	// 0 = disabled, and it is the zero value, so a direct library caller is
	// silent unless it opts in. Both the CLI (--warn-event-threshold) and the
	// in-daemon folds (consoleapp's daemonFoldWarnEventThreshold) default it to
	// 5,000,000; keep those in step.
	//
	// The reported threshold is SCALED by Parallelism (#842), so two callers
	// with the same raw value warn at the same TOTAL concurrent event volume
	// while their per-table triggers differ.
	WarnEventThreshold int64

	// RemediationHint replaces the advice attached to that warning.
	//
	// Empty uses the wording for the attended CLI commands, which names the
	// flags those commands actually register. That wording is WRONG on a daemon:
	// bintrail-console registers no --at, --parallelism or --warn-event-threshold,
	// so an operator told to lower one goes looking for a flag their binary does
	// not have. Same class as the project's rule that an MCP error must never
	// name a CLI flag the client cannot pass. A caller on such a surface sets
	// advice its own operator can act on.
	RemediationHint string

	// FetchBatchSize is the page size used to stream a table's event window
	// (#1097). 0 → query.DefaultStreamBatchSize, the zero-value convention this
	// struct already uses for ArchiveFetcher/WarnEventThreshold/DuckDBTuning.
	//
	// It trades resident memory against round trips: a page holds both decoded
	// JSON row images per event (roughly 1-2 KB per event on a narrow table),
	// while each page costs one MySQL query plus one read per archive source.
	//
	// The archive cost is not uniform, and the arithmetic matters for S3, where
	// parquetquery downloads each file rather than scanning it in place. Cursor
	// scoping advances at HOUR granularity and archive partitions are hourly,
	// so a given hour's file is re-fetched roughly (events in that hour /
	// FetchBatchSize) times — ~1x at the default for an hour holding 100k
	// events, ~10x for one holding 1M. Shrinking this is therefore free only up
	// to the point where a page stops spanning a whole busy hour.
	FetchBatchSize int

	// ArchiveFetcher fetches archived binlog events for a table. nil →
	// parquetquery.Fetch (the container-safe DuckDB budget). The CLI sets it
	// to a tuned fetcher under --ultrafast so the flag is honored on the
	// full-table path, not just single-row reconstruct (#510).
	ArchiveFetcher query.ArchiveFetcher

	// DuckDBTuning is the resource budget applied to the merge/baseline DuckDB
	// sessions this package opens directly (mergeBaselineImages,
	// materializeBaselineLocal's S3 download) — the ArchiveFetcher above only
	// covers archive Parquet reads, not these (#842). The zero value means
	// "not specified" (mirrors ArchiveFetcher==nil and WarnEventThreshold==0
	// on this same struct) and falls back to duckdbutil.DefaultTuning(), the
	// same container-safe 2-threads/4GB budget parquetquery.Fetch applies —
	// without this, each of these DuckDB instances defaulted to ~80% of host
	// RAM regardless of any --ultrafast/--duckdb-* flag the caller set. The
	// CLI sets this to the same resolved Tuning it hands ArchiveFetcher, so an
	// explicit operator flag is honored here too, not just on archive reads.
	DuckDBTuning duckdbutil.Tuning
}

// effectiveDuckDBTuning normalizes a caller-supplied Tuning for the merge/
// baseline DuckDB sessions this package opens (#842): the zero value means
// "not specified" and falls back to the container-safe budget
// (duckdbutil.DefaultTuning()) rather than DuckDB's native host-greedy default
// (~80% RAM, one thread per core). A caller that genuinely wants the
// host-greedy budget passes duckdbutil.Ultrafast() (which sets S3Direct, so it
// is never the zero value) or any other explicit non-zero Tuning.
func effectiveDuckDBTuning(t duckdbutil.Tuning) duckdbutil.Tuning {
	if t == (duckdbutil.Tuning{}) {
		return duckdbutil.DefaultTuning()
	}
	return t
}

// applyDuckDBTuning normalizes t via effectiveDuckDBTuning and applies it to a
// freshly opened DuckDB session, ALWAYS paired with
// duckdbutil.SetTempDirectory: a memory_limit cap with nowhere to spill is
// worse than no cap at all (see duckdbutil.Tuning.Apply's doc comment). Every
// DuckDB session this package opens for the merge/baseline path goes through
// this single call so the pairing can't be forgotten at a future call site.
func applyDuckDBTuning(ctx context.Context, db *sql.DB, t duckdbutil.Tuning) {
	effectiveDuckDBTuning(t).Apply(ctx, db)
	duckdbutil.SetTempDirectory(ctx, db)
}

// TableReport carries the per-table outcome stats that the CLI summary prints.
type TableReport struct {
	Schema, Table  string
	BaselineRows   int64 // rows streamed through from the baseline unchanged
	EventsApplied  int64 // total events observed from the event index
	InsertsEmitted int64 // rows appended after the baseline pass (new PKs)
	UpdatesApplied int64 // baseline rows whose PK matched an UPDATE/INSERT event
	DeletesSkipped int64 // baseline rows whose PK matched a DELETE event
	// RowsWritten counts the row tuples actually written into chunk files —
	// the writer's own tally, exact by construction (not derived from the
	// baseline/insert/delete counters). `bintrail drill` loads the dump and
	// checks COUNT(*) against it.
	RowsWritten int64
	Files       []string
	Duration    time.Duration
	// BinlogOnly is true when the table had no baseline at all and was
	// recovered entirely from the binlog (#766's ErrNoBaseline fallback,
	// reconstructBinlogOnly). Files is non-empty in this case too, so
	// ReconstructTables' shared-metadata-file selector must check this flag
	// as well — a binlog-only report has no baseline GTID/binlog coordinates
	// to embed in the metadata file.
	BinlogOnly bool
	// CarriedForward is true when the delta window held no events for this
	// table, so its previous Parquet file was published into the new snapshot
	// unchanged instead of being folded and re-emitted. The row counters above
	// are all zero in that case — nothing was streamed, applied or written —
	// and reading them as "the table is empty" would be wrong. See carryForward
	// for why an untouched table's anchor stays valid.
	CarriedForward bool
	// CarriedByLink narrows CarriedForward to the arm that shares the
	// previous file's inode — the only one that saves disk (#1578). A carried
	// table with this false was published by a full COPY (no-link filesystem,
	// a permission fault, or a cross-device output): still a reuse — no fold
	// ran, no source was read — but every byte was written again, and a
	// consumer that renders "reused" as a disk saving must count THIS, not
	// CarriedForward, or it confirms a saving the daemon log denies.
	CarriedByLink bool
}

// shouldWarnEvents reports whether a fetched event count should trigger the
// large-window memory warning (#654). threshold <= 0 disables the warning, so
// the zero-value FullTableConfig stays silent for direct library callers.
func shouldWarnEvents(n, threshold int64) bool {
	return threshold > 0 && n > threshold
}

// scaledEventThreshold divides threshold by parallelism so a per-table
// warning threshold reflects the RAM footprint of parallelism tables
// reconstructing CONCURRENTLY, not just one (#842): ReconstructTables runs up
// to Parallelism table goroutines at a time, each holding its own page +
// change map in memory, so a per-table threshold alone lets N tables
// each just under the limit pass silently while the process holds N times
// that much. threshold<=0 (disabled) and parallelism<=1 (no concurrency to
// account for) pass through unchanged. The division floors, with a minimum of
// 1 so a very large parallelism never silences the warning outright.
func scaledEventThreshold(threshold int64, parallelism int) int64 {
	if threshold <= 0 || parallelism <= 1 {
		return threshold
	}
	scaled := threshold / int64(parallelism)
	if scaled < 1 {
		scaled = 1
	}
	return scaled
}

// effectiveParallelism returns the divisor scaledEventThreshold should use for
// this run: cfg.Parallelism (defaulting to runtime.NumCPU() the same way
// ReconstructTables does), clamped down to len(cfg.Tables) when that's
// smaller. Without the clamp, a single-table run (cfg.Tables has one entry)
// on a big box would divide the threshold by NumCPU purely because
// Parallelism defaults high, warning on a table nowhere near actually running
// concurrently with anything — only min(Parallelism, len(Tables)) tables can
// ever be in flight at once, so that's the real upper bound on concurrent RAM
// this run can hold. Extracted so callers that don't go through
// ReconstructTables' own Parallelism normalization (a direct ReconstructTable
// or reconstructBinlogOnly caller, or a unit test) still get the right
// divisor.
func effectiveParallelism(cfg FullTableConfig) int {
	p := cfg.Parallelism
	if p <= 0 {
		p = runtime.NumCPU()
	}
	if n := len(cfg.Tables); n > 0 && n < p {
		p = n
	}
	return p
}

// maybeWarnEventVolume emits the #654 large-window memory warning when the
// fetched event count exceeds threshold, SCALED by parallelism (#842) so the
// warning reflects the total concurrent RAM footprint across every table
// ReconstructTables may run at once, not just this one. Extracted from
// ReconstructTable so the emission — not just the predicate — is unit-testable.
func maybeWarnEventVolume(schema, table string, n int64, threshold int64, parallelism int, remediation string) {
	effThreshold := scaledEventThreshold(threshold, parallelism)
	if !shouldWarnEvents(n, effThreshold) {
		return
	}
	// The event window itself is streamed a page at a time since #1097, so the
	// resident cost this warns about is the change map — one entry per DISTINCT
	// touched row, which paging does not bound (that is #1107). The event count
	// stays the trigger because it is what the fetch knows; a window this large
	// is the reliable predictor of a map large enough to matter.
	slog.Warn("reconstruct: very large event window — full-table reconstruct holds one change-map "+
		"entry per touched row in memory and may exhaust RAM",
		"schema", schema, "table", table,
		"events", n, "threshold", effThreshold, "raw_threshold", threshold, "parallelism", parallelism,
		"hint", remediationOrDefault(remediation))
}

// withFoldBudgets copies every caller-facing budget from a FullTableConfig onto
// the foldConfig a fold actually runs with.
//
// It exists because these four assignments used to be written out at each
// foldEventWindow call site, and a one-line deletion at EITHER site silently
// reverted the budget with the whole suite green: nothing drives
// FullTableConfig through foldConfig into the warning, so the seam had no
// coverage at all. One function means one place to delete and one place to
// test.
func withFoldBudgets(cfg FullTableConfig, fc foldConfig) foldConfig {
	fc.BatchSize = cfg.FetchBatchSize
	fc.WarnEventThreshold = cfg.WarnEventThreshold
	// The DIVISOR, not the raw field: effectiveParallelism clamps to
	// len(cfg.Tables), so a single-table run is not divided by a parallelism it
	// can never reach.
	fc.Parallelism = effectiveParallelism(cfg)
	fc.RemediationHint = cfg.RemediationHint
	return fc
}

// remediationOrDefault falls back to the attended-CLI wording. Callers on a
// surface that registers none of these flags set FullTableConfig.RemediationHint
// instead; see the field's doc for why naming an absent flag is a defect.
func remediationOrDefault(remediation string) string {
	if remediation != "" {
		return remediation
	}
	return "narrow the window with a later --at or a fresher baseline snapshot, lower --parallelism, or raise/silence " +
		"via --warn-event-threshold / BINTRAIL_RECONSTRUCT_WARN_EVENTS (0 disables)"
}

// Refusal classes a caller can act on without parsing error text. Every
// full-table refusal that a REFRESH loop needs to report distinctly wraps one of
// these; anything else is an ordinary failure.
//
// They exist for `bintrail baseline refresh`, which must tell an operator WHY a
// table could not be refreshed — "the events are gone" and "the table changed
// shape" have completely different remedies — without the summary code
// string-matching messages that are free to be reworded.
var (
	// ErrCaptureGap: the fold window spans a permanent capture loss (or an
	// index that cannot rule one out). Remedy: --allow-gaps to accept a
	// knowingly-incomplete result, or a fresh dump.
	ErrCaptureGap = errors.New("capture gap in the reconstruction window")
	// ErrSchemaChanged: the table's shape moved between the baseline and the
	// target — an ALTER, a destructive DDL, or delta events disagreeing with
	// the baseline's columns. Remedy: a real re-dump; no flag helps.
	ErrSchemaChanged = errors.New("schema changed since the baseline")
)

// TableFailure is one table's refusal, kept separate from the joined error so a
// caller can classify and report per table.
type TableFailure struct {
	Schema string
	Table  string
	Err    error
}

// ReconstructTablesDetailed is ReconstructTables plus the per-table failures.
//
// ReconstructTables joins every failure into one error, which is right for a
// command that either produces a dump or doesn't. A refresh has to render a
// per-table verdict — refreshed / refused-gap / refused-ddl — so it needs the
// failures apart, classifiable against the sentinels above. The run semantics
// are identical, including all-or-nothing publication: any failure leaves the
// snapshot directory marked _INCOMPLETE and therefore undiscoverable.
func ReconstructTablesDetailed(ctx context.Context, cfg FullTableConfig) ([]*TableReport, []TableFailure, error) {
	var failures []TableFailure
	reports, err := reconstructTables(ctx, cfg, &failures)
	return reports, failures, err
}

// ReconstructTables runs ReconstructTable concurrently for every entry in
// cfg.Tables, sharing a single *sql.DB + *query.Engine + *metadata.Resolver.
// Returns the list of reports in arbitrary order plus a joined error
// containing every per-table failure (via errors.Join).
func ReconstructTables(ctx context.Context, cfg FullTableConfig) ([]*TableReport, error) {
	return reconstructTables(ctx, cfg, nil)
}

// foldOneTable is ReconstructTable behind a seam, so a test can make ONE
// table's fold panic. The fan-out goroutine below is only reachable with a
// live index and a schema snapshot, and the panic it has to survive comes from
// deep inside a real fold (DuckDB over customer Parquet), which no fixture can
// stage on demand. Written by tests only; production never reassigns it.
var foldOneTable = ReconstructTable

// recoverTableFold turns a panic in one table's fold into that table's
// FAILURE, instead of letting it end the process.
//
// recover() is per-goroutine, so the guard consoleapp puts on its four
// baseline job goroutines cannot reach here: this is a CHILD goroutine, and a
// panic in it kills the process however well the parent is guarded. That
// matters because under `bintrail-console watch` the process that runs this
// fold is also the capture plane, and this is the surface #1472 named. The
// fold decodes a customer's own Parquet through DuckDB, with column types that
// came from their CREATE TABLE.
//
// It is a failure, not a swallow. The panic is logged at error level WITH the
// stack, which is where the diagnosis now lives since the process no longer
// dies printing it, and the table is recorded in BOTH sinks its ordinary error
// path uses. So it flows on as an ordinary per-table failure: the joined error
// fails the run, the completeness marker stays _INCOMPLETE, nothing is
// published, and the caller's own reporting (a run history row, a refusal
// count, a non-zero exit) happens exactly as it would for any other bad table.
// The CLI keeps everything it had: it exits non-zero and the stack is in the
// log, rather than on stderr from a dying process.
//
// One table's crash does not stop its siblings, which is deliberate: they are
// independent folds, and the run is failing as a whole anyway.
//
// mu must be the mutex guarding errs and failures. failures may be nil (the
// ReconstructTables entry point passes none); errs never is.
func recoverTableFold(schema, table string, mu *sync.Mutex, errs *[]error, failures *[]TableFailure) {
	r := recover()
	if r == nil {
		return
	}
	slog.Error("full-table reconstruct hit an internal error on one table; the other tables continue and "+
		"the run will fail without publishing. Please report this with the stack recorded here.",
		"schema", schema, "table", table, "panic", r, "stack", string(debug.Stack()))
	err := fmt.Errorf("%s.%s: internal error: %v", schema, table, r)
	mu.Lock()
	defer mu.Unlock()
	*errs = append(*errs, err)
	if failures != nil {
		*failures = append(*failures, TableFailure{Schema: schema, Table: table, Err: err})
	}
}

// reconstructTables is the implementation both entry points share. failures, when
// non-nil, collects each per-table error alongside the joined one.
func reconstructTables(ctx context.Context, cfg FullTableConfig, failures *[]TableFailure) ([]*TableReport, error) {
	if cfg.IndexDSN == "" {
		return nil, errors.New("FullTableConfig: IndexDSN is required")
	}
	if cfg.BaselineSrc == "" {
		return nil, errors.New("FullTableConfig: BaselineSrc is required")
	}
	if len(cfg.Tables) == 0 {
		return nil, errors.New("FullTableConfig: at least one table is required")
	}
	if cfg.OutputDir == "" {
		return nil, errors.New("FullTableConfig: OutputDir is required")
	}
	if cfg.At.IsZero() {
		cfg.At = time.Now().UTC()
	}
	if cfg.Parallelism <= 0 {
		cfg.Parallelism = runtime.NumCPU()
	}
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 256 << 20
	}
	if cfg.OutputFormat == "" {
		cfg.OutputFormat = OutputFormatMydumper
	}
	if cfg.OutputFormat != OutputFormatMydumper && cfg.OutputFormat != OutputFormatParquet {
		return nil, fmt.Errorf("FullTableConfig: unknown OutputFormat %q (want %q or %q)",
			cfg.OutputFormat, OutputFormatMydumper, OutputFormatParquet)
	}
	parquetMode := cfg.OutputFormat == OutputFormatParquet

	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return nil, fmt.Errorf("create output dir: %w", err)
	}

	// In Parquet mode the files land in a per-snapshot subdirectory of the
	// baselines root, not in OutputDir itself, and it is THAT directory whose
	// completeness marker discovery reads (#467). Everything below therefore
	// operates on markerDir rather than cfg.OutputDir.
	markerDir := cfg.OutputDir
	if parquetMode {
		cfg.snapshotDir = filepath.Join(cfg.OutputDir, SnapshotDirName(cfg.At))
		// Refuse rather than merge into an existing snapshot directory. Unlike a
		// reconstruct output dir — an operator-chosen path that is routinely
		// re-run into, which markRunIncomplete handles by clearing the stale
		// marker — a snapshot directory is identified by its timestamp and is
		// meant to be written once. Two runs sharing one (same --at, or two
		// refreshes inside the same second) would interleave table files from
		// different folds under a single anchor, and the second run's _SUCCESS
		// would publish the mixture as one coherent snapshot.
		//
		// The exception is a directory holding NOTHING BUT the _INCOMPLETE
		// marker: that is a previous run of this exact target that published
		// nothing, so there is no data to interleave. Refusing it would make the
		// most ordinary recovery impossible — fix the problem the run reported,
		// retry the same --at, and be told the directory is in the way.
		leftover, err := snapshotDirLeftovers(cfg.snapshotDir)
		if err != nil {
			return nil, err
		}
		if len(leftover) > 0 {
			return nil, fmt.Errorf("snapshot directory %s already holds %s; "+
				"a baseline snapshot is written once — remove it, or target a different instant with --at",
				cfg.snapshotDir, strings.Join(leftover, ", "))
		}
		if err := os.MkdirAll(cfg.snapshotDir, 0o755); err != nil {
			return nil, fmt.Errorf("create snapshot directory %s: %w", cfg.snapshotDir, err)
		}
		markerDir = cfg.snapshotDir
	}

	// Crash-safety completeness marker (#842): flag the output dir _INCOMPLETE
	// before any table is written, and only replace it with _SUCCESS once every
	// table has converted without error (see finalizeCompletenessMarker below).
	// Without this, an OOM-killed (or otherwise uncatchably killed) run leaves
	// finished tables' data + schema files on disk with no signal that other
	// requested tables are missing — a dump that looks complete but silently
	// isn't. Reuses the exact marker convention `bintrail baseline` established
	// for the same failure mode (#467, internal/baseline/marker.go) rather than
	// inventing a second one, so any future consumer of reconstruct's output
	// dir can check completeness the same way. The write is FATAL, mirroring
	// baseline.go: proceeding without the crash-safety net deployed and then
	// dying uncatchably mid-run would leave a markerless partial dump that the
	// marker-absent-is-complete legacy-compat rule reads as complete.
	if err := markRunIncomplete(markerDir); err != nil {
		return nil, fmt.Errorf("could not write incomplete-dump marker in %s (refusing to reconstruct without the crash-safety marker): %w", markerDir, err)
	}

	db, err := config.Connect(cfg.IndexDSN)
	if err != nil {
		return nil, fmt.Errorf("connect to index DB: %w", err)
	}
	defer db.Close()
	// Give per-table goroutines enough connections for concurrent fetches.
	db.SetMaxOpenConns(2 * cfg.Parallelism)

	// Run the idempotent schema migration before NewResolver. NewResolver
	// reads schema_snapshots.column_type (added in #212), and pre-upgrade
	// databases where EnsureSchema hasn't been called from some other
	// command yet would fail with Error 1054: Unknown column 'column_type'.
	// Every other consumer of NewResolver runs EnsureSchema first; doing it
	// at the library boundary here means library callers (not just the CLI)
	// also get the migration automatically.
	if err := indexer.EnsureSchema(db); err != nil {
		return nil, indexer.WrapSchemaMigrationErr(err)
	}

	// Derive DBName for the query planner.
	var dbName string
	if dsnCfg, perr := mysqldriver.ParseDSN(cfg.IndexDSN); perr == nil {
		dbName = dsnCfg.DBName
	}

	// Load schema resolver once (latest snapshot). All PK encoding goes
	// through event.BuildPKValues with the resolver's ColumnMetas so the
	// keys are byte-identical to what the indexer stored in pk_values.
	resolver, err := metadata.NewResolver(db, 0)
	if err != nil {
		return nil, fmt.Errorf("load schema resolver: %w; run `bintrail snapshot` first", err)
	}

	engine := query.New(db)

	// Resolve the snapshot's binlog cut ONCE for the whole run, before any table
	// is folded. One index database tracks one source, so a single coordinate is
	// the right anchor for every table this run FOLDS, and resolving it per table
	// would anchor tables that were folded together at different points for no
	// reason.
	//
	// A snapshot can nonetheless hold mixed anchors, and that is deliberate
	// rather than a hole: a table with no events in the window is published by
	// carrying its previous file forward, keeping its own older anchor, which
	// still points exactly where ITS deltas resume. Discovery is unaffected
	// because it dates a snapshot by its directory name, never by a footer. Any
	// reader that treats one table's anchor as the snapshot's will be wrong for
	// such a table; see carryForward.
	//
	// The cut also bounds every table's fetch (Options.UntilPos below), so it
	// must be pinned before the first fetch: reading it afterwards would anchor
	// the snapshot at a coordinate later than the events it actually folded, and
	// the next refresh would skip everything in between.
	if parquetMode {
		cut, cutErr := ResolveSnapshotCut(ctx, db, cfg.At)
		if cutErr != nil {
			return nil, cutErr
		}
		cfg.cut = cut
		if cut == nil {
			slog.Warn("index holds no events; the emitted snapshot will carry its source baseline's coordinates unchanged",
				"at", cfg.At.UTC().Format(time.RFC3339))
		} else {
			slog.Info("snapshot anchored", "binlog_file", cut.File, "binlog_pos", cut.Pos,
				"at", cfg.At.UTC().Format(time.RFC3339))
		}
	}

	// Resolve archive sources once — the same set is used for every table.
	archSources, archErr := query.ResolveArchiveSources(ctx, db)
	if archErr != nil {
		if !cfg.AllowGaps {
			return nil, fmt.Errorf("resolve archive sources, cannot verify coverage: %w", archErr)
		}
		slog.Warn("archive source discovery failed; proceeding without archives", "error", archErr)
	}

	// Report slice is protected by a mutex for the parallel goroutines.
	reports := make([]*TableReport, 0, len(cfg.Tables))
	var (
		mu   sync.Mutex
		errs []error
	)

	sem := make(chan struct{}, cfg.Parallelism)
	var wg sync.WaitGroup

	for _, entry := range cfg.Tables {
		schema, table, ok := splitSchemaTable(entry)
		if !ok {
			return nil, fmt.Errorf("invalid --tables entry %q: must be schema.table", entry)
		}
		wg.Add(1)
		go func(schema, table string) {
			defer wg.Done()
			// Registered AFTER wg.Done so it runs BEFORE it: the parent reads
			// errs/failures once wg.Wait returns, and appending after that
			// would be a data race.
			defer recoverTableFold(schema, table, &mu, &errs, failures)
			sem <- struct{}{}
			defer func() { <-sem }()

			if ctx.Err() != nil {
				return
			}

			rep, err := foldOneTable(ctx, cfg, schema, table, db, engine, archSources, resolver, dbName)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				slog.Error("full-table reconstruct failed",
					"schema", schema, "table", table, "error", err)
				errs = append(errs, fmt.Errorf("%s.%s: %w", schema, table, err))
				if failures != nil {
					*failures = append(*failures, TableFailure{Schema: schema, Table: table, Err: err})
				}
				return
			}
			reports = append(reports, rep)
		}(schema, table)
	}
	wg.Wait()

	// Write the shared metadata file once after every table completes.
	// We use the metadata from the FIRST reconstructed table — all tables
	// share the same baseline set, so their positions should agree. If a
	// user somehow reconstructs tables from baselines of different ages,
	// the metadata file will reflect the first one and the rest will log
	// a warning in ReconstructTable itself.
	if len(reports) > 0 && !parquetMode {
		// Pick the first report that actually produced output files from a
		// real baseline (skip tables that had no baseline: an empty report,
		// or a #766 binlog-only report — neither has baseline GTID/binlog
		// coordinates to embed in the metadata file).
		var metaReport *TableReport
		for _, r := range reports {
			if len(r.Files) > 0 && !r.BinlogOnly {
				metaReport = r
				break
			}
		}
		if metaReport != nil {
			tableName := metaReport.Schema + "." + metaReport.Table
			baselinePath, _, _, perr := FindBaseline(ctx, cfg.BaselineSrc, metaReport.Schema, metaReport.Table, cfg.At)
			if perr == nil {
				bmeta, merr := baseline.ReadParquetMetadataAny(ctx, baselinePath)
				if merr == nil {
					if err := WriteMetadataFile(cfg.OutputDir, cfg.At,
						bmeta.GTIDSet, bmeta.BinlogFile, bmeta.BinlogPos); err != nil {
						// FATAL to the run's completeness, not just a slog.Warn (#842):
						// #842 itself calls the absent metadata file "the only hint" that
						// an otherwise-complete-looking output dir is missing something —
						// a dir marked _SUCCESS but missing `metadata` is a smaller
						// instance of the exact silent-partial the completeness marker
						// exists to close. Fold into errs so finalizeCompletenessMarker
						// leaves _INCOMPLETE in place and the error surfaces to the CLI,
						// even though every individual table converted cleanly.
						slog.Error("could not write metadata file; run will not be marked complete", "error", err)
						errs = append(errs, fmt.Errorf("write metadata file: %w", err))
					}
				} else {
					slog.Error("could not read baseline metadata for metadata file; run will not be marked complete",
						"table", tableName, "error", merr)
					errs = append(errs, fmt.Errorf("read baseline metadata for metadata file (table %s): %w", tableName, merr))
				}
			} else {
				slog.Error("could not find baseline for metadata file; run will not be marked complete",
					"table", tableName, "error", perr)
				errs = append(errs, fmt.Errorf("find baseline for metadata file (table %s): %w", tableName, perr))
			}
		} else {
			// NOT an error: every reconstructed table is legitimately baseline-less
			// (e.g. a single-table run that hit the #766 binlog-only fallback) — the
			// metadata file's absence is correctly documented behavior here, not a
			// silent gap, so this must not block _SUCCESS.
			slog.Warn("no reconstructed table has a real baseline (all empty or binlog-only); metadata file not written")
		}
	}

	// A Parquet snapshot carries an at-rest integrity manifest (#636), written
	// before the _SUCCESS marker so a complete snapshot ALWAYS has its checksums
	// — identical ordering and identical fatality to baseline.Run, because a
	// complete-but-manifestless snapshot is indistinguishable at read time from a
	// legacy one and silently forfeits corruption detection. Only attempted when
	// the run is otherwise clean; a failed run stays _INCOMPLETE and needs no
	// manifest.
	if parquetMode && ctx.Err() == nil && len(errs) == 0 {
		if err := baselineintegrity.WriteManifest(cfg.snapshotDir); err != nil {
			errs = append(errs, fmt.Errorf("snapshot complete but could not write integrity manifest: %w", err))
		}
	}

	if err := finalizeCompletenessMarker(markerDir, ctx.Err(), errs); err != nil {
		return reports, err
	}
	return reports, nil
}

// snapshotDirLeftovers lists what an existing snapshot directory holds that a
// new run must not write alongside. An absent directory, or one holding only the
// _INCOMPLETE marker a previous failed run left, yields nothing — see the caller
// for why that exception is load-bearing.
func snapshotDirLeftovers(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("inspect snapshot directory %s: %w", dir, err)
	}
	var out []string
	for _, e := range entries {
		if e.Name() == baseline.IncompleteMarker {
			continue
		}
		out = append(out, e.Name())
	}
	return out, nil
}

// markRunIncomplete stamps outputDir _INCOMPLETE for a NEW reconstruct run
// (#842), removing any stale _SUCCESS a PREVIOUS, successful run into the
// same dir left behind first. This matters specifically because reconstruct's
// OutputDir — unlike a `bintrail baseline` snapshot dir, which is always a
// fresh <output>/<timestamp>/ — is an operator-chosen path that is routinely
// REUSED across runs (e.g. re-running with a later --at). baseline.SnapshotComplete
// checks _SUCCESS first and returns true regardless of _INCOMPLETE, so without
// this removal a stale _SUCCESS from run 1 would keep masking run 2 as
// complete even after run 2 gets OOM-killed mid-way — the exact silent-partial
// this marker exists to close, still open on the single most ordinary re-run
// path.
//
// The removal is FATAL (see the code below): a surviving _SUCCESS means the
// crash-safety net for THIS run did not deploy — SnapshotComplete would keep
// reading this run as complete even after an interrupted, partial retry —
// which is exactly as much a failed-to-deploy net as a missing _INCOMPLETE
// write would be. Proceeding anyway risks the silent-partial this whole
// marker exists to close, so this stays fatal like the _INCOMPLETE write
// immediately below it; do not soften it to a log-and-continue.
func markRunIncomplete(outputDir string) error {
	// FATAL, same stance as the _INCOMPLETE write below: SnapshotComplete
	// checks _SUCCESS FIRST, so a stale one left in place is just as much a
	// crash-safety net that failed to deploy as a missing _INCOMPLETE would
	// be — proceeding anyway risks the exact silent-partial this marker
	// exists to close.
	if err := os.Remove(filepath.Join(outputDir, baseline.SuccessMarker)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("could not remove stale %s marker from a prior run in %s (refusing to reconstruct with a completeness marker that could mask this run's own incomplete output): %w", baseline.SuccessMarker, outputDir, err)
	}
	return baseline.WriteIncompleteMarker(outputDir)
}

// finalizeCompletenessMarker decides and writes the terminal #842 completeness
// marker for a ReconstructTables run's output dir: _SUCCESS when the run
// finished with no per-table errors and was not cancelled, otherwise the
// _INCOMPLETE marker ReconstructTables wrote before the run started is left in
// place (this function writes nothing in that branch) and the triggering
// error(s) are returned joined. Split out from ReconstructTables so the marker
// decision is unit-testable without a live index DB.
//
//   - cancelErr must be ctx.Err(): a cancelled run's per-table goroutines
//     return early WITHOUT recording an error (see the ctx.Err() check inside
//     the per-table goroutine in ReconstructTables), so an empty tableErrs
//     alone is not proof every table converted.
//   - tableErrs is every per-table failure collected by ReconstructTables.
//   - Both are joined into one returned error (via errors.Join, which
//     surfaces every wrapped error, not just the first) rather than cancelErr
//     alone winning: a table that genuinely failed before a Ctrl-C landed
//     must stay visible on the returned error / CLI exit status, not just in
//     the logs each per-table failure already went to individually.
//
// (Before this function existed, a cancelled run with an EMPTY tableErrs
// silently returned nil — success — even though the run never finished; that
// was a real pre-existing bug this refactor also fixes, not just the marker
// itself.)
func finalizeCompletenessMarker(outputDir string, cancelErr error, tableErrs []error) error {
	// errors.Join discards nil inputs and returns nil only when EVERY input is
	// nil, so this single call covers all three cases: cancelled-only,
	// per-table-errors-only, both (a table failed before the cancellation was
	// observed — those failures must not go invisible, log-only, behind the
	// cancellation error), and neither (falls through to the _SUCCESS write
	// below). errors.Is/As on the result still finds cancelErr and each
	// individual tableErrs entry.
	if joined := errors.Join(cancelErr, errors.Join(tableErrs...)); joined != nil {
		return joined
	}
	if err := baseline.WriteSuccessMarker(outputDir); err != nil {
		// The dump is complete on disk but unmarked; without _SUCCESS (and
		// absent _INCOMPLETE, which this branch would have just removed) it is
		// STILL treated as complete by the legacy-compat default, so this is a
		// degraded-observability failure, not a data one — mirrors baseline.go's
		// own WriteSuccessMarker error handling. Fail loud so the operator can
		// re-run rather than silently ship a dump that never got explicitly
		// confirmed complete.
		return fmt.Errorf("reconstruct complete but could not write %s marker: %w", baseline.SuccessMarker, err)
	}
	return nil
}

// ReconstructTable is the per-table worker. Safe to call concurrently with
// other ReconstructTable invocations that share the same db / engine /
// archSources / resolver.
func ReconstructTable(
	ctx context.Context,
	cfg FullTableConfig,
	schema, table string,
	db *sql.DB,
	engine *query.Engine,
	archSources []string,
	resolver *metadata.Resolver,
	dbName string,
) (*TableReport, error) {
	start := time.Now()
	rep := &TableReport{Schema: schema, Table: table}

	// ── 1. Find the baseline snapshot ──────────────────────────────────────
	// FindBaseline already logs a stale-fallback warning (#466) server-side.
	baselinePath, snapshotTime, _, err := FindBaseline(ctx, cfg.BaselineSrc, schema, table, cfg.At)
	if err != nil {
		if !errors.Is(err, ErrNoBaseline) {
			return nil, fmt.Errorf("find baseline: %w", err)
		}
		// Full-table reconstruct is out of GA scope for PostgreSQL (#597). With no
		// baseline found the LSN anchor is unavailable, so detect PG by the recorded
		// source flavor and refuse here too — otherwise a PG full-table reconstruct
		// of a table absent from the baseline would fall through to the binlog-only
		// (delta-only) report below, mislabeled as a full-table reconstruct (#916).
		// Mirrors the with-baseline PG gate further down.
		if query.SourceFlavor(db) == "postgres" {
			return nil, fmt.Errorf(
				"full-table reconstruct is not yet supported for PostgreSQL sources (#597); " +
					"use single-row `reconstruct` or the shim `_flashback` for PostgreSQL time-travel")
		}
		// No baseline exists for this table. This can happen when: (1) the
		// table was empty at dump time and the baseline predates 0-row
		// Parquet support, or (2) the table was created after the last
		// baseline snapshot. Case (2) can hold real binlog-only rows (#766),
		// so fall back to a binlog-only reconstruction instead of silently
		// emitting an empty report — parity with the shim's _snapshot→
		// _flashback degrade (internal/shim/snapshot.go runSnapshotFullTable).
		// That degrade has no Parquet-mode counterpart. The fallback synthesizes a
		// placeholder CREATE TABLE from the observed row images
		// (binlogOnlySchemaPlaceholder) and reconstructs only the rows the window
		// happens to touch — neither is a baseline. Publishing it as one would
		// anchor every future reconstruct on a snapshot that silently omits every
		// row the window never touched, so refuse and name the only correct fix.
		if cfg.OutputFormat == OutputFormatParquet {
			return nil, fmt.Errorf(
				"%s.%s has no baseline snapshot at or before %s, so it cannot be re-emitted as one: "+
					"a snapshot derived only from binlog deltas would omit every row the window never touched — "+
					"take a real snapshot for this table first (`bintrail dump` + `bintrail baseline`)",
				schema, table, cfg.At.UTC().Format(time.RFC3339))
		}
		return reconstructBinlogOnly(ctx, cfg, schema, table, db, engine, resolver, dbName, rep, start)
	}
	slog.Debug("baseline selected",
		"schema", schema, "table", table,
		"path", baselinePath, "snapshot_time", snapshotTime.UTC().Format(time.RFC3339))

	// ── 2. Read baseline Parquet metadata ──────────────────────────────────
	bmeta, err := baseline.ReadParquetMetadataAny(ctx, baselinePath)
	if err != nil {
		return nil, fmt.Errorf("read baseline metadata: %w", err)
	}
	// PostgreSQL baselines deliberately omit CreateTableSQL (pgbaseline embeds
	// an LSN anchor, not a mydumper CREATE TABLE), so the generic
	// "re-run `bintrail baseline`" remediation below is impossible to satisfy
	// for a PG source and sends the operator into a loop re-dumping baselines
	// that will never carry that metadata. Full-table reconstruct is out of GA
	// scope for PostgreSQL (#597). Detect PG via the LSN anchor first (no DB
	// read; written only by pgbaseline) then the recorded source flavor
	// (catches pre-#593 PG baselines with LSN==0), and fail with the correct
	// message BEFORE the MySQL-only CreateTableSQL check so a genuine MySQL
	// baseline missing that metadata still gets the "re-run `bintrail baseline`"
	// guidance.
	if bmeta.LSN != 0 || query.SourceFlavor(db) == "postgres" {
		return nil, fmt.Errorf(
			"full-table reconstruct is not yet supported for PostgreSQL sources (#597); " +
				"use single-row `reconstruct` or the shim `_flashback` for PostgreSQL time-travel")
	}
	if bmeta.CreateTableSQL == "" {
		return nil, fmt.Errorf(
			"baseline at %s lacks bintrail.create_table_sql metadata; "+
				"re-run `bintrail baseline` to embed the CREATE TABLE statement",
			baselinePath)
	}

	// ── 3. Resolve PK columns from the schema resolver ─────────────────────
	tm, err := resolver.Resolve(schema, table)
	if err != nil {
		return nil, fmt.Errorf("resolve schema for %s.%s: %w; run `bintrail snapshot` to refresh", schema, table, err)
	}
	pkCols := tm.PKColumnMetas()
	if len(pkCols) == 0 {
		return nil, fmt.Errorf("%s.%s has no primary key in the loaded snapshot; full-table reconstruct requires a PK", schema, table)
	}
	// Refuse to proceed when a PK column uses a type the canonicalizer
	// cannot handle. Emitting a warning isn't enough because operators
	// running with --log-level error won't see it and would silently get
	// wrong output — the same class of bug the full-table reconstruct
	// hardening exists to prevent. What is actually left out today is
	// FLOAT/DOUBLE, TIME, BIT, JSON and the spatial family; supportedPKType is the
	// authority, and this comment used to also name DECIMAL and the
	// BINARY/VARBINARY/BLOB family, which have been supported since #214 and
	// #1155 respectively.
	//
	// This loop deliberately does NOT skip an empty DataType, so a PG-shaped
	// snapshot that reached the MySQL path still gets PKTypeGateReason's
	// wrong-path verdict. Do not swap it for reconstruct.FirstUnsupportedPKType,
	// whose empty-skip is for callers with no upstream flavor check: it would
	// retire that verdict silently.
	for _, pkCol := range pkCols {
		if !supportedPKType(pkCol.DataType) {
			return nil, fullTablePKTypeRefusal(schema, table, pkCol)
		}
	}
	// A generated column inside the PK — the MariaDB system-versioning shape
	// (#1266) — can never canonicalize: the baseline omits generated columns,
	// so every probe row would die with MissingPKColumnError deep in the
	// merge. Refuse up front with the versioning-aware message instead; see
	// GeneratedPKColumn for why a reduced join key is NOT the fix.
	// Deliberately AFTER the type loop: an empty DataType must keep winning
	// the #1009 wrong-path verdict (PG-shaped snapshot on the MySQL path),
	// which only the type gate discriminates.
	if pkCol, ok := GeneratedPKColumn(pkCols); ok {
		return nil, fullTableGeneratedPKRefusal(schema, table, pkCol)
	}

	// For DATETIME/TIMESTAMP PK columns, warn loudly if the column_type
	// metadata is missing — the canonicalizer will fall back to a
	// Nanosecond()==0 heuristic that is correct for DATETIME(0) but
	// silently wrong for DATETIME(N>0) whole-second values. Operators
	// should re-run `bintrail snapshot` to refresh schema_snapshots with
	// the new column_type field (added in the precision-aware PK fix).
	for _, pkCol := range pkCols {
		dt := strings.ToLower(strings.TrimSpace(pkCol.DataType))
		if (dt == "datetime" || dt == "timestamp") && pkCol.ColumnType == "" {
			slog.Warn("full-table reconstruct: DATETIME/TIMESTAMP PK column has no column_type in schema_snapshots; "+
				"using best-effort precision heuristic — DATETIME(N>0) whole-second values may silently miss the baseline. "+
				"Re-run `bintrail snapshot` to refresh.",
				"schema", schema, "table", table, "column", pkCol.Name)
		}
	}

	// ── 3a-bis. Parquet mode only: refuse when the baseline's schema is no ──
	// longer the current one. The #602/#843 guards below catch drift only when
	// a delta event carries (or stops carrying) the column, so an ALTER that no
	// write followed is invisible to them — and the emitted snapshot would then
	// declare an obsolete CREATE TABLE over rows nobody can place. Only Parquet
	// mode: a mydumper dump is read by a human or myloader against a table they
	// chose, while a baseline is picked up automatically by every later
	// reconstruct.
	if cfg.OutputFormat == OutputFormatParquet {
		if err := checkBaselineSchemaCurrent(bmeta.CreateTableSQL, tm, schema, table); err != nil {
			return nil, err
		}
		// A gapped ancestor taints every descendant: the events it lost are
		// still lost. Warn on READ as well as stamping on write, so an operator
		// reconstructing from such a chain is told once per run rather than
		// having to inspect a Parquet footer.
		if bmeta.CaptureGap != "" {
			slog.Warn("the baseline this reconstruction is anchored on was itself published over a KNOWN capture gap; "+
				"its missing events are missing here too",
				"schema", schema, "table", table, "baseline", baselinePath, "capture_gap", bmeta.CaptureGap)
		}
	}

	// ── 3b. Refuse if a TRUNCATE/DROP/RENAME hit this table in the window ──
	// TRUNCATE/DROP emit no row events (#764): without this check the merge
	// below would replay the baseline straight through and silently
	// resurrect rows the DDL actually deleted.
	if err := CheckDestructiveDDL(ctx, db, schema, table, snapshotTime, cfg.At); err != nil {
		return nil, err
	}

	// ── 3c. Refuse/warn on a stamped capture gap inside the window (#765) ──
	// stream_state.gap_lost_at records an irreparable capture gap (source
	// binlogs purged before the stream caught up); unlike the archive-coverage
	// gap the fetch below already guards against, no amount of archive
	// resolution can fill this — it must be checked directly.
	// The finding is kept, not just acted on: under --allow-gaps the run
	// proceeds over a known permanent loss, and a Parquet snapshot published
	// that way must carry the fact in its own metadata (#1170). A log line
	// cannot: the artifact outlives the terminal.
	capGap, err := CheckCaptureGapStatus(ctx, db, schema, table, snapshotTime, cfg.At, cfg.AllowGaps)
	if err != nil {
		return nil, err
	}

	// ── 4. Fetch events via the shared helper (gap-aware) ──────────────────
	fetchOpts := query.Options{
		Schema: schema,
		Table:  table,
		Since:  &snapshotTime,
		Until:  &cfg.At,
		// No PKValues filter — we want every event for this table.
	}
	// Anchor the delta window's lower bound on the baseline's exact recorded
	// binlog position instead of its imprecise dump-start DATETIME (#797): a
	// transaction whose statement executed just before snapshotTime but
	// committed just after it would otherwise fall through both the dump's
	// MVCC snapshot AND a Since-only fetch, silently missing from the
	// reconstruction. Older baselines that never recorded a position
	// (BinlogFile=="" or BinlogPos==0, the established "absent" convention —
	// see baseline.DumpMetadata) fall back to the plain Since-only fetch.
	if bmeta.BinlogFile != "" && bmeta.BinlogPos > 0 {
		fetchOpts.SincePos = &query.BinlogPos{File: bmeta.BinlogFile, Pos: uint64(bmeta.BinlogPos)}
	}
	// In Parquet mode the window's upper bound is the run's binlog cut, not just
	// the target time. This is what makes the emitted snapshot a valid anchor:
	// the set folded here (end_pos <= cut) and the set the NEXT reconstruct
	// fetches from this snapshot (start_pos >= cut) partition the binlog exactly.
	// The `Until: cfg.At` filter above stays and is a strict superset of the
	// positional window by construction — see ResolveSnapshotCut for the proof
	// and for why the reverse derivation (a position from a time cut) is not
	// safe. cfg.cut is nil only when the index holds no events at all, where the
	// window is empty either way.
	if cfg.OutputFormat == OutputFormatParquet && cfg.cut != nil {
		fetchOpts.UntilPos = cfg.cut
	}
	// nil ArchiveFetcher → the container-safe parquetquery.Fetch. Resolved here
	// at the point of use so both ReconstructTables and any direct
	// ReconstructTable caller get the default (#510).
	fetcher := cfg.ArchiveFetcher
	if fetcher == nil {
		fetcher = parquetquery.Fetch
	}
	// ── 5. Stream the window and fold it into the change map ──────────────
	// Paged, not materialized (#1097): the window is walked in ascending
	// (event_timestamp, event_id) order and folded page by page, so the raw
	// event slice never exists in full. foldEventWindow also runs the per-event
	// ENUM/base64 decoding (#475/#476/#668) and the #592/#782 guards on each
	// page before trimming it into the map — see its doc comment for why those
	// guards MUST live there and not on the finished map.
	//
	// NoArchive is passed false unconditionally and the stream decides whether
	// to query archives — it already handles the empty-archive case in its fast
	// path. The pre-#1097 `len(archSources)==0` gate was wrong: it disabled
	// archive routing even when the fetch could have resolved sources itself.
	fold, err := foldEventWindow(ctx, withFoldBudgets(cfg, foldConfig{
		DB:             db,
		Engine:         engine,
		DBName:         dbName,
		Resolver:       resolver,
		Schema:         schema,
		Table:          table,
		PKCols:         pkCols,
		Opts:           fetchOpts,
		AllowGaps:      cfg.AllowGaps,
		ArchiveFetcher: fetcher,
	}))
	if err != nil {
		return nil, err
	}
	rep.EventsApplied = fold.Total
	changes := fold.Changes

	// Warn on a gap between the baseline anchor and the first indexed event.
	// The single-row path already does this (cli/reconstruct.go); the full-table
	// path previously emitted a dump silently missing that gap with no signal
	// (#781). Same call/semantics as single-row: warn-only, --allow-gaps governs
	// the coverage-gap fetch above and CheckCaptureGap, not this
	// baseline-vs-first-event visibility warning. bmeta was read in step 2; the
	// first event is carried out of the fold because its page is gone by now.
	if fold.First != nil {
		flavor := query.SourceFlavor(db)
		start, startOK := query.OldestIndexedEvent(db)
		WarnBaselineFirstEventGap(flavor, bmeta, *fold.First, start, startOK, schema, table)
	}

	// ── 5b. Nothing changed: publish the previous file instead of rewriting ─
	// Placed HERE on purpose: after the fold, so the change map is known, and
	// after steps 3a-bis/3b/3c, so every refusal still runs first. A TRUNCATE
	// emits no row events, so an empty change map alone would not mean the
	// table is untouched — CheckDestructiveDDL is what makes it mean that.
	//
	// This also skips step 6, which reads the whole baseline back for DuckDB,
	// so the saving is the read as well as the write.
	//
	// capGap is passed in because step 3c does NOT refuse under --allow-gaps:
	// it returns the finding and lets the run proceed. See carryForwardEligible
	// for why a known gap disqualifies a table from being carried at all.
	if carryForwardEligible(cfg.CarryForwardUnchanged, cfg.OutputFormat, baselinePath, len(changes), capGap) {
		linked, cerr := carryForward(ctx, baselinePath, cfg.snapshotDir, schema, table)
		if cerr != nil {
			return nil, fmt.Errorf("carry %s.%s forward unchanged: %w", schema, table, cerr)
		}
		rep.CarriedForward = true
		rep.CarriedByLink = linked
		// Relative to the snapshot dir, matching what mergeBaselineIntoParquet
		// sets for a table it writes: one field cannot mean two things, and
		// consumers join it themselves (internal/cli/drill.go does).
		rep.Files = []string{filepath.Join(schema, table+".parquet")}
		rep.Duration = time.Since(start)
		// CarriedForward stays "published by reuse rather than by folding",
		// true for the copy arm too; the link-versus-copy split rides
		// CarriedByLink (#1578) so the console's disk claim can count only
		// the arm that saved disk. carryForward logs the cause of a copy.
		slog.Info("table carried forward unchanged", "schema", schema, "table", table,
			"reason", "no events in the window", "linked", linked)
		return rep, nil
	}

	// ── 6. Materialize the baseline locally for DuckDB streaming ───────────
	localPath, cleanup, err := materializeBaselineLocal(ctx, baselinePath, cfg.DuckDBTuning)
	if err != nil {
		return nil, fmt.Errorf("materialize baseline: %w", err)
	}
	defer cleanup()

	// ── 7-9. Merge baseline + changes into the output writer ──────────────
	// The merge loop is extracted so it can be unit-tested without MySQL.
	in := mergeInput{
		LocalBaselinePath: localPath,
		CreateTableSQL:    bmeta.CreateTableSQL,
		Schema:            schema,
		Table:             table,
		PKCols:            pkCols,
		Changes:           changes,
		ImageColumns:      fold.ImageColumns,
		SawImage:          fold.SawImage,
		CurrentGenerated:  generatedByName(tm.Columns),
		OutputDir:         cfg.OutputDir,
		ChunkSize:         cfg.ChunkSize,
		DuckDBTuning:      cfg.DuckDBTuning,
	}
	if cfg.OutputFormat == OutputFormatParquet {
		in.SnapshotDir = cfg.snapshotDir
		in.SnapshotAt = cfg.At
		in.Cut = cfg.cut
		in.CaptureGap = capGap
		in.SourceBaseline = baselineMeta{
			Path:     baselinePath,
			Time:     snapshotTime,
			Metadata: bmeta,
		}
		if err := mergeBaselineIntoParquet(ctx, in, rep); err != nil {
			return nil, err
		}
	} else if err := mergeBaselineIntoWriter(ctx, in, rep); err != nil {
		return nil, err
	}
	rep.Duration = time.Since(start)

	slog.Info("table reconstructed",
		"schema", schema, "table", table,
		"baseline_rows", rep.BaselineRows,
		"events_applied", rep.EventsApplied,
		"updates_applied", rep.UpdatesApplied,
		"inserts_emitted", rep.InsertsEmitted,
		"deletes_skipped", rep.DeletesSkipped,
		"duration_ms", rep.Duration.Milliseconds())
	return rep, nil
}

// prepareMerge reads the baseline's column list and runs the two schema-drift
// guards that must fire BEFORE any output file is opened, so a refused table
// leaves nothing on disk. Shared by both output formats — the drift they detect
// is a property of the reconstruction, not of the artifact it is written into,
// and a guard that ran for one format only would make the other silently wrong.
func prepareMerge(ctx context.Context, in mergeInput) ([]string, error) {
	colNames, err := readBaselineColumns(ctx, in.LocalBaselinePath, in.DuckDBTuning)
	if err != nil {
		return nil, fmt.Errorf("read baseline columns: %w", err)
	}

	// Fail loud on a column ADDED after the baseline (#602). This path projects
	// every emitted row onto the baseline column set and carries the baseline's
	// CREATE TABLE as the schema; it does not reconstruct intermediate DDL. So a
	// delta event's row_after key absent from the baseline columns would be
	// dropped silently (its value never reaches the output). Refuse instead, the
	// same fail-loud choice the supportedPKType guard in ReconstructTable makes:
	// a warning isn't enough because an operator running --log-level error would
	// not see it and would get output silently missing a column.
	// A STORED/VIRTUAL generated column is absent from the baseline BY DESIGN:
	// `bintrail baseline` (mydumper) leaves it out of the dump because the
	// server refuses an explicit value for it, while every ROW image carries
	// it. Without this exclusion the first fold over any table with such a
	// column refused with ErrSchemaChanged (#1624), and on a schedule that
	// refusal fell back to a FULL backup at every slot. Dropping the value is
	// the right thing: the server recomputes it on load, the same way the
	// baseline path already excludes it from the emitted column list.
	extra := postBaselineColumns(in.Changes, colNames)
	if gen := generatedColumnsIn(in.CreateTableSQL); len(gen) > 0 && len(extra) > 0 {
		kept := extra[:0]
		var dropped []string
		for _, col := range extra {
			if _, ok := gen[col]; ok {
				if cur, known := in.CurrentGenerated[col]; known && !cur {
					return nil, fmt.Errorf(
						"full-table reconstruct: %s.%s column %s was a generated column when the baseline was taken and is a plain column now; "+
							"the baseline holds no value for it — re-run `bintrail baseline` to capture a snapshot that includes it: %w",
						in.Schema, in.Table, col, ErrSchemaChanged)
				}
				dropped = append(dropped, col)
				continue
			}
			kept = append(kept, col)
		}
		extra = kept
		if len(dropped) > 0 {
			slog.Debug("full-table reconstruct: generated column(s) in delta events left out of the output, the server recomputes them",
				"schema", in.Schema, "table", in.Table, "columns", strings.Join(dropped, ", "))
		}
	}
	if len(extra) > 0 {
		return nil, fmt.Errorf(
			"full-table reconstruct: %s.%s has column(s) %s present in delta events but absent from the baseline schema "+
				"(added after the baseline snapshot); their values cannot be emitted without dropping data silently — "+
				"re-run `bintrail baseline` to capture a snapshot that includes the new column(s): %w",
			in.Schema, in.Table, strings.Join(extra, ", "), ErrSchemaChanged)
	}

	// Fail loud on a baseline column DROPPED after the baseline (#843), the
	// symmetric direction of the #602 guard above: post-drop delta events stop
	// carrying the column in row_after, so projecting them onto the baseline
	// columns would NULL-fill it while never-touched pass-through rows keep
	// the pre-drop value — one artifact mixing two schema epochs (a state that
	// never existed) under a CREATE TABLE that still declares the column.
	// Column ABSENCE from the image is the signal (binlog_row_image=FULL is a
	// hard requirement, so an after-image always carries every column live at
	// event time); a genuinely-NULL value is present in the map with a nil value
	// and passes through untouched. Aggregated over the whole change map — not
	// the old per-row×column slog.Warn.
	if missing := droppedBaselineColumns(in.ImageColumns, in.SawImage, colNames); len(missing) > 0 {
		return nil, fmt.Errorf(
			"full-table reconstruct: %s.%s has baseline column(s) %s absent from delta-event row images "+
				"(dropped from the source table after the baseline snapshot); emitting would mix schema epochs — "+
				"rows touched after the drop would carry NULL while never-touched rows keep the pre-drop value — "+
				"re-run `bintrail baseline` to capture a snapshot of the current schema: %w",
			in.Schema, in.Table, strings.Join(missing, ", "), ErrSchemaChanged)
	}
	return colNames, nil
}

// mergeInput bundles everything mergeBaselineIntoWriter needs. Extracted so
// unit tests can exercise the merge loop without standing up MySQL.
type mergeInput struct {
	LocalBaselinePath string
	CreateTableSQL    string
	Schema            string
	Table             string
	PKCols            []metadata.ColumnMeta
	// Changes is the completed build side of the merge, as produced by
	// foldEventWindow. Its entries are TRIMMED (retainEvent blanks RowBefore
	// and the query-text fields), which is why no guard reading a before-image
	// may run against this map — see the note above mergeBaselineIntoWriter.
	Changes map[string]*query.ResultRow
	// ImageColumns/SawImage come from foldResult and carry the #843 signal the
	// trimmed Changes map can no longer provide (see droppedBaselineColumns).
	ImageColumns map[string]struct{}
	SawImage     bool
	// CurrentGenerated is the schema snapshot's view of which columns are
	// generated today, keyed by name (absent = the snapshot does not know the
	// column). prepareMerge cross-checks it against the baseline CREATE TABLE:
	// a column the footer calls generated but the snapshot calls plain was
	// converted after the snapshot (MySQL allows MODIFY on a STORED generated
	// column), so the baseline holds no value for it and dropping it would
	// lose data. Parquet mode also refuses that shape in
	// checkBaselineSchemaCurrent; mydumper mode has only this check.
	CurrentGenerated map[string]bool
	OutputDir        string
	ChunkSize        int64

	// SnapshotDir / SnapshotAt / Cut / SourceBaseline are set only under
	// OutputFormatParquet and drive mergeBaselineIntoParquet: where the snapshot
	// goes, the instant it is labelled with, the binlog coordinate it is anchored
	// at (nil = index holds no events, so the source baseline's own anchor
	// carries over), and the snapshot it was derived from.
	SnapshotDir    string
	SnapshotAt     time.Time
	Cut            *query.BinlogPos
	SourceBaseline baselineMeta
	// CaptureGap is the permanent-loss finding this run OVERRODE under
	// AllowGaps, or nil when the window was verifiably clean. Non-nil means the
	// emitted snapshot is knowingly incomplete and must say so in its metadata.
	CaptureGap *CaptureGap

	// DuckDBTuning sets the resource budget for the DuckDB sessions this
	// function opens (readBaselineColumns, mergeBaselineImages) (#842). Zero
	// value → the container-safe default; see effectiveDuckDBTuning.
	DuckDBTuning duckdbutil.Tuning
}

// mergeBaselineIntoWriter streams the local baseline Parquet via DuckDB,
// applies the change map to produce the final row set, and writes the result
// through a MydumperWriter. Updates counters on rep in place. Drains the
// Changes map: after this function returns, entries still present are rows
// that were NOT found in the baseline (appended as new INSERTs).
//
// The actual merge is delegated to mergeBaselineImages so the same logic
// backs both this writer path and the shim's in-memory full-table _snapshot
// path (SnapshotFullTableImages). This function is the thin writer wrapper:
// it owns the MydumperWriter and turns each emitted row map into an ordered
// tuple the writer expects.
//
// Error-path cleanup (#1162): on a non-nil return the deferred Discard
// unlinks everything this table's writer created — the in-progress chunk,
// already-rotated chunks, and the schema file — so a failed table leaves no
// artifacts on disk. Close would instead FINALIZE the current chunk
// (terminating semicolon, flush, keep the file), leaving a syntactically
// valid, loadable chunk holding a PREFIX of the table, with the run-level
// _INCOMPLETE marker as the only signal — one that myloader and
// `cat out/*.sql | mysql` never read. The guards that predate the writer
// opening — #602, #843 — refuse before any file exists; a per-row guard like
// #1158 fires mid-scan and cannot, which is what makes the discard necessary.
// The writer tracks only its own table's files, so sibling tables' completed
// output in the same OutputDir is never touched.
//
// GUARD PLACEMENT (#1097) — read before adding a check here.
//
// The two guards that inspect an event's BEFORE-image — #592 (residual
// unresolved-TOAST marker) and #782 (PK-changing UPDATE) — used to run at the
// top of this function, against in.Changes. They do not, and must not, any
// more: the map handed in by ReconstructTable is trimmed (retainEvent blanks
// RowBefore so a streamed page can be released), so a before-image check
// against it would find nothing to look at, return "clean" on every input, and
// still read like a guard at the call site. That is a worse state than having
// no guard at all.
//
// Both now run per event inside foldEventWindow, on the untrimmed event, before
// it is ever folded — which is also strictly stronger for #782: the map only
// held the surviving last event per PK, so a PK-changing UPDATE whose old key a
// later event reused was invisible to a map scan. The map-level helpers
// (checkChangesToast, pkChangingUpdate) still exist for the two callers whose
// maps DO carry before-images (the shim's _snapshot path and the binlog-only
// fallback); they are simply not applicable to this one.
func mergeBaselineIntoWriter(ctx context.Context, in mergeInput, rep *TableReport) (retErr error) {
	colNames, err := prepareMerge(ctx, in)
	if err != nil {
		return err
	}

	mw, err := NewMydumperWriter(in.OutputDir, in.Schema, in.Table, colNames, in.ChunkSize)
	if err != nil {
		return fmt.Errorf("open mydumper writer: %w", err)
	}
	// Success finalizes via the explicit Close below (before capturing
	// rep.Files); ANY error return instead discards every file this writer
	// wrote — see the #1162 note in the function comment. The discard also
	// covers a failed explicit Close: its rotated chunks are complete files,
	// but the table as a whole is not, and a partial table must leave nothing
	// behind. A removal failure is logged, never returned — it must not
	// shadow the error that triggered the abort.
	defer func() {
		if retErr == nil {
			return
		}
		if derr := mw.Discard(); derr != nil {
			slog.Warn("could not remove partial mydumper output for failed table",
				"schema", in.Schema, "table", in.Table, "error", derr)
		}
	}()

	if err := mw.WriteSchema(in.CreateTableSQL); err != nil {
		return err
	}

	// BLOB/TEXT base64 of the delta-event after-images is already decoded by
	// the caller (DecodeEventBinaries, run epoch-aware over the full events
	// slice before the Changes map was built, #668) — in.Changes entries alias
	// those same event structs. Baseline pass-through rows are untouched here:
	// their TEXT values arrive from the DuckDB scan as Go strings and must NOT
	// be decoded (#660).

	// emit re-orders each emitted row map into the baseline Parquet column
	// order the writer was constructed with. For baseline pass-through rows
	// the map is keyed by exactly those columns (so rowAfterOrdered is an
	// order-preserving identity); for event after-images it aligns the
	// event's row_after to the baseline schema — drift in either direction
	// was already refused up front (#602/#843), so the alignment is total.
	stats, err := mergeBaselineImages(ctx, mergeCore{
		LocalBaselinePath: in.LocalBaselinePath,
		Schema:            in.Schema,
		Table:             in.Table,
		PKCols:            in.PKCols,
		Changes:           in.Changes,
		DuckDBTuning:      in.DuckDBTuning,
	}, func(rowMap map[string]any) error {
		return mw.WriteRow(rowAfterOrdered(rowMap, colNames, in.Schema, in.Table))
	})
	if err != nil {
		return err
	}
	rep.BaselineRows = stats.BaselineRows
	rep.UpdatesApplied = stats.UpdatesApplied
	rep.InsertsEmitted = stats.InsertsEmitted
	rep.DeletesSkipped = stats.DeletesSkipped

	// Close explicitly to promote close errors into the happy-path return
	// value (the deferred Close above is a no-op after this succeeds, and
	// a no-op from an already-closed writer is safe).
	if err := mw.Close(); err != nil {
		return fmt.Errorf("close mydumper writer: %w", err)
	}
	rep.Files = mw.Files()
	rep.RowsWritten = mw.Rows()
	return nil
}

// mergeCore is the writer-agnostic input to mergeBaselineImages.
type mergeCore struct {
	LocalBaselinePath string
	Schema            string
	Table             string
	PKCols            []metadata.ColumnMeta
	Changes           map[string]*query.ResultRow
	// PGTextPK skips the MySQL PK canonicalizer for a PostgreSQL source (text
	// PK on both baseline and delta sides). See SnapshotFullTableInput.PGTextPK.
	PGTextPK bool
	// DuckDBTuning sets the resource budget for the DuckDB session this
	// function opens to scan the baseline (#842). Zero value → the
	// container-safe default; see effectiveDuckDBTuning.
	DuckDBTuning duckdbutil.Tuning
}

// mergeStats counts what mergeBaselineImages did, for the offline
// TableReport. The shim path ignores it.
type mergeStats struct {
	BaselineRows   int64
	UpdatesApplied int64
	InsertsEmitted int64
	DeletesSkipped int64
}

// mergeBaselineImages streams the local baseline Parquet via DuckDB, applies
// the change map, and calls emit once per row of the reconstructed table, in
// baseline Parquet column order:
//
//   - a baseline row not present in Changes is emitted as-is (pass-through);
//   - a baseline row whose PK is in Changes is emitted as the event's
//     row_after image (UPDATE/INSERT wins over the baseline), or skipped
//     entirely when the latest event is a DELETE;
//   - PKs left in Changes after the baseline scan are post-snapshot inserts
//     (rows that did not exist at baseline time) and are emitted last, in
//     deterministic PK order.
//
// The emitted map is keyed by column name. Pass-through rows carry the
// baseline's values verbatim (DuckDB scan types — time.Time, []byte, …);
// event rows carry the binlog event's row_after (JSON-decoded). Callers that
// build a wire resultset normalise the values; the writer path re-orders them.
// emit returning a non-nil error stops the merge and propagates that error
// (used by the shim to enforce its row cap). Drains in.Changes.
//
// This is the shared core for both the offline `bintrail reconstruct` writer
// (mergeBaselineIntoWriter) and the shim full-table _snapshot path
// (SnapshotFullTableImages, via runSnapshotFullTable), so the two reconstruction
// surfaces can never drift in the merge ALGORITHM (baseline/event matching,
// ordering, drain). Per-value decoding is NOT done here — every caller decodes
// its own Changes map before it ever reaches this function: the writer caller
// upstream in ReconstructTable, on the full events slice before the Changes
// map is even built (DecodeEventBinaries, #668); the shim caller upstream in
// runSnapshotFullTable, via mapEventImages (#661); `verify`'s callers
// (reconstructDigest and ExplainBaselinePairMismatch) the same way, upstream
// of their own Changes map builds (#672). Every SnapshotFullTableImages caller
// now decodes before this function ever sees a value.
func mergeBaselineImages(ctx context.Context, in mergeCore, emit func(map[string]any) error) (mergeStats, error) {
	var stats mergeStats

	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		return stats, fmt.Errorf("open duckdb: %w", err)
	}
	defer ddb.Close()
	applyDuckDBTuning(ctx, ddb, in.DuckDBTuning)

	safePath := strings.ReplaceAll(in.LocalBaselinePath, "'", "''")
	q := fmt.Sprintf("SELECT * FROM parquet_scan('%s')", safePath)
	drows, err := ddb.QueryContext(ctx, q)
	if err != nil {
		return stats, fmt.Errorf("duckdb baseline query: %w", err)
	}
	defer drows.Close()

	dcols, err := drows.Columns()
	if err != nil {
		return stats, fmt.Errorf("duckdb columns: %w", err)
	}

	warnUndetectableBinaryPK(in.Schema, in.Table, in.PKCols)

	scan := make([]any, len(dcols))
	ptrs := make([]any, len(dcols))
	for i := range scan {
		ptrs[i] = &scan[i]
	}

	for drows.Next() {
		if err := drows.Scan(ptrs...); err != nil {
			return stats, fmt.Errorf("scan baseline row: %w", err)
		}
		// zipMap reads the scanned values into a fresh map; database/sql
		// clones []byte into the *any destinations and reassigns (never
		// mutates) scan[i] on the next Next(), so the map is safe to retain
		// past this iteration — which the shim relies on when it buffers
		// emitted rows.
		rowMap := zipMap(dcols, scan)
		// Canonicalise PK values before hashing so they match what the
		// indexer stored in binlog_events.pk_values. Without this,
		// DATETIME/TIMESTAMP PKs silently miss the change map because
		// DuckDB returns time.Time while the indexer stored a
		// go-mysql-formatted string (#212). canonicalizePKMap does not
		// mutate rowMap, so the emitted pass-through row keeps its original
		// values.
		// PostgreSQL: baseline COPY text and delta pgoutput text are the same
		// bytes, so the PK match is string-identity — skip the MySQL DATA_TYPE
		// canonicalizer, which errors on PG's empty DATA_TYPE token. MySQL still
		// canonicalizes (DuckDB returns time.Time for a DATETIME/TIMESTAMP PK,
		// which must become the indexer's stored string, #212).
		pkMap := rowMap
		if !in.PGTextPK {
			var err error
			if pkMap, err = canonicalizePKMap(rowMap, in.PKCols); err != nil {
				return stats, fmt.Errorf("canonicalize baseline PK for %s.%s: %w", in.Schema, in.Table, err)
			}
		}
		pk := event.BuildPKValues(in.PKCols, pkMap)

		// Before deciding what claims this row, check the ONE other spelling
		// its key could carry (#1158) — see altFixedBinaryPK.
		//
		// Hoisted above the branch on purpose. in.Changes is keyed by STRING,
		// so two spellings of one logical row are two INDEPENDENT entries: an
		// entry under the canonical spelling would send this row down the
		// claimed branch while a sibling entry under the alternate one
		// survives the scan and lands in the leftover tail, where a DELETE is
		// dropped without a word. Checking only the unclaimed branch would
		// leave exactly the resurrection this guard exists to stop.
		if alt, ok := altFixedBinaryPK(in.PKCols, pkMap); ok {
			if ev, pending := in.Changes[alt]; pending {
				return stats, pkSpellingJoinErr(in.Schema, in.Table, pk, alt, ev.EventType)
			}
		}

		if ev, ok := in.Changes[pk]; ok {
			delete(in.Changes, pk)
			switch ev.EventType {
			case event.EventDelete:
				stats.DeletesSkipped++
				continue
			case event.EventUpdate, event.EventInsert:
				// Defensive: a non-DELETE event with nil RowAfter would
				// otherwise emit an all-NULL tuple. This indicates a
				// corrupt event or parser bug, not a normal code path.
				if ev.RowAfter == nil {
					slog.Error("event has nil RowAfter; skipping to avoid emitting all-NULL tuple",
						"schema", in.Schema, "table", in.Table, "pk", pk,
						"event_type", ev.EventType, "event_id", ev.EventID)
					continue
				}
				if err := emit(ev.RowAfter); err != nil {
					return stats, err
				}
				stats.UpdatesApplied++
			}
		} else {
			if err := emit(rowMap); err != nil {
				return stats, err
			}
			stats.BaselineRows++
		}
	}
	if err := drows.Err(); err != nil {
		return stats, fmt.Errorf("iterate baseline rows: %w", err)
	}

	// Append events for PKs that weren't in the baseline (rows inserted
	// after the snapshot). Deterministic order: sort by PK string so tests
	// can assert on the output without flakiness.
	newPKs := make([]string, 0, len(in.Changes))
	for pk := range in.Changes {
		newPKs = append(newPKs, pk)
	}
	sort.Strings(newPKs)
	for _, pk := range newPKs {
		ev := in.Changes[pk]
		if ev.EventType == event.EventDelete {
			continue
		}
		if ev.RowAfter == nil {
			slog.Error("event has nil RowAfter; skipping to avoid emitting all-NULL tuple",
				"schema", in.Schema, "table", in.Table, "pk", pk,
				"event_type", ev.EventType, "event_id", ev.EventID)
			continue
		}
		if err := emit(ev.RowAfter); err != nil {
			return stats, err
		}
		stats.InsertsEmitted++
	}

	return stats, nil
}

// SnapshotFullTableInput drives SnapshotFullTableImages.
type SnapshotFullTableInput struct {
	// BaselinePath is the baseline Parquet path from FindBaseline — either a
	// local path or an s3:// URL (downloaded to a temp file before merge).
	BaselinePath string
	Schema       string
	Table        string
	// PKCols are the table's primary-key column metas, used to canonicalize
	// baseline PK values so they match the indexer's pk_values strings. The
	// caller is expected to have guarded every PKCol with SupportedPKType.
	PKCols []metadata.ColumnMeta
	// Changes maps pk_values string → the latest binlog event for that PK in
	// (baseline, AsOf]. Drained by the merge.
	Changes map[string]*query.ResultRow
	// Events is the RAW event slice Changes was folded from, for the
	// window-complete #782 PK-change guard. Callers fetching LimitPerPK=1 (shim
	// `_snapshot`, verify) hand a slice already collapsed to the latest event
	// per PK by the query, so their detection is bounded by that fetch; nil
	// falls back to the map-only backstop (pkChangingUpdate).
	Events []query.ResultRow
	// PGTextPK marks a PostgreSQL source: the baseline Parquet and the delta
	// pk_values BOTH store every column as raw text (pgbaseline COPY text ==
	// pgoutput text), so the PK match is string-identity — skip the MySQL
	// DATA_TYPE canonicalizer, which errors on PG's empty DATA_TYPE token. A
	// caller that sets this must have skipped the SupportedPKType precondition
	// accordingly. Zero value (false) preserves the MySQL path verbatim.
	PGTextPK bool
	// DuckDBTuning sets the resource budget for the DuckDB sessions this call
	// opens (materializeBaselineLocal, mergeBaselineImages) (#842). Zero value
	// → the container-safe default (see effectiveDuckDBTuning) — the right
	// choice for every current caller (shim, verify), which are long-lived or
	// short-lived-but-unbudgeted and were never meant to run host-greedy.
	DuckDBTuning duckdbutil.Tuning
}

// SnapshotFullTableImages reconstructs the full row state of a table at the
// AsOf instant by merging the baseline snapshot with the change map, calling
// emit once per surviving row (baseline column order, values verbatim from
// DuckDB / the binlog event). It is the in-memory sibling of
// mergeBaselineIntoWriter, for the shim's full-table _snapshot path: instead
// of writing mydumper output it streams row maps to the caller, which builds
// the wire resultset and enforces its own row cap (by returning an error from
// emit).
//
// s3:// baselines are downloaded to a temp file first; the temp dir is removed
// before return. Real failures (unreadable baseline, DuckDB error, a PK value
// the canonicalizer can't translate) are returned as errors for the caller to
// surface; the caller decides separately — before calling this — whether a
// missing baseline or an unsupported PK type should instead fall back to a
// binlog-only path.
func SnapshotFullTableImages(ctx context.Context, in SnapshotFullTableInput, emit func(map[string]any) error) error {
	// Residual unchanged-TOAST marker (#592) → refuse before materializing the
	// baseline (possibly an S3 download); see mergeBaselineIntoWriter.
	if err := checkChangesToast(in.Changes); err != nil {
		return err
	}

	// PK-changing UPDATE (#782) → refuse before materializing the baseline, for
	// the same reason as the mydumper writer path: the change map is keyed by
	// the before-image PK, so a changed PK silently duplicates/resurrects rows.
	// Authoritative raw-slice scan first (window-complete when the caller fetched
	// the full window), map backstop second.
	if b, a, ok := pkChangingUpdateInEvents(in.Events, in.PKCols); ok {
		return pkChangingUpdateErr(in.Schema, in.Table, b, a)
	}
	if b, a, ok := pkChangingUpdate(in.Changes, in.PKCols); ok {
		return pkChangingUpdateErr(in.Schema, in.Table, b, a)
	}

	localPath, cleanup, err := materializeBaselineLocal(ctx, in.BaselinePath, in.DuckDBTuning)
	if err != nil {
		return fmt.Errorf("materialize baseline: %w", err)
	}
	defer cleanup()

	_, err = mergeBaselineImages(ctx, mergeCore{
		LocalBaselinePath: localPath,
		Schema:            in.Schema,
		Table:             in.Table,
		PKCols:            in.PKCols,
		Changes:           in.Changes,
		PGTextPK:          in.PGTextPK,
		DuckDBTuning:      in.DuckDBTuning,
	}, emit)
	return err
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// checkChangesToast scans a full-table change map for a residual
// unchanged-TOAST marker (#592). Both full-table entry points (the mydumper
// writer and the shim's in-memory _snapshot) call it BEFORE any IO — every
// change is destined for the output (an UPDATE/INSERT overwrites the baseline
// row with its row_after, a leftover change is appended as a new row), so a
// marker anywhere in the map means the output would carry the marker's JSON
// instead of a real value.
func checkChangesToast(changes map[string]*query.ResultRow) error {
	for _, ev := range changes {
		if err := checkEventToast(*ev); err != nil {
			return err
		}
	}
	return nil
}

// pkChangedInEvent reports whether a single event is an UPDATE whose
// before-image primary key differs from its after-image primary key (#782).
// before/after are recomputed from the SAME event's images (not compared
// against the stored map key) so a numeric-representation difference between
// the parser-native stored key and the JSON-decoded images can't produce a
// false positive. Non-UPDATE events, and UPDATEs missing a before OR after
// image (a partial image under an unsupported non-FULL row image), report
// changed=false — this bug needs both PKs.
func pkChangedInEvent(ev *query.ResultRow, pkCols []metadata.ColumnMeta) (before, after string, changed bool) {
	if ev == nil || ev.EventType != event.EventUpdate {
		return "", "", false
	}
	if ev.RowBefore == nil || ev.RowAfter == nil {
		return "", "", false
	}
	b := event.BuildPKValues(pkCols, ev.RowBefore)
	a := event.BuildPKValues(pkCols, ev.RowAfter)
	if b != a {
		return b, a, true
	}
	return "", "", false
}

// pkChangingUpdateInEvents scans the RAW, pre-collapse event slice for a
// PK-changing UPDATE (#782). This is the AUTHORITATIVE, window-complete check.
// The change map every full-table entry point folds events into is keyed by the
// BEFORE-image PK (the parser stores an UPDATE's pk_values from its before
// image, parser.go), so a PK-changing UPDATE whose old key is later reused by
// another event — e.g. `UPDATE pk 1→2; INSERT pk=1` (both stored under key 1) —
// is OVERWRITTEN in the map by that later event and vanishes from a map-only
// scan, silently DROPPING the moved row (pk=2) from the output. Scanning the
// raw slice, before it collapses, sees every event and cannot miss it.
//
// The failure shapes a folded PK-changing UPDATE produces in the merged output:
//
//   - Resurrection: `UPDATE pk 1→2; DELETE pk=2` — the DELETE is keyed by the
//     new PK (2), never colliding with the UPDATE entry (keyed by 1), so the
//     merge emits the pk=2 after-image for a row the DELETE actually removed.
//   - Duplication: `UPDATE pk 1→2; UPDATE pk=2` — pk=2 is emitted twice (once
//     as the first UPDATE's after-image under key 1, once by the second event
//     under key 2), a 1062 that only surfaces at restore time.
//   - Silent drop: `UPDATE pk 1→2; INSERT pk=1` — the map-only scan misses the
//     overwritten UPDATE, so the moved row (pk=2) is never emitted.
//
// The slice is already sorted ascending by (event_timestamp, event_id), so this
// returns the EARLIEST offender deterministically. Callers that fetch the full
// window (reconstruct dump, binlog-only fallback) pass their raw events here so
// the check is window-complete; the map-scan backstop (pkChangingUpdate) covers
// callers that don't. Note that callers fetching LimitPerPK=1 (shim `_snapshot`,
// verify) hand a raw slice already collapsed by the query to the latest event
// per PK, so their detection is bounded by that fetch, not by this scan.
func pkChangingUpdateInEvents(events []query.ResultRow, pkCols []metadata.ColumnMeta) (before, after string, found bool) {
	for i := range events {
		if b, a, ok := pkChangedInEvent(&events[i], pkCols); ok {
			return b, a, true
		}
	}
	return "", "", false
}

// pkChangingUpdate scans a COLLAPSED full-table change map for a PK-changing
// UPDATE (#782). It is a cheap BACKSTOP to pkChangingUpdateInEvents (the
// authoritative raw-slice scan): the map is keyed by the before-image PK, so a
// PK-changing UPDATE whose old key was reused by a later event is absent from
// the map entirely and only the raw-slice scan catches it. This map scan still
// catches the common cases where the PK-changing UPDATE is the surviving entry
// for its old key, and guards callers that build a map without a raw slice.
// Iterates in sorted key order for a deterministic first-offender result.
func pkChangingUpdate(changes map[string]*query.ResultRow, pkCols []metadata.ColumnMeta) (before, after string, found bool) {
	keys := make([]string, 0, len(changes))
	for k := range changes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if b, a, ok := pkChangedInEvent(changes[k], pkCols); ok {
			return b, a, true
		}
	}
	return "", "", false
}

// pkChangingUpdateErr is the shared fail-loud error the full-table entry points
// return when pkChangingUpdate finds a PK-changing UPDATE (#782).
func pkChangingUpdateErr(schema, table, before, after string) error {
	return fmt.Errorf(
		"full-table reconstruct: %s.%s has a PK-changing UPDATE in the window "+
			"(primary key %q → %q); reconstruct folds events by the before-image "+
			"primary key and cannot safely apply a row whose PK changed — doing so "+
			"would duplicate or resurrect rows in the output. Re-run `bintrail baseline` "+
			"to capture a snapshot at or after the PK change, then reconstruct from there",
		schema, table, before, after)
}

// pkSpellingJoinErr reports a baseline row and a pending event that denote the
// same row under two different key spellings (#1158).
//
// This is a fail-loud guard, not a diagnosis of a data problem: it means
// bintrail's own canonicalization and the spelling in binlog_events.pk_values
// disagree, so the merge is about to emit the baseline row while the event that
// supersedes it sits undrained. The consequences are asymmetric and the DELETE
// one is why this refuses rather than warns:
//
//   - UPDATE/INSERT — the stale baseline row is emitted AND the event is
//     appended as a new PK. A duplicate key, which a restore rejects with
//     error 1062: loud, but only once the dump is being loaded.
//   - DELETE — the event is skipped and the stale baseline row was already
//     emitted, so a row deleted before the target instant is RESURRECTED into
//     the output. Nothing downstream catches that: the dump restores cleanly
//     and is simply wrong.
func pkSpellingJoinErr(schema, table, canonical, alternate string, evType event.EventType) error {
	kind, outcome := "UPDATE/INSERT", "emitted twice"
	if evType == event.EventDelete {
		kind, outcome = "DELETE", "kept alive past its DELETE"
	}
	return fmt.Errorf(
		"baseline merge: %s.%s has a baseline row keyed %q while an undrained %s event for the same row "+
			"is keyed %q — the two spellings of a fixed BINARY(n) key (padded on storage, trailing-0x00-stripped in "+
			"the binlog row image) are not being reconciled, so this row would be %s in the reconstructed row set. "+
			"First check that the schema snapshot matches the live column type (`bintrail snapshot`): a snapshot "+
			"saying BINARY(n) for a column since ALTERed to VARBINARY(n) produces this same disagreement and is "+
			"fixed by re-snapshotting. Otherwise this is a bintrail canonicalization fault — please report it with "+
			"the table's PK definition",
		schema, table, canonical, kind, alternate, outcome)
}

// warnUndetectableBinaryPK reports, once per table, that a fixed-binary PK
// column carries no declared width so the #1158 key-spelling guard cannot run
// for it. The CANONICAL key is unaffected — canonicalizePKValue trims without
// reading the width — so this is not a correctness warning about the output; it
// says the detector is off, which matters because the failure it detects is the
// silent one. Mirrors the pre-#212 DATETIME-precision warning above.
func warnUndetectableBinaryPK(schema, table string, pkCols []metadata.ColumnMeta) {
	for _, c := range pkCols {
		if !strings.EqualFold(strings.TrimSpace(c.DataType), "binary") {
			continue
		}
		if FixedBinaryWidth(c.ColumnType) != 0 {
			continue
		}
		slog.Warn("BINARY primary-key column has no column_type in the schema snapshot; the key-spelling guard (#1158) "+
			"is disabled for this table — a baseline row and its event keyed under different spellings would go "+
			"undetected. Re-run `bintrail snapshot` to capture column_type",
			"schema", schema, "table", table, "column", c.Name)
	}
}

// reconstructBinlogOnly is the ErrNoBaseline fallback for full-table
// reconstruct (#766). A table created after the last baseline snapshot has no
// Parquet to merge against; previously ReconstructTable stopped there and
// returned an empty report, silently discarding every row that only ever
// existed in the binlog — even a table with thousands of rows. This mirrors
// the shim's binlog-only degrade (internal/shim/snapshot.go
// runSnapshotFullTable falling back to runFullTable): fetch every event for
// the table up to cfg.At and emit the latest surviving row per PK, skipping
// DELETEs. There is no baseline Parquet to read a CREATE TABLE statement
// from, and fabricating one from schema_snapshots column metadata risks
// silently shipping a wrong PK/engine/charset/index definition as fact — so
// the schema file records why it's missing instead, and the caller must
// supply the table structure before loading the accompanying data file(s).
func reconstructBinlogOnly(
	ctx context.Context,
	cfg FullTableConfig,
	schema, table string,
	db *sql.DB,
	engine *query.Engine,
	resolver *metadata.Resolver,
	dbName string,
	rep *TableReport,
	start time.Time,
) (*TableReport, error) {
	tm, err := resolver.Resolve(schema, table)
	if err != nil {
		return nil, fmt.Errorf("resolve schema for %s.%s: %w; run `bintrail snapshot` to refresh", schema, table, err)
	}
	pkCols := tm.PKColumnMetas()
	if len(pkCols) == 0 {
		return nil, fmt.Errorf("%s.%s has no primary key in the loaded snapshot; full-table reconstruct requires a PK", schema, table)
	}
	// Same generated-PK gate as the baseline path (#1266), and this path is
	// MORE exposed, not less: with no baseline probe to fail loudly, a
	// versioned table's history-row inserts fold under their own full
	// pk_values and would be emitted as duplicate live rows (the output
	// column list excludes generated columns, so nothing distinguishes them).
	if pkCol, ok := GeneratedPKColumn(pkCols); ok {
		return nil, fullTableGeneratedPKRefusal(schema, table, pkCol)
	}

	// Generated columns can't be set explicitly in an INSERT, so exclude them
	// from the emitted schema — same exclusion `bintrail baseline` applies.
	colNames := make([]string, 0, len(tm.Columns))
	for _, c := range tm.Columns {
		if c.IsGenerated {
			continue
		}
		colNames = append(colNames, c.Name)
	}
	if len(colNames) == 0 {
		return nil, fmt.Errorf("%s.%s has no non-generated columns in the loaded snapshot", schema, table)
	}

	fetcher := cfg.ArchiveFetcher
	if fetcher == nil {
		fetcher = parquetquery.Fetch
	}
	// Streamed and folded page by page, same as the baseline path (#1097):
	// this fallback has no baseline to bound the window, so it is if anything
	// the more exposed of the two — it fetches the WHOLE retained binlog
	// history for the table.
	fold, err := foldEventWindow(ctx, withFoldBudgets(cfg, foldConfig{
		DB:       db,
		Engine:   engine,
		DBName:   dbName,
		Resolver: resolver,
		Schema:   schema,
		Table:    table,
		PKCols:   pkCols,
		Opts: query.Options{
			Schema: schema,
			Table:  table,
			Until:  &cfg.At,
			// No Since — there is no baseline instant to anchor from; fetch
			// the whole retained binlog-only window up to cfg.At.
		},
		AllowGaps:      cfg.AllowGaps,
		ArchiveFetcher: fetcher,
	}))
	if err != nil {
		return nil, err
	}
	rep.EventsApplied = fold.Total
	changes := fold.Changes

	// A table that only ever existed after the last baseline was very likely
	// CREATEd during the retained binlog window, and the schema-drift guard
	// (#700) records every CREATE TABLE's exact DDL text in schema_changes.
	// Prefer that real, captured statement over a fabricated one; only fall
	// back to the explanatory placeholder when no such record exists (e.g.
	// the table predates schema_changes, or genuinely was never baselined
	// for another reason).
	createSQL, ddlFound, err := findCapturedCreateTableDDL(ctx, db, schema, table, cfg.At)
	if err != nil {
		slog.Warn("could not query schema_changes for a captured CREATE TABLE; using placeholder schema file",
			"schema", schema, "table", table, "error", err)
	}
	if ddlFound {
		// The captured DDL is from CREATE time; it does not reflect any ALTER
		// applied later, up to cfg.At (that class of point-in-time schema
		// drift is the separately-tracked #600/#601/#602 family). Label it so
		// a reader doesn't mistake it for a verified-current definition.
		createSQL = "-- bintrail: CREATE TABLE captured from schema_changes at table-creation time\n" +
			"-- (binlog-only fallback, #766). If the table was ALTERed afterwards, this may\n" +
			"-- not match the columns in the accompanying data file(s) — verify before loading.\n" +
			createSQL
	} else {
		createSQL = binlogOnlySchemaPlaceholder(schema, table)
	}

	rep.BinlogOnly = true
	if err := writeBinlogOnlyChanges(cfg.OutputDir, schema, table, pkCols, colNames, cfg.ChunkSize, createSQL, changes, rep); err != nil {
		return nil, err
	}
	rep.Duration = time.Since(start)

	slog.Warn("full-table reconstruct: no baseline found for table; recovered via binlog-only fallback "+
		"(rows outside the retained binlog window, if any, are not included)",
		"schema", schema, "table", table,
		"events_applied", rep.EventsApplied,
		"inserts_emitted", rep.InsertsEmitted,
		"deletes_skipped", rep.DeletesSkipped)

	return rep, nil
}

// binlogOnlySchemaPlaceholder is the schema-file text used when no captured
// CREATE TABLE DDL is available: fabricating one from column metadata risks
// silently shipping a wrong PK/engine/charset/index definition as fact.
//
// The whole return value is a "--" comment block, so the names are sanitized
// (#1120): a line break in either would end the comment and leave the remainder
// of the identifier executing as SQL. That matters despite the body telling the
// operator to create the table themselves — this text occupies the
// <db>.<table>-schema.sql slot that myloader applies, so ANY non-comment line in
// it runs.
//
// There is no executable sibling here carrying the exact bytes: the comment is
// the whole artifact, and the operator's task is precisely to read the name off
// it. That is why SanitizeForComment renders losslessly rather than flattening —
// the name a break-bearing table shows up under stays recoverable from the text,
// instead of silently becoming a different, plausible-looking one.
func binlogOnlySchemaPlaceholder(schema, table string) string {
	schema, table = recovery.SanitizeForComment(schema), recovery.SanitizeForComment(table)
	return fmt.Sprintf(
		"-- bintrail: no baseline snapshot exists for %s.%s (table created after the last\n"+
			"-- `bintrail baseline` run, or never baselined), and no CREATE TABLE statement for\n"+
			"-- it was found in schema_changes either. The rows in the accompanying data\n"+
			"-- file(s) were recovered entirely from the binlog (binlog-only fallback, #766);\n"+
			"-- the table structure is deliberately NOT fabricated here. Create the table\n"+
			"-- structure yourself before loading the data file(s).\n",
		schema, table)
}

// findCapturedCreateTableDDL looks up the most recent CREATE TABLE statement
// the schema-drift guard (#700) recorded for schema.table at-or-before at, in
// schema_changes.ddl_query. found is false (with a nil error) when no such
// row exists — the caller falls back to binlogOnlySchemaPlaceholder.
func findCapturedCreateTableDDL(ctx context.Context, db *sql.DB, schema, table string, at time.Time) (ddl string, found bool, err error) {
	row := db.QueryRowContext(ctx, `
		SELECT ddl_query FROM schema_changes
		WHERE schema_name = ? AND table_name = ? AND ddl_type = ? AND detected_at <= ?
		ORDER BY detected_at DESC, id DESC
		LIMIT 1`,
		schema, table, event.DDLCreateTable, at)
	if err := row.Scan(&ddl); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, err
	}
	return ddl, true, nil
}

// writeBinlogOnlyChanges writes the reconstructBinlogOnly output: a schema
// file (createSQL — either a real captured CREATE TABLE or the explanatory
// placeholder, decided by the caller) and one data row per surviving entry in
// changes, in deterministic PK order. Updates rep.InsertsEmitted/
// DeletesSkipped/Files in place. Split out from reconstructBinlogOnly so it's
// unit-testable without a MySQL index connection (it does no IO beyond
// outputDir).
func writeBinlogOnlyChanges(
	outputDir, schema, table string,
	pkCols []metadata.ColumnMeta,
	colNames []string,
	chunkSize int64,
	createSQL string,
	changes map[string]*query.ResultRow,
	rep *TableReport,
) (retErr error) {
	// The #592 (unresolved-TOAST) and #782 (PK-changing UPDATE) guards are NOT
	// here: like the baseline path, this one now streams its window through
	// foldEventWindow, which runs both per event on the untrimmed event before
	// folding it. Re-adding a before-image check against `changes` would be a
	// no-op that reads as a guard — retainEvent has blanked RowBefore by the
	// time the map reaches this function. See the note above
	// mergeBaselineIntoWriter.
	mw, err := NewMydumperWriter(outputDir, schema, table, colNames, chunkSize)
	if err != nil {
		return fmt.Errorf("open mydumper writer: %w", err)
	}
	// Same #1162 error-path discard as mergeBaselineIntoWriter: this path has
	// no pre-writer guards at all, so any mid-write failure would otherwise
	// finalize a loadable, silently-truncated chunk plus the schema file.
	defer func() {
		if retErr == nil {
			return
		}
		if derr := mw.Discard(); derr != nil {
			slog.Warn("could not remove partial mydumper output for failed table",
				"schema", schema, "table", table, "error", derr)
		}
	}()

	if err := mw.WriteSchema(createSQL); err != nil {
		return err
	}

	// Deterministic order: sort by PK string so tests can assert on output
	// without flakiness (mirrors the "new PKs" tail loop in mergeBaselineImages).
	pks := make([]string, 0, len(changes))
	for pk := range changes {
		pks = append(pks, pk)
	}
	sort.Strings(pks)
	for _, pk := range pks {
		ev := changes[pk]
		if ev.EventType == event.EventDelete {
			rep.DeletesSkipped++
			continue
		}
		if ev.RowAfter == nil {
			slog.Error("event has nil RowAfter; skipping to avoid emitting all-NULL tuple",
				"schema", schema, "table", table, "pk", pk,
				"event_type", ev.EventType, "event_id", ev.EventID)
			continue
		}
		if err := mw.WriteRow(rowAfterOrdered(ev.RowAfter, colNames, schema, table)); err != nil {
			return err
		}
		rep.InsertsEmitted++
	}

	if err := mw.Close(); err != nil {
		return fmt.Errorf("close mydumper writer: %w", err)
	}
	rep.Files = mw.Files()
	rep.RowsWritten = mw.Rows()
	return nil
}

// splitSchemaTable parses "db.table" into (db, table, true). Rejects entries
// with zero or more than one dot.
func splitSchemaTable(entry string) (string, string, bool) {
	parts := strings.SplitN(entry, ".", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	if parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	if strings.Contains(parts[1], ".") {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// materializeBaselineLocal ensures the baseline Parquet is available on the
// local filesystem. Local paths are returned as-is with a no-op cleanup. S3
// URLs are downloaded to a temp file via DuckDB's httpfs + COPY so DuckDB
// can then query the resulting local file without an outbound connection.
// tuning sets the DuckDB session's resource budget for that download (#842);
// effectiveDuckDBTuning normalizes a zero-value Tuning to the container-safe
// default, so passing duckdbutil.Tuning{} (any caller that hasn't been wired
// with an explicit budget — e.g. the shim, verify) is always safe.
func materializeBaselineLocal(ctx context.Context, path string, tuning duckdbutil.Tuning) (string, func(), error) {
	if !strings.HasPrefix(path, "s3://") {
		// At-rest integrity (#636): validate the local file against its snapshot's
		// _MANIFEST before any reader trusts it (DuckDB validates nothing). Fail
		// loud on corruption; a legacy snapshot with no manifest is a no-op.
		if err := baselineintegrity.ValidateLocalFile(path); err != nil {
			return "", nil, err
		}
		return path, func() {}, nil
	}
	// At-rest integrity (#636/#698): validate the ORIGINAL S3 object bytes
	// against the snapshot's _MANIFEST before the DuckDB COPY below re-encodes
	// them — the temp copy is no longer byte-identical to the object, so this
	// pre-pass is the only point where the manifest's raw-byte CRC applies.
	if err := baselineintegrity.ValidateS3File(ctx, path); err != nil {
		return "", nil, err
	}
	// Download via DuckDB httpfs. Keep the temp file around until cleanup().
	tmpDir, err := os.MkdirTemp("", "bintrail-baseline-*")
	if err != nil {
		return "", nil, fmt.Errorf("mkdir temp: %w", err)
	}
	tmpPath := filepath.Join(tmpDir, "baseline.parquet")

	db, err := sql.Open("duckdb", "")
	if err != nil {
		os.RemoveAll(tmpDir)
		return "", nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	applyDuckDBTuning(ctx, db, tuning)

	if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
		os.RemoveAll(tmpDir)
		return "", nil, fmt.Errorf("load httpfs: %w", err)
	}
	if err := duckdbutil.EnableS3CredentialChain(ctx, db); err != nil {
		os.RemoveAll(tmpDir)
		return "", nil, err
	}
	safeSrc := strings.ReplaceAll(path, "'", "''")
	safeDst := strings.ReplaceAll(tmpPath, "'", "''")
	copyQ := fmt.Sprintf("COPY (SELECT * FROM parquet_scan('%s')) TO '%s' (FORMAT PARQUET)", safeSrc, safeDst)
	if _, err := db.ExecContext(ctx, copyQ); err != nil {
		os.RemoveAll(tmpDir)
		return "", nil, fmt.Errorf("download s3 baseline: %w", err)
	}

	cleanup := func() { os.RemoveAll(tmpDir) }
	return tmpPath, cleanup, nil
}

// ReadBaselineColumns returns the column names of a baseline Parquet file (local
// or s3://). This is the authoritative non-generated column set for fingerprinting
// the table: mydumper excludes true STORED/VIRTUAL generated columns from the dump
// but keeps ordinary expression-default (DEFAULT_GENERATED) columns, so the Parquet
// schema is exactly the set to hash — deriving it from a schema snapshot's
// is_generated flag instead would wrongly drop DEFAULT_GENERATED columns (the
// trap consistency.ConsistentTableChecksum documents) and silently under-verify them.
//
// tuning sets the DuckDB session's resource budget (#842); zero value falls
// back to the container-safe default (effectiveDuckDBTuning). Callers that
// resolve an operator-supplied --ultrafast/--duckdb-* tuning (verify) should
// pass it through here so it's honored on this path too, not just archive
// fetches.
func ReadBaselineColumns(ctx context.Context, path string, tuning duckdbutil.Tuning) ([]string, error) {
	localPath, cleanup, err := materializeBaselineLocal(ctx, path, tuning)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	return readBaselineColumns(ctx, localPath, tuning)
}

// readBaselineColumns opens the local Parquet file with DuckDB and returns
// the column names in the order parquet_scan() emits them. This order is
// the canonical column order for the emitted INSERT statements. tuning sets
// the session's resource budget (#842); see effectiveDuckDBTuning.
func readBaselineColumns(ctx context.Context, localPath string, tuning duckdbutil.Tuning) ([]string, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	applyDuckDBTuning(ctx, db, tuning)

	safePath := strings.ReplaceAll(localPath, "'", "''")
	q := fmt.Sprintf("SELECT * FROM parquet_scan('%s') LIMIT 0", safePath)
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("describe baseline: %w", err)
	}
	defer rows.Close()
	return rows.Columns()
}

// zipMap pairs cols with vals to produce a map[string]any view of a row.
// In mergeBaselineImages the result is used both for PK canonicalization and,
// for a pass-through baseline row, as the emitted row itself — so the map is
// retained downstream (see the retention-safety note in that loop). It must
// not be backed by a buffer reused across DuckDB Scan iterations.
func zipMap(cols []string, vals []any) map[string]any {
	out := make(map[string]any, len(cols))
	for i, c := range cols {
		out[c] = vals[i]
	}
	return out
}

// postBaselineColumns returns, sorted and de-duplicated, the column names
// that appear in some non-DELETE event's row_after image but are absent from
// the baseline column set — i.e. columns ADDED to the source table after the
// baseline snapshot. The mydumper writer projects every emitted row onto the
// baseline columns (rowAfterOrdered), so these columns' values would be
// dropped silently; mergeBaselineIntoWriter calls this up front to refuse the
// run instead (#602). DELETE events carry no row_after and are skipped.
func postBaselineColumns(changes map[string]*query.ResultRow, colNames []string) []string {
	baseline := make(map[string]struct{}, len(colNames))
	for _, c := range colNames {
		baseline[c] = struct{}{}
	}
	extra := make(map[string]struct{})
	for _, ev := range changes {
		if ev == nil || ev.EventType == event.EventDelete || ev.RowAfter == nil {
			continue
		}
		for col := range ev.RowAfter {
			if _, ok := baseline[col]; !ok {
				extra[col] = struct{}{}
			}
		}
	}
	if len(extra) == 0 {
		return nil
	}
	out := make([]string, 0, len(extra))
	for col := range extra {
		out = append(out, col)
	}
	sort.Strings(out)
	return out
}

// generatedColumnRe matches one column definition line of a SHOW CREATE TABLE
// that declares a STORED or VIRTUAL generated column, once string literals
// have been blanked out of the line: the MySQL spelling `GENERATED ALWAYS AS`
// and the short `AS (expr) STORED|VIRTUAL|PERSISTENT` that MariaDB also
// prints, the same two shapes baseline.generatedRe accepts, so this guard and
// the dump agree on what a baseline holds. SHOW CREATE TABLE puts every
// column on its own line and always backticks the name. Expression defaults
// (`DEFAULT (expr)`) are NOT generated columns: mydumper keeps them in the
// dump, so they never reach the #602 guard and must not be listed here.
var generatedColumnRe = regexp.MustCompile("(?i)^\\s*`((?:[^`]|``)+)`\\s+.*(?:\\bGENERATED\\s+ALWAYS\\s+AS\\b|\\bAS\\s*\\(.*\\)\\s*(?:VIRTUAL|STORED|PERSISTENT)\\b)")

// sqlStringLiteralRe finds single-quoted SQL string literals (with ” as the
// escaped quote), so a COMMENT or DEFAULT that happens to contain the words
// GENERATED ALWAYS AS cannot promote its column.
var sqlStringLiteralRe = regexp.MustCompile("'(?:[^']|'')*'")

// generatedColumnsIn returns the names of the STORED/VIRTUAL generated columns
// a CREATE TABLE declares, keyed for lookup. An empty or non-MySQL statement
// (PostgreSQL baselines carry none) yields an empty set, which keeps the #602
// guard exactly as strict as before for every other column.
func generatedColumnsIn(createTableSQL string) map[string]struct{} {
	out := map[string]struct{}{}
	for line := range strings.SplitSeq(createTableSQL, "\n") {
		line = sqlStringLiteralRe.ReplaceAllString(line, "''")
		m := generatedColumnRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		out[strings.ReplaceAll(m[1], "``", "`")] = struct{}{}
	}
	return out
}

// generatedByName keys a schema snapshot's columns by name with their
// IsGenerated flag, the shape mergeInput.CurrentGenerated wants.
func generatedByName(cols []metadata.ColumnMeta) map[string]bool {
	out := make(map[string]bool, len(cols))
	for _, c := range cols {
		out[c.Name] = c.IsGenerated
	}
	return out
}

// droppedBaselineColumns is the symmetric counterpart of postBaselineColumns:
// it returns, sorted and de-duplicated, the baseline column names ABSENT (key
// missing — not value NULL) from some event's row image, i.e. columns DROPPED
// from the source table after the baseline snapshot. binlog_row_image=FULL
// (validated at index time) guarantees a complete image, so key absence means
// the column no longer existed at event time; a genuinely-NULL value is
// present in the map as a nil value and is never flagged. mergeBaselineIntoWriter
// calls this up front to refuse the run instead of letting rowAfterOrdered
// NULL-fill the column row by row (#843).
//
// The signal is computed during the FOLD, not here (#1097): a window whose
// last event for a post-drop-touched PK is a DELETE carries no row_after, and
// its row_before — itself a post-drop image, and the only evidence — is
// discarded by retainEvent before the change map ever reaches this function.
// foldResult.observeImages therefore narrows an intersection over BOTH images
// of every event while they are still intact, and this function does the part
// that needs colNames, which is not known until the baseline is materialized.
// A column absent from the intersection is a column some image lacked, which
// is exactly what the old per-map scan flagged.
//
// sawImage guards the degenerate case: an intersection over zero images is
// empty, which would otherwise read as "every baseline column was dropped" and
// refuse every run with no events at all.
//
// A PK with zero post-drop activity in the window remains undetectable by
// construction (no image ever samples the post-drop schema for it) and is out
// of scope here.
func droppedBaselineColumns(imageCols map[string]struct{}, sawImage bool, colNames []string) []string {
	if !sawImage {
		return nil
	}
	var out []string
	for _, col := range colNames {
		if _, ok := imageCols[col]; !ok {
			out = append(out, col)
		}
	}
	if len(out) == 0 {
		return nil
	}
	sort.Strings(out)
	return out
}

// These helpers reverse the storage-side base64 encoding of BLOB/TEXT
// delta-event values for the mydumper reconstruct path (#653/#660). go-mysql
// delivers BLOB and TEXT (both MYSQL_TYPE_BLOB) as []byte, which marshalRow
// base64-encodes into the binlog_events JSON; a delta event's row_after
// therefore carries them as base64 STRINGS, which would otherwise reach
// FormatSQLValue's string branch and be written to the mydumper dump verbatim.
//
// Provenance matters: decoding is applied ONLY to event-sourced values (the
// Changes map, where every value came from binlog_events JSON), never to a
// merged row at emit time — a baseline TEXT value reaches the DuckDB scan as a
// Go string (parquet.String()), indistinguishable from a base64 event value, so
// decoding by Go-type at emit time would corrupt valid-base64 baseline text.
//
// base64StoredKind and decodeStoredBase64 mirror the decode added for the recover
// path in #653 (sibling PR #662, not yet in main); they are duplicated here
// because that copy is unexported. binaryColsFromTableMeta is the
// fulltable-specific builder. A follow-up should hoist one shared copy once both
// land (#661 is the third consumer).
// "json" is included (non-binary) as a defense-in-depth companion to #736:
// marshalRow now only promotes a []byte to raw JSON when it looks like a
// JSON container ({ or [), so a JSON column whose top-level value is itself a
// bare scalar (rare, but legal) falls through to this same base64 path
// instead of failing to round-trip.
//
// "binary"/"varbinary" are included (binary) since #756: metadata.MapRow now
// reinterprets those two DataTypes as []byte (they arrive from go-mysql as a
// raw Go string with no charset, which json.Marshal could silently corrupt to
// U+FFFD), so they take the same []byte-to-base64 storage path as BLOB and
// must be decoded the same way here.
//
// Retroactive-reclassification risk (#756, accepted): unlike BLOB/TEXT (always
// []byte-and-base64 from day one), a BINARY/VARBINARY event indexed BEFORE
// this fix was stored as a plain, non-base64 string. decodeStoredBase64 can't
// tell that apart from a post-fix base64 string, so a pre-fix value whose raw
// bytes happen to satisfy the base64 alphabet+padding decodes to different,
// wrong bytes with no error — astronomically unlikely for random binary
// content, but plausible for a VARBINARY column storing ASCII-like data. See
// the fuller rationale on the sibling copy in internal/recovery/recovery.go.
// The spatial family + VECTOR are included (binary) since #1136: go-mysql
// delivers them via decodeBlob as []byte — a geometry is MySQL's internal
// 4-byte SRID + WKB, a VECTOR is packed floats — so they take the same
// []byte-to-base64 storage path as BLOB and must be decoded the same way.
// Decoding restores exactly the bytes a raw source SELECT and a mydumper
// baseline carry, which is what makes them comparable (verify) and
// restorable (reconstruct --output-format mydumper). The retroactive-
// reclassification risk above does NOT apply to them: like BLOB they were
// []byte-and-base64 from the day they were first captured.
func base64StoredKind(dataType string) (binary, ok bool) {
	switch strings.ToLower(dataType) {
	case "blob", "tinyblob", "mediumblob", "longblob", "binary", "varbinary",
		"geometry", "point", "linestring", "polygon",
		"multipoint", "multilinestring", "multipolygon",
		// MySQL 8.0.11+ reports a GEOMETRYCOLLECTION column's DATA_TYPE as
		// "geomcollection"; MariaDB and pre-8.0.11 report "geometrycollection".
		"geometrycollection", "geomcollection",
		"vector":
		return true, true
	case "text", "tinytext", "mediumtext", "longtext", "json":
		return false, true
	default:
		return false, false
	}
}

// decodeStoredBase64 also repairs events indexed before marshalRow was fixed
// (#736) to gate on looksLikeJSONContainer: such an event may hold a
// BLOB/TEXT value mis-promoted to a bare JSON scalar (e.g. the literal string
// "false" stored as the JSON boolean false), decoding here as a Go
// bool/json.Number instead of a string. That value IS the column's original
// textual literal, so it is restored directly. A value that decoded to Go
// nil (originally the string "null") is NOT repairable — indistinguishable
// from a genuine SQL NULL — and is left as nil. This nil case, and a bare
// JSON *string* scalar (bytes like `"YWJj"`, quotes included) that was
// mis-promoted the same way, are historical-only gaps: by the time this
// runs, the pre-#736 marshalRow had already parsed the outer quotes away as
// ordinary JSON-string syntax, so the value arriving here is the
// already-quote-stripped text (`YWJj`), indistinguishable from genuine
// base64 content and wrongly re-decoded on top of the original corruption —
// not repairable, a real fix belongs at the storage encoding, out of scope
// here. A genuine JSON column captured AFTER this fix with a bare
// string-scalar value does NOT hit this gap: it takes the ordinary
// []byte-to-base64 path (same as any TEXT/BLOB), and this function correctly
// reverses it to the original bytes, quotes included — which is exactly the
// text MySQL needs to re-parse the value back into that JSON column.
func decodeStoredBase64(v any, binary bool) any {
	var text string
	switch val := v.(type) {
	case string:
		b, err := base64.StdEncoding.DecodeString(val)
		if err != nil {
			return v
		}
		if binary {
			return b
		}
		return string(b)
	case bool:
		text = strconv.FormatBool(val)
	case json.Number:
		text = string(val)
	default:
		return v
	}
	if binary {
		return []byte(text)
	}
	return text
}

// binaryColsFromTableMeta maps each BLOB/TEXT column of a table to whether it is
// binary, for decoding delta-event row_after values. Returns nil when no column
// needs decoding.
func binaryColsFromTableMeta(tm *metadata.TableMeta) map[string]bool {
	var m map[string]bool
	for _, c := range tm.Columns {
		if binary, ok := base64StoredKind(c.DataType); ok {
			if m == nil {
				m = make(map[string]bool)
			}
			m[c.Name] = binary
		}
	}
	return m
}

// rowAfterOrdered walks colNames and looks up each name in rowAfter (a
// map[string]any from a binlog event's row_after image), returning a slice
// of values aligned to the baseline Parquet column order. On the baseline
// merge path both schema-drift directions are refused up front by
// mergeBaselineIntoWriter — a column ADDED after the baseline by the
// postBaselineColumns guard (#602), a column DROPPED after the baseline by
// the droppedBaselineColumns guard (#843) — so a missing key here is
// unreachable there. The NULL-fill-with-warn below remains as
// defense-in-depth and for the binlog-only fallback path
// (writeBinlogOnlyChanges), whose colNames come from a resolver snapshot
// rather than a baseline.
//
// Both baseline pass-through rows and delta-event after-images flow through
// here, so it must NOT base64-decode BLOB/TEXT values: a baseline TEXT value is
// a Go string (DuckDB scans parquet.String() as string) indistinguishable from
// a base64 event value, and decoding it would corrupt valid-base64 baseline
// text. Event values are decoded upstream, epoch-aware, before the change map
// is even built (see DecodeEventBinaries in ReconstructTable, #668).
func rowAfterOrdered(rowAfter map[string]any, colNames []string, schema, table string) []any {
	out := make([]any, len(colNames))
	for i, col := range colNames {
		v, ok := rowAfter[col]
		if !ok {
			slog.Warn("event row_after missing column present in baseline; emitting NULL",
				"schema", schema, "table", table, "column", col)
			out[i] = nil
			continue
		}
		out[i] = v
	}
	return out
}
