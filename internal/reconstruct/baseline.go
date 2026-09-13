package reconstruct

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
)

// ErrNoBaseline is returned by FindBaseline when no baseline snapshot exists
// for the requested table at or before the target time.
var ErrNoBaseline = errors.New("no baseline snapshot found")

// StaleWarning describes a baseline that was selected by falling back to an
// older snapshot because the table is absent from a newer one (#461/#466).
// Empty Message means "not stale" — the table is present in the newest eligible
// snapshot. Both the local and S3 lookups populate it identically; callers that
// surface staleness (the console reconstruct response, #466) read Message,
// while callers that only log can ignore it.
type StaleWarning struct {
	Message        string    // human-readable, empty when not stale
	UsingSnapshot  time.Time // snapshot the table was actually read from
	NewestSnapshot time.Time // newest eligible snapshot (lacks the table)
}

// Stale reports whether the baseline was a stale fallback.
func (s StaleWarning) Stale() bool { return s.Message != "" }

// FindBaseline finds the most recent baseline Parquet file at or before `at`
// for the given schema and table, returning its path, snapshot timestamp, and a
// staleness warning (#466). The warning's Message is non-empty only when the
// table is absent from a newer eligible snapshot, meaning the result is an
// older-snapshot fallback — both the local and S3 paths report it now.
//
// source may be:
//   - A local directory path (parent of RFC3339-named snapshot subdirectories)
//   - An S3 URL prefix (e.g. "s3://bucket/baselines")
func FindBaseline(ctx context.Context, source, schema, table string, at time.Time) (path string, snapshotTime time.Time, stale StaleWarning, err error) {
	if strings.HasPrefix(source, "s3://") {
		return findBaselineS3(ctx, source, schema, table, at)
	}
	return findBaselineLocal(source, schema, table, at)
}

// ReadBaselineRow opens the Parquet file at path using DuckDB and returns the
// first row matching pkFilter (column name → value string). Returns nil when
// no row matches. Loads the httpfs extension automatically for s3:// paths.
//
// pkMetas, when non-empty, are the searched table's primary-key column metas
// (see ResolvePKMetasAt) and enable the fixed BINARY(n) reconciliation
// (#1155/#1157): a key copied out of binlog_events.pk_values carries the ROW
// image's trailing-0x00-stripped spelling, which is SHORTER than the padded
// value the baseline stores, so after an exact miss the lookup is retried once
// with every fixed BINARY(n) component re-padded to its declared width. The
// retry runs only on a miss, so it can never override a correct hit. Callers
// with no index open — and therefore no declared column widths — pass nil and
// keep the exact-match-only behavior.
func ReadBaselineRow(ctx context.Context, path string, pkFilter map[string]string, pkMetas []metadata.ColumnMeta) (map[string]any, error) {
	rows, err := ReadBaselineRows(ctx, path, pkFilter, 1)
	if err != nil {
		return nil, err
	}
	if len(rows) > 0 {
		return rows[0], nil
	}
	padded, changed := padFixedBinaryFilter(pkFilter, pkMetas)
	if !changed {
		return nil, nil
	}
	slog.Debug("retrying baseline lookup with the fixed BINARY(n) storage padding", "pk_filter", padded)
	rows, err = ReadBaselineRows(ctx, path, padded, 1)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return rows[0], nil
}

// ReadBaselineRows returns every baseline row matching filter (an arbitrary
// column→value map, AND-ed), up to limit rows (limit <= 0 = no cap). Unlike
// ReadBaselineRow it is not restricted to the primary key, so cascade Phase-2
// can scan a child snapshot by a foreign-key column and recover ALL matching
// children. Values are bound as query parameters; column names are quoted
// identifiers; the path is single-quote-escaped.
func ReadBaselineRows(ctx context.Context, path string, filter map[string]string, limit int) ([]map[string]any, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	if err := pinDuckDBSessionUTC(ctx, db); err != nil {
		return nil, err
	}

	if strings.HasPrefix(path, "s3://") {
		// At-rest integrity (#636/#698): pre-pass-stream the S3 object through
		// CRC-32C against its snapshot's _MANIFEST before parquet_scan trusts
		// the bytes. Runs before LoadHTTPFS so a corrupt object fails loud
		// without DuckDB ever touching S3.
		if err := baselineintegrity.ValidateS3File(ctx, path); err != nil {
			return nil, err
		}
		if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
			return nil, fmt.Errorf("load httpfs extension: %w", err)
		}
		if err := duckdbutil.EnableS3CredentialChain(ctx, db); err != nil {
			return nil, err
		}
	} else if err := baselineintegrity.ValidateLocalFile(path); err != nil {
		// At-rest integrity (#636): fail loud on a corrupt local baseline before
		// the cascade Phase-2 scan trusts it.
		return nil, err
	}

	// Build sorted conditions for deterministic SQL + arg ordering.
	conds := buildCondsList(filter)
	safePath := strings.ReplaceAll(path, "'", "''")
	q := "SELECT * FROM parquet_scan('" + safePath + "')"
	if len(conds) > 0 {
		parts := make([]string, len(conds))
		for i, c := range conds {
			parts[i] = quoteIdent(c.col) + " = ?"
		}
		q += " WHERE " + strings.Join(parts, " AND ")
	}
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}

	args, err := bindFilterArgs(ctx, db, safePath, conds)
	if err != nil {
		return nil, err
	}
	return runBaselineQuery(ctx, db, q, args)
}

// runBaselineQuery executes a built baseline query and materializes its rows.
func runBaselineQuery(ctx context.Context, db *sql.DB, q string, args []any) ([]map[string]any, error) {
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("baseline query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("baseline columns: %w", err)
	}
	var out []map[string]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan baseline row: %w", err)
		}
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			row[col] = vals[i]
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// ExecSQL runs arbitrary SQL against an in-memory DuckDB instance and returns
// result rows and column names. The httpfs extension is loaded automatically
// when source or sqlStr references an s3:// URL.
func ExecSQL(ctx context.Context, source, sqlStr string) ([]map[string]any, []string, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	if err := pinDuckDBSessionUTC(ctx, db); err != nil {
		return nil, nil, err
	}

	if strings.Contains(source, "s3://") || strings.Contains(sqlStr, "s3://") {
		if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
			return nil, nil, fmt.Errorf("load httpfs extension: %w", err)
		}
		if err := duckdbutil.EnableS3CredentialChain(ctx, db); err != nil {
			return nil, nil, err
		}
	}

	rows, err := db.QueryContext(ctx, sqlStr)
	if err != nil {
		return nil, nil, fmt.Errorf("execute SQL: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, fmt.Errorf("columns: %w", err)
	}
	var results []map[string]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, fmt.Errorf("scan row: %w", err)
		}
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			row[col] = vals[i]
		}
		results = append(results, row)
	}
	return results, cols, rows.Err()
}

// ─── listing ──────────────────────────────────────────────────────────────────

// BaselineFile is one table's Parquet file discovered in a baseline source
// listing. Path-derived only — no file contents are read — so listing stays
// cheap over both a local directory and an s3:// prefix. Binlog coordinates
// live in Parquet metadata and are the caller's (optional) enrichment step.
type BaselineFile struct {
	SnapshotTime time.Time
	Schema       string
	Table        string
	Path         string
}

// NewestSnapshotTables returns the schema.table entries of the NEWEST
// discoverable snapshot under source, sorted; nil when nothing is discoverable.
//
// It is the table list a REFRESH operates on. Deliberately the newest snapshot
// rather than every table the index has ever seen: a refreshed snapshot is a
// strict successor of the one it was folded from, and a table absent from the
// source has nothing to fold onto — inventing an entry for it would publish a
// snapshot claiming coverage it does not have.
func NewestSnapshotTables(ctx context.Context, source string) ([]string, error) {
	files, err := ListBaselines(ctx, source)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, nil
	}
	newest := files[0].SnapshotTime // ListBaselines returns newest first
	seen := map[string]bool{}
	var out []string
	for _, f := range files {
		if !f.SnapshotTime.Equal(newest) {
			continue
		}
		entry := f.Schema + "." + f.Table
		if seen[entry] {
			continue
		}
		seen[entry] = true
		out = append(out, entry)
	}
	sort.Strings(out)
	return out, nil
}

// SnapshotTablesAt returns the schema.table entries of the newest
// discoverable snapshot at or before at, sorted; nil when no snapshot is that
// old. It is the table list a point-in-time restore folds forward — NOT
// always the newest snapshot's: a restore to a moment before the newest
// snapshot anchors on the older snapshot FindBaseline will pick, and must
// fold that snapshot's tables.
func SnapshotTablesAt(ctx context.Context, source string, at time.Time) ([]string, error) {
	tables, _, err := SnapshotAt(ctx, source, at)
	return tables, err
}

// SnapshotAt is SnapshotTablesAt plus the anchor it settled on: the snapshot
// time of the newest discoverable snapshot at or before at (zero when none is
// that old). A caller that is about to PUBLISH a snapshot named by at needs
// the anchor to notice that one already exists at exactly that instant.
func SnapshotAt(ctx context.Context, source string, at time.Time) (tables []string, anchor time.Time, err error) {
	files, err := ListBaselines(ctx, source)
	if err != nil {
		return nil, time.Time{}, err
	}
	seen := map[string]bool{}
	var out []string
	for _, f := range files { // newest first
		if f.SnapshotTime.After(at) {
			continue
		}
		if anchor.IsZero() {
			anchor = f.SnapshotTime
		}
		if !f.SnapshotTime.Equal(anchor) {
			break
		}
		entry := f.Schema + "." + f.Table
		if seen[entry] {
			continue
		}
		seen[entry] = true
		out = append(out, entry)
	}
	sort.Strings(out)
	return out, anchor, nil
}

// ListBaselines enumerates every baseline snapshot file under source (a local
// directory or an s3:// prefix), newest snapshot first (then schema/table for
// a stable render order). Entries that don't match the
// <timestamp>/<schema>/<table>.parquet layout are skipped, mirroring
// FindBaseline's tolerance; unreadable snapshot subdirectories are skipped
// WITH a warning — the listing is an observability surface, and a silently
// shrunken one would misreport "latest baseline" with nothing in any log.
// internal/baseline.DiscoverBaselines walks the same local layout (plus
// Parquet footers) for `bintrail status`; keep the two in sync if the layout
// ever changes.
func ListBaselines(ctx context.Context, source string) ([]BaselineFile, error) {
	if strings.HasPrefix(source, "s3://") {
		return listBaselinesS3(ctx, source)
	}
	return listBaselinesLocal(source)
}

func listBaselinesLocal(baselineDir string) ([]BaselineFile, error) {
	entries, err := os.ReadDir(baselineDir)
	if err != nil {
		return nil, fmt.Errorf("read baseline directory %q: %w", baselineDir, err)
	}
	var out []BaselineFile
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		ts, ok := parseDirTimestamp(entry.Name())
		if !ok {
			continue
		}
		snapDir := filepath.Join(baselineDir, entry.Name())
		// Skip a partially-converted snapshot (#467) so the listing doesn't
		// advertise an incomplete snapshot as the latest baseline.
		if !baseline.SnapshotComplete(snapDir) {
			slog.Warn("baseline listing: skipping incomplete snapshot", "path", snapDir)
			continue
		}
		dbDirs, err := os.ReadDir(snapDir)
		if err != nil {
			slog.Warn("baseline listing: skipping unreadable snapshot directory", "path", snapDir, "error", err)
			continue
		}
		for _, dbDir := range dbDirs {
			if !dbDir.IsDir() {
				continue
			}
			schemaDir := filepath.Join(snapDir, dbDir.Name())
			files, err := os.ReadDir(schemaDir)
			if err != nil {
				slog.Warn("baseline listing: skipping unreadable schema directory", "path", schemaDir, "error", err)
				continue
			}
			for _, f := range files {
				if f.IsDir() || !strings.HasSuffix(f.Name(), ".parquet") {
					continue
				}
				out = append(out, BaselineFile{
					SnapshotTime: ts,
					Schema:       dbDir.Name(),
					Table:        strings.TrimSuffix(f.Name(), ".parquet"),
					Path:         filepath.Join(schemaDir, f.Name()),
				})
			}
		}
	}
	sortBaselineFiles(out)
	return out, nil
}

func listBaselinesS3(ctx context.Context, s3URL string) ([]BaselineFile, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	if err := pinDuckDBSessionUTC(ctx, db); err != nil {
		return nil, err
	}
	if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
		return nil, fmt.Errorf("load httpfs extension: %w", err)
	}
	if err := duckdbutil.EnableS3CredentialChain(ctx, db); err != nil {
		return nil, err
	}

	prefix := strings.TrimSuffix(s3URL, "/")

	// Exclude partially-converted snapshots (#467) so the listing doesn't
	// advertise an incomplete snapshot as the latest baseline.
	incomplete, err := s3IncompleteSnapshots(ctx, db, prefix)
	if err != nil {
		return nil, err
	}

	safeGlob := strings.ReplaceAll(prefix+"/*/*/*.parquet", "'", "''")
	rows, err := db.QueryContext(ctx, "SELECT * FROM glob('"+safeGlob+"')")
	if err != nil {
		return nil, fmt.Errorf("list S3 baseline snapshots: %w", err)
	}
	defer rows.Close()

	var out []BaselineFile
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			// The glob returns exactly one VARCHAR column; a Scan failure is a
			// driver/DuckDB fault, never an expected layout condition — and
			// rows.Err() below would NOT catch it. Fail loud rather than let a
			// listing whose purpose is observability silently drop snapshots.
			return nil, fmt.Errorf("scan S3 baseline path: %w", err)
		}
		rest := strings.TrimPrefix(path, prefix+"/")
		parts := strings.Split(rest, "/")
		if len(parts) != 3 || !strings.HasSuffix(parts[2], ".parquet") {
			continue
		}
		ts, ok := parseDirTimestamp(parts[0])
		if !ok {
			continue
		}
		if incomplete[ts.UTC().Format(time.RFC3339)] {
			continue
		}
		out = append(out, BaselineFile{
			SnapshotTime: ts,
			Schema:       parts[1],
			Table:        strings.TrimSuffix(parts[2], ".parquet"),
			Path:         path,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate S3 baseline list: %w", err)
	}
	sortBaselineFiles(out)
	return out, nil
}

func sortBaselineFiles(files []BaselineFile) {
	slices.SortFunc(files, func(a, b BaselineFile) int {
		if c := b.SnapshotTime.Compare(a.SnapshotTime); c != 0 {
			return c
		}
		if c := strings.Compare(a.Schema, b.Schema); c != 0 {
			return c
		}
		return strings.Compare(a.Table, b.Table)
	})
}

// ─── local ────────────────────────────────────────────────────────────────────

func findBaselineLocal(baselineDir, schema, table string, at time.Time) (string, time.Time, StaleWarning, error) {
	entries, err := os.ReadDir(baselineDir)
	if err != nil {
		return "", time.Time{}, StaleWarning{}, fmt.Errorf("read baseline directory %q: %w", baselineDir, err)
	}

	type candidate struct {
		t    time.Time
		path string
	}
	var candidates []candidate
	var newestSnap time.Time // newest eligible snapshot, whether or not it has the table
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		t, ok := parseDirTimestamp(entry.Name())
		if !ok || t.After(at) {
			continue
		}
		// Skip a partially-converted snapshot (#467): treating it as the newest
		// would reconstruct missing tables from an older one or hit ErrNoBaseline.
		if !baseline.SnapshotComplete(filepath.Join(baselineDir, entry.Name())) {
			continue
		}
		if t.After(newestSnap) {
			newestSnap = t
		}
		p := filepath.Join(baselineDir, entry.Name(), schema, table+".parquet")
		if _, err := os.Stat(p); err != nil {
			continue // table not in this snapshot
		}
		candidates = append(candidates, candidate{t: t, path: p})
	}
	if len(candidates) == 0 {
		return "", time.Time{}, StaleWarning{}, fmt.Errorf("%w: %s.%s at or before %s in %q",
			ErrNoBaseline, schema, table, at.UTC().Format(time.RFC3339), baselineDir)
	}
	slices.SortFunc(candidates, func(a, b candidate) int { return b.t.Compare(a.t) })
	best := candidates[0]
	stale := staleFallback(schema, table, best.t, newestSnap)
	return best.path, best.t, stale, nil
}

// staleFallback builds the staleness warning (and logs it) when the table is
// absent from a newer eligible snapshot, so the chosen snapshot is an older
// fallback (#461/#466). When newestSnap is not after using, the result is the
// zero StaleWarning ("not stale"). The local and S3 lookups share this so the
// server-side log message and the surfaced warning are identical.
func staleFallback(schema, table string, using, newestSnap time.Time) StaleWarning {
	if !newestSnap.After(using) {
		return StaleWarning{}
	}
	// The table dropped out of newer snapshots (dump filter change, lost SELECT
	// privilege, rename): falling back to an older snapshot is the designed
	// behavior, but doing it silently means reconstructing from ever-staler data
	// with no signal anywhere (#461). Warn server-side AND return the message so
	// callers (the console, #466) can surface it in-band.
	usingStr := using.UTC().Format(time.RFC3339)
	newestStr := newestSnap.UTC().Format(time.RFC3339)
	slog.Warn("baseline: table is absent from the newest snapshot; using an older one — re-dump to refresh it",
		"schema", schema, "table", table, "using", usingStr, "newest_snapshot", newestStr)
	return StaleWarning{
		Message: fmt.Sprintf("baseline for %s.%s is stale: the table is absent from the newest snapshot (%s); reconstructing from an older snapshot (%s) — re-dump to refresh it",
			schema, table, newestStr, usingStr),
		UsingSnapshot:  using,
		NewestSnapshot: newestSnap,
	}
}

// ─── S3 ───────────────────────────────────────────────────────────────────────

// findBaselineS3 mirrors findBaselineLocal over an s3:// prefix. It now also
// warns when the result is an older-snapshot fallback (#466): the prior
// table-scoped glob made snapshots lacking the table invisible, so it could
// never compute a "newest eligible snapshot" to compare against. We resolve
// that by running ONE broader listing (prefix/*/*/*.parquet, the same glob
// listBaselinesS3 uses) — bounding the listing cost to a single extra glob —
// to derive the newest complete snapshot at-or-before `at`, and ONE marker glob
// (prefix/*/_SUCCESS and _INCOMPLETE) to exclude partial snapshots (#467).
//
// The two glob steps differ in fatality: the marker glob (s3IncompleteSnapshots)
// is a CORRECTNESS filter — its error fails the lookup so a partial snapshot can
// never slip through — while the broad newest-snapshot glob is purely ADVISORY
// (staleWarningS3) and its error must NOT discard the already-located baseline
// (#524 review).
func findBaselineS3(ctx context.Context, s3URL, schema, table string, at time.Time) (string, time.Time, StaleWarning, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return "", time.Time{}, StaleWarning{}, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	if err := pinDuckDBSessionUTC(ctx, db); err != nil {
		return "", time.Time{}, StaleWarning{}, err
	}

	if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
		return "", time.Time{}, StaleWarning{}, fmt.Errorf("load httpfs extension: %w", err)
	}
	if err := duckdbutil.EnableS3CredentialChain(ctx, db); err != nil {
		return "", time.Time{}, StaleWarning{}, err
	}

	prefix := strings.TrimSuffix(s3URL, "/")

	// Snapshot dirs flagged incomplete (#467) — excluded from both the
	// table-scoped candidate scan and the broad newest-snapshot scan.
	incomplete, err := s3IncompleteSnapshots(ctx, db, prefix)
	if err != nil {
		return "", time.Time{}, StaleWarning{}, err
	}

	// Table-scoped glob: the snapshots that actually contain this table.
	globPat := prefix + "/*/" + schema + "/" + table + ".parquet"
	safeGlob := strings.ReplaceAll(globPat, "'", "''")
	rows, err := db.QueryContext(ctx, "SELECT * FROM glob('"+safeGlob+"')")
	if err != nil {
		return "", time.Time{}, StaleWarning{}, fmt.Errorf("list S3 baseline snapshots: %w", err)
	}
	defer rows.Close()

	type candidate struct {
		t    time.Time
		path string
	}
	var candidates []candidate
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			continue
		}
		t, ok := extractTimestampFromS3Path(path, prefix, schema, table)
		if !ok || t.After(at) {
			continue
		}
		if incomplete[t.UTC().Format(time.RFC3339)] {
			continue // partial snapshot (#467)
		}
		candidates = append(candidates, candidate{t: t, path: path})
	}
	if err := rows.Err(); err != nil {
		return "", time.Time{}, StaleWarning{}, fmt.Errorf("iterate S3 baseline list: %w", err)
	}
	if len(candidates) == 0 {
		return "", time.Time{}, StaleWarning{}, fmt.Errorf("%w: %s.%s at or before %s in %q",
			ErrNoBaseline, schema, table, at.UTC().Format(time.RFC3339), s3URL)
	}
	slices.SortFunc(candidates, func(a, b candidate) int { return b.t.Compare(a.t) })
	best := candidates[0]

	// `best` is a VALID located baseline. The staleness warning is ADVISORY, so
	// its computation must never fail the recovery (see staleWarningS3).
	stale := staleWarningS3(ctx, db, prefix, schema, table, best.t, at, incomplete)
	return best.path, best.t, stale, nil
}

// staleWarningS3 derives the advisory staleness warning for an already-located
// S3 baseline. The broad newest-snapshot glob (s3NewestSnapshot) lists every
// parquet across all snapshots and is the most likely of the lookup's globs to
// throttle/timeout on a large bucket; findBaselineS3 is on the per-request shim
// `_snapshot` / console reconstruct hot path (no caching). A transient S3 blip
// on this purely-advisory step must NOT throw away the baseline we already
// found — that would fail a recovery that pre-#466 succeeded, the inverse of
// the goal. So on error we warn and return the zero StaleWarning ("not stale");
// only the FATAL filters (incomplete-snapshot exclusion, the table-scoped glob)
// can fail the lookup (#524 review).
func staleWarningS3(ctx context.Context, db *sql.DB, prefix, schema, table string, using, at time.Time, incomplete map[string]bool) StaleWarning {
	// Broad scan for the newest complete snapshot at-or-before `at`, whether or
	// not it contains this table — the missing piece that let S3 fall back
	// silently (#466).
	newestSnap, err := s3NewestSnapshot(ctx, db, prefix, at, incomplete)
	if err != nil {
		slog.Warn("baseline: staleness check failed; returning the located baseline without a stale warning",
			"schema", schema, "table", table, "error", err)
		return StaleWarning{}
	}
	return staleFallback(schema, table, using, newestSnap)
}

// s3NewestSnapshot returns the newest complete snapshot timestamp at-or-before
// `at` across ALL tables under prefix, using the broad prefix/*/*/*.parquet
// glob. Snapshots in the incomplete set (#467) are excluded.
func s3NewestSnapshot(ctx context.Context, db *sql.DB, prefix string, at time.Time, incomplete map[string]bool) (time.Time, error) {
	safeGlob := strings.ReplaceAll(prefix+"/*/*/*.parquet", "'", "''")
	rows, err := db.QueryContext(ctx, "SELECT * FROM glob('"+safeGlob+"')")
	if err != nil {
		return time.Time{}, fmt.Errorf("list S3 baseline snapshots (broad): %w", err)
	}
	defer rows.Close()

	var newest time.Time
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			continue
		}
		rest := strings.TrimPrefix(path, prefix+"/")
		parts := strings.Split(rest, "/")
		if len(parts) != 3 || !strings.HasSuffix(parts[2], ".parquet") {
			continue
		}
		t, ok := parseDirTimestamp(parts[0])
		if !ok || t.After(at) {
			continue
		}
		if incomplete[t.UTC().Format(time.RFC3339)] {
			continue
		}
		if t.After(newest) {
			newest = t
		}
	}
	if err := rows.Err(); err != nil {
		return time.Time{}, fmt.Errorf("iterate S3 baseline list (broad): %w", err)
	}
	return newest, nil
}

// s3IncompleteSnapshots returns the set of snapshot timestamps (keyed by
// RFC3339 UTC) that carry an _INCOMPLETE marker without a _SUCCESS marker, so a
// partially-converted snapshot (#467) is excluded from S3 discovery. Pre-marker
// snapshots have neither and are complete-by-default (absent from this set).
//
// The glob is prefix/*/_* — DuckDB's glob() does NOT brace-expand
// {_SUCCESS,_INCOMPLETE} (verified empirically), and the only underscore-prefixed
// entries in the snapshot layout are these two markers; we still filter by exact
// basename so an unrelated _* file can't be mistaken for a marker.
func s3IncompleteSnapshots(ctx context.Context, db *sql.DB, prefix string) (map[string]bool, error) {
	markerGlob := strings.ReplaceAll(prefix+"/*/_*", "'", "''")
	rows, err := db.QueryContext(ctx, "SELECT * FROM glob('"+markerGlob+"')")
	if err != nil {
		return nil, fmt.Errorf("list S3 baseline markers: %w", err)
	}
	defer rows.Close()

	hasSuccess := map[string]bool{}
	hasIncomplete := map[string]bool{}
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			// This is a CORRECTNESS filter, not an observability listing: a
			// silently dropped row could be an _INCOMPLETE marker, which would
			// demote its partial snapshot to complete-by-default (residual #467).
			// Fail loud — the safe-on-error direction for a marker filter is
			// "treat as incomplete / surface the error", never silently complete.
			// Mirrors the hardened listBaselinesS3 Scan branch (#524 review).
			return nil, fmt.Errorf("scan S3 baseline marker path: %w", err)
		}
		rest := strings.TrimPrefix(path, prefix+"/")
		parts := strings.Split(rest, "/")
		if len(parts) != 2 {
			continue
		}
		t, ok := parseDirTimestamp(parts[0])
		if !ok {
			continue
		}
		key := t.UTC().Format(time.RFC3339)
		switch parts[1] {
		case baseline.SuccessMarker:
			hasSuccess[key] = true
		case baseline.IncompleteMarker:
			hasIncomplete[key] = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate S3 baseline markers: %w", err)
	}
	incomplete := map[string]bool{}
	for key := range hasIncomplete {
		if !hasSuccess[key] {
			incomplete[key] = true
		}
	}
	return incomplete, nil
}

// extractTimestampFromS3Path parses the snapshot timestamp from a full S3 path:
// s3://bucket/prefix/2026-02-28T00-00-00Z/mydb/orders.parquet
func extractTimestampFromS3Path(path, prefix, schema, table string) (time.Time, bool) {
	base := strings.TrimSuffix(prefix, "/") + "/"
	rest := strings.TrimPrefix(path, base)
	// rest: 2026-02-28T00-00-00Z/mydb/orders.parquet
	suffix := "/" + schema + "/" + table + ".parquet"
	dirName, ok := strings.CutSuffix(rest, suffix)
	if !ok {
		return time.Time{}, false
	}
	return parseDirTimestamp(dirName)
}

// ─── shared helpers ───────────────────────────────────────────────────────────

// parseDirTimestamp converts a baseline directory name like "2026-02-28T00-00-00Z"
// to a time.Time. The format is RFC3339 with colons in the time+offset portion
// replaced by hyphens for filesystem compatibility.
// parseDirTimestamp delegates to snapshotdir.ParseTime so the rule lives in one
// place: query reads the same directory names, and two hand-written parsers of
// one format is how they drift.
func parseDirTimestamp(name string) (time.Time, bool) {
	return snapshotdir.ParseTime(name)
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// pinDuckDBSessionUTC pins a freshly-opened DuckDB connection's session time
// zone to UTC so string→timestamp casts are deterministic regardless of the
// host OS timezone (#359).
//
// bintrail stores all temporal values UTC-anchored: DATETIME/TIMESTAMP land in
// the baseline Parquet as micros-since-epoch (internal/baseline/schema.go),
// which DuckDB reads back as TIMESTAMP WITH TIME ZONE. Without this pin,
// ReadBaselineRow's `pkcol = ?` predicate binds the PK value as a string and
// DuckDB casts that literal to TIMESTAMPTZ using the *session* timezone
// inherited from the OS TZ. On a non-UTC host `'2020-01-01 00:00:00'` resolves
// to a different UTC instant than the stored micros, so a datetime/timestamp
// PK row silently fails to match. Pinning UTC makes the cast match the stored
// instant on every host. The other DuckDB-opening helpers here pin it too for
// consistency, so returned temporal values render as their UTC wall-clock.
func pinDuckDBSessionUTC(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, "SET TimeZone='UTC'"); err != nil {
		return fmt.Errorf("pin duckdb session to UTC: %w", err)
	}
	return nil
}

type colCond struct {
	col   string
	value string
}

// bindFilterArgs turns the filter conditions into DuckDB bind arguments.
//
// Every value arrives as a string (that is the at-rest spelling of a
// binlog_events.pk_values component, which is what callers hand us), and
// binding it verbatim is correct for every column type DuckDB can cast a
// VARCHAR into — including a BLOB column holding printable bytes, where
// DuckDB's implicit VARCHAR→BLOB cast compares the raw characters and
// matches.
//
// The one spelling that cast CANNOT resolve is the "0x"+hex form
// event.formatPKValue produces for PK bytes that are not valid UTF-8 (#1132):
// bound as a VARCHAR it compares the six literal characters "0xB281" against
// the two bytes {0xB2,0x81} and silently matches nothing — a BINARY(16) UUID
// primary key could not be reconstructed at all (#1155). For those, decode
// the hex and bind the BYTES.
//
// The decode is gated on the value's shape AND on the column really being a
// Parquet BYTE_ARRAY (BLOB), so a VARCHAR column whose value happens to read
// "0xAB" keeps binding as literal text.
//
// Worth being precise about what the type gate does and does not buy, because
// this is NOT the rule formatPKValue applies when PRODUCING the spelling —
// that one is gated purely on content (valid UTF-8 → stored verbatim), this
// one on content AND type. The asymmetry looks like it should strand a binary
// column whose bytes are literally the ASCII text "0x<even-hex>": pk_values
// holds that text, and decoding it would look for entirely different bytes.
//
// It does not, and the reason is that the WRITE side decodes identically.
// internal/baseline's decodeBinaryLiteral is applied to every binary-family
// column, and its doc records the same residual from the other end: a binary
// value whose actual bytes are the ASCII text "0x…" is indistinguishable from
// a --hex-blob literal and IS decoded on the way into the Parquet. So the
// baseline never stores those characters as characters, and the two ends of
// the ambiguity resolve the same way by construction rather than by agreement.
// TestReadBaselineRow_binaryHexTextSymmetry pins that, because it is the
// property this gate leans on and it lives in another package.
func bindFilterArgs(ctx context.Context, db *sql.DB, safePath string, conds []colCond) ([]any, error) {
	args := make([]any, len(conds))
	decoded := make([][]byte, len(conds))
	anyHex := false
	for i, c := range conds {
		args[i] = c.value
		if b, ok := decodeHexPKLiteral(c.value); ok {
			decoded[i] = b
			anyHex = true
		}
	}
	if !anyHex {
		return args, nil
	}

	blobCols, err := parquetBlobColumns(ctx, db, safePath)
	if err != nil {
		// The probe only runs once a value already carries the 0x-hex spelling,
		// which cannot match a BLOB column bound as text — so this fallback is
		// a guaranteed miss for exactly the lookup the probe exists to serve,
		// not graceful degradation. Warn (not Debug): downstream a miss is
		// indistinguishable from a genuinely absent row.
		slog.Warn("could not probe baseline column types; binding a 0x-hex filter value as text, which will not match a binary column",
			"error", err)
		return args, nil
	}
	for i, c := range conds {
		if decoded[i] == nil {
			continue
		}
		// Case-insensitive: DuckDB resolves the quoted identifier in the WHERE
		// clause case-insensitively, so an exact-only match here would bind a
		// differently-cased operator-typed column name as text against a BLOB
		// and silently miss — making this the ONE link in the chain that cares
		// about case.
		if !blobCols[strings.ToLower(c.col)] {
			continue
		}
		args[i] = decoded[i]
	}
	return args, nil
}

// decodeHexPKLiteral decodes the "0x"+uppercase-hex spelling
// event.formatPKValue produces for non-UTF-8 PK bytes. Case-insensitive on
// the hex digits so an operator who pasted a lowercase key still resolves;
// an odd digit count is not a valid byte string and is rejected.
func decodeHexPKLiteral(s string) ([]byte, bool) {
	if len(s) < 4 || len(s)%2 != 0 || !strings.HasPrefix(s, "0x") {
		return nil, false
	}
	b, err := hex.DecodeString(s[2:])
	if err != nil {
		return nil, false
	}
	return b, true
}

// parquetBlobColumns reports which columns of the baseline file are stored as
// a Parquet BYTE_ARRAY (DuckDB BLOB) — the binary/spatial family per
// internal/baseline/schema.go. The LIMIT 0 query reads footer metadata only.
func parquetBlobColumns(ctx context.Context, db *sql.DB, safePath string) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, "SELECT * FROM parquet_scan('"+safePath+"') LIMIT 0")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	// Keyed lowercase: the caller looks up an operator-typed column name and
	// DuckDB itself resolves identifiers case-insensitively.
	out := make(map[string]bool, len(types))
	for _, t := range types {
		if strings.EqualFold(t.DatabaseTypeName(), "BLOB") {
			out[strings.ToLower(t.Name())] = true
		}
	}
	return out, rows.Err()
}

// buildCondsList returns sorted column conditions for deterministic SQL + arg order.
func buildCondsList(pkFilter map[string]string) []colCond {
	conds := make([]colCond, 0, len(pkFilter))
	for col, val := range pkFilter {
		conds = append(conds, colCond{col: col, value: val})
	}
	slices.SortFunc(conds, func(a, b colCond) int { return strings.Compare(a.col, b.col) })
	return conds
}
