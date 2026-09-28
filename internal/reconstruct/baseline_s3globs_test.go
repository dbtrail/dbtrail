package reconstruct

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
)

// The table lookup an s3:// source had before #1740, word for word: three
// DuckDB globs over the whole prefix. It is no longer what FindBaseline runs
// (baseline_s3find.go is). It is kept in a test file as the reference the
// new lookup is compared with: on a local folder in the unit tests
// (findBaselineGlob, which needs no httpfs) and on an S3-compatible store in
// the integration test (findBaselineS3Globs).

// findBaselineS3Globs mirrors findBaselineLocal over an s3:// prefix. It also
// warns when the result is an older-snapshot fallback (#466): the prior
// table-scoped glob made snapshots lacking the table invisible, so it could
// never compute a "newest eligible snapshot" to compare against. We resolve
// that by running ONE broader listing (prefix/*/*/*.parquet, the glob the
// listing used before #1679) — bounding the listing cost to a single extra glob —
// to derive the newest complete snapshot at-or-before `at`, and ONE marker glob
// (prefix/*/_SUCCESS and _INCOMPLETE) to exclude partial snapshots (#467).
//
// The two glob steps differ in fatality: the marker glob (s3IncompleteSnapshots)
// is a CORRECTNESS filter — its error fails the lookup so a partial snapshot can
// never slip through — while the broad newest-snapshot glob is purely ADVISORY
// (staleWarningS3) and its error must NOT discard the already-located baseline
// (#524 review).
func findBaselineS3Globs(ctx context.Context, s3URL, schema, table string, at time.Time) (string, time.Time, StaleWarning, error) {
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

	return findBaselineGlob(ctx, db, s3URL, schema, table, at)
}

// findBaselineGlob is findBaselineS3 past the session setup: the three globs,
// over whatever the session can reach (a local directory too, which is how
// the tests run it).
func findBaselineGlob(ctx context.Context, db *sql.DB, s3URL, schema, table string, at time.Time) (string, time.Time, StaleWarning, error) {
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
			// Mirrors the hardened Scan branch the listing had before #1679 (#524 review).
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
