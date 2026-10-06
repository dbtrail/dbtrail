package cascadebaseline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// writeChildBaselineParquet writes a minimal real Parquet baseline snapshot
// for <schema>.child (columns id, pid — id is the PK, pid is the FK the
// cascade engine filters on) under <dir>/<snapshotDir>/<schema>/child.parquet.
// meta, when non-nil, is written into the Parquet file's key/value metadata
// (that is where the #797 binlog coordinates live).
// Mirrors internal/reconstruct's writeTestBaseline fixture pattern.
func writeChildBaselineParquet(t *testing.T, dir, snapshotDir, schema string, rows [][]string, meta map[string]string) string {
	t.Helper()
	path := filepath.Join(dir, snapshotDir, schema, "child.parquet")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "pid", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 10, Metadata: meta})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	for _, r := range rows {
		if err := w.WriteRow(r, []bool{false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("writer close: %v", err)
	}
	return path
}

// childResolver builds an in-memory schema resolver (no DB) for <schema>.child
// with PK "id" and FK column "pid" — the shape Provider expects.
func childResolver(schema string) *metadata.Resolver {
	tm := &metadata.TableMeta{
		Schema: schema,
		Table:  "child",
		Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
			{Name: "pid", OrdinalPosition: 2, DataType: "int"},
		},
		PKColumns: []string{"id"},
	}
	return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{schema + ".child": tm})
}

// TestProvider_staleFallbackPropagates pins #618 at the (now shared) provider
// boundary: when the baseline lookup falls back to an older snapshot because
// the child table is absent from the newest one, the returned
// cascade.BaselineLookup.StaleMessage must carry that signal instead of
// discarding it (the pre-#618 behavior: `path, snap, _, err := FindBaseline(...)`).
// A lookup where the chosen snapshot IS the newest eligible one must leave
// StaleMessage empty.
func TestProvider_staleFallbackPropagates(t *testing.T) {
	dir := t.TempDir()
	schema := "shop"
	writeChildBaselineParquet(t, dir, "2026-01-01T00-00-00Z", schema, [][]string{{"10", "1"}}, nil)
	// Newer, complete snapshot that does NOT have shop.child — this is what
	// makes the 2026-01-01 pick a stale fallback.
	if err := os.MkdirAll(filepath.Join(dir, "2026-02-01T00-00-00Z"), 0o755); err != nil {
		t.Fatal(err)
	}

	provider := New(Source(dir), childResolver(schema), nil)

	// `at` is after both snapshots, so the newer (childless) one is the
	// "newest eligible" snapshot and the older one is a stale fallback.
	at := time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC)
	lookup, ok, err := provider.BaselineChildren(context.Background(), schema, "child", "pid", "1", at, 100)
	if err != nil {
		t.Fatalf("BaselineChildren: %v", err)
	}
	if !ok {
		t.Fatalf("BaselineChildren ok = false, want true (the older snapshot covers the table)")
	}
	if len(lookup.Rows) != 1 || lookup.Rows[0].PKValues != "10" {
		t.Fatalf("Rows = %+v, want one row with PKValues=10", lookup.Rows)
	}
	if lookup.StaleMessage == "" {
		t.Fatal("StaleMessage is empty, want the #466 stale-fallback signal to be carried through")
	}
	if !strings.Contains(lookup.StaleMessage, schema+".child") || !strings.Contains(lookup.StaleMessage, "absent from the newest snapshot") {
		t.Fatalf("StaleMessage = %q, want it to name %s.child and the fallback reason (matches reconstruct.staleFallback's format)", lookup.StaleMessage, schema)
	}

	// When the chosen snapshot IS the newest eligible one, StaleMessage must
	// be empty — not a spurious caveat every time Phase-2 runs.
	at2 := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	lookup2, ok2, err := provider.BaselineChildren(context.Background(), schema, "child", "pid", "1", at2, 100)
	if err != nil {
		t.Fatalf("BaselineChildren (non-stale): %v", err)
	}
	if !ok2 {
		t.Fatalf("BaselineChildren (non-stale) ok = false, want true")
	}
	if lookup2.StaleMessage != "" {
		t.Fatalf("StaleMessage = %q, want empty when the chosen snapshot is the newest eligible one", lookup2.StaleMessage)
	}
}

// TestProvider_sincePosFromBaselineFooter pins #797/#1101: when the baseline
// Parquet footer recorded the binlog coordinates the snapshot was taken at, the
// provider must anchor the candidate-victim fetch on that exact position rather
// than on the coarse SnapshotTime DATETIME alone. A footer WITHOUT coordinates
// must leave SincePos nil (timestamp-only anchoring, the pre-#797 behavior).
func TestProvider_sincePosFromBaselineFooter(t *testing.T) {
	schema := "shop"
	at := time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC)

	withPos := t.TempDir()
	writeChildBaselineParquet(t, withPos, "2026-01-01T00-00-00Z", schema, [][]string{{"10", "1"}}, map[string]string{
		baseline.MetaKeyBinlogFile: "binlog.000042",
		baseline.MetaKeyBinlogPos:  "12345",
	})
	lookup, ok, err := New(Source(withPos), childResolver(schema), nil).
		BaselineChildren(context.Background(), schema, "child", "pid", "1", at, 100)
	if err != nil || !ok {
		t.Fatalf("BaselineChildren = (ok=%v, err=%v), want ok with no error", ok, err)
	}
	if lookup.SincePos == nil {
		t.Fatal("SincePos is nil, want the baseline's recorded binlog position (#797)")
	}
	if lookup.SincePos.File != "binlog.000042" || lookup.SincePos.Pos != 12345 {
		t.Fatalf("SincePos = %+v, want {binlog.000042 12345}", *lookup.SincePos)
	}

	noPos := t.TempDir()
	writeChildBaselineParquet(t, noPos, "2026-01-01T00-00-00Z", schema, [][]string{{"10", "1"}}, nil)
	lookup2, ok2, err := New(Source(noPos), childResolver(schema), nil).
		BaselineChildren(context.Background(), schema, "child", "pid", "1", at, 100)
	if err != nil || !ok2 {
		t.Fatalf("BaselineChildren (no footer position) = (ok=%v, err=%v), want ok with no error", ok2, err)
	}
	if lookup2.SincePos != nil {
		t.Fatalf("SincePos = %+v, want nil when the footer records no binlog position", *lookup2.SincePos)
	}
}

// TestProvider_metadataReadFailureDoesNotBlockScan pins the best-effort contract
// on the #797 metadata read: the baseline row scan has ALREADY succeeded by the
// time the footer is read, so an unreadable footer must degrade to timestamp-only
// anchoring (SincePos nil) rather than fail the whole Phase-2 lookup.
func TestProvider_metadataReadFailureDoesNotBlockScan(t *testing.T) {
	dir := t.TempDir()
	schema := "shop"
	writeChildBaselineParquet(t, dir, "2026-01-01T00-00-00Z", schema, [][]string{{"10", "1"}}, map[string]string{
		baseline.MetaKeyBinlogFile: "binlog.000042",
		baseline.MetaKeyBinlogPos:  "12345",
	})

	// A glob path is the cheapest way to split the two reads: DuckDB's
	// parquet_scan resolves it (so the row scan succeeds) while the footer read
	// opens it literally and fails.
	glob := filepath.Join(dir, "2026-01-01T00-00-00Z", schema, "*.parquet")
	find := func(ctx context.Context, sch, table string, at time.Time) (string, time.Time, reconstruct.StaleWarning, error) {
		return glob, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), reconstruct.StaleWarning{}, nil
	}
	lookup, ok, err := New(find, childResolver(schema), nil).
		BaselineChildren(context.Background(), schema, "child", "pid", "1", time.Now(), 100)
	if err != nil {
		t.Fatalf("BaselineChildren: %v, want the unreadable footer to be logged and ignored", err)
	}
	if !ok || len(lookup.Rows) != 1 {
		t.Fatalf("lookup = (ok=%v, rows=%+v), want the already-succeeded row scan to be returned", ok, lookup.Rows)
	}
	if lookup.SincePos != nil {
		t.Fatalf("SincePos = %+v, want nil (degrade to timestamp-only anchoring) when the footer cannot be read", *lookup.SincePos)
	}
}

// TestProvider_usesInjectedLookup pins the #1102 design: the provider never
// calls reconstruct.FindBaseline itself — it calls whatever lookup it was
// constructed with, so each surface composes with its own baseline-resolution
// policy (the console's bundle.findBaseline carries the #766 local→S3 fallback).
func TestProvider_usesInjectedLookup(t *testing.T) {
	dir := t.TempDir()
	schema := "shop"
	writeChildBaselineParquet(t, dir, "2026-01-01T00-00-00Z", schema, [][]string{{"10", "1"}}, nil)

	calls := 0
	primaryEmpty := t.TempDir()
	// A two-source lookup shaped exactly like bundle.findBaseline: primary
	// misses with ErrNoBaseline, fallback hits.
	find := func(ctx context.Context, sch, table string, at time.Time) (string, time.Time, reconstruct.StaleWarning, error) {
		calls++
		path, snap, stale, err := reconstruct.FindBaseline(ctx, primaryEmpty, sch, table, at)
		if !errors.Is(err, reconstruct.ErrNoBaseline) {
			return path, snap, stale, err
		}
		return reconstruct.FindBaseline(ctx, dir, sch, table, at)
	}

	lookup, ok, err := New(find, childResolver(schema), nil).
		BaselineChildren(context.Background(), schema, "child", "pid", "1", time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), 100)
	if err != nil {
		t.Fatalf("BaselineChildren: %v", err)
	}
	if calls != 1 {
		t.Fatalf("injected lookup called %d times, want exactly 1", calls)
	}
	if !ok || len(lookup.Rows) != 1 || lookup.Rows[0].PKValues != "10" {
		t.Fatalf("lookup = (ok=%v, rows=%+v), want the row the injected lookup's fallback source holds", ok, lookup.Rows)
	}
}

// TestProvider_noBaselineIsPhase1Only pins that a table the lookup does not
// cover degrades to Phase-1 (ok=false, nil error) instead of failing the whole
// cascade recovery.
func TestProvider_noBaselineIsPhase1Only(t *testing.T) {
	lookup, ok, err := New(Source(t.TempDir()), childResolver("shop"), nil).
		BaselineChildren(context.Background(), "shop", "child", "pid", "1", time.Now(), 100)
	if err != nil {
		t.Fatalf("BaselineChildren on an empty baseline dir: %v, want nil (Phase-1 only)", err)
	}
	if ok {
		t.Fatalf("ok = true, want false when no baseline covers the table (lookup=%+v)", lookup)
	}
}

// A lookup that failed (#1740: a snapshot location that could not be read) is
// an error. It is never "not covered", which would let the recovery go on
// with Phase-1 alone and miss the rows only the baseline holds.
func TestProvider_aFailedLookupIsNotPhase1Only(t *testing.T) {
	failed := errors.New("could not read the snapshot folder s3://b/base/2026-09-01T00-00-00Z: SlowDown")
	find := func(context.Context, string, string, time.Time) (string, time.Time, reconstruct.StaleWarning, error) {
		return "", time.Time{}, reconstruct.StaleWarning{}, failed
	}
	_, ok, err := New(find, childResolver("shop"), nil).
		BaselineChildren(context.Background(), "shop", "child", "pid", "1", time.Now(), 100)
	if !errors.Is(err, failed) || ok {
		t.Fatalf("ok = %v err = %v, want the lookup's own error", ok, err)
	}
}

func TestFkFilterSafe(t *testing.T) {
	safe := []string{"int", "BIGINT", " varchar ", "text", "enum"}
	for _, d := range safe {
		if !fkFilterSafe(d) {
			t.Errorf("fkFilterSafe(%q) = false, want true", d)
		}
	}
	// Types whose string form may not coerce exactly in DuckDB must be refused
	// rather than silently zero-match.
	unsafe := []string{"datetime", "decimal", "date", "timestamp", "blob", "json", ""}
	for _, d := range unsafe {
		if fkFilterSafe(d) {
			t.Errorf("fkFilterSafe(%q) = true, want false", d)
		}
	}
}
