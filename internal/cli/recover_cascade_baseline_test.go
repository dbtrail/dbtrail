package cli

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// writeChildBaselineParquet writes a minimal real Parquet baseline snapshot
// for <schema>.child (columns id, pid — id is the PK, pid is the FK the
// cascade engine filters on) under <dir>/<snapshotDir>/<schema>/child.parquet.
// meta, when non-nil, lands in the Parquet footer's key/value metadata — that
// is where the #797 binlog coordinates live.
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
// with PK "id" and FK column "pid" — the shape the cascade baseline provider
// expects.
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

// TestCascadeBaselineProviderFor_wiresSingleSourceLookup pins the CLI half of
// the #1101/#1102 unification: recover-cascade's Phase-2 provider is the shared
// internal/cascadebaseline implementation bound to the ONE --baseline-dir /
// --baseline-s3 source the operator passed (the CLI has no local→S3 fallback —
// that is a console/bundle concern), and it carries the #797 SincePos anchor
// the shared provider produces.
func TestCascadeBaselineProviderFor_wiresSingleSourceLookup(t *testing.T) {
	dir := t.TempDir()
	schema := "shop"
	writeChildBaselineParquet(t, dir, "2026-01-01T00-00-00Z", schema, [][]string{{"10", "1"}}, map[string]string{
		baseline.MetaKeyBinlogFile: "binlog.000042",
		baseline.MetaKeyBinlogPos:  "12345",
	})

	provider := cascadeBaselineProviderFor(dir, childResolver(schema), nil)
	if provider == nil {
		t.Fatal("cascadeBaselineProviderFor returned nil; a typed-nil provider would report BaselineActive with no baseline behind it")
	}

	at := time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC)
	lookup, ok, err := provider.BaselineChildren(context.Background(), schema, "child", "pid", "1", at, 100)
	if err != nil {
		t.Fatalf("BaselineChildren: %v", err)
	}
	if !ok || len(lookup.Rows) != 1 || lookup.Rows[0].PKValues != "10" {
		t.Fatalf("lookup = (ok=%v, rows=%+v), want one row with PKValues=10", ok, lookup.Rows)
	}
	if lookup.SincePos == nil || lookup.SincePos.File != "binlog.000042" || lookup.SincePos.Pos != 12345 {
		t.Fatalf("SincePos = %+v, want the baseline's recorded binlog position (#797)", lookup.SincePos)
	}

	// A source with no baseline for the table degrades to Phase-1 only, never an
	// error that would abort the whole recovery.
	_, ok2, err := cascadeBaselineProviderFor(t.TempDir(), childResolver(schema), nil).
		BaselineChildren(context.Background(), schema, "child", "pid", "1", at, 100)
	if err != nil {
		t.Fatalf("BaselineChildren against an empty source: %v, want nil (Phase-1 only)", err)
	}
	if ok2 {
		t.Fatal("ok = true against an empty baseline source, want false")
	}
}
