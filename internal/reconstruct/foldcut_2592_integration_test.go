//go:build integration

package reconstruct_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2592: the fold cut reaches the footer a real run writes, the table file
// or the pair beside it, and only a strict run claims one.
func TestReconstructParquet_foldCutOnlyFromAStrictRun_2592(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	dsn := testutil.BaseDSN() + "/" + dbName
	const schema = "shop"
	base := time.Now().UTC().Truncate(time.Hour)
	seedOrdersSnapshot(t, db, schema, base)
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, base.Add(10*time.Second).Format("2006-01-02 15:04:05"), nil,
		schema, "orders", 2, "1", nil, nil, []byte(`{"id":1,"status":"A"}`))
	at := base.Add(30 * time.Second)

	for _, c := range []struct {
		name        string
		deltas, gap bool
		want        bool
	}{
		{"a rewritten table, strict", false, false, true},
		{"a rewritten table, gaps allowed", false, true, false},
		{"a table delta, strict", true, false, true},
		{"a table delta, gaps allowed", true, true, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			seedSourceBaseline(t, root, base, schema)
			if _, err := reconstruct.ReconstructTables(ctx, reconstruct.FullTableConfig{
				IndexDSN: dsn, BaselineSrc: root, Tables: []string{schema + ".orders"}, At: at,
				OutputDir: root, OutputFormat: reconstruct.OutputFormatParquet,
				TableDeltas: c.deltas, AllowGaps: c.gap,
			}); err != nil {
				t.Fatalf("ReconstructTables: %v", err)
			}
			path := filepath.Join(root, reconstruct.SnapshotDirName(at), schema, "orders.parquet")
			if chain, err := baseline.ListTableDelta(ctx, path); err != nil {
				t.Fatal(err)
			} else if chain != nil && len(chain.Files) > 0 {
				path = chain.LastFileUpserts()
			}
			md, err := baseline.ReadParquetMetadata(path)
			if err != nil {
				t.Fatal(err)
			}
			if !md.SnapshotTimestamp.Equal(at) {
				t.Fatalf("fixture: %s was not written by this run (%v)", path, md.SnapshotTimestamp)
			}
			if got := md.FoldCut != ""; got != c.want {
				t.Fatalf("%s: fold cut %q, want claimed=%v", filepath.Base(path), md.FoldCut, c.want)
			}
		})
	}
}
