//go:build integration

package reconstruct_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestReconstructParquet_inheritsTheLastRead is #1570 through the whole
// pipeline, the part a unit test cannot reach: ReconstructTable reading the
// previous snapshot's footer and handing it to the writer, with table deltas
// off (the table rewritten) and on (a pair beside it). The source is a dump
// written before #1570, with its producer and no keys, so the derivation for
// a legacy dump is exercised end to end too.
func TestReconstructParquet_inheritsTheLastRead(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	for _, deltas := range []bool{false, true} {
		name := "rewrite"
		if deltas {
			name = "table deltas"
		}
		t.Run(name, func(t *testing.T) {
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
			t0, cut1, cut2 := base, base.Add(30*time.Second), base.Add(60*time.Second)
			seedOrdersSnapshot(t, db, schema, t0)

			root := t.TempDir()
			snapDir := filepath.Join(root, strings.ReplaceAll(t0.Format(time.RFC3339), ":", "-"))
			cols, err := baseline.ParseSchemaText(ordersCreateSQL)
			if err != nil {
				t.Fatal(err)
			}
			w, err := baseline.NewWriter(filepath.Join(snapDir, schema, "orders.parquet"), cols, baseline.WriterConfig{
				Compression: "none", RowGroupSize: 100,
				Metadata: map[string]string{
					baseline.MetaKeyCreateTableSQL:    ordersCreateSQL,
					baseline.MetaKeyBinlogFile:        "binlog.000001",
					baseline.MetaKeyBinlogPos:         "4",
					baseline.MetaKeySnapshotTimestamp: t0.Format(time.RFC3339),
					baseline.MetaKeySnapshotProducer:  baseline.ProducerDump,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := w.WriteRow([]string{"1", "new"}, []bool{false, false}); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if err := baseline.WriteSuccessMarker(snapDir); err != nil {
				t.Fatal(err)
			}

			ts := func(d time.Duration) string { return base.Add(d).Format("2006-01-02 15:04:05") }
			testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts(10*time.Second), nil, schema, "orders", 2, "1",
				nil, nil, []byte(`{"id":1,"status":"A"}`))
			testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts(40*time.Second), nil, schema, "orders", 2, "1",
				nil, nil, []byte(`{"id":1,"status":"B"}`))

			run := func(at time.Time) {
				t.Helper()
				if _, err := reconstruct.ReconstructTables(ctx, reconstruct.FullTableConfig{
					IndexDSN: dsn, BaselineSrc: root, Tables: []string{schema + ".orders"},
					At: at, OutputDir: root, OutputFormat: reconstruct.OutputFormatParquet, TableDeltas: deltas,
				}); err != nil {
					t.Fatalf("ReconstructTables(at=%s): %v", at.Format(time.RFC3339), err)
				}
			}
			// The newest file that describes the table after each run: the
			// newest pair with deltas on, the table file otherwise.
			describing := func(at time.Time) baseline.DumpMetadata {
				t.Helper()
				path := filepath.Join(root, strings.ReplaceAll(at.Format(time.RFC3339), ":", "-"), schema, "orders.parquet")
				if deltas {
					chain, err := baseline.ListTableDelta(ctx, path)
					if err != nil || chain == nil {
						t.Fatalf("no chain beside %s: %v", path, err)
					}
					path = chain.LastFileUpserts()
				}
				md, err := baseline.ReadParquetMetadata(path)
				if err != nil {
					t.Fatal(err)
				}
				return md
			}
			for i, at := range []time.Time{cut1, cut2} {
				run(at)
				md := describing(at)
				if !md.LastDumpAt.Equal(t0) || md.FoldGeneration != i+1 {
					t.Fatalf("after run %d: stamped {%s, %d folds}, want {%s, %d folds}",
						i+1, md.LastDumpAt.Format(time.RFC3339), md.FoldGeneration, t0.Format(time.RFC3339), i+1)
				}
			}
		})
	}
}
