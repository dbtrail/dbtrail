//go:build integration

package reconstruct_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestRefresh_everyFoldedTableSharesOneCut pins #1635: one fold anchors every
// table it folds at the SAME binlog coordinate, the one ResolveSnapshotCut
// returns for the run's target time. That is what "all tables at one position"
// means, and today it holds only because the cut is resolved once, before the
// table loop.
//
// Three guards, because one alone cannot see every way to break it:
//
//   - The resolver is counted: two tables, one call. Re-resolving per table (or
//     again after a fetch) against this quiet index returns the same
//     coordinate, so only the count sees it. In production capture keeps
//     writing during a refresh "to now", and a second resolution lands later.
//   - The footers equal the cut for the target time, binlog.000001:300, the
//     START of the third event rather than the end of the index. A cut taken
//     from the wall clock instead of the target lands elsewhere: the fixture
//     sits an hour in the future, so the clock is before every event and the
//     cut lands at the first one (100), at any time of day.
//   - Table a holds a fourth event that EXECUTED before the target time but
//     was written after the cut. Event timestamps are execution time, so only
//     the positional bound keeps it out; without it a holds id 4.
func TestRefresh_everyFoldedTableSharesOneCut(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	const schema = "shop"

	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	// An hour ahead: were base the current hour, a wall-clock cut taken 20 to
	// 40 seconds past it would also land at 300 and the clock mutation would
	// pass. The index's hourly partitions already cover the next hour.
	base := time.Now().UTC().Truncate(time.Hour).Add(time.Hour)
	at := base.Add(30 * time.Second)
	ts := func(d time.Duration) string { return base.Add(d).Format("2006-01-02 15:04:05") }

	root := t.TempDir()
	snapDir := filepath.Join(root, strings.ReplaceAll(base.Format(time.RFC3339), ":", "-"))
	const createFmt = "CREATE TABLE `%s` (\n  `id` int NOT NULL,\n  `v` varchar(16) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	for _, table := range []string{"a", "b"} {
		testutil.InsertSnapshot(t, db, 1, ts(0), schema, table, "id", 1, "PRI", "int", "NO")
		testutil.InsertSnapshot(t, db, 1, ts(0), schema, table, "v", 2, "", "varchar", "YES")
		createSQL := fmt.Sprintf(createFmt, table)
		cols, err := baseline.ParseSchemaText(createSQL)
		if err != nil {
			t.Fatalf("ParseSchemaText: %v", err)
		}
		w, err := baseline.NewWriter(filepath.Join(snapDir, schema, table+".parquet"), cols, baseline.WriterConfig{
			Compression:  "none",
			RowGroupSize: 100,
			Metadata: map[string]string{
				baseline.MetaKeyCreateTableSQL:    createSQL,
				baseline.MetaKeyBinlogFile:        "binlog.000001",
				baseline.MetaKeyBinlogPos:         "4",
				baseline.MetaKeySnapshotTimestamp: base.Format(time.RFC3339),
			},
		})
		if err != nil {
			t.Fatalf("baseline.NewWriter: %v", err)
		}
		if err := w.WriteRow([]string{"1", "seed"}, []bool{false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatalf("WriteSuccessMarker: %v", err)
	}

	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts(10*time.Second), nil,
		schema, "a", 1, "2", nil, nil, []byte(`{"id":2,"v":"first"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts(20*time.Second), nil,
		schema, "b", 1, "2", nil, nil, []byte(`{"id":2,"v":"second"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, ts(40*time.Second), nil,
		schema, "a", 1, "3", nil, nil, []byte(`{"id":3,"v":"third"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 400, 500, ts(25*time.Second), nil,
		schema, "a", 1, "4", nil, nil, []byte(`{"id":4,"v":"skew"}`))

	cut, err := reconstruct.ResolveSnapshotCut(ctx, db, at)
	if err != nil {
		t.Fatalf("ResolveSnapshotCut: %v", err)
	}
	// Fixture sanity: the cut sits between the batches, so neither a cut at the
	// first event (a wall-clock read, see above) nor one at the end of the index
	// could equal it and pass the footer checks below.
	if cut == nil || cut.File != "binlog.000001" || cut.Pos != 300 {
		t.Fatalf("cut for the target time = %+v, want binlog.000001:300 (the start of the third event)", cut)
	}

	var calls atomic.Int32
	t.Cleanup(reconstruct.CountSnapshotCutsForTest(&calls))
	if _, err := reconstruct.ReconstructTables(ctx, reconstruct.FullTableConfig{
		IndexDSN:     testutil.BaseDSN() + "/" + dbName,
		BaselineSrc:  root,
		Tables:       []string{schema + ".a", schema + ".b"},
		At:           at,
		OutputDir:    root,
		OutputFormat: reconstruct.OutputFormatParquet,
		Parallelism:  2,
	}); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("the fold resolved the snapshot cut %d times for two tables, want once", n)
	}

	published := map[string]string{}
	for _, table := range []string{"a", "b"} {
		path, _, _, err := reconstruct.FindBaseline(ctx, root, schema, table, at.Add(time.Second))
		if err != nil {
			t.Fatalf("FindBaseline %s: %v", table, err)
		}
		if strings.HasPrefix(path, snapDir+string(filepath.Separator)) {
			t.Fatalf("table %s was not published in a new snapshot: %s", table, path)
		}
		published[table] = path
		meta, err := baseline.ReadParquetMetadata(path)
		if err != nil {
			t.Fatalf("ReadParquetMetadata %s: %v", table, err)
		}
		if meta.BinlogFile != cut.File || meta.BinlogPos != int64(cut.Pos) {
			t.Errorf("table %s is anchored at %s:%d, want the run's one cut %s:%d",
				table, meta.BinlogFile, meta.BinlogPos, cut.File, cut.Pos)
		}
	}
	if da, dbDir := filepath.Dir(filepath.Dir(published["a"])), filepath.Dir(filepath.Dir(published["b"])); da != dbDir {
		t.Errorf("the two tables were published into different snapshots: %s and %s", da, dbDir)
	}

	if got, want := readIDs(t, published["a"]), []string{"1", "2"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("table a holds ids %v, want %v (id 3 is past the target time, id 4 past the cut)", got, want)
	}
	if got, want := readIDs(t, published["b"]), []string{"1", "2"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("table b holds ids %v, want %v", got, want)
	}
}

// readIDs reads the id column of a published table, sorted.
func readIDs(t *testing.T, path string) []string {
	t.Helper()
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer ddb.Close()
	rows, err := ddb.Query(fmt.Sprintf("SELECT CAST(id AS VARCHAR) FROM parquet_scan('%s')", path))
	if err != nil {
		t.Fatalf("parquet_scan %s: %v", path, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	sort.Strings(out)
	return out
}
