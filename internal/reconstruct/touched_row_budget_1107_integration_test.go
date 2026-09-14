//go:build integration

package reconstruct_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestRefresh_touchedRowBudget pins #1107's first slice: a fold refuses a
// table whose window changes more DISTINCT rows than MaxTouchedRows allows,
// divided by the tables folding at once, and publishes nothing. Rows changed
// many times count once, and a count at the limit folds.
func TestRefresh_touchedRowBudget(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	const schema = "shop"

	// run seeds a baseline with tables a and b, applies events (table -> pk
	// values, one event each), and folds with the given budget.
	run := func(t *testing.T, events map[string][]string, tables []string, budget int64, parallelism int) (published bool, failures []reconstruct.TableFailure, err error) {
		t.Helper()
		db, dbName := testutil.CreateTestDB(t)
		if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
			t.Fatalf("CreateIndexTables: %v", err)
		}
		if err := indexer.EnsureSchema(db); err != nil {
			t.Fatalf("EnsureSchema: %v", err)
		}
		base := time.Now().UTC().Truncate(time.Hour)
		at := base.Add(50 * time.Minute)
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
				Compression: "none", RowGroupSize: 100,
				Metadata: map[string]string{
					baseline.MetaKeyCreateTableSQL: createSQL,
					baseline.MetaKeyBinlogFile:     "binlog.000001",
					baseline.MetaKeyBinlogPos:      "4",
				},
			})
			if err != nil {
				t.Fatalf("NewWriter: %v", err)
			}
			if err := w.WriteRow([]string{"1", "seed"}, []bool{false, false}); err != nil {
				t.Fatalf("WriteRow: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		}
		if err := baseline.WriteSuccessMarker(snapDir); err != nil {
			t.Fatalf("WriteSuccessMarker: %v", err)
		}
		pos, sec := uint64(100), time.Duration(0)
		for _, table := range []string{"a", "b"} {
			for _, pk := range events[table] {
				sec += time.Second
				testutil.InsertEvent(t, db, "binlog.000001", pos, pos+100, ts(sec), nil,
					schema, table, 1, pk, nil, nil, []byte(`{"id":`+pk+`,"v":"x"}`))
				pos += 100
			}
		}
		var want []string
		for _, table := range tables {
			want = append(want, schema+"."+table)
		}
		_, failures, err = reconstruct.ReconstructTablesDetailed(ctx, reconstruct.FullTableConfig{
			IndexDSN:       testutil.BaseDSN() + "/" + dbName,
			BaselineSrc:    root,
			Tables:         want,
			At:             at,
			OutputDir:      root,
			OutputFormat:   reconstruct.OutputFormatParquet,
			Parallelism:    parallelism,
			MaxTouchedRows: budget,
		})
		path, _, _, ferr := reconstruct.FindBaseline(ctx, root, schema, tables[0], at.Add(time.Second))
		if ferr != nil {
			t.Fatalf("FindBaseline: %v", ferr)
		}
		return !strings.HasPrefix(path, snapDir+string(filepath.Separator)), failures, err
	}
	refusedByBudget := func(failures []reconstruct.TableFailure, table string) bool {
		for _, f := range failures {
			if f.Table == table && errors.Is(f.Err, reconstruct.ErrTouchedRowBudget) {
				return true
			}
		}
		return false
	}

	t.Run("at the limit folds", func(t *testing.T) {
		published, failures, err := run(t, map[string][]string{"a": {"2", "3", "4"}}, []string{"a"}, 3, 1)
		if err != nil || !published {
			t.Fatalf("published=%v err=%v failures=%+v", published, err, failures)
		}
	})
	t.Run("past the limit refuses and publishes nothing", func(t *testing.T) {
		published, failures, err := run(t, map[string][]string{"a": {"2", "3", "4"}}, []string{"a"}, 2, 1)
		if err == nil || published || !refusedByBudget(failures, "a") {
			t.Fatalf("published=%v err=%v failures=%+v; want the budget refusal and no snapshot", published, err, failures)
		}
		if msg := failures[0].Err.Error(); failures[0].Table != "a" || !strings.Contains(msg, "more than 2 distinct rows") || strings.Contains(msg, "shop.a") {
			t.Errorf("refusal for table %q does not state the limit once, unprefixed: %q", failures[0].Table, msg)
		}
		if !strings.Contains(err.Error(), "shop.a: too many changed rows to build this from the recorded changes: more than 2 distinct rows") {
			t.Errorf("the run's error does not name the table once: %v", err)
		}
	})
	t.Run("a row changed many times counts once", func(t *testing.T) {
		published, failures, err := run(t, map[string][]string{"a": {"2", "2", "2", "2", "2"}}, []string{"a"}, 1, 1)
		if err != nil || !published {
			t.Fatalf("published=%v err=%v failures=%+v", published, err, failures)
		}
	})
	t.Run("the limit is shared by the tables folding at once", func(t *testing.T) {
		// 4 for the run, two tables at once: 2 per table, and a changes 3.
		published, failures, err := run(t, map[string][]string{"a": {"2", "3", "4"}, "b": {"2"}}, []string{"a", "b"}, 4, 2)
		if err == nil || published || !refusedByBudget(failures, "a") {
			t.Fatalf("published=%v err=%v failures=%+v; want a refused at 2 per table", published, err, failures)
		}
	})
}
