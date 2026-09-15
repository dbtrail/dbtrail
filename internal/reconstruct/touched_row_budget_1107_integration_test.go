//go:build integration

package reconstruct_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestRefresh_touchedRowBudget pins #1107 through the real fold. Past
// MaxTouchedRows (divided by the tables folding at once) a table's changes go
// to disk and merge in passes, and the snapshot holds exactly the rows an
// unlimited fold produces. It refuses, and publishes nothing, only when one of
// the on-disk groups alone passes the limit.
func TestRefresh_touchedRowBudget(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	const schema = "shop"

	type outcome struct {
		published bool
		rows      map[string][]string // table -> sorted "id=v"
		failures  []reconstruct.TableFailure
		err       error
	}
	// run seeds a baseline of tables a and b holding ids 1..seedRows, applies
	// events (table -> pk values, one UPDATE-shaped event each, v = "x"+pk),
	// and folds with the given budget.
	run := func(t *testing.T, seedRows int, events map[string][]string, tables []string, budget int64, parallelism int) outcome {
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
			for id := 1; id <= seedRows; id++ {
				if err := w.WriteRow([]string{strconv.Itoa(id), "seed"}, []bool{false, false}); err != nil {
					t.Fatalf("WriteRow: %v", err)
				}
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
					schema, table, 1, pk, nil, nil, []byte(`{"id":`+pk+`,"v":"x`+pk+`"}`))
				pos += 100
			}
		}
		var want []string
		for _, table := range tables {
			want = append(want, schema+"."+table)
		}
		// A temp directory of its own, so a fold that leaves its on-disk changes
		// behind is seen.
		tmp := t.TempDir()
		t.Setenv("TMPDIR", tmp)
		var out outcome
		_, out.failures, out.err = reconstruct.ReconstructTablesDetailed(ctx, reconstruct.FullTableConfig{
			IndexDSN:       testutil.BaseDSN() + "/" + dbName,
			BaselineSrc:    root,
			Tables:         want,
			At:             at,
			OutputDir:      root,
			OutputFormat:   reconstruct.OutputFormatParquet,
			Parallelism:    parallelism,
			MaxTouchedRows: budget,
			// On, so a fold whose changes all went to disk is not mistaken for
			// one with no changes and carried forward unchanged.
			CarryForwardUnchanged: true,
		})
		if left, _ := filepath.Glob(filepath.Join(tmp, "bintrail-fold-*")); len(left) > 0 {
			t.Errorf("the fold left its changes on disk: %v", left)
		}
		path, _, _, ferr := reconstruct.FindBaseline(ctx, root, schema, tables[0], at.Add(time.Second))
		if ferr != nil {
			t.Fatalf("FindBaseline: %v", ferr)
		}
		out.published = !strings.HasPrefix(path, snapDir+string(filepath.Separator))
		if out.published {
			out.rows = map[string][]string{}
			for _, table := range tables {
				p, _, _, err := reconstruct.FindBaseline(ctx, root, schema, table, at.Add(time.Second))
				if err != nil {
					t.Fatalf("FindBaseline %s: %v", table, err)
				}
				out.rows[table] = readIDValues(t, p)
			}
		}
		return out
	}
	refusedByBudget := func(failures []reconstruct.TableFailure, table string) bool {
		for _, f := range failures {
			if f.Table == table && errors.Is(f.Err, reconstruct.ErrTouchedRowBudget) {
				return true
			}
		}
		return false
	}
	pks := func(from, to int) []string {
		var out []string
		for id := from; id <= to; id++ {
			out = append(out, strconv.Itoa(id))
		}
		return out
	}

	t.Run("past the limit merges from disk and publishes what an unlimited fold does", func(t *testing.T) {
		// 40 seeded rows; the window changes 30 of them and inserts 20 more.
		events := map[string][]string{"a": pks(11, 60)}
		limited := run(t, 40, events, []string{"a"}, 8, 1)
		if limited.err != nil || !limited.published {
			t.Fatalf("published=%v err=%v failures=%+v", limited.published, limited.err, limited.failures)
		}
		unlimited := run(t, 40, events, []string{"a"}, 0, 1)
		if !slices.Equal(limited.rows["a"], unlimited.rows["a"]) {
			t.Fatalf("the fold from disk published different rows:\n got %v\nwant %v", limited.rows["a"], unlimited.rows["a"])
		}
		if len(unlimited.rows["a"]) != 60 {
			t.Fatalf("fixture: the unlimited fold published %d rows, want 60", len(unlimited.rows["a"]))
		}
	})
	t.Run("a group past the limit refuses and publishes nothing", func(t *testing.T) {
		// 65 changed rows over 64 groups with a limit of 1: some group holds two.
		out := run(t, 1, map[string][]string{"a": pks(2, 66)}, []string{"a"}, 1, 1)
		if out.err == nil || out.published || !refusedByBudget(out.failures, "a") {
			t.Fatalf("published=%v err=%v failures=%+v; want the budget refusal and no snapshot", out.published, out.err, out.failures)
		}
		if msg := out.failures[0].Err.Error(); out.failures[0].Table != "a" || !strings.Contains(msg, "about 64 or more distinct rows") || strings.Contains(msg, "shop.a") {
			t.Errorf("refusal for table %q does not state the limit once, unprefixed: %q", out.failures[0].Table, msg)
		}
		if !strings.Contains(out.err.Error(), "shop.a: too many changed rows to build this from the recorded changes: about 64 or more") {
			t.Errorf("the run's error does not name the table once: %v", out.err)
		}
	})
	t.Run("a row changed many times counts once", func(t *testing.T) {
		out := run(t, 1, map[string][]string{"a": {"2", "2", "2", "2", "2"}}, []string{"a"}, 1, 1)
		if out.err != nil || !out.published {
			t.Fatalf("published=%v err=%v failures=%+v", out.published, out.err, out.failures)
		}
	})
	t.Run("the limit is shared by the tables folding at once", func(t *testing.T) {
		// 4 for the run, two tables at once: 2 per table, so a's 3 changes go to
		// disk and b's 1 stays in memory; both publish.
		out := run(t, 1, map[string][]string{"a": {"2", "3", "4"}, "b": {"2"}}, []string{"a", "b"}, 4, 2)
		if out.err != nil || !out.published {
			t.Fatalf("published=%v err=%v failures=%+v", out.published, out.err, out.failures)
		}
		if want := []string{"1=seed", "2=x2", "3=x3", "4=x4"}; !slices.Equal(out.rows["a"], want) {
			t.Fatalf("a = %v, want %v", out.rows["a"], want)
		}
	})
}

// readIDValues reads a snapshot table of (id, v) back as sorted "id=v".
func readIDValues(t *testing.T, path string) []string {
	t.Helper()
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	rows, err := ddb.Query(fmt.Sprintf("SELECT id, v FROM parquet_scan('%s')", path))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id int32
		var v sql.NullString
		if err := rows.Scan(&id, &v); err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprintf("%d=%s", id, v.String))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	slices.Sort(out)
	return out
}
