package console

import (
	"database/sql"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #2478, the wiring: the views the console generates for a statement read a
// table whose one pair removes no row without the dead-row join, and keep it
// once the pair removes one. The script is the one the worker would run, and
// it is run here, so the rows are the state and not just a shape of SQL.
func TestSQLAPI_pairWithoutDeadRowsIsReadWithoutTheDeadJoin(t *testing.T) {
	const deadJoin = "file_row_number"
	f := newSQLFixture(t, &fakeSQLRunner{}, false)
	base := f.schemaDir + "/orders.parquet"
	cols, err := baseline.ParseSchemaText("CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n")
	if err != nil {
		t.Fatal(err)
	}
	pair := func(dead []int64, rows ...[2]string) {
		t.Helper()
		err := baseline.WriteTableDeltaPair(base, 0, cols, nil, dead, func(emit func([]string, []bool) error) error {
			for _, r := range rows {
				if err := emit([]string{r[0], r[1], r[0], baseline.TableDeltaOpUpsert}, []bool{false, false, false, false}); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	state := func() (script string, rows []string) {
		t.Helper()
		if w := f.post(t, `{"sql":"SELECT * FROM shop.orders"}`); w.Code != http.StatusOK {
			t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
		}
		script = f.runner.jobs[len(f.runner.jobs)-1].ViewsSQL
		ddb, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatal(err)
		}
		defer ddb.Close()
		if _, err := ddb.Exec(script); err != nil {
			t.Fatalf("DuckDB rejected the views: %v\n%s", err, script)
		}
		res, err := ddb.Query(`SELECT id::VARCHAR || '=' || status FROM shop.orders ORDER BY id`)
		if err != nil {
			t.Fatalf("%v\n%s", err, script)
		}
		defer res.Close()
		for res.Next() {
			var s string
			if err := res.Scan(&s); err != nil {
				t.Fatal(err)
			}
			rows = append(rows, s)
		}
		if err := res.Err(); err != nil {
			t.Fatal(err)
		}
		return script, rows
	}

	// The base holds 1=new and 2=paid. A pair that only adds a row.
	pair(nil, [2]string{"3", "new"})
	script, rows := state()
	if strings.Contains(script, deadJoin) {
		t.Errorf("a pair with no dead row is read through the dead-row join:\n%s", script)
	}
	if want := []string{"1=new", "2=paid", "3=new"}; !reflect.DeepEqual(rows, want) {
		t.Errorf("inserts only: rows = %v, want %v", rows, want)
	}

	// The same pair rewritten to remove row 1 (2=paid) and change it.
	pair([]int64{1}, [2]string{"2", "shipped"})
	script, rows = state()
	if !strings.Contains(script, deadJoin) {
		t.Errorf("a pair with a dead row is read without the dead-row join:\n%s", script)
	}
	// Its one dead row is the base's last, half the base or less: the join
	// is confined to it, the base's first row is read as it is.
	if !strings.Contains(script, "file_row_number BETWEEN 1 AND 1") {
		t.Errorf("the dead-row join is not confined to the dead row's stretch:\n%s", script)
	}
	if want := []string{"1=new", "2=shipped"}; !reflect.DeepEqual(rows, want) {
		t.Errorf("a dead row: rows = %v, want %v", rows, want)
	}
}
