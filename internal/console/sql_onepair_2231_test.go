package console

import (
	"database/sql"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #2231, the wiring: the views the console generates for a statement name
// the chain's files one by one, so a chain of one pair is read without the
// aggregate and join that choose the newest version of each key, and a chain
// of two is read with them. The script is the one the worker would run; it
// is run here, so the rows are the state and not just a shape of SQL.
func TestSQLAPI_onePairChainIsReadWithoutTheJoin(t *testing.T) {
	const join = "bintrail_latest"
	f := newSQLFixture(t, &fakeSQLRunner{}, false)
	base := f.schemaDir + "/orders.parquet"
	cols, err := baseline.ParseSchemaText("CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n")
	if err != nil {
		t.Fatal(err)
	}
	// rows are (id, status, op); a tombstone's columns are NULL.
	pair := func(seq int, dead []int64, rows ...[3]string) {
		t.Helper()
		err := baseline.WriteTableDeltaPair(base, seq, cols, nil, dead, func(emit func([]string, []bool) error) error {
			for _, r := range rows {
				tomb := r[2] == baseline.TableDeltaOpDelete
				if err := emit([]string{r[0], r[1], r[0], r[2]}, []bool{tomb, tomb, false, false}); err != nil {
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

	// The base holds 1=new and 2=paid. One pair: 1 changes, 2 is deleted, 3 is
	// new.
	pair(0, []int64{0, 1}, [3]string{"1", "shipped", "u"}, [3]string{"2", "", "d"}, [3]string{"3", "new", "u"})
	script, rows := state()
	if strings.Contains(script, join) {
		t.Errorf("a chain of one pair is read through the join:\n%s", script)
	}
	if want := []string{"1=shipped", "3=new"}; !reflect.DeepEqual(rows, want) {
		t.Errorf("one pair: rows = %v, want %v", rows, want)
	}

	// A second pair: 1 changes again, 3 is deleted.
	pair(1, []int64{0}, [3]string{"1", "returned", "u"}, [3]string{"3", "", "d"})
	script, rows = state()
	if !strings.Contains(script, join) {
		t.Errorf("a chain of two pairs is read without the join:\n%s", script)
	}
	if want := []string{"1=returned"}; !reflect.DeepEqual(rows, want) {
		t.Errorf("two pairs: rows = %v, want %v", rows, want)
	}
}
