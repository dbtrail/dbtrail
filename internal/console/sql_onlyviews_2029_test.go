package console

import (
	"context"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #2029: which views a statement gets. When in doubt, every view (nil): an
// extra view costs half a millisecond, a missing one turns a working query
// into "does not exist" (and loses DuckDB's "Did you mean").
func TestSQLWantedViews_2029(t *testing.T) {
	in := views.Input{
		ArchiveSources: []string{"/a/bintrail_id=x"},
		Baselines: []views.BaselineTable{
			{Schema: "shop", Table: "orders", Path: "/b/shop/orders.parquet"},
			{Schema: "shop", Table: "items", Path: "/b/shop/items.parquet"},
			{Schema: "Crm", Table: "Orders", Path: "/b/Crm/Orders.parquet"},
		},
	}
	ref := func(cat, schema, name string) sqlsandbox.TableRef {
		return sqlsandbox.TableRef{Catalog: cat, Schema: schema, Name: name}
	}
	set := func(keys ...string) views.ViewSet {
		s := views.ViewSet{}
		for _, k := range keys {
			s[k] = true
		}
		return s
	}
	cases := []struct {
		name string
		refs sqlsandbox.Refs
		want views.ViewSet
	}{
		{"SELECT 1 needs none", sqlsandbox.Refs{}, set()},
		{"qualified", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("", "shop", "orders")}}, set("shop.orders")},
		{"qualified, other case", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("", "SHOP", "Orders")}}, set("shop.orders")},
		{"memory catalog", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("memory", "shop", "items")}}, set("shop.items")},
		{"unqualified matches every schema", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("", "", "orders")}}, set("shop.orders", "crm.orders")},
		{"events bare", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("", "", "events")}}, set("events")},
		{"main.events", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("", "main", "events")}}, set("events")},
		{"memory.main.events", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("memory", "main", "events")}}, set("events")},
		{"join", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("", "shop", "orders"), ref("", "shop", "items")}}, set("shop.orders", "shop.items")},
		{"a CTE name is not a view", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("", "", "q"), ref("", "shop", "orders")}, CTEs: []string{"q"}}, set("shop.orders")},
		// A WITH that shadows a view reads the view inside its own body.
		{"a CTE shadowing a view keeps the view", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("", "", "events"), ref("", "", "events")}, CTEs: []string{"events"}}, set("events")},
		{"unsure", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("", "shop", "orders")}, Unsure: true}, nil},
		{"system table", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("", "information_schema", "tables")}}, nil},
		{"typo keeps every view for Did you mean", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("", "shop", "ordrs")}}, nil},
		{"right name, wrong schema", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("", "crm", "items")}}, nil},
		{"another catalog", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("other", "shop", "orders")}}, nil},
		{"a qualified CTE-looking name is not a CTE", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("", "x", "q")}, CTEs: []string{"q"}}, nil},
	}
	for _, c := range cases {
		if got := sqlWantedViews(in, c.refs); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

// The route hands the worker's names to sqlWantedViews: a statement naming
// nothing gets a script that defines no view, and one the worker is unsure
// of gets every view.
func TestSQL_scriptFollowsWhatTheStatementNames_2029(t *testing.T) {
	runner := &fakeSQLRunner{res: sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "n", Type: "INTEGER"}}, Rows: [][]any{}}}
	f := newSQLFixture(t, runner, false)

	runner.refs = &sqlsandbox.Refs{}
	if rec := postSQL(t, f.s, `{"sql":"SELECT 1 AS n"}`); rec.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", rec.Code, rec.Body.String())
	}
	if c := strings.Count(runner.last(t).ViewsSQL, "CREATE OR REPLACE VIEW"); c != 0 {
		t.Errorf("a statement naming nothing got %d views:\n%s", c, runner.last(t).ViewsSQL)
	}

	runner.refs = &sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{{Schema: "shop", Name: "orders"}}}
	postSQL(t, f.s, `{"sql":"SELECT * FROM shop.orders"}`)
	if c := strings.Count(runner.last(t).ViewsSQL, "CREATE OR REPLACE VIEW"); c != 1 {
		t.Errorf("a statement naming shop.orders got %d views, want 1", c)
	}

	runner.refs = nil // Unsure
	postSQL(t, f.s, `{"sql":"SHOW TABLES"}`)
	if c := strings.Count(runner.last(t).ViewsSQL, "CREATE OR REPLACE VIEW"); c != 1 {
		t.Errorf("an unsure statement got %d views, want every view (1)", c)
	}
}

// With the real worker: a statement naming nothing runs over no view, a
// catalog listing still lists every table, and the port's USE still applies
// when the statement names no view of that schema.
func TestSQL_realWorkerNarrowedViews_2029(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	f := newSQLFixture(t, runner, false)

	if w := f.post(t, `{"sql":"SELECT 41 + 1 AS n"}`); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"rows":[[42]]`) {
		t.Errorf("SELECT with no table: code=%d body=%s", w.Code, w.Body.String())
	}
	if w := f.post(t, `{"sql":"SHOW ALL TABLES"}`); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"orders"`) {
		t.Errorf("SHOW ALL TABLES: code=%d body=%s", w.Code, w.Body.String())
	}
	if w := f.post(t, `{"sql":"SELECT * FROM shop.ordrs"}`); w.Code != http.StatusUnprocessableEntity || !strings.Contains(w.Body.String(), "orders") {
		t.Errorf("a typo should still be pointed at the real view: code=%d body=%s", w.Code, w.Body.String())
	}

	out, err := f.s.runSQL(context.Background(), f.s.cm.boot, "u", "SELECT current_schema() AS s", "shop", 0)
	if err != nil {
		t.Fatalf("USE shop; SELECT current_schema(): %v", err)
	}
	if got := out.Result.Rows; len(got) != 1 || got[0][0] != "shop" {
		t.Errorf("current_schema() under USE shop = %v, want shop", got)
	}
}
