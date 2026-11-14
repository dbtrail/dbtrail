package console

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #2829: on the MySQL port, routed or not, the copy declines a statement that
// may print a FLOAT or DOUBLE column MySQL prints with fixed decimals or zero
// padding (0.1 in a DOUBLE(10,2) is 0.10 there, 0.1 on the copy). The search
// errs toward refusing: a name counts inside a string or quotes too.
func TestSQLFixedPrintRefusalFor_2829(t *testing.T) {
	tables := map[string]views.BaselineTable{
		"m":     {Schema: "shop", Table: "m", Path: "/c/shop/m.parquet", SchemaKnown: true, Columns: []string{"id", "d102", "d"}, FixedPrint: []string{"d102"}},
		"odd":   {Schema: "shop", Table: "odd", Path: "/c/shop/odd.parquet", SchemaKnown: true, Columns: []string{"id", "my col"}, FixedPrint: []string{"my col"}},
		"plain": {Schema: "shop", Table: "plain", Path: "/c/shop/plain.parquet", SchemaKnown: true, Columns: []string{"id", "d"}},
	}
	for _, c := range []struct {
		stmt   string
		refuse bool
	}{
		{"SELECT d102 FROM shop.m", true},
		{"SELECT `D102` FROM shop.m", true},
		{`SELECT "d102" FROM shop.m`, true},
		{"SELECT id FROM shop.m WHERE note = 'd102'", true},
		{"SELECT AVG(x.d102) FROM shop.m x", true},
		{"SELECT * FROM shop.m", true},
		{"SELECT m.* FROM shop.m", true},
		{"SELECT id * 2 FROM shop.m", true},
		{"TABLE shop.m", true},
		{"FROM shop.m", true},
		{"SELECT `my col` FROM shop.odd", true},
		{"WITH c AS (SELECT 1) FROM shop.m", false}, // MySQL has no FROM-first statement: an error there
		{"SELECT id, d FROM shop.m", false},
		{"SELECT COUNT(*) FROM shop.m", false},
		{"SELECT count( * ), MAX(id) FROM shop.m", false},
		{"SELECT d1020, xd102, d102x FROM shop.m", false},
		{"SELECT id FROM shop.odd", false},
		{"SELECT * FROM shop.plain", false},
		{"", true}, // nothing to search: may be anything
	} {
		table := tables["m"]
		for _, name := range []string{"odd", "plain"} {
			if strings.Contains(c.stmt, name) {
				table = tables[name]
			}
		}
		narrowed := views.Input{Baselines: []views.BaselineTable{table}}
		got := sqlFixedPrintRefusalFor(narrowed, c.stmt, false)
		if (got != "") != c.refuse {
			t.Errorf("routed %q: refusal %q, want refused %v", c.stmt, got, c.refuse)
		}
		// Without routing, any statement over such a table: the copy's
		// dialect reaches a column in ways no search tells.
		unrouted := sqlFixedPrintRefusalFor(narrowed, c.stmt, true)
		if want := len(table.FixedPrint) > 0; (unrouted != "") != want {
			t.Errorf("without routing %q: refusal %q, want refused %v", c.stmt, unrouted, want)
		}
	}
}

// Through the real worker: the MySQL port's session (MySQLText), routed or
// not, is refused a DOUBLE(10,2) column, as a decision the routing ladder
// forwards; the SQL card's session gets the copy's answer.
func TestSQL_realWorkerFixedPrintColumn_2829(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	f := newSQLFixture(t, runner, false)
	writeSQLStarTable(t, f.root, "m", "CREATE TABLE `m` (\n  `id` int NOT NULL,\n  `d102` double(10,2) DEFAULT NULL,\n"+
		"  `d` double DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n", []string{"1", "0.1", "0.1"})
	ctx := context.Background()
	const stmt = "SELECT d102 FROM shop.m"

	if _, err := f.s.runSQL(ctx, f.s.cm.boot, "u", stmt, "", 0, sqlsandbox.Session{}); err != nil {
		t.Errorf("the SQL card: %v, want the copy's answer", err)
	}
	for name, sess := range map[string]sqlsandbox.Session{
		"port":   {MySQLText: true},
		"routed": {MySQLText: true, StrictStar: true, Types: readrouter.ShapeOf(stmt)},
	} {
		_, err := (&SQLOnCopy{s: f.s, b: f.s.cm.boot, user: "server:x"}).Run(ctx, stmt, "", sess)
		var differ *sqlsandbox.ColumnsDifferError
		if !errors.As(err, &differ) || !strings.Contains(differ.Reason, "shop.m") || !strings.Contains(differ.Reason, "d102") ||
			!strings.Contains(differ.Reason, "fixed decimals") {
			t.Errorf("%s: err = %v (%T), want a ColumnsDifferError naming d102 and shop.m", name, err, err)
		}
	}
	routedPlain := "SELECT id, d FROM shop.m"
	if _, err := (&SQLOnCopy{s: f.s, b: f.s.cm.boot, user: "server:x"}).Run(ctx, routedPlain, "",
		sqlsandbox.Session{MySQLText: true, StrictStar: true, Types: readrouter.ShapeOf(routedPlain)}); err != nil {
		t.Errorf("routed, a plain DOUBLE: %v, want the copy's answer", err)
	}
}
