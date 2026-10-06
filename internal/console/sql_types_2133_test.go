package console

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #2133 through the real worker: the copy answers arithmetic on a DATE
// column with a date where MySQL answers a number, with no error. A caller
// that asked for MySQL's answer (read routing) is refused instead, from the
// column types in the snapshot's table definition.
func TestSQL_realWorkerDateColumnArithmetic_2133(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	f := newSQLFixture(t, runner, false)
	writeSQLStarTable(t, f.root, "ev", "CREATE TABLE `ev` (\n  `id` int NOT NULL,\n  `created_on` date DEFAULT NULL,\n"+
		"  `tm` time DEFAULT NULL,\n  `n` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n", []string{"1", "2026-01-01", "10:00:00", "5"})
	writeSQLStarTable(t, f.root, "plain", "CREATE TABLE `plain` (\n  `id` int NOT NULL,\n  `created_on` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n", []string{"1", "7"})
	ctx := context.Background()
	strict := func(stmt string) sqlsandbox.Session {
		return sqlsandbox.Session{StrictStar: true, Types: readrouter.ShapeOf(stmt)}
	}

	// Not asked for MySQL's answer, the copy gives its own: a date.
	out, err := f.s.runSQL(ctx, f.s.cm.boot, "u", "SELECT created_on + 1 FROM shop.ev", "", 0, sqlsandbox.Session{})
	if err != nil || !strings.Contains(fmt.Sprintf("%v", out.Result.Rows), "2026-01-02") {
		t.Fatalf("the copy, not asked for MySQL's answer: rows %v, err %v; want the date 2026-01-02 (MySQL answers 20260102)", out.Result.Rows, err)
	}

	for _, c := range []struct {
		stmt    string
		refused string // what the refusal says, or "" when the copy answers
	}{
		{"SELECT created_on + 1 FROM shop.ev", "created_on"},
		{"SELECT e.created_on - 1 FROM shop.ev e", "created_on"},
		{"SELECT AVG(created_on) FROM shop.ev", "created_on"},
		{"SELECT d + 1 FROM (SELECT created_on AS d FROM shop.ev) x", "subquery"},
		{"SELECT n FROM shop.ev WHERE tm >= '9:00:00'", "tm"},
		{"SELECT p.id FROM shop.plain p JOIN shop.ev e ON e.id = p.id WHERE e.created_on + 0 > 5", "shop.ev"},
		{"SELECT n + 1, created_on FROM shop.ev", ""},
		{"SELECT id FROM shop.ev WHERE created_on >= '2026-01-01'", ""},
		{"SELECT created_on + INTERVAL 1 DAY FROM shop.ev", ""},
		// The same name, an integer on this table: nothing to keep back.
		{"SELECT created_on + 1 FROM shop.plain", ""},
	} {
		_, err := f.s.runSQL(ctx, f.s.cm.boot, "u", c.stmt, "", 0, strict(c.stmt))
		var refusal *sqlStarRefusal
		switch {
		case c.refused == "" && err != nil:
			t.Errorf("%s: %v, want the copy's answer", c.stmt, err)
		case c.refused != "" && !errors.As(err, &refusal):
			t.Errorf("%s: err = %v, want a refusal", c.stmt, err)
		case c.refused != "" && (!strings.Contains(refusal.Message, c.refused) || !strings.Contains(refusal.Message, "date, time or year column")):
			t.Errorf("%s: refusal %q does not say %q", c.stmt, refusal.Message, c.refused)
		}
	}

	// A caller that asked for MySQL's answer and handed no shape over is
	// refused over a table with such a column, and answered over one with
	// none: not known is never "nothing to find".
	noShape := sqlsandbox.Session{StrictStar: true}
	if _, err := f.s.runSQL(ctx, f.s.cm.boot, "u", "SELECT id FROM shop.ev", "", 0, noShape); err == nil {
		t.Error("no shape over a table with a date column: answered, want a refusal")
	}
	if _, err := f.s.runSQL(ctx, f.s.cm.boot, "u", "SELECT id FROM shop.plain", "", 0, noShape); err != nil {
		t.Errorf("no shape over a table with no such column: %v", err)
	}

	// Through the port it is the decision the routing ladder forwards under
	// copy_columns_differ, not a fault.
	stmt := "SELECT created_on + 1 FROM shop.ev"
	_, err = (&SQLOnCopy{s: f.s, b: f.s.cm.boot, user: "server:x"}).Run(ctx, stmt, "", strict(stmt))
	var differ *sqlsandbox.ColumnsDifferError
	if !errors.As(err, &differ) || !strings.Contains(differ.Reason, "shop.ev") || !strings.Contains(differ.Reason, "created_on") {
		t.Errorf("port: err = %v (%T), want a ColumnsDifferError naming the table and the column", err, err)
	}
}

// The rule asks every table the statement reads, and stops at the first that
// keeps it back.
func TestSQLTypesRefusalFor(t *testing.T) {
	table := func(name string, cols ...baseline.TemporalColumn) views.BaselineTable {
		return views.BaselineTable{Schema: "shop", Table: name, Path: "/c/shop/" + name + ".parquet", SchemaKnown: true, Columns: []string{"id"}, Temporal: cols}
	}
	in := views.Input{Baselines: []views.BaselineTable{
		table("plain"),
		table("orders", baseline.TemporalColumn{Name: "created_on", Type: "date"}),
		table("shifts", baseline.TemporalColumn{Name: "starts", Type: "time"}, baseline.TemporalColumn{Name: "odd"}),
	}}
	for _, c := range []struct {
		stmt, want string
	}{
		{"SELECT created_on + 1 FROM orders", "shop.orders"},
		{"SELECT starts FROM shifts", "shop.shifts"},
		{"SELECT odd - 1 FROM shifts", "shop.shifts"}, // a type that is not known is treated as a date could be
		{"SELECT odd FROM shifts", ""},
		{"SELECT id + 1 FROM plain", ""},
		{"SELECT created_on FROM orders", ""},
	} {
		got := sqlTypesRefusalFor(in, sqlsandbox.Refs{}, readrouter.ShapeOf(c.stmt))
		if (c.want == "") != (got == "") || !strings.Contains(got, c.want) {
			t.Errorf("%s: refusal %q, want one naming %q", c.stmt, got, c.want)
		}
	}
	// Only the tables the statement reads are asked.
	narrowed := in
	narrowed.OnlyViews = sqlWantedViews(in, sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{{Schema: "shop", Name: "plain"}}})
	if got := sqlTypesRefusalFor(narrowed, sqlsandbox.Refs{}, readrouter.ShapeOf("SELECT created_on + 1 FROM plain")); got != "" {
		t.Errorf("a statement that reads only plain: %q, want none", got)
	}
	if got := sqlStrictRefusalFor(in, in, sqlsandbox.Refs{}, "SELECT created_on + 1 FROM orders", readrouter.ShapeOf("SELECT created_on + 1 FROM orders")); !strings.Contains(got, "shop.orders") {
		t.Errorf("sqlStrictRefusalFor does not ask the types: %q", got)
	}
}

// askedTypes records what the statement's reading is asked.
type askedTypes struct {
	calls        int
	dates, whole []string
	star         bool
}

func (a *askedTypes) ColumnVeto(dates, whole []string, star bool) string {
	a.calls++
	a.dates, a.whole, a.star = dates, whole, star
	return ""
}

// The statement is read once however many tables it reads: every table's
// columns go in one question (a walk that is not certain of the tables asks
// about every table of the copy), with the star the parse saw.
func TestSQLTypesRefusalFor_oneQuestionForEveryTable(t *testing.T) {
	var in views.Input
	for i := 0; i < 40; i++ {
		tb := views.BaselineTable{Schema: "shop", Table: fmt.Sprintf("t%d", i), Path: fmt.Sprintf("/c/shop/t%d.parquet", i), SchemaKnown: true, Columns: []string{"id"}}
		if i%2 == 0 {
			tb.Temporal = []baseline.TemporalColumn{{Name: fmt.Sprintf("d%d", i), Type: "date"}, {Name: fmt.Sprintf("y%d", i), Type: "year"}}
		}
		in.Baselines = append(in.Baselines, tb)
	}
	asked := &askedTypes{}
	if got := sqlTypesRefusalFor(in, sqlsandbox.Refs{StarTables: []sqlsandbox.TableRef{{Name: "t0"}}}, asked); got != "" {
		t.Fatalf("refusal %q, want none", got)
	}
	if asked.calls != 1 || len(asked.dates) != 20 || len(asked.whole) != 20 || !asked.star {
		t.Errorf("asked %d time(s) with %d dates, %d TIME or YEAR columns, star %v; want once, 20, 20, true", asked.calls, len(asked.dates), len(asked.whole), asked.star)
	}
	for _, refs := range []sqlsandbox.Refs{{}, {Star: true}, {StarNamedJoin: true}} {
		asked = &askedTypes{}
		sqlTypesRefusalFor(in, refs, asked)
		if want := refs.Star || refs.StarNamedJoin; asked.star != want {
			t.Errorf("refs %+v: star = %v, want %v", refs, asked.star, want)
		}
	}
	// No table with such a column: nothing is asked.
	asked = &askedTypes{}
	plain := views.Input{Baselines: []views.BaselineTable{{Schema: "shop", Table: "p", Path: "/c/shop/p.parquet", SchemaKnown: true, Columns: []string{"id"}}}}
	if got := sqlTypesRefusalFor(plain, sqlsandbox.Refs{}, asked); got != "" || asked.calls != 0 {
		t.Errorf("no such column: refusal %q after %d question(s), want none and none", got, asked.calls)
	}
}
