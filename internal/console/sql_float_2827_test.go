package console

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #2827 through the real worker: the copy finds 3.14 in a FLOAT column where
// MySQL finds no row (MySQL compares the column as the double
// 3.140000104904175). A caller that asked for MySQL's answer (read routing)
// is refused a statement that reads a FLOAT column, from the snapshot's table
// definition, and answered one over the same table that does not.
func TestSQL_realWorkerFloatColumn_2827(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	f := newSQLFixture(t, runner, false)
	writeSQLStarTable(t, f.root, "fl", "CREATE TABLE `fl` (\n  `id` int NOT NULL,\n  `f` float DEFAULT NULL,\n"+
		"  `f72` float(7,2) DEFAULT NULL,\n  `d` double DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n", []string{"1", "3.14", "3.14", "3.14"})
	writeSQLStarTable(t, f.root, "plain", "CREATE TABLE `plain` (\n  `id` int NOT NULL,\n  `f` double DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n", []string{"1", "3.14"})
	ctx := context.Background()
	strict := func(stmt string) sqlsandbox.Session {
		return sqlsandbox.Session{StrictStar: true, Types: readrouter.ShapeOf(stmt)}
	}

	// Not asked for MySQL's answer, the copy gives its own: the row.
	out, err := f.s.runSQL(ctx, f.s.cm.boot, "u", "SELECT id FROM shop.fl WHERE f = 3.14", "", 0, sqlsandbox.Session{})
	if err != nil || fmt.Sprint(out.Result.Rows) != "[[1]]" {
		t.Fatalf("the copy, not asked for MySQL's answer: rows %v, err %v; want the row 1 (MySQL finds none)", out.Result.Rows, err)
	}

	for _, c := range []struct {
		stmt    string
		refused string // what the refusal names, or "" when the copy answers
	}{
		{"SELECT id FROM shop.fl WHERE f = 3.14", "f"},
		{"SELECT id FROM shop.fl WHERE `f72` = 3.14", "f72"},
		{"SELECT MIN(x.f * 2) FROM shop.fl x", "f"},
		{"SELECT id FROM shop.fl ORDER BY f", "f"},
		{"SELECT * FROM shop.fl", "star"},
		{"SELECT p.id FROM shop.plain p JOIN shop.fl x ON x.id = p.id", ""},
		{"SELECT id, d FROM shop.fl WHERE d > 3", ""},
		{"SELECT COUNT(*) FROM shop.fl", ""},
		// The same name, a DOUBLE on this table: nothing to keep back.
		{"SELECT id FROM shop.plain WHERE f = 3.14", ""},
	} {
		_, err := f.s.runSQL(ctx, f.s.cm.boot, "u", c.stmt, "", 0, strict(c.stmt))
		var refusal *sqlStarRefusal
		switch {
		case c.refused == "" && err != nil:
			t.Errorf("%s: %v, want the copy's answer", c.stmt, err)
		case c.refused != "" && !errors.As(err, &refusal):
			t.Errorf("%s: err = %v, want a refusal", c.stmt, err)
		case c.refused != "" && (!strings.Contains(refusal.Message, c.refused) || !strings.Contains(refusal.Message, "FLOAT column of shop.fl")):
			t.Errorf("%s: refusal %q does not say %q and the table", c.stmt, refusal.Message, c.refused)
		}
	}

	// No shape handed over: refused over a table with a FLOAT column,
	// answered over one with none.
	noShape := sqlsandbox.Session{StrictStar: true}
	if _, err := f.s.runSQL(ctx, f.s.cm.boot, "u", "SELECT id FROM shop.fl", "", 0, noShape); err == nil {
		t.Error("no shape over a table with a FLOAT column: answered, want a refusal")
	}
	if _, err := f.s.runSQL(ctx, f.s.cm.boot, "u", "SELECT id FROM shop.plain", "", 0, noShape); err != nil {
		t.Errorf("no shape over a table with no FLOAT column: %v", err)
	}

	// Through the port it is a decision the routing ladder forwards, not a
	// fault.
	stmt := "SELECT id FROM shop.fl WHERE f = 3.14"
	_, err = (&SQLOnCopy{s: f.s, b: f.s.cm.boot, user: "server:x"}).Run(ctx, stmt, "", strict(stmt))
	var differ *sqlsandbox.ColumnsDifferError
	if !errors.As(err, &differ) || !strings.Contains(differ.Reason, "shop.fl") || !strings.Contains(differ.Reason, "FLOAT") {
		t.Errorf("port: err = %v (%T), want a ColumnsDifferError naming the table and FLOAT", err, err)
	}
}

// The rule asks about every table's FLOAT columns in one question, with the
// star the parse saw, and asks nothing when no table has one.
func TestSQLFloatRefusalFor_2827(t *testing.T) {
	table := func(name string, floats ...string) views.BaselineTable {
		return views.BaselineTable{Schema: "shop", Table: name, Path: "/c/shop/" + name + ".parquet", SchemaKnown: true, Columns: []string{"id"}, Float: floats}
	}
	in := views.Input{Baselines: []views.BaselineTable{table("a", "f"), table("b"), table("c", "g", "h")}}
	asked := &floatAsked{}
	if got := sqlFloatRefusalFor(in, sqlsandbox.Refs{StarTables: []sqlsandbox.TableRef{{Name: "a"}}}, asked); got != "" {
		t.Errorf("a reading that vetoes nothing: %q", got)
	}
	if asked.calls != 1 || strings.Join(asked.floats, ",") != "f,g,h" || !asked.star {
		t.Errorf("asked %d times about %q (star %v), want once about f,g,h with the star", asked.calls, asked.floats, asked.star)
	}
	asked.why = "x"
	if got := sqlFloatRefusalFor(in, sqlsandbox.Refs{}, asked); !strings.Contains(got, "shop.a, shop.c") || !strings.Contains(got, "(x)") {
		t.Errorf("refusal %q, want the tables and the reason", got)
	}
	none := views.Input{Baselines: []views.BaselineTable{table("b")}}
	asked = &floatAsked{why: "x"}
	if got := sqlFloatRefusalFor(none, sqlsandbox.Refs{}, asked); got != "" || asked.calls != 0 {
		t.Errorf("no FLOAT column: %q after %d questions, want none asked", got, asked.calls)
	}
	if got := sqlFloatRefusalFor(in, sqlsandbox.Refs{}, nil); got == "" {
		t.Error("no reading of the statement over a FLOAT column: answered, want a refusal")
	}
}

// floatAsked records what FloatVeto is asked; every other question vetoes
// nothing.
type floatAsked struct {
	askedTypes
	calls  int
	floats []string
	star   bool
	why    string
}

func (a *floatAsked) FloatVeto(floats []string, star bool) string {
	a.calls++
	a.floats, a.star = floats, star
	return a.why
}
