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

// #2848 through the real worker: a caller that asked for MySQL's answer
// (read routing) is refused a statement that does arithmetic on an UNSIGNED
// whole-number column, from the snapshot's table definition, and answered
// one that reads the column without arithmetic.
func TestSQL_realWorkerUnsignedColumn_2848(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	f := newSQLFixture(t, runner, false)
	writeSQLStarTable(t, f.root, "un", "CREATE TABLE `un` (\n  `id` int NOT NULL,\n  `b` bigint DEFAULT NULL,\n"+
		"  `u` bigint unsigned DEFAULT NULL,\n  `ui` int unsigned DEFAULT NULL,\n  `dcu` decimal(10,2) unsigned DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n",
		[]string{"1", "-4", "4", "3", "3.00"})
	writeSQLStarTable(t, f.root, "plain", "CREATE TABLE `plain` (\n  `id` int NOT NULL,\n  `ui` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n", []string{"1", "3"})
	ctx := context.Background()
	strict := func(stmt string) sqlsandbox.Session {
		return sqlsandbox.Session{StrictStar: true, Types: readrouter.ShapeOf(stmt)}
	}
	for _, c := range []struct {
		stmt    string
		refused string // what the refusal names, or "" when the copy answers
	}{
		{"SELECT b - u FROM shop.un", "u"},
		{"SELECT ui - 4 FROM shop.un", "ui"},
		{"SELECT -x.u FROM shop.un x", "u"},
		{"SELECT u, ui FROM shop.un WHERE u > -1 AND ui BETWEEN -5 AND 5", ""},
		{"SELECT b - 1, dcu - 10 FROM shop.un", ""},
		{"SELECT SUM(ui) - 100 FROM shop.un", ""},
		// The same name, signed on this table: nothing to keep back.
		{"SELECT ui - 4 FROM shop.plain", ""},
	} {
		_, err := f.s.runSQL(ctx, f.s.cm.boot, "u", c.stmt, "", 0, strict(c.stmt))
		var refusal *sqlStarRefusal
		switch {
		case c.refused == "" && err != nil:
			t.Errorf("%s: %v, want the copy's answer", c.stmt, err)
		case c.refused != "" && !errors.As(err, &refusal):
			t.Errorf("%s: err = %v, want a refusal", c.stmt, err)
		case c.refused != "" && (!strings.Contains(refusal.Message, c.refused) || !strings.Contains(refusal.Message, "UNSIGNED whole-number column of shop.un")):
			t.Errorf("%s: refusal %q does not say %q and the table", c.stmt, refusal.Message, c.refused)
		}
	}

	// No shape handed over: refused over a table with an UNSIGNED column,
	// answered over one with none.
	noShape := sqlsandbox.Session{StrictStar: true}
	if _, err := f.s.runSQL(ctx, f.s.cm.boot, "u", "SELECT id FROM shop.un", "", 0, noShape); err == nil {
		t.Error("no shape over a table with an UNSIGNED column: answered, want a refusal")
	}
	if _, err := f.s.runSQL(ctx, f.s.cm.boot, "u", "SELECT id FROM shop.plain", "", 0, noShape); err != nil {
		t.Errorf("no shape over a table with no UNSIGNED column: %v", err)
	}

	// Through the port it is a decision the routing ladder forwards, not a
	// fault.
	stmt := "SELECT b - u FROM shop.un"
	_, err = (&SQLOnCopy{s: f.s, b: f.s.cm.boot, user: "server:x"}).Run(ctx, stmt, "", strict(stmt))
	var differ *sqlsandbox.ColumnsDifferError
	if !errors.As(err, &differ) || !strings.Contains(differ.Reason, "shop.un") || !strings.Contains(differ.Reason, "UNSIGNED") {
		t.Errorf("port: err = %v (%T), want a ColumnsDifferError naming the table and UNSIGNED", err, err)
	}
}

// The rule asks about every table's UNSIGNED columns in one question, with
// the star the parse saw, and asks nothing when no table has one.
func TestSQLUnsignedRefusalFor_2848(t *testing.T) {
	table := func(name string, unsigned ...string) views.BaselineTable {
		return views.BaselineTable{Schema: "shop", Table: name, Path: "/c/shop/" + name + ".parquet", SchemaKnown: true, Columns: []string{"id"}, Unsigned: unsigned}
	}
	in := views.Input{Baselines: []views.BaselineTable{table("a", "u"), table("b"), table("c", "v", "w")}}
	asked := &unsignedAsked{}
	if got := sqlUnsignedRefusalFor(in, sqlsandbox.Refs{StarTables: []sqlsandbox.TableRef{{Name: "a"}}}, asked); got != "" {
		t.Errorf("a reading that vetoes nothing: %q", got)
	}
	if asked.calls != 1 || strings.Join(asked.unsigned, ",") != "u,v,w" || !asked.star {
		t.Errorf("asked %d times about %q (star %v), want once about u,v,w with the star", asked.calls, asked.unsigned, asked.star)
	}
	asked.why = "x"
	if got := sqlUnsignedRefusalFor(in, sqlsandbox.Refs{}, asked); !strings.Contains(got, "shop.a, shop.c") || !strings.Contains(got, "(x)") {
		t.Errorf("refusal %q, want the tables and the reason", got)
	}
	none := views.Input{Baselines: []views.BaselineTable{table("b")}}
	asked = &unsignedAsked{why: "x"}
	if got := sqlUnsignedRefusalFor(none, sqlsandbox.Refs{}, asked); got != "" || asked.calls != 0 {
		t.Errorf("no UNSIGNED column: %q after %d questions, want none asked", got, asked.calls)
	}
	if got := sqlUnsignedRefusalFor(in, sqlsandbox.Refs{}, nil); got == "" {
		t.Error("no reading of the statement over an UNSIGNED column: answered, want a refusal")
	}
}

// unsignedAsked records what UnsignedVeto is asked; every other question
// vetoes nothing.
type unsignedAsked struct {
	askedTypes
	calls    int
	unsigned []string
	star     bool
	why      string
}

func (a *unsignedAsked) UnsignedVeto(unsigned []string, star bool) string {
	a.calls++
	a.unsigned, a.star = unsigned, star
	return a.why
}
