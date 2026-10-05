package console

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #2123 through runSQL and the real worker. The copy holds no generated
// column, and a statement that names one does not always fail there: the
// name binds to whatever else answers to it. First what the copy answers
// when nobody asked for MySQL's answer (the measurement the rule rests on),
// then the same statements for a caller that did (read routing).
//
// gen is (id, twice GENERATED AS id*2, a) with the row (1, 2, 3) on MySQL
// and (1, 3) on the copy; g2 is (id, twice, b) with (1, 99, 7).
func TestSQL_realWorkerGeneratedColumnNames_2123(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	f := newSQLFixture(t, runner, false)
	writeSQLStarTable(t, f.root, "gen", "CREATE TABLE `gen` (\n  `id` int NOT NULL,\n  `twice` int GENERATED ALWAYS AS (`id` * 2) STORED,\n"+
		"  `a` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n", []string{"1", "3"})
	writeSQLStarTable(t, f.root, "g2", "CREATE TABLE `g2` (\n  `id` int NOT NULL,\n  `twice` int DEFAULT NULL,\n  `b` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n", []string{"1", "99", "7"})
	// A VIRTUAL column named as a function DuckDB calls without parentheses.
	writeSQLStarTable(t, f.root, "kw", "CREATE TABLE `kw` (\n  `id` int NOT NULL,\n  `user` varchar(8) GENERATED ALWAYS AS (concat('u',`id`)) VIRTUAL,\n"+
		"  `a` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n", []string{"1", "3"})
	// A generated column MySQL's star leaves out and a statement can name.
	writeSQLStarTable(t, f.root, "hid", "CREATE TABLE `hid` (\n  `id` int NOT NULL,\n  `twice` int GENERATED ALWAYS AS (`id` * 2) VIRTUAL /*!80023 INVISIBLE */,\n"+
		"  `a` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n", []string{"1", "3"})
	ctx := context.Background()
	strict := sqlsandbox.Session{StrictStar: true}

	// What the copy answers on its own. MySQL's answer is in the comment;
	// the integration test measures it on real servers.
	for _, c := range []struct {
		stmt string
		want string // the rows as text; "error" for a statement the copy fails
	}{
		// The issue's three. MySQL: 1,2 / one row / error (ambiguous).
		{"SELECT o.id, (SELECT twice FROM shop.gen g WHERE g.id = o.id) AS t FROM shop.g2 o", "[[1 99]]"},
		{"SELECT o.id FROM shop.g2 o WHERE EXISTS (SELECT 1 FROM shop.gen g WHERE g.id = o.id AND twice = 2)", "[]"},
		{"SELECT twice FROM shop.g2 JOIN shop.gen ON g2.id = gen.id", "[[99]]"},
		// One table is enough. MySQL reads the column in each: no row (twice
		// is 2), one group with 99, 99 and 3, and 2.
		{"SELECT a AS twice FROM shop.gen WHERE twice = 3", "[[3]]"},
		{"SELECT 99 AS twice, twice + 1 AS n FROM shop.gen", "[[99 100]]"},
		{"SELECT id AS twice FROM shop.gen GROUP BY twice", "[[1]]"},
		{"SELECT \"user\" = 'u1' AS mine FROM shop.kw", "[[false]]"},
		// The plain case does fail on the copy, as does the qualified one.
		{"SELECT twice FROM shop.gen", "error"},
		{"SELECT g.twice FROM shop.gen g", "error"},
		{"SELECT id FROM shop.gen ORDER BY twice", "error"},
		{"SELECT twice FROM shop.hid", "error"},
	} {
		out, err := f.s.runSQL(ctx, f.s.cm.boot, "u", c.stmt, "", 0, sqlsandbox.Session{})
		got := "error"
		if err == nil {
			got = fmt.Sprintf("%v", out.Result.Rows)
		} else {
			var qerr *sqlsandbox.QueryError
			if !errors.As(err, &qerr) {
				t.Errorf("%s: %v (%T), want a query error or an answer", c.stmt, err, err)
			}
		}
		if got != c.want {
			t.Errorf("the copy, not asked for MySQL's answer: %s\n  got  %s (err %v)\n  want %s", c.stmt, got, err, c.want)
		}
	}
	// The whole row as one value, under the column's name.
	if out, err := f.s.runSQL(ctx, f.s.cm.boot, "u", "SELECT twice FROM shop.gen AS twice", "", 0, sqlsandbox.Session{}); err != nil || len(out.Result.Rows) != 1 {
		t.Errorf("SELECT twice FROM shop.gen AS twice: rows %v, err %v; want the copy to answer with the row as one value", out.Result.Rows, err)
	}

	refused := func(schema, stmt string, wantInMessage ...string) {
		t.Helper()
		_, err := f.s.runSQL(ctx, f.s.cm.boot, "u", stmt, schema, 0, strict)
		var refusal *sqlStarRefusal
		if !errors.As(err, &refusal) {
			t.Errorf("%s under StrictStar: err = %v, want a refusal", stmt, err)
			return
		}
		for _, w := range wantInMessage {
			if !strings.Contains(refusal.Message, w) {
				t.Errorf("%s: refusal %q does not say %q", stmt, refusal.Message, w)
			}
		}
	}
	answered := func(schema, stmt, want string) {
		t.Helper()
		out, err := f.s.runSQL(ctx, f.s.cm.boot, "u", stmt, schema, 0, strict)
		if err != nil {
			t.Errorf("%s under StrictStar: %v, want the copy's answer", stmt, err)
			return
		}
		if got := fmt.Sprintf("%v", out.Result.Rows); want != "" && got != want {
			t.Errorf("%s under StrictStar: rows %s, want %s", stmt, got, want)
		}
	}

	// Asked for MySQL's answer: every statement that names the column is refused.
	refused("", "SELECT o.id, (SELECT twice FROM shop.gen g WHERE g.id = o.id) AS t FROM shop.g2 o", "shop.gen", "twice")
	refused("", "SELECT o.id FROM shop.g2 o WHERE EXISTS (SELECT 1 FROM shop.gen g WHERE g.id = o.id AND twice = 2)", "shop.gen", "twice")
	refused("", "SELECT twice FROM shop.g2 JOIN shop.gen ON g2.id = gen.id", "shop.gen", "twice")
	refused("", "SELECT a AS twice FROM shop.gen WHERE twice = 3", "shop.gen")
	refused("", "SELECT 99 AS twice, twice + 1 AS n FROM shop.gen", "shop.gen")
	refused("", "SELECT id AS twice FROM shop.gen GROUP BY twice", "shop.gen")
	refused("", "SELECT twice FROM shop.gen AS twice", "shop.gen")
	refused("", "SELECT \"user\" = 'u1' AS mine FROM shop.kw", "shop.kw", "user")
	refused("", "SELECT twice FROM shop.hid", "shop.hid", "twice")
	// The ones the copy fails anyway are refused before it tries.
	refused("", "SELECT twice FROM shop.gen", "shop.gen")
	refused("", "SELECT id FROM shop.gen ORDER BY twice", "shop.gen")
	refused("", "SELECT id, count(*) FROM shop.gen GROUP BY id HAVING max(twice) > 1", "shop.gen")
	refused("", "SELECT id, sum(a) OVER (ORDER BY twice) FROM shop.gen", "shop.gen")
	// However the table and the column are written.
	refused("", `SELECT "TWICE" FROM "SHOP"."GEN" AS x JOIN shop.g2 ON true`, "shop.gen")
	refused("", "SELECT Twice FROM gen JOIN g2 ON true", "shop.gen")
	refused("shop", "SELECT twice FROM gen, g2", "shop.gen")
	refused("", "SELECT * FROM (SELECT id FROM shop.g2 WHERE twice = 99) d JOIN (SELECT id, a FROM shop.gen) e ON e.id = d.id", "shop.gen")
	refused("", "WITH c AS (SELECT 99 AS twice) SELECT (SELECT twice FROM shop.gen LIMIT 1) FROM c", "shop.gen")
	refused("", "SELECT g.a FROM shop.gen g JOIN shop.gen h ON h.id = g.id WHERE twice = 2", "shop.gen")
	refused("", "SELECT id FROM shop.gen WHERE twice = ?", "shop.gen") // a prepared statement's text
	// The text is searched: a string holding the name is enough.
	refused("", "SELECT id FROM shop.gen WHERE a = 'twice'", "shop.gen")

	// A statement that does not hold the name is the copy's, with as many
	// tables as it likes: the rule is about the name, not about company.
	answered("", "SELECT id, a FROM shop.gen", "[[1 3]]")
	answered("", "SELECT g.a, o.b FROM shop.gen g JOIN shop.g2 o ON o.id = g.id", "[[3 7]]")
	answered("", "SELECT o.id, (SELECT g.a FROM shop.gen g WHERE g.id = o.id) AS t FROM shop.g2 o", "[[1 3]]")
	answered("", "SELECT g.a FROM shop.gen g JOIN shop.gen h ON h.id = g.id", "[[3]]")
	// The name, in a statement that does not read the table that lacks it.
	answered("", "SELECT twice FROM shop.g2", "[[99]]")
	answered("", "SELECT o.twice FROM shop.g2 o JOIN shop.g2 l ON l.id = o.id", "[[99]]")
	// A WITH named as the table is not the table, on MySQL as on the copy.
	answered("", "WITH gen AS (SELECT 5 AS twice) SELECT twice FROM gen", "[[5]]")

	// A table with no CREATE TABLE in its snapshot (shop.orders): what it
	// lacks on the copy is not known, which is not "nothing".
	refused("", "SELECT id, status FROM shop.orders", "shop.orders", "no table definition")
	refused("", "SELECT count(*) FROM shop.orders", "shop.orders")
	refused("", "SELECT g.b FROM shop.g2 g JOIN shop.orders o ON o.id = g.id", "shop.orders")
	// A statement the walk is not sure about may read any table.
	refused("", "SELECT range FROM range(3)", "shop.")

	// Nothing is refused for a caller that did not ask.
	if _, err := f.s.runSQL(ctx, f.s.cm.boot, "u", "SELECT id, status FROM shop.orders", "", 0, sqlsandbox.Session{}); err != nil {
		t.Errorf("shop.orders without StrictStar: %v", err)
	}

	// On the port's wire it is the refusal read routing forwards.
	_, err = (&SQLOnCopy{s: f.s, b: f.s.cm.boot, user: "server:x"}).Run(ctx, "SELECT twice FROM shop.g2 JOIN shop.gen ON g2.id = gen.id", "", strict)
	var differ *sqlsandbox.ColumnsDifferError
	if !errors.As(err, &differ) || !strings.Contains(differ.Reason, "shop.gen") || !strings.Contains(differ.Reason, "twice") {
		t.Errorf("port: err = %v (%T), want a ColumnsDifferError naming the table and the column", err, err)
	}
}

// The star check's fallback (#2111), which had no guard: a star table that
// matches no view of the copy means the walk and the copy disagree about what
// the statement reads, and the check then looks at every table the statement
// reads instead of at none.
func TestSQLStarRefusalFor_starTableThatMatchesNoView(t *testing.T) {
	in := views.Input{Baselines: []views.BaselineTable{
		{Schema: "shop", Table: "gen", Path: "/c/shop/gen.parquet", SchemaKnown: true, Columns: []string{"id", "a"}, StarDiffers: "MySQL also returns generated column twice"},
		{Schema: "shop", Table: "lines", Path: "/c/shop/lines.parquet", SchemaKnown: true, Columns: []string{"id"}},
	}}
	gen := sqlsandbox.TableRef{Schema: "shop", Name: "gen"}
	narrowed := in
	narrowed.OnlyViews = sqlWantedViews(in, sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{gen}})
	if narrowed.OnlyViews == nil {
		t.Fatal("the fixture's table matches no view")
	}
	for _, c := range []struct {
		name string
		star []sqlsandbox.TableRef
		want string
	}{
		{"the star is over the table that differs", []sqlsandbox.TableRef{gen}, "shop.gen"},
		{"the star is over a table whose columns are MySQL's", []sqlsandbox.TableRef{{Schema: "shop", Name: "lines"}}, ""},
		{"the star is over a name the copy has no view for", []sqlsandbox.TableRef{{Name: "r"}}, "shop.gen"},
		{"one star table matches and one does not", []sqlsandbox.TableRef{{Schema: "shop", Name: "lines"}, {Name: "r"}}, "shop.gen"},
		{"another catalog", []sqlsandbox.TableRef{{Catalog: "other", Schema: "shop", Name: "lines"}}, "shop.gen"},
	} {
		got := sqlStarRefusalFor(in, narrowed, sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{gen}, StarTables: c.star})
		if (c.want == "") != (got == "") || !strings.Contains(got, c.want) {
			t.Errorf("%s: refusal %q, want one naming %q", c.name, got, c.want)
		}
	}
}
