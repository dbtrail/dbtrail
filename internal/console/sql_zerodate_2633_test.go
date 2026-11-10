package console

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/hiderules"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #2633: a zero date is NULL on the copy, and a column where a NULL can only
// be one (views.BaselineTable.ZeroDate) is checked for a NULL before the copy
// answers a routed statement that names it, as a text column is checked.
func TestSQLTextCheck_zeroDate(t *testing.T) {
	orders := views.BaselineTable{Schema: "shop", Table: "orders", Path: "/c/shop/orders.parquet", SchemaKnown: true,
		DistinctSafe: []string{"id"}, DistinctOther: []string{"made", "shipped", "note"},
		Text:     []views.TextColumn{{Name: "note", Kind: views.TextFolded}},
		ZeroDate: []string{"made"}}
	in := views.Input{Baselines: []views.BaselineTable{orders}}
	cols := func(names ...string) []sqlsandbox.ColumnRef {
		var out []sqlsandbox.ColumnRef
		for _, n := range names {
			out = append(out, sqlsandbox.ColumnRef{Name: n})
		}
		return out
	}
	desc := func(gs []sqlTextGuard) []string {
		var out []string
		for _, g := range gs {
			out = append(out, g.column+" "+map[bool]string{true: "zero-date", false: g.pattern}[g.zeroDate])
		}
		return out
	}
	for _, c := range []struct {
		name string
		refs sqlsandbox.Refs
		want []string
	}{
		{"named, any case", sqlsandbox.Refs{Columns: cols("id", "MADE")}, []string{"made zero-date"}},
		{"beside a text column", sqlsandbox.Refs{Columns: cols("note", "made")}, []string{"note " + sqlTextPrintable, "made zero-date"}},
		{"a nullable date, its NULLs the source's own", sqlsandbox.Refs{Columns: cols("shipped")}, nil},
		{"not named", sqlsandbox.Refs{Columns: cols("id")}, nil},
		{"a star brings it in", sqlsandbox.Refs{Star: true}, []string{"note " + sqlTextPrintable, "made zero-date"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			why, guards := sqlTextCheck(in, c.refs)
			if why != "" {
				t.Fatalf("refused: %s", why)
			}
			if got := desc(guards); !slices.Equal(got, c.want) {
				t.Errorf("guards = %q, want %q", got, c.want)
			}
		})
	}

	set, err := hiderules.Parse(hiderules.Rules{Columns: []string{"shop.orders.made"}})
	if err != nil {
		t.Fatal(err)
	}
	hidden := in
	hidden.Hidden = set
	if _, guards := sqlTextCheck(hidden, sqlsandbox.Refs{Columns: cols("made")}); len(guards) != 0 {
		t.Errorf("a hidden column is guarded: %+v", guards)
	}
}

// The check where it runs: DuckDB, over a view holding a NULL where the
// source had 0000-00-00, and over one that holds none.
func TestSQLTextDeclineSQL_zeroDate(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	for _, q := range []string{
		`CREATE VIEW "zeroed" AS SELECT * FROM (VALUES (1, TIMESTAMP '2020-01-01 00:00:00', 'a'), (2, NULL, 'b')) v(id, made, note)`,
		`CREATE VIEW "clean" AS SELECT * FROM (VALUES (1, TIMESTAMP '2020-01-01 00:00:00', 'a')) v(id, made, note)`,
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	run := func(view string) error {
		script := sqlTextDeclineSQL([]sqlTextGuard{
			{view: `"` + view + `"`, label: "shop.orders", column: "note", pattern: sqlTextPrintable},
			{view: `"` + view + `"`, label: "shop.orders", column: "made", zeroDate: true},
		})
		_, err := db.ExecContext(ctx, script)
		return err
	}
	err = run("zeroed")
	if err == nil {
		t.Fatal("a NULL in a column where it stands for a zero date did not decline")
	}
	for _, want := range []string{"shop.orders (note, made)", "a zero date (0000-00-00), which the copy holds as NULL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the decline %q does not say %q", err, want)
		}
	}
	if err := run("clean"); err != nil {
		t.Errorf("a view with no NULL declined: %v", err)
	}
}

// #2633 through runSQL and the real worker, over files the baseline writer
// wrote: it maps the source's 0000-00-00 to NULL, the definition in the
// footer says the column is NOT NULL, and a routed statement that names it
// is declined; one that does not, or a table with no zero date, is answered.
func TestSQL_realWorkerZeroDate_2633(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	f := newSQLFixture(t, runner, false)
	const create = "CREATE TABLE `%s` (\n  `id` int NOT NULL,\n  `made` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',\n" +
		"  `shipped` datetime DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	writeSQLStarTable(t, f.root, "zd", fmt.Sprintf(create, "zd"), []string{"1", "0000-00-00 00:00:00", "2026-01-01 10:00:00"})
	writeSQLStarTable(t, f.root, "fine", fmt.Sprintf(create, "fine"), []string{"1", "2026-01-01 10:00:00", "2026-01-01 10:00:00"})
	for _, c := range []struct {
		stmt     string
		declined bool
	}{
		{"SELECT max(made) FROM shop.zd", true},
		{"SELECT id FROM shop.zd ORDER BY made", true},
		{"SELECT count(*) FROM shop.zd", false},
		{"SELECT max(shipped) FROM shop.zd", false},
		{"SELECT max(made) FROM shop.fine", false},
	} {
		sess := sqlsandbox.Session{StrictStar: true, Types: readrouter.ShapeOf(c.stmt)}
		_, err := f.s.runSQL(context.Background(), f.s.cm.boot, "u", c.stmt, "", 0, sess)
		switch {
		case c.declined && (err == nil || !strings.Contains(err.Error(), "a zero date (0000-00-00), which the copy holds as NULL")):
			t.Errorf("%s: err = %v, want the copy to decline it for a zero date", c.stmt, err)
		case !c.declined && err != nil:
			t.Errorf("%s: %v, want the copy's answer", c.stmt, err)
		}
	}
}
