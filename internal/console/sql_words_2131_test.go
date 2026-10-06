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

// #2131 through the real worker. A column's name right before a string is a
// constant of a type called that on the copy: an error for status, the
// constant itself for text (which the routing layer vetoes from the text,
// before this is reached). A caller that asked for MySQL's answer (read
// routing) is refused before the views are built, from the column names in
// the snapshot, and the refusal is a decision about the table's columns,
// not a fault of the copy.
//
// A column named with a word the copy reserves (at, #2158) is not this
// rule's: the worker cannot parse the statement, so it never asks for the
// views. The routing layer keeps that one on the source from its text.
func TestSQL_realWorkerColumnWords_2131(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	f := newSQLFixture(t, runner, false)
	writeSQLStarTable(t, f.root, "kw", "CREATE TABLE `kw` (\n  `id` int NOT NULL,\n  `at` int DEFAULT NULL,\n"+
		"  `status` varchar(32) DEFAULT NULL,\n  `text` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n", []string{"1", "7", "open", "body"})
	writeSQLStarTable(t, f.root, "plain", "CREATE TABLE `plain` (\n  `id` int NOT NULL,\n  `made` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n", []string{"1", "7"})
	ctx := context.Background()
	strict := func(stmt string) sqlsandbox.Session {
		return sqlsandbox.Session{StrictStar: true, Types: readrouter.ShapeOf(stmt)}
	}

	// What the copy does when nobody asked for MySQL's answer: the two
	// shapes this spares it, and the one a text veto keeps from it.
	for stmt, want := range map[string]string{
		"SELECT id FROM shop.kw WHERE at >= 1":  "syntax error",
		"SELECT status 'Label' FROM shop.kw":    "status",
		"SELECT text 'Label' FROM shop.kw":      "",
		`SELECT id FROM shop.kw WHERE "at" = 7`: "",
		"SELECT id FROM shop.kw k WHERE k.at=7": "",
	} {
		out, err := f.s.runSQL(ctx, f.s.cm.boot, "u", stmt, "", 0, sqlsandbox.Session{})
		switch {
		case want == "" && err != nil:
			t.Errorf("the copy, %s: %v, want an answer", stmt, err)
		case want != "" && (err == nil || !strings.Contains(err.Error(), want)):
			t.Errorf("the copy, %s: rows %v, err %v; want it refused with %q", stmt, out, err, want)
		}
		if stmt == "SELECT text 'Label' FROM shop.kw" && err == nil {
			if got := fmt.Sprintf("%v", out.Result.Rows); !strings.Contains(got, "Label") {
				t.Errorf("the copy, %s: rows %s, want the constant Label (MySQL answers body)", stmt, got)
			}
		}
	}

	for _, c := range []struct {
		stmt    string
		refused string // what the refusal says, or "" when the copy answers
	}{
		{"SELECT status 'Label' FROM shop.kw", "column name status right before a string"},
		{"SELECT p.id, k.status 'Label' FROM shop.plain p JOIN shop.kw k ON k.id = p.id", "column name status right before a string"},
		{`SELECT id FROM shop.kw WHERE "at" = 7`, ""},
		{"SELECT id FROM shop.kw k WHERE k.at = 7", ""},
		{"SELECT id, status FROM shop.kw WHERE status = 'open'", ""},
		// The same word over a table with no such column is the copy's to
		// refuse, as before.
		{"SELECT id AS at FROM shop.plain", ""},
	} {
		_, err := f.s.runSQL(ctx, f.s.cm.boot, "u", c.stmt, "", 0, strict(c.stmt))
		var refusal *sqlStarRefusal
		switch {
		case c.refused == "" && err != nil:
			t.Errorf("%s: %v, want the copy's answer", c.stmt, err)
		case c.refused != "" && !errors.As(err, &refusal):
			t.Errorf("%s: err = %v, want a refusal", c.stmt, err)
		case c.refused != "" && !strings.Contains(refusal.Message, c.refused):
			t.Errorf("%s: refusal %q does not say %q", c.stmt, refusal.Message, c.refused)
		}
	}

	// A word that is no column of the table read is the copy's own refusal,
	// as before.
	stmt := "SELECT status 'Label' FROM shop.plain"
	var refusal *sqlStarRefusal
	if _, err := f.s.runSQL(ctx, f.s.cm.boot, "u", stmt, "", 0, strict(stmt)); err == nil || errors.As(err, &refusal) {
		t.Errorf("%s: err = %v, want the copy's own refusal", stmt, err)
	}

	// With no reading of the statement handed over, this rule asks nothing:
	// it spares an attempt, and the copy's own refusal is still there.
	if _, err := f.s.runSQL(ctx, f.s.cm.boot, "u", "SELECT id FROM shop.kw", "", 0, sqlsandbox.Session{StrictStar: true}); err != nil {
		t.Errorf("no shape over the table: %v, want the copy's answer", err)
	}

	// Through the port it is the decision the routing ladder forwards under
	// copy_columns_differ, not a fault of the copy.
	stmt = "SELECT status 'Label' FROM shop.kw"
	_, err = (&SQLOnCopy{s: f.s, b: f.s.cm.boot, user: "server:x"}).Run(ctx, stmt, "", strict(stmt))
	var differ *sqlsandbox.ColumnsDifferError
	if !errors.As(err, &differ) || !strings.Contains(differ.Reason, "column name status") {
		t.Errorf("port: err = %v (%T), want a ColumnsDifferError naming the column", err, err)
	}
}

// One question with the columns of every table the statement reads, after
// the older rules had their say.
func TestSQLWordsRefusalFor(t *testing.T) {
	var in views.Input
	for i := 0; i < 40; i++ {
		in.Baselines = append(in.Baselines, views.BaselineTable{Schema: "shop", Table: fmt.Sprintf("t%d", i), Path: fmt.Sprintf("/c/shop/t%d.parquet", i),
			SchemaKnown: true, Columns: []string{"id", fmt.Sprintf("c%d", i)}})
	}
	asked := &askedTypes{}
	if got := sqlWordsRefusalFor(in, asked); got != "" {
		t.Fatalf("refusal %q, want none", got)
	}
	if asked.nameCalls != 1 || len(asked.names) != 80 {
		t.Errorf("asked %d time(s) with %d names; want once, 80", asked.nameCalls, len(asked.names))
	}
	// Only the tables the statement reads are asked.
	narrowed := in
	narrowed.OnlyViews = sqlWantedViews(in, sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{{Schema: "shop", Name: "t3"}}})
	asked = &askedTypes{}
	sqlWordsRefusalFor(narrowed, asked)
	if fmt.Sprint(asked.names) != "[id c3]" {
		t.Errorf("a statement that reads only t3 was asked about %v", asked.names)
	}
	if got := sqlWordsRefusalFor(in, nil); got != "" {
		t.Errorf("no reading of the statement: %q, want none", got)
	}
	at := views.Input{Baselines: []views.BaselineTable{{Schema: "shop", Table: "ev", Path: "/c/shop/ev.parquet", SchemaKnown: true, Columns: []string{"id", "status"}}}}
	const stmt = "SELECT status 'Label' FROM ev"
	if got := sqlStrictRefusalFor(at, at, sqlsandbox.Refs{}, stmt, readrouter.ShapeOf(stmt)); !strings.Contains(got, "column name status") {
		t.Errorf("sqlStrictRefusalFor does not ask about the names: %q", got)
	}
}
