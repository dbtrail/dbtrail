package pgshim

import "testing"

func TestQuoteCopyWords(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"SELECT id, at FROM t", `SELECT id, "at" FROM t`},
		{"SELECT id, v FROM pivot", `SELECT id, v FROM "pivot"`},
		{"SELECT id, v FROM pivot AS p", `SELECT id, v FROM "pivot" AS p`},
		{"SELECT id FROM t WHERE describe IS NOT NULL", `SELECT id FROM t WHERE "describe" IS NOT NULL`},
		{"SELECT sum(by) FROM t", `SELECT sum("by") FROM t`},
		{"SELECT x AS qualify FROM t", `SELECT x AS "qualify" FROM t`},
		{"SELECT AT, Glob FROM t", `SELECT "at", "glob" FROM t`},
		// PostgreSQL's own syntax is kept.
		{"SELECT by, count(*) FROM t GROUP BY by ORDER BY by", `SELECT "by", count(*) FROM t GROUP BY "by" ORDER BY "by"`},
		{"SELECT row_number() OVER (PARTITION BY a ORDER BY b) FROM t", "SELECT row_number() OVER (PARTITION BY a ORDER BY b) FROM t"},
		{"SELECT string_agg(x, ',' ORDER BY by) FROM t", `SELECT string_agg(x, ',' ORDER BY "by") FROM t`},
		{"SELECT ts AT TIME ZONE 'UTC' FROM t", "SELECT ts AT TIME ZONE 'UTC' FROM t"},
		{"SELECT at AT TIME ZONE 'UTC' FROM t", `SELECT "at" AT TIME ZONE 'UTC' FROM t`},
		{"SELECT ts AT LOCAL FROM t", "SELECT ts AT LOCAL FROM t"},
		{"SHOW search_path", "SHOW search_path"},
		{"DESCRIBE t", "DESCRIBE t"},
		{"(SHOW TABLES)", "(SHOW TABLES)"},
		{"SELECT show FROM t", `SELECT "show" FROM t`},
		// A name the copy reads already, a call, a string, a quoted name.
		{"SELECT t.at, t.pivot FROM t", "SELECT t.at, t.pivot FROM t"},
		{"SELECT glob('x')", "SELECT glob('x')"},
		{"SELECT 'at by pivot', \"at\" FROM t", "SELECT 'at by pivot', \"at\" FROM t"},
		{"SELECT x FROM t -- at\nWHERE y", "SELECT x FROM t -- at\nWHERE y"},
		{"SELECT 1", "SELECT 1"},
		{"", ""},
	} {
		if got := quoteCopyWords(c.in); got != c.want {
			t.Errorf("%q\n got %q\nwant %q", c.in, got, c.want)
		}
	}
}

// Through a forwarding account, a statement that names a column qualify or
// a table pivot is no longer sent to the source for the word: quoted, the
// copy reads it as PostgreSQL does.
func TestCopyScreen_ownWordsAsNames(t *testing.T) {
	types := map[string][]string{"id": {"INTEGER"}, "qualify": {"INTEGER"}, "asof": {"INTEGER"}, "v": {"INTEGER"}}
	for _, q := range []string{
		"SELECT id, qualify FROM t ORDER BY id",
		"SELECT id, asof FROM t WHERE asof > 1",
		"SELECT id, v FROM pivot ORDER BY id",
	} {
		if veto := CopyScreen(q, types); veto != "" {
			t.Errorf("%s: sent to the source for %s", q, veto)
		}
	}
	// Without forwarding, only the words are quoted.
	q := "SELECT round(a / b, 2), at FROM t"
	if got := routeSession(false).copyText(q); got != `SELECT round(a / b, 2), "at" FROM t` {
		t.Errorf("without a forwarding account: %q", got)
	}
}
