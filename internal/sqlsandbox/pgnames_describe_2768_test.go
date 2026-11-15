package sqlsandbox

import (
	"reflect"
	"strings"
	"testing"
)

// #2768: the PostgreSQL port describes a prepared statement by running
// SELECT * FROM (<statement>) AS dbtrail_describe LIMIT 0. A star over a
// subquery takes its column names, so the statement's own select list names
// the columns, as it does when the statement runs. Only that exact wrapper:
// a star over anything else, or the wrapper with anything added, keeps the
// engine's names as before.
func TestPostgresColumnNames_describeWrapper_2768(t *testing.T) {
	const from = " FROM (VALUES (1, 'x')) t(a, b)"
	wrap := func(inner string) string {
		return "SELECT * FROM (\n" + inner + "\n) AS dbtrail_describe LIMIT 0"
	}
	for _, c := range []struct {
		sql  string
		want string // space separated, "-" for the engine's; "nil" for no renaming
	}{
		{wrap("SELECT count(*)" + from), "count"},
		{wrap("SELECT count(*), sum(a), a, a + 1, lower(b) AS l" + from + " GROUP BY a, b"), "count sum - ?column? -"},
		// A statement that ends in a comment, as a client may send one.
		{wrap("SELECT count(*)" + from + " -- the total"), "count"},
		{wrap("SELECT count(*)" + from + " UNION ALL SELECT a" + from), "count"},
		{wrap("WITH c AS (SELECT a" + from + ") SELECT max(a) FROM c"), "max"},
		// Inside, what the statement alone would get: a star renames nothing.
		{wrap("SELECT *" + from), "nil"},
		// Not the wrapper: another alias, another limit, an offset, a WHERE,
		// a qualified or excluding star, a column alias list, a second item.
		{"SELECT * FROM (SELECT count(*)" + from + ") AS other LIMIT 0", "nil"},
		{"SELECT * FROM (SELECT count(*)" + from + ") AS dbtrail_describe LIMIT 1", "nil"},
		{"SELECT * FROM (SELECT count(*)" + from + ") AS dbtrail_describe", "nil"},
		{"SELECT * FROM (SELECT count(*)" + from + ") AS dbtrail_describe LIMIT 0 OFFSET 1", "nil"},
		{"SELECT * FROM (SELECT count(*)" + from + ") AS dbtrail_describe WHERE true LIMIT 0", "nil"},
		{"SELECT dbtrail_describe.* FROM (SELECT count(*)" + from + ") AS dbtrail_describe LIMIT 0", "nil"},
		{"SELECT * EXCLUDE (x) FROM (SELECT count(*), 1 AS x" + from + ") AS dbtrail_describe LIMIT 0", "nil"},
		{"SELECT * FROM (SELECT count(*)" + from + ") AS dbtrail_describe(n) LIMIT 0", "nil"},
		{"SELECT *, 1 FROM (SELECT count(*)" + from + ") AS dbtrail_describe LIMIT 0", "nil"},
		{"SELECT * FROM (SELECT count(*)" + from + ") AS dbtrail_describe ORDER BY 1 LIMIT 0", "nil"},
		{"SELECT DISTINCT * FROM (SELECT count(*)" + from + ") AS dbtrail_describe LIMIT 0", "nil"},
	} {
		stmt := parseForNames(t, c.sql)
		if c.want == "nil" {
			for _, n := range []int{1, 2} {
				if got := postgresColumnNames(stmt, n); got != nil {
					t.Errorf("%s\n  with %d columns: %q, want no renaming", c.sql, n, got)
				}
			}
			continue
		}
		want := strings.Fields(c.want)
		for i, w := range want {
			if w == "-" {
				want[i] = ""
			}
		}
		if got := postgresColumnNames(stmt, len(want)); !reflect.DeepEqual(got, want) {
			t.Errorf("%s\n   got %q\n  want %q", c.sql, got, want)
		}
		if got := postgresColumnNames(stmt, len(want)+1); got != nil {
			t.Errorf("%s\n  with one column too many: %q, want no renaming", c.sql, got)
		}
	}
}
