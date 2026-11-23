package pgshim

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// #3073: the casts written around the expressions PostgreSQL types otherwise
// than the copy's engine. What they give on the wire is measured against
// PostgreSQL in internal/pgcompare; these are the texts.

var sumGuard = func(x string) string {
	return "CASE WHEN typeof(" + x + ") IN ('TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT') THEN " + x + " ELSE error('" + notWholeSum + "') END"
}

func TestAsPostgresTypes(t *testing.T) {
	types := map[string][]string{
		"id": {"INTEGER", "BIGINT"}, "s": {"SMALLINT"}, "i": {"INTEGER"}, "b": {"BIGINT"}, "n": {"DECIMAL(12,2)"},
		"f": {"DOUBLE"}, "t": {"VARCHAR"}, "d": {"DATE"}, "ts": {"TIMESTAMP"},
	}
	for _, c := range []struct{ in, want string }{
		// An integer on PostgreSQL, a bigint on the copy.
		{"SELECT length(t) FROM v", "SELECT CAST(length(t) AS INTEGER) FROM v"},
		{"SELECT LENGTH(t) AS n FROM v", "SELECT CAST(LENGTH(t) AS INTEGER) AS n FROM v"},
		{"SELECT char_length(t), character_length(t), strpos(t, 'a') FROM v",
			"SELECT CAST(char_length(t) AS INTEGER), CAST(character_length(t) AS INTEGER), CAST(strpos(t, 'a') AS INTEGER) FROM v"},
		{"SELECT position('a' in t) FROM v", "SELECT CAST(position('a' in t) AS INTEGER) FROM v"},
		{"SELECT ntile(4) OVER (PARTITION BY length(t) ORDER BY id) FROM v",
			"SELECT CAST(ntile(4) OVER (PARTITION BY CAST(length(t) AS INTEGER) ORDER BY id) AS INTEGER) FROM v"},
		{"SELECT ntile(4) OVER w FROM v", "SELECT CAST(ntile(4) OVER w AS INTEGER) FROM v"},
		// Inside another expression the engine works the outer type out.
		{"SELECT length(t) + 1, coalesce(length(t), 0) FROM v", "SELECT CAST(length(t) AS INTEGER) + 1, coalesce(CAST(length(t) AS INTEGER), 0) FROM v"},
		{"SELECT length(length(t)::text) FROM v", "SELECT CAST(length(CAST(length(t) AS INTEGER)::text) AS INTEGER) FROM v"},
		// In a subquery, a WITH, a WHERE, an ORDER BY.
		{"SELECT * FROM (SELECT length(t) AS l FROM v) x WHERE l > 1", "SELECT * FROM (SELECT CAST(length(t) AS INTEGER) AS l FROM v) x WHERE l > 1"},
		{"WITH c AS (SELECT length(t) AS l FROM v) SELECT l FROM c ORDER BY length('x')",
			"WITH c AS (SELECT CAST(length(t) AS INTEGER) AS l FROM v) SELECT l FROM c ORDER BY CAST(length('x') AS INTEGER)"},
		// Not a call of the function, or one of another schema.
		{"SELECT length FROM v", "SELECT length FROM v"},
		{`SELECT "length"(t), my.length(t), pg_catalog.length(t), v.length FROM v`, `SELECT "length"(t), my.length(t), pg_catalog.length(t), v.length FROM v`},
		{"SELECT 'length(t)' FROM v -- length(t)", "SELECT 'length(t)' FROM v -- length(t)"},
		// Text around the statement and comments inside it stay.
		{"/* a */ SELECT length(t -- c\n) FROM v; -- end", "/* a */ SELECT CAST(length(t) AS INTEGER) FROM v; -- end"},

		// sum of a smallint or an integer is a bigint; of a bigint, a
		// numeric or a float it is what the engine has.
		{"SELECT sum(i) FROM v", "SELECT CAST(sum(" + sumGuard("i") + ") AS BIGINT) FROM v"},
		{"SELECT SUM(v.s) AS total FROM v", "SELECT CAST(SUM(" + sumGuard("v.s") + ") AS BIGINT) AS total FROM v"},
		{"SELECT sum(b), sum(n), sum(f), sum(t), sum(unknown) FROM v", "SELECT sum(b), sum(n), sum(f), sum(t), sum(unknown) FROM v"},
		{"SELECT sum(id) FROM v", "SELECT sum(id) FROM v"}, // an integer in one table, a bigint in another
		{"SELECT sum(DISTINCT i) FROM v", "SELECT CAST(sum(DISTINCT " + sumGuard("i") + ") AS BIGINT) FROM v"},
		{"SELECT sum(i) FILTER (WHERE s > 1) OVER (ORDER BY b) FROM v",
			"SELECT CAST(sum(" + sumGuard("i") + ") FILTER (WHERE s > 1) OVER (ORDER BY b) AS BIGINT) FROM v"},
		{"SELECT sum(i) + 1 FROM v", "SELECT CAST(sum(" + sumGuard("i") + ") AS BIGINT) + 1 FROM v"},
		{"SELECT sum(i + 1), sum(-s), sum(2), sum((i - s) * 3 % 2) FROM v",
			"SELECT CAST(sum(" + sumGuard("i + 1") + ") AS BIGINT), CAST(sum(" + sumGuard("-s") + ") AS BIGINT), CAST(sum(" + sumGuard("2") + ") AS BIGINT), CAST(sum(" + sumGuard("(i - s) * 3 % 2") + ") AS BIGINT) FROM v"},
		{"SELECT sum(i + b), sum(i + 3000000000), sum(i + 1.5), sum(i * f), sum(i || 1), sum(count(*)) FROM v",
			"SELECT sum(i + b), sum(i + 3000000000), sum(i + 1.5), sum(i * f), sum(i || 1), sum(count(*)) FROM v"},
		{"SELECT sum(CASE WHEN b > 1 THEN 1 ELSE 0 END), sum(CASE WHEN b > 1 THEN i END), sum(CASE WHEN b > 1 THEN 1 ELSE 0.5 END) FROM v",
			"SELECT CAST(sum(" + sumGuard("CASE WHEN b > 1 THEN 1 ELSE 0 END") + ") AS BIGINT), CAST(sum(" + sumGuard("CASE WHEN b > 1 THEN i END") + ") AS BIGINT), sum(CASE WHEN b > 1 THEN 1 ELSE 0.5 END) FROM v"},
		{"SELECT sum(CASE WHEN b > 1 THEN 1 END + CASE WHEN b > 2 THEN 1 END) FROM v", "SELECT sum(CASE WHEN b > 1 THEN 1 END + CASE WHEN b > 2 THEN 1 END) FROM v"},
		{"SELECT sum(b::int), sum(i::bigint), sum(CAST(n AS integer)), sum(CAST(i AS numeric)), sum(abs(s)) FROM v",
			"SELECT CAST(sum(" + sumGuard("b::int") + ") AS BIGINT), sum(i::bigint), CAST(sum(" + sumGuard("CAST(n AS integer)") + ") AS BIGINT), sum(CAST(i AS numeric)), CAST(sum(" + sumGuard("abs(s)") + ") AS BIGINT) FROM v"},
		{"SELECT sum(length(t)) FROM v", "SELECT CAST(sum(" + sumGuard("CAST(length(t) AS INTEGER)") + ") AS BIGINT) FROM v"},
		// A name the statement defines is whatever it was defined as.
		{"SELECT sum(i) FROM (SELECT f AS i FROM v) q", "SELECT sum(i) FROM (SELECT f AS i FROM v) q"},
		{"SELECT sum(i) FROM (SELECT 1.5 AS i) q", "SELECT sum(i) FROM (SELECT 1.5 AS i) q"},
		{"SELECT sum(i) FROM (SELECT 1.5 i) q", "SELECT sum(i) FROM (SELECT 1.5 i) q"},
		{"SELECT sum(q.i) FROM (SELECT f i FROM v) q", "SELECT sum(q.i) FROM (SELECT f i FROM v) q"},
		{`SELECT sum(i) FROM (SELECT f AS "I" FROM v) q`, `SELECT sum(i) FROM (SELECT f AS "I" FROM v) q`},
		{"SELECT sum(i) FROM unnest(x) i", "SELECT sum(i) FROM unnest(x) i"},
		{"SELECT sum(i) FROM unnest(x) AS i", "SELECT sum(i) FROM unnest(x) AS i"},
		{"SELECT sum(i) FROM (SELECT f FROM v) AS q(i)", "SELECT sum(i) FROM (SELECT f FROM v) AS q(i)"},
		{"WITH c(i) AS (SELECT f FROM v) SELECT sum(i) FROM c", "WITH c(i) AS (SELECT f FROM v) SELECT sum(i) FROM c"},
		// A column of a subquery that is the table's own.
		{"SELECT sum(q.i) FROM (SELECT * FROM v) q", "SELECT CAST(sum(" + sumGuard("q.i") + ") AS BIGINT) FROM (SELECT * FROM v) q"},
		{"WITH c AS (SELECT sum(s) AS small FROM v) SELECT small FROM c", "WITH c AS (SELECT CAST(sum(" + sumGuard("s") + ") AS BIGINT) AS small FROM v) SELECT small FROM c"},

		// extract and date_part of a whole field.
		{"SELECT extract(year FROM ts), EXTRACT('month' FROM d) AS m FROM v", "SELECT CAST(extract(year FROM ts) AS HUGEINT), CAST(EXTRACT('month' FROM d) AS HUGEINT) AS m FROM v"},
		// extract, as date_part, only as a whole item of the statement's own
		// select list: read again by the engine it is left as it was, so a
		// division of one, a filter or a group over one, and a function that
		// takes it as an argument answer as before.
		{"SELECT extract(year FROM d) + 1, make_date(extract(year FROM d), 1, 1), extract(month FROM d) / 3, extract(year FROM d)::text FROM v WHERE extract(month FROM d) / 3 = 1 GROUP BY (extract(month FROM d) - 1) / 3",
			"SELECT extract(year FROM d) + 1, make_date(extract(year FROM d), 1, 1), extract(month FROM d) / 3, extract(year FROM d)::text FROM v WHERE extract(month FROM d) / 3 = 1 GROUP BY (extract(month FROM d) - 1) / 3"},
		{"SELECT y FROM (SELECT extract(year FROM d) AS y FROM v) q", "SELECT y FROM (SELECT extract(year FROM d) AS y FROM v) q"},
		{"SELECT DISTINCT extract(year FROM d) FROM v", "SELECT DISTINCT extract(year FROM d) FROM v"},

		// The heading of a column stays the one it had, or PostgreSQL's: in a
		// list with a star, and in a subquery's list, an item that is the
		// call alone takes the function's name, and one with a name keeps it.
		{"SELECT *, length(t), sum(i) OVER () FROM v",
			`SELECT *, CAST(length(t) AS INTEGER) AS "length", CAST(sum(` + sumGuard("i") + `) OVER () AS BIGINT) AS "sum" FROM v`},
		{"SELECT v.*, extract(year FROM d), date_part('year', d), NULL, length(t) AS n FROM v",
			`SELECT v.*, CAST(extract(year FROM d) AS HUGEINT) AS "extract", CAST(date_part('year', d) AS DOUBLE) AS "date_part", ` + unnamedNull + `, CAST(length(t) AS INTEGER) AS n FROM v`},
		{"SELECT * FROM (SELECT length(t) FROM v) q", `SELECT * FROM (SELECT CAST(length(t) AS INTEGER) AS "length" FROM v) q`},
		{"SELECT * FROM (SELECT DISTINCT ntile(2) OVER (ORDER BY i), strpos(t, 'a') n, position('a' in t) AS p FROM v) q",
			`SELECT * FROM (SELECT DISTINCT CAST(ntile(2) OVER (ORDER BY i) AS INTEGER) AS "ntile", CAST(strpos(t, 'a') AS INTEGER) n, CAST(position('a' in t) AS INTEGER) AS p FROM v) q`},
		// Two items of one name, or a call inside a larger item with no name:
		// left as written, and so is every other call of that function, so
		// GROUP BY still names the item's expression.
		{"SELECT * FROM (SELECT length(t), length(t || 'x'), strpos(t, 'a') FROM v GROUP BY length(t), 2, 3) q",
			`SELECT * FROM (SELECT length(t), length(t || 'x'), CAST(strpos(t, 'a') AS INTEGER) AS "strpos" FROM v GROUP BY length(t), 2, 3) q`},
		{"SELECT *, length(t) + 1 FROM v WHERE length(t) > 1", "SELECT *, length(t) + 1 FROM v WHERE length(t) > 1"},
		{"SELECT x FROM (SELECT (SELECT sum(i) FROM v) + 1, i AS x FROM v) q WHERE x < (SELECT sum(i) FROM v)",
			"SELECT x FROM (SELECT (SELECT sum(i) FROM v) + 1, i AS x FROM v) q WHERE x < (SELECT sum(i) FROM v)"},
		{"SELECT date_part('year', ts), date_part('DOW', d) FROM v", "SELECT CAST(date_part('year', ts) AS DOUBLE), CAST(date_part('DOW', d) AS DOUBLE) FROM v"},
		{"SELECT extract(epoch FROM ts), extract(second FROM ts), date_part('second', ts), date_part(t, ts), extract(milliseconds FROM ts) FROM v",
			"SELECT extract(epoch FROM ts), extract(second FROM ts), date_part('second', ts), date_part(t, ts), extract(milliseconds FROM ts) FROM v"},

		// date_part only as a whole item of the statement's own select list:
		// read again by the engine, a double precision would be written 5.0.
		{"SELECT date_part('month', d)::text, lpad(date_part('month', d)::text, 2, '0'), date_part('year', d) + 1, date_part('year', d) || 'x' FROM v",
			"SELECT date_part('month', d)::text, lpad(date_part('month', d)::text, 2, '0'), date_part('year', d) + 1, date_part('year', d) || 'x' FROM v"},
		{"SELECT y FROM (SELECT date_part('year', d) AS y FROM v) q WHERE date_part('year', now()) > 1", "SELECT y FROM (SELECT date_part('year', d) AS y FROM v) q WHERE date_part('year', now()) > 1"},
		{"SELECT date_part('year', d) AS y, i FROM v ORDER BY date_part('year', d)", "SELECT CAST(date_part('year', d) AS DOUBLE) AS y, i FROM v ORDER BY date_part('year', d)"},
		{"SELECT DISTINCT date_part('year', d) FROM v ORDER BY date_part('year', d)", "SELECT DISTINCT date_part('year', d) FROM v ORDER BY date_part('year', d)"},

		// A function called inside one of the port's macros is cast nowhere
		// in the statement: GROUP BY must still name the same expression.
		{"SELECT length(t), _bintrail_pg_round_div(1, length(t), 2), strpos(t, 'a') FROM v GROUP BY length(t), strpos(t, 'a')",
			"SELECT length(t), _bintrail_pg_round_div(1, length(t), 2), CAST(strpos(t, 'a') AS INTEGER) FROM v GROUP BY length(t), CAST(strpos(t, 'a') AS INTEGER)"},
		// A relation with a function's name, a NULL in an array, an ORDER BY
		// inside a sum, a name defined after a quoted one.
		{"WITH length(n) AS (SELECT 1) SELECT n FROM length", "WITH length(n) AS (SELECT 1) SELECT n FROM length"},
		{"SELECT a FROM f() AS position(a, b)", "SELECT a FROM f() AS position(a, b)"},
		{"SELECT ARRAY[1, NULL, 2], [NULL] FROM v", "SELECT ARRAY[1, NULL, 2], [NULL] FROM v"},
		{"SELECT sum(i ORDER BY b::int) FROM v", "SELECT sum(i ORDER BY b::int) FROM v"},
		{`SELECT sum(i) FROM (SELECT "Amount" i FROM v) q`, `SELECT sum(i) FROM (SELECT "Amount" i FROM v) q`},
		// A name right after END, NULL, TRUE or FALSE is an alias too.
		{"SELECT sum(i) FROM (SELECT CASE WHEN b > 1 THEN n ELSE 0 END i FROM v) q", "SELECT sum(i) FROM (SELECT CASE WHEN b > 1 THEN n ELSE 0 END i FROM v) q"},
		{"SELECT sum(i), sum(s) FROM (SELECT NULL i, true s) q", "SELECT sum(i), sum(s) FROM (SELECT NULL i, true s) q"},
		// A table's alias with a column list, named as a function.
		{"SELECT * FROM (SELECT 1) length(a)", "SELECT * FROM (SELECT 1) length(a)"},
		// A sum inside a sum's argument is written as it is.
		{"SELECT sum(length(sum(length('a'))::text))", "SELECT CAST(sum(" + sumGuard("CAST(length(sum(CAST(length('a') AS INTEGER))::text) AS INTEGER)") + ") AS BIGINT)"},

		// The port's own macros keep their arguments.
		{"SELECT _bintrail_pg_round_k(sum(i), 2, 0.01), length(t), _bintrail_pg_epoch(ts), sum(s) FROM v",
			"SELECT _bintrail_pg_round_k(sum(i), 2, 0.01), CAST(length(t) AS INTEGER), _bintrail_pg_epoch(ts), sum(s) FROM v"},

		// A lone NULL is text.
		{"SELECT NULL", "SELECT " + unnamedNull},
		{"select null, 1, NULL AS x, i FROM v;", "select " + unnamedNull + ", 1, " + textNull + " AS x, i FROM v;"},
		{"WITH c AS (SELECT NULL AS x) SELECT NULL FROM c", "WITH c AS (SELECT NULL AS x) SELECT " + unnamedNull + " FROM c"},
		// Only the first without a name: a Describe would rename the second.
		{"SELECT NULL, NULL, NULL AS a, NULL AS b", "SELECT " + unnamedNull + ", NULL, " + textNull + " AS a, " + textNull + " AS b"},
		{"SELECT NULL::int, NULL + 1, coalesce(NULL, 1), (NULL), DISTINCT NULL, NULL x FROM v", "SELECT NULL::int, NULL + 1, coalesce(NULL, 1), (NULL), DISTINCT NULL, NULL x FROM v"},
		{"SELECT NULL UNION ALL SELECT 1", "SELECT NULL UNION ALL SELECT 1"},
		{"SELECT 1 EXCEPT SELECT NULL", "SELECT 1 EXCEPT SELECT NULL"},
		{"SELECT i FROM v WHERE t IS NULL ORDER BY NULL, 1", "SELECT i FROM v WHERE t IS NULL ORDER BY NULL, 1"},
		{"SELECT i FROM v WHERE i IN (SELECT NULL)", "SELECT i FROM v WHERE i IN (SELECT NULL)"},

		// Not a query, or text the port does not read: as written.
		{"SHOW TABLES", "SHOW TABLES"},
		{"INSERT INTO v SELECT length(t), NULL FROM v", "INSERT INTO v SELECT length(t), NULL FROM v"},
		{"SELECT length(E'a\\n')", "SELECT length(E'a\\n')"},
		{"SELECT length(t", "SELECT length(t"},
		{"", ""},
	} {
		if got := AsPostgresTypes(c.in, types); got != c.want {
			t.Errorf("%s\n   got  %s\n   want %s", c.in, got, c.want)
		}
	}
}

// A Describe asks for the statement inside a subquery: it gets the casts the
// statement gets when it runs, the lone NULL's included.
func TestAsPostgresTypes_describe(t *testing.T) {
	for _, stmt := range []string{"SELECT NULL, length(t), sum(i) FROM v", "SELECT 1"} {
		types := map[string][]string{"i": {"INTEGER"}}
		want := describeHead + AsPostgresTypes(stmt, types) + describeTail
		if got := AsPostgresTypes(describeHead+stmt+describeTail, types); got != want {
			t.Errorf("got  %s\nwant %s", got, want)
		}
	}
	if got := AsPostgresTypes(describeHead+"SELECT NULL"+describeTail, nil); !strings.Contains(got, unnamedNull) {
		t.Errorf("a described lone NULL is not text: %s", got)
	}
}

// Without the copy's column types no sum of a column is decided, and they
// are read only for a statement with a sum.
func TestAsPostgresTypes_columnTypes(t *testing.T) {
	if got := AsPostgresTypes("SELECT sum(i), sum(2), length(t) FROM v", nil); got != "SELECT sum(i), CAST(sum("+sumGuard("2")+") AS BIGINT), CAST(length(t) AS INTEGER) FROM v" {
		t.Errorf("without types: %s", got)
	}
	asked := 0
	read := func() map[string][]string { asked++; return map[string][]string{"i": {"INTEGER"}} }
	asPostgresTypes("SELECT length(t), extract(year FROM d), NULL FROM v", read)
	if asked != 0 {
		t.Errorf("the column types were read %d times for a statement without a sum", asked)
	}
	asPostgresTypes("SELECT sum(i), sum(i + 1), sum(2) FROM v", read)
	if asked != 1 {
		t.Errorf("the column types were read %d times, want once", asked)
	}
}

func TestWholeType(t *testing.T) {
	types := func() map[string][]string {
		return map[string][]string{"s": {"SMALLINT"}, "i": {"INTEGER", "INTEGER"}, "b": {"BIGINT"}, "mixed": {"INTEGER", "BIGINT"}, "n": {"DECIMAL(10,2)"}}
	}
	for expr, want := range map[string]string{
		"s": "int2", "i": "int4", "b": "int8", "o.i": "int4", "(i)": "int4", "mixed": "", "n": "", "nope": "",
		"1": "int4", "2147483647": "int4", "2147483648": "int8", "1.0": "", "'1'": "", "999999999999999999": "int8",
		"s + s": "int2", "s + 1": "int4", "s * i": "int4", "i - b": "int8", "-i": "int4", "- s": "int2", "i / 2": "int4", "i % s": "int4",
		"i + n": "", "i + 1.5": "", "i || s": "", "i > s": "", "i + nope": "", "i +": "", "i = 1": "",
		"i::bigint": "int8", "b::int": "int4", "n::smallint": "int2", "i::numeric": "", "i::int + s": "int4", "(i + b)::int4": "int4",
		"CAST(n AS int)": "int4", "CAST(i AS numeric)": "", "cast(b as bigint) + 1": "int8",
		"length(n)": "int4", "count(*)": "int8", "abs(s)": "int2", "abs(n)": "", "upper(i)": "", "sum(i)": "",
		"CASE WHEN i > 1 THEN 1 ELSE 0 END": "int4", "CASE WHEN i > 1 THEN s END": "int2", "CASE WHEN i > 1 THEN s ELSE b END": "int8",
		"CASE WHEN i > 1 THEN 1 ELSE NULL END": "int4", "CASE WHEN i > 1 THEN NULL END": "", "CASE WHEN i > 1 THEN 1 ELSE 'x' END": "",
		"CASE i WHEN 1 THEN 1 END + 1": "", "i IS NULL": "", "i AND s": "", "": "", "()": "", "i ORDER BY b::int": "", "i b::int": "",
	} {
		toks, _ := lexSQL(expr)
		if got := wholeType(toks, types); got != want {
			t.Errorf("%s: %q, want %q", expr, got, want)
		}
	}
}

func TestLoneNulls(t *testing.T) {
	for stmt, want := range map[string]int{
		"SELECT NULL": 1, "SELECT NULL, NULL": 1, "SELECT NULL AS a, NULL AS b, NULL": 3, "SELECT NULL AS a, 1 FROM t": 1, "SELECT NULL FROM t WHERE x IS NULL": 1,
		"SELECT a, NULL FROM t ORDER BY NULL": 1, "SELECT (SELECT NULL)": 0, "SELECT NULL UNION SELECT NULL": 0,
		"SELECT NULL FROM (VALUES (1)) v": 0, "SELECT x FROM t WHERE y IN (1, NULL)": 0, "SELECT NULL::text": 0, "SELECT ARRAY[1, NULL, 2]": 0, "SELECT DISTINCT NULL": 0,
		"SELECT CASE WHEN a THEN 1 END, NULL FROM t": 1, "SELECT 1 ORDER BY 1, NULL": 0, "SELECT a FROM t GROUP BY a, NULL": 0,
	} {
		toks, _ := lexSQL(stmt)
		items, combined := selectItems(toks)
		got := 0
		if !combined {
			got = len(loneNulls(toks, items))
		}
		if got != want {
			t.Errorf("%s: %d, want %d", stmt, got, want)
		}
	}
}

// What the connection hands the copy carries the casts, when it runs a
// statement and when it describes one first, so both announce one type.
func TestAsPostgresTypes_handedToTheCopy(t *testing.T) {
	modes := map[string]pgx.QueryExecMode{"SimpleProtocol": pgx.QueryExecModeSimpleProtocol, "DescribeExec": pgx.QueryExecModeDescribeExec}
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			l := &typedLogin{answer: func(st string) (sqlsandbox.Result, error) {
				res := sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "length", Type: "INTEGER"}}}
				if !strings.Contains(st, "dbtrail_describe") {
					res.Rows = [][]any{{json.Number("3")}}
				}
				return res, nil
			}}
			conn := dialTyped(t, l, mode)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var n int32
			if err := conn.QueryRow(ctx, "SELECT length(t) FROM v").Scan(&n); err != nil || n != 3 {
				t.Fatalf("got %d, %v", n, err)
			}
			ran, described := 0, 0
			for _, st := range l.statements() {
				if !strings.Contains(st, "length") {
					continue
				}
				ran++
				if !strings.Contains(st, "CAST(length(t) AS INTEGER)") {
					t.Errorf("handed to the copy without its cast: %s", st)
				}
				if strings.Contains(st, "dbtrail_describe") {
					described++
				}
			}
			if ran == 0 || (mode == pgx.QueryExecModeDescribeExec) != (described > 0) {
				t.Errorf("%d statements reached the copy, %d of them a Describe: %q", ran, described, l.statements())
			}
		})
	}
}

// A statement of sums nested in sums is written in about its own size: the
// argument of a sum is written twice, and only the outermost one's.
func TestAsPostgresTypes_nestedSumsStaySmall(t *testing.T) {
	stmt := "'a'"
	for range 40 {
		stmt = "sum(length(" + stmt + "))::text"
	}
	stmt = "SELECT " + stmt
	if got := AsPostgresTypes(stmt, nil); len(got) > 10*len(stmt) {
		t.Errorf("%d bytes written for a statement of %d", len(got), len(stmt))
	}
}

// The rewrite is bounded: a statement that would be written past the longest
// one the port takes, that nests casts too deep, or that is only parentheses,
// is handed over as written, in about the time it takes to read it.
func TestAsPostgresTypes_bounded(t *testing.T) {
	const size = 1 << 20
	types := map[string][]string{"i": {"INTEGER"}}
	for name, stmt := range map[string]string{
		"sums":           "SELECT " + strings.Repeat("sum(1),", size/7) + "1",
		"sums of a name": "SELECT " + strings.Repeat("sum(i),", size/7) + "1 FROM v",
		"nested calls":   "SELECT " + strings.Repeat("length(", size/7) + "'a'" + strings.Repeat(")", size/7),
		"nested IN":      "SELECT 1 WHERE 1 " + strings.Repeat("in (1 ", size/6) + strings.Repeat(")", size/6),
		"nested selects": "SELECT " + strings.Repeat("(SELECT length(t), ", size/18) + "1" + strings.Repeat(")", size/18),
		"open only":      "SELECT sum" + strings.Repeat("(", size),
		"too long":       "SELECT length(t) FROM v -- " + strings.Repeat("x", maxStatementMessage),
	} {
		started := time.Now()
		got := AsPostgresTypes(stmt, types)
		if took := time.Since(started); took > 5*time.Second {
			t.Errorf("%s: %v for a statement of %d bytes", name, took, len(stmt))
		}
		if got != stmt {
			t.Errorf("%s: a statement past the bounds was rewritten (%d bytes from %d)", name, len(got), len(stmt))
		}
	}
	// Within them it is rewritten, and what is written stays within the
	// longest statement the port takes.
	stmt := "SELECT " + strings.Repeat("sum(1),", 1000) + "1"
	if got := AsPostgresTypes(stmt, nil); got == stmt || len(got) > maxStatementMessage || strings.Count(got, "AS BIGINT") != 1000 {
		t.Errorf("1000 sums: %d bytes from %d", len(got), len(stmt))
	}
	deep := "SELECT " + strings.Repeat("length(", maxCastDepth-1) + "'a'" + strings.Repeat("::text)", maxCastDepth-1)
	if got := AsPostgresTypes(deep, nil); strings.Count(got, "AS INTEGER") != maxCastDepth-1 {
		t.Errorf("%d calls one inside another: %d casts", maxCastDepth-1, strings.Count(got, "AS INTEGER"))
	}
}
