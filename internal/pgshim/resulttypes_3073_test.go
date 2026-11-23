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
	same := func(stmt string) [2]string { return [2]string{stmt, stmt} }
	for _, c := range [][2]string{
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
		// Every call of the function in the statement: the item, WHERE, GROUP
		// BY, HAVING and ORDER BY stay one expression.
		{"SELECT length(t), count(*) FROM v WHERE length(t) > 1 GROUP BY length(t) HAVING length(t) < 9 ORDER BY length(t)",
			"SELECT CAST(length(t) AS INTEGER), count(*) FROM v WHERE CAST(length(t) AS INTEGER) > 1 GROUP BY CAST(length(t) AS INTEGER) HAVING CAST(length(t) AS INTEGER) < 9 ORDER BY CAST(length(t) AS INTEGER)"},
		{"SELECT DISTINCT id, length(t) FROM v ORDER BY length(t)", "SELECT DISTINCT id, CAST(length(t) AS INTEGER) FROM v ORDER BY CAST(length(t) AS INTEGER)"},
		// In a subquery and a WITH, under a name.
		{"SELECT * FROM (SELECT length(t) AS l FROM v) x WHERE l > 1", "SELECT * FROM (SELECT CAST(length(t) AS INTEGER) AS l FROM v) x WHERE l > 1"},
		{"WITH c AS (SELECT length(t) AS l FROM v) SELECT l FROM c ORDER BY length('x')",
			"WITH c AS (SELECT CAST(length(t) AS INTEGER) AS l FROM v) SELECT l FROM c ORDER BY CAST(length('x') AS INTEGER)"},
		{"SELECT * FROM (SELECT a IS DISTINCT FROM b AS x, length(t) AS l FROM v) q", "SELECT * FROM (SELECT a IS DISTINCT FROM b AS x, CAST(length(t) AS INTEGER) AS l FROM v) q"},

		// The heading of a column stays the one it had. The engine heads an
		// item with no name by the item's own text in a subquery's list and in
		// a list with a star, so a function called in such an item is left as
		// written, there and everywhere else in the statement.
		same("SELECT * FROM (SELECT length(t) FROM v) q"),
		same("SELECT *, length(t) FROM v WHERE length(t) > 1"),
		same("SELECT v.*, sum(i) OVER () FROM v"),
		same("SELECT * FROM (SELECT a IS DISTINCT FROM b AS x, length(t) FROM v) q"),
		same("SELECT x FROM (SELECT (SELECT sum(i) FROM v) + 1, i AS x FROM v) q WHERE x < (SELECT sum(i) FROM v)"),
		{"SELECT * FROM (SELECT length(t), strpos(t, 'a') AS p FROM v GROUP BY length(t), 2) q WHERE strpos('a', 'a') = 1",
			"SELECT * FROM (SELECT length(t), CAST(strpos(t, 'a') AS INTEGER) AS p FROM v GROUP BY length(t), 2) q WHERE CAST(strpos('a', 'a') AS INTEGER) = 1"},
		{"SELECT *, length(t) AS n, sum(i) OVER () AS total FROM v", "SELECT *, CAST(length(t) AS INTEGER) AS n, CAST(sum(" + sumGuard("i") + ") OVER () AS BIGINT) AS total FROM v"},
		// Any subquery's list is read that way, whatever reads the subquery.
		{"SELECT id FROM v WHERE i IN (SELECT length(t) FROM v)", "SELECT id FROM v WHERE i IN (SELECT length(t) FROM v)"},

		// Not a call of the function, or one of another schema.
		same("SELECT length FROM v"),
		same(`SELECT "length"(t), my.length(t), pg_catalog.length(t), v.length FROM v`),
		same("SELECT 'length(t)' FROM v -- length(t)"),
		// A word that may be a relation's name: left as written everywhere.
		same("WITH length(n) AS (SELECT 1) SELECT n, length('a') FROM length"),
		same("SELECT a, position('a' in 'b') FROM f() AS position(a, b)"),
		same("SELECT *, length('a') AS n FROM (SELECT 1) length(a)"),
		same("SELECT substring(t FROM length(t) - 2) FROM v GROUP BY length(t)"),
		// Text around the statement and comments inside it stay.
		{"/* a */ SELECT length(t -- c\n) FROM v; -- end", "/* a */ SELECT CAST(length(t) AS INTEGER) FROM v; -- end"},

		// sum of a smallint or an integer is a bigint; of a bigint, a
		// numeric or a float it is what the engine has.
		{"SELECT sum(i) FROM v", "SELECT CAST(sum(" + sumGuard("i") + ") AS BIGINT) FROM v"},
		{"SELECT SUM(v.s) AS total FROM v", "SELECT CAST(SUM(" + sumGuard("v.s") + ") AS BIGINT) AS total FROM v"},
		same("SELECT sum(b), sum(n), sum(f), sum(t), sum(unknown) FROM v"),
		same("SELECT sum(id) FROM v"), // an integer in one table, a bigint in another
		{"SELECT sum(DISTINCT i) FROM v", "SELECT CAST(sum(DISTINCT " + sumGuard("i") + ") AS BIGINT) FROM v"},
		{"SELECT sum(i) FILTER (WHERE s > 1) OVER (ORDER BY b) FROM v",
			"SELECT CAST(sum(" + sumGuard("i") + ") FILTER (WHERE s > 1) OVER (ORDER BY b) AS BIGINT) FROM v"},
		{"SELECT sum(i) + 1 FROM v HAVING sum(i) > 0", "SELECT CAST(sum(" + sumGuard("i") + ") AS BIGINT) + 1 FROM v HAVING CAST(sum(" + sumGuard("i") + ") AS BIGINT) > 0"},
		{"SELECT sum(i + 1), sum(-s), sum(2), sum((i - s) * 3 % 2) FROM v",
			"SELECT CAST(sum(" + sumGuard("i + 1") + ") AS BIGINT), CAST(sum(" + sumGuard("-s") + ") AS BIGINT), CAST(sum(" + sumGuard("2") + ") AS BIGINT), CAST(sum(" + sumGuard("(i - s) * 3 % 2") + ") AS BIGINT) FROM v"},
		same("SELECT sum(i + b), sum(i + 3000000000), sum(i + 1.5), sum(i * f), sum(i || 1), sum(count(*)) FROM v"),
		{"SELECT sum(CASE WHEN b > 1 THEN 1 ELSE 0 END), sum(CASE WHEN b > 1 THEN i END), sum(CASE WHEN b > 1 THEN 1 ELSE 0.5 END) FROM v",
			"SELECT CAST(sum(" + sumGuard("CASE WHEN b > 1 THEN 1 ELSE 0 END") + ") AS BIGINT), CAST(sum(" + sumGuard("CASE WHEN b > 1 THEN i END") + ") AS BIGINT), sum(CASE WHEN b > 1 THEN 1 ELSE 0.5 END) FROM v"},
		same("SELECT sum(CASE WHEN b > 1 THEN 1 END + CASE WHEN b > 2 THEN 1 END) FROM v"),
		{"SELECT sum(b::int), sum(i::bigint), sum(CAST(n AS integer)), sum(CAST(i AS numeric)), sum(abs(s)) FROM v",
			"SELECT CAST(sum(" + sumGuard("b::int") + ") AS BIGINT), sum(i::bigint), CAST(sum(" + sumGuard("CAST(n AS integer)") + ") AS BIGINT), sum(CAST(i AS numeric)), CAST(sum(" + sumGuard("abs(s)") + ") AS BIGINT) FROM v"},
		{"SELECT sum(length(t)) FROM v", "SELECT CAST(sum(" + sumGuard("CAST(length(t) AS INTEGER)") + ") AS BIGINT) FROM v"},
		// A name the statement defines is whatever it was defined as.
		same("SELECT sum(i) FROM (SELECT f AS i FROM v) q"),
		same("SELECT sum(i) FROM (SELECT 1.5 AS i) q"),
		same("SELECT sum(i) FROM (SELECT 1.5 i) q"),
		same("SELECT sum(q.i) FROM (SELECT f i FROM v) q"),
		same(`SELECT sum(i) FROM (SELECT f AS "I" FROM v) q`),
		same(`SELECT sum(i) FROM (SELECT "Amount" i FROM v) q`),
		same("SELECT sum(i) FROM (SELECT CASE WHEN b > 1 THEN n ELSE 0 END i FROM v) q"),
		same("SELECT sum(i), sum(s) FROM (SELECT NULL i, true s) q"),
		same("SELECT sum(i) FROM unnest(x) i"),
		same("SELECT sum(i) FROM unnest(x) AS i"),
		same("SELECT sum(i) FROM (SELECT f FROM v) AS q(i)"),
		same("WITH c(i) AS (SELECT f FROM v) SELECT sum(i) FROM c"),
		same("SELECT sum(i ORDER BY b::int) FROM v"),
		// A column of a subquery that is the table's own.
		{"SELECT sum(q.i) FROM (SELECT * FROM v) q", "SELECT CAST(sum(" + sumGuard("q.i") + ") AS BIGINT) FROM (SELECT * FROM v) q"},
		{"WITH c AS (SELECT sum(s) AS small FROM v) SELECT small FROM c", "WITH c AS (SELECT CAST(sum(" + sumGuard("s") + ") AS BIGINT) AS small FROM v) SELECT small FROM c"},
		// A sum of whole numbers inside another's argument: no statement
		// PostgreSQL takes; handed over as written.
		same("SELECT sum(length(sum(length('a'))::text))"),

		// A function the statement also calls inside one of the port's
		// macros is cast nowhere in it; the others are.
		{"SELECT _bintrail_pg_round_k(sum(i), 2, 0.01), length(t), _bintrail_pg_epoch(ts), sum(s) FROM v",
			"SELECT _bintrail_pg_round_k(sum(i), 2, 0.01), CAST(length(t) AS INTEGER), _bintrail_pg_epoch(ts), sum(s) FROM v"},
		{"SELECT length(t), _bintrail_pg_round_div(1, length(t), 2), strpos(t, 'a') FROM v GROUP BY length(t), strpos(t, 'a')",
			"SELECT length(t), _bintrail_pg_round_div(1, length(t), 2), CAST(strpos(t, 'a') AS INTEGER) FROM v GROUP BY length(t), CAST(strpos(t, 'a') AS INTEGER)"},

		// Not in the table: what PostgreSQL types them as is not a narrower
		// whole number, and the engine may read the value again.
		same("SELECT extract(year FROM d), date_part('year', d), NULL, extract(month FROM d) / 3 FROM v GROUP BY 1 HAVING extract(year FROM d) > 2000 ORDER BY date_part('year', d)"),

		// Not a query, or text the port does not read: as written.
		same("SHOW TABLES"),
		same("INSERT INTO v SELECT length(t), NULL FROM v"),
		same("SELECT length(E'a\\n')"),
		same("SELECT length(t"),
		same(""),
	} {
		if got := AsPostgresTypes(c[0], types); got != c[1] {
			t.Errorf("%s\n   got  %s\n   want %s", c[0], got, c[1])
		}
	}
}

// A Describe asks for the statement inside a subquery: it gets the casts the
// statement gets when it runs, its own select list still the statement's.
func TestAsPostgresTypes_describe(t *testing.T) {
	types := map[string][]string{"i": {"INTEGER"}}
	for _, stmt := range []string{"SELECT length(t), sum(i), length(t) + 1 FROM v", "SELECT 1"} {
		want := describeHead + AsPostgresTypes(stmt, types) + describeTail
		if got := AsPostgresTypes(describeHead+stmt+describeTail, types); got != want {
			t.Errorf("got  %s\nwant %s", got, want)
		}
	}
	if got := AsPostgresTypes(describeHead+"SELECT length(t) + 1 FROM v"+describeTail, nil); !strings.Contains(got, "CAST(length(t) AS INTEGER) + 1") {
		t.Errorf("a described statement's own list was read as a subquery's: %s", got)
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
	asPostgresTypesOf("SELECT length(t), sum(2), sum(count(*)) FROM v", "", read)
	if asked != 0 {
		t.Errorf("the column types were read %d times for a statement without a sum", asked)
	}
	asPostgresTypesOf("SELECT sum(i), sum(i + 1), sum(2) FROM v", "", read)
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

// A statement of sums nested in sums is handed over as written: the
// argument of a sum is written twice, and so would each one inside it be.
func TestAsPostgresTypes_nestedSumsStaySmall(t *testing.T) {
	stmt := "'a'"
	for range 40 {
		stmt = "sum(length(" + stmt + "))::text"
	}
	stmt = "SELECT " + stmt
	if got := AsPostgresTypes(stmt, nil); got != stmt {
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
	// The bound is of what is really written: as many sums as fit under it
	// are cast, and one more hands the statement over as written.
	each := len("CAST(sum(" + sumGuard("1") + ") AS BIGINT),")
	fit := (maxStatementMessage - len("SELECT 1")) / each
	if got := AsPostgresTypes("SELECT "+strings.Repeat("sum(1),", fit)+"1", nil); len(got) > maxStatementMessage || strings.Count(got, "AS BIGINT") != fit {
		t.Errorf("%d sums, which fit: %d bytes, %d casts", fit, len(got), strings.Count(got, "AS BIGINT"))
	}
	over := "SELECT " + strings.Repeat("sum(1),", fit+1) + "1"
	if got := AsPostgresTypes(over, nil); got != over {
		t.Errorf("%d sums, one too many: %d bytes written", fit+1, len(got))
	}
	deep := "SELECT " + strings.Repeat("length(", maxCastDepth-1) + "'a'" + strings.Repeat("::text)", maxCastDepth-1)
	if got := AsPostgresTypes(deep, nil); strings.Count(got, "AS INTEGER") != maxCastDepth-1 {
		t.Errorf("%d calls one inside another: %d casts", maxCastDepth-1, strings.Count(got, "AS INTEGER"))
	}
}

// The screen's types hold more than columns since #3237: a table's column
// order under a key no statement can write, and a mark on names that only
// may be text. Neither is a whole number to a sum.
func TestWholeType_screenMarks(t *testing.T) {
	types := map[string][]string{
		colsKey + "t": {"INTEGER", "INTEGER"},
		relKey + "t":  {relType},
		"maybe":       {maybeTextType},
		"mixed":       {"INTEGER", maybeTextType},
		"qty":         {"INTEGER"},
	}
	read := func() map[string][]string { return types }
	for expr, want := range map[string]string{
		"maybe": "", "mixed": "", "t": "", "t.maybe": "", "maybe + 1": "", "qty": "int4",
	} {
		toks, why := lexSQL(expr)
		if why != "" {
			t.Fatalf("%s: %s", expr, why)
		}
		if got := wholeType(toks, read); got != want {
			t.Errorf("wholeType(%s) = %q, want %q", expr, got, want)
		}
	}
	for _, key := range []string{colsKey + "t", relKey + "t"} {
		if got := operandType([]rtToken{{kind: rtWord, text: key}}, func() map[string][]string {
			return map[string][]string{key: {relType}}
		}); got != "" {
			t.Errorf("operandType(%q) = %q, want none", key, got)
		}
	}
}

// A prepared statement's sum is decided on the text with its $n: the number
// written in its place says nothing of the argument's declared type.
func TestAsPostgresTypes_heldToTheStatement(t *testing.T) {
	types := func() map[string][]string { return map[string][]string{"qty": {"INTEGER"}, "amount": {"DECIMAL(12,2)"}} }
	for _, tc := range []struct {
		name, screened, filled string
		casts                  int
	}{
		{"argument outside the sum", "SELECT sum(qty) FROM t WHERE id < $1", "SELECT sum(qty) FROM t WHERE id <  9 ", 1},
		{"argument in the condition of a CASE", "SELECT sum(CASE WHEN qty > $1 THEN 1 ELSE 0 END) FROM t", "SELECT sum(CASE WHEN qty >  2  THEN 1 ELSE 0 END) FROM t", 1},
		{"argument with a cast", "SELECT sum(qty + $1::int) FROM t", "SELECT sum(qty +  5 ::int) FROM t", 1},
		{"argument in the value", "SELECT sum(qty + $1) FROM t", "SELECT sum(qty +  5 ) FROM t", 0},
		{"the argument is the value", "SELECT sum($1) FROM t", "SELECT sum( 5 ) FROM t", 0},
		{"one of two sums", "SELECT sum(qty), sum(qty + $1) FROM t", "SELECT sum(qty), sum(qty +  5 ) FROM t", 1},
		{"a string that spells a sum", "SELECT sum(qty), $1 FROM t", "SELECT sum(qty),  'sum(1)'  FROM t", 1},
		{"statement unlike its text", "SELECT sum(qty) FROM t", "SELECT sum(qty), sum(qty) FROM t", 0},
	} {
		for _, wrap := range []bool{false, true} {
			filled := tc.filled
			if wrap {
				filled = describeHead + filled + describeTail
			}
			got := asPostgresTypesOf(filled, tc.screened, types)
			if n := strings.Count(got, "AS BIGINT)"); n != tc.casts {
				t.Errorf("%s (in a Describe: %v): %d casts, want %d: %s", tc.name, wrap, n, tc.casts, got)
			}
		}
	}
}
