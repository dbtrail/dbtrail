package readrouter

import (
	"strings"
	"testing"
)

// #2532: on a connection the copy answers alone, a construct that means one
// thing on MySQL and another on the copy, with no error either way, is
// refused with what to write instead. What is written the copy's way, and
// the differences the documentation lists instead (LIKE and DISTINCT under
// the copy's case rules), is answered.
func TestUnroutedVeto(t *testing.T) {
	refused := map[string]string{
		"select\n  GROUP_CONCAT( a )\nfrom t -- comment": "string_agg",
		"SELECT /*! STRAIGHT_JOIN */ a FROM t":           "executable comment",
		"SELECT /*M!100000 1 */ a FROM t":                "executable comment",
		"SELECT GROUP_CONCAT(name) FROM t":               "string_agg",
		"SELECT WEEK(d) FROM t":                          "weekofyear",
		"SELECT YEARWEEK(d) FROM t":                      "weekofyear",
		"SELECT EXTRACT(WEEK FROM d) FROM t":             "weekofyear",
		"SELECT STR_TO_DATE(s, '%Y') FROM t":             "strptime",
		"SELECT DATEDIFF(a, b) FROM t":                   "date_diff",
		"SELECT TIMEDIFF(a, b) FROM t":                   "end - start",
		"SELECT @@version":                               "variable",
		"SELECT a FROM t WHERE b = @x":                   "variable",
		"SELECT FOUND_ROWS ( )":                          "MySQL",
		"SELECT SQL_CALC_FOUND_ROWS a FROM t":            "MySQL",
		"SELECT MICROSECOND(ts) FROM t":                  "% 1000000",
		"SELECT TIMESTAMPDIFF(DAY, a, b) FROM t":         "date_diff",
		"SELECT CAST(x AS UNSIGNED) FROM t":              "BIGINT",
		"SELECT CAST(x AS SIGNED) FROM t":                "BIGINT",
		"SELECT a DIV b FROM t":                          "//",
		"SELECT FOUND_ROWS()":                            "MySQL",
		"SELECT DATABASE()":                              "current_database",
		"SELECT VERSION()":                               "pragma_version",
		"SELECT CURRENT_USER":                            "MySQL",
		"SELECT @x":                                      "variable",
		"SELECT a FROM t FOR UPDATE":                     "lock",
		"SELECT a FROM t LOCK IN SHARE MODE":             "lock",
		"SELECT a FROM t INTO OUTFILE '/tmp/x'":          "OUTFILE",
		"SELECT * FROM t WHERE MATCH(a) AGAINST ('x')":   "full-text",
		"SELECT * FROM t WHERE BINARY a = 'x'":           "BINARY",
		"SELECT * FROM t WHERE a REGEXP 'x'":             "regexp_matches",
		"SELECT * FROM t WHERE a RLIKE 'x'":              "regexp_matches",
		"SELECT 0x10":                                    "hexadecimal",
		"SELECT x'41'":                                   "hexadecimal",
		"SELECT b'101'":                                  "hexadecimal",
		"SELECT a FROM t LIMIT 10, 5":                    "OFFSET",
		"SELECT _binary'x'":                              "_binary",
		"SELECT DAYOFWEEK(d) FROM t":                     "isodow",
		"SELECT WEEKDAY(d) FROM t":                       "isodow",
	}
	for stmt, instead := range refused {
		r, ok := UnroutedVeto(stmt)
		if !ok {
			t.Errorf("%q answered, want it refused", stmt)
			continue
		}
		if r.What == "" || r.Instead == "" || !strings.Contains(r.What+" "+r.Instead, instead) {
			t.Errorf("%q: refusal %+v, want it to mention %q", stmt, r, instead)
		}
	}
	answered := []string{
		// The copy's own way of writing things.
		`SELECT "order", "group" FROM "my table"`,
		"SELECT a || b FROM t",
		"SELECT 2 ^ 3",
		"SELECT x::INTEGER FROM t",
		"SELECT * FROM t WHERE name ILIKE '%x%'",
		"SELECT * FROM t WHERE name LIKE '%ORIGINAL%'",
		"SELECT COUNT(DISTINCT name), COUNT(DISTINCT ol_w_id) FROM t",
		"SELECT string_agg(name, ',') FROM t",
		"SELECT get_current_timestamp(), today(), epoch(d), to_timestamp(1)",
		"SELECT weekofyear(d), isodow(d) % 7 + 1, strptime(s, '%Y'), date_diff('day', a, b) FROM t",
		"SELECT a // b, CAST(x AS BIGINT) FROM t",
		"SELECT current_database(), current_schema()",
		"SELECT regexp_matches(a, 'x', 'i') FROM t",
		"SELECT d + INTERVAL '1 day', d - INTERVAL 2 HOUR, d1 - d2 FROM t",
		"SELECT a & b, a | b, a >> 1, ~a FROM t",
		"SELECT a FROM t UNION SELECT a FROM u",
		"WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r",
		"SELECT j->>'$.a', json_extract(j, '$.b') FROM t",
		"SELECT name COLLATE nocase FROM t",
		"SELECT $$it's$$",
		"SELECT E'a\\nb'",
		"SELECT 'C:\\dir'",
		"SELECT a FROM t -- DIV in a comment",
		"SELECT a FROM t --GROUP_CONCAT( in a comment the copy's way",
		"SELECT a FROM t /* WEEK(d) */",
		"SELECT /*+ hint */ a FROM t",
		"SELECT DATE '2026-01-15', TIMESTAMP '2026-01-15 10:00:00'",
		"SELECT * FROM information_schema.tables",
		"SELECT a FROM t LIMIT 5 OFFSET 10",
		"SELECT 'now()', 'GROUP_CONCAT(x)', 'a DIV b'",
		`SELECT "now" FROM t`,
		"SELECT d FROM t WHERE d = '2026-01-15'",
		"SELECT 1e5, 1.5, 10",
		"SELECT * FROM t WHERE a = ?",
		"SELECT position('a' IN b), instr(b, 'a') FROM t",
		"SELECT a FROM t ORDER BY a NULLS LAST",
		// Found by review: the copy's SQL a dashboard writes.
		"SELECT count(*) AS row_count FROM t",
		"SELECT s.row_count, found_rows, last_insert_id FROM s",
		"SELECT * FROM t WHERE code = '10-20-30' OR d = '21-04-2026' OR d = '26-01-15'",
		"SELECT datediff('day', a, b) FROM t",
		"SELECT [1, 2] @> [1], [1] <@ [1, 2], 'abc' ^@ 'a'",
		"SELECT 1_000_000",
		"SELECT 'it''s /*! not a comment */' FROM t",
		"SELECT a FROM t -- /*! in a comment",
		"SELECT 'user@example.com' AS email",
		// The clock answers in the connection's zone (#2035).
		"SELECT NOW(), current_date, current_timestamp",
		// A backslash is a character in the copy's strings.
		`SELECT replace(p, '\', '/') FROM f WHERE q = 'a/b'`,
		`SELECT 'x\' AS a, b FROM t`,
	}
	for _, stmt := range answered {
		if r, ok := UnroutedVeto(stmt); ok {
			t.Errorf("%q refused (%+v), want it answered", stmt, r)
		}
	}
}

// The message names the construct, what to write instead and the other way
// out, in one sentence a client shows as it is.
func TestUnroutedRefusal_message(t *testing.T) {
	r, ok := UnroutedVeto("SELECT a DIV 2 FROM t")
	if !ok {
		t.Fatal("DIV answered")
	}
	msg := r.Message()
	for _, want := range []string{r.What, r.Instead, "DuckDB", "Engine Routing"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message %q does not say %q", msg, want)
		}
	}
	t.Log(msg)
}
