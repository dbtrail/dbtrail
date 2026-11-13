package readrouter

import (
	"strings"
	"testing"
)

// #2628: on the port without routing, MySQL's LIMIT offset, count is written
// LIMIT count OFFSET offset for the copy, which only reads that spelling.
// Measured on MySQL 8.0.46 and DuckDB 1.5.6: the two answer the same rows for
// offsets of 0, past the last row and up to 9223372036854775807, for a count
// of 0, for leading zeros (007 is 7 on both), and for a LIMIT in a subquery,
// a derived table, a CTE and either side of a UNION. Past 9223372036854775807
// MySQL still answers and the copy fails, so such a LIMIT is not rewritten.
func TestForCopyLimitOffsetComma(t *testing.T) {
	on := CopyOptions{LimitOffsetComma: true}
	rewritten := map[string]string{
		"SELECT a FROM t LIMIT 10, 5":                                       "SELECT a FROM t LIMIT 5 OFFSET 10",
		"SELECT a FROM t LIMIT 10,5":                                        "SELECT a FROM t LIMIT 5 OFFSET 10",
		"select a from t limit 0, 2":                                        "select a from t LIMIT 2 OFFSET 0",
		"SELECT a FROM t LIMIT 2, 0":                                        "SELECT a FROM t LIMIT 0 OFFSET 2",
		"SELECT a FROM t LIMIT\n1\n,\n2":                                    "SELECT a FROM t LIMIT 2 OFFSET 1",
		"SELECT a FROM t LIMIT\t1 ,\t2;":                                    "SELECT a FROM t LIMIT 2 OFFSET 1;",
		"SELECT a FROM t LIMIT 007, 010":                                    "SELECT a FROM t LIMIT 10 OFFSET 7",
		"SELECT a FROM t LIMIT 000, 00":                                     "SELECT a FROM t LIMIT 0 OFFSET 0",
		"SELECT a FROM t LIMIT 1, 9223372036854775807":                      "SELECT a FROM t LIMIT 9223372036854775807 OFFSET 1",
		"SELECT a FROM t LIMIT 9223372036854775807, 1":                      "SELECT a FROM t LIMIT 1 OFFSET 9223372036854775807",
		"SELECT a FROM t LIMIT 1, 0009223372036854775807":                   "SELECT a FROM t LIMIT 9223372036854775807 OFFSET 1",
		"SELECT `a` FROM `t` LIMIT 1, 2":                                    `SELECT "a" FROM "t" LIMIT 2 OFFSET 1`,
		"SELECT a FROM t LIMIT 1, 2 /* page */":                             "SELECT a FROM t LIMIT 2 OFFSET 1 /* page */",
		"SELECT a FROM t LIMIT 1, 2 -- page":                                "SELECT a FROM t LIMIT 2 OFFSET 1 -- page",
		"SELECT a FROM t LIMIT 1, 2 LOCK IN SHARE MODE":                     "SELECT a FROM t LIMIT 2 OFFSET 1 LOCK IN SHARE MODE",
		"SELECT a FROM t LIMIT 1, 2 UNION ALL SELECT 1":                     "SELECT a FROM t LIMIT 2 OFFSET 1 UNION ALL SELECT 1",
		"SELECT a FROM t LIMIT 1, 2 FOR UPDATE":                             "SELECT a FROM t LIMIT 2 OFFSET 1 FOR UPDATE",
		"SELECT * FROM (SELECT a FROM t LIMIT 1,2) d":                       "SELECT * FROM (SELECT a FROM t LIMIT 2 OFFSET 1) d",
		"WITH c AS (SELECT a FROM t LIMIT 3, 5) SELECT * FROM c LIMIT 0, 1": "WITH c AS (SELECT a FROM t LIMIT 5 OFFSET 3) SELECT * FROM c LIMIT 1 OFFSET 0",
		"(SELECT a FROM t LIMIT 1,1) UNION ALL (SELECT a FROM u LIMIT 0,1)": "(SELECT a FROM t LIMIT 1 OFFSET 1) UNION ALL (SELECT a FROM u LIMIT 1 OFFSET 0)",
		"SELECT a FROM t WHERE s = 'LIMIT 1, 2' LIMIT 3, 4":                 "SELECT a FROM t WHERE s = 'LIMIT 1, 2' LIMIT 4 OFFSET 3",
		// The arguments of a prepared statement, as interpolate writes them.
		"SELECT a FROM t LIMIT  20 ,  10 ": "SELECT a FROM t LIMIT 10 OFFSET 20 ",
	}
	for stmt, want := range rewritten {
		got, why := ForCopyOpts(stmt, on)
		if why != "" || got != want {
			t.Errorf("ForCopyOpts(%q) = %q, %q; want %q", stmt, got, why, want)
		}
	}
	// Left as written: not two plain whole numbers the copy reads as
	// MySQL does, or not a LIMIT at all. UnroutedLimitVeto refuses what is
	// left with a comma.
	kept := []string{
		"SELECT a FROM t LIMIT 1 /* c */ , 2",           // a comment inside: where it ends is not rewritten around
		"SELECT a FROM t LIMIT 1, 2, 3",                 // a syntax error on MySQL
		"SELECT a FROM t LIMIT 1,2e3",                   // likewise
		"SELECT a FROM t LIMIT 1, 2.5",                  // likewise; the copy would read 2.5
		"SELECT a FROM t LIMIT 1.5, 2",                  // likewise
		"SELECT a FROM t LIMIT -1, 2",                   // likewise
		"SELECT a FROM t LIMIT NULL, 2",                 // a prepared statement's NULL
		"SELECT a FROM t LIMIT '1', 2",                  // a prepared statement's string
		"SELECT a FROM t LIMIT ?, ?",                    // placeholders: the arguments are not here yet
		"SELECT a FROM t LIMIT 1, 9223372036854775808",  // past the copy's 64-bit integer
		"SELECT a FROM t LIMIT 9223372036854775808, 1",  // likewise
		"SELECT a FROM t LIMIT 1, 18446744073709551615", // MySQL's own largest
		"SELECT a FROM t LIMIT 1, 2UNION SELECT 1",      // a longer word after the count
		"SELECT a FROM t LIMIT 1, 2 ROWS",               // would be OFFSET 1 ROWS on the copy
		"SELECT a FROM t LIMIT 5, 2 IS NULL",            // would be OFFSET 5 IS NULL
		"SELECT a FROM t LIMIT 1, 2 AND 0",              // likewise
		"SELECT a FROM t LIMIT 1, 2 x",                  // a syntax error on MySQL
		"SELECT a FROM t LIMIT\u00a01, 2",               // a no-break space: part of a name on MySQL
		"SELECT a FROM t LIMIT 5 OFFSET 10",
		"SELECT a FROM t LIMIT 5",
		"SELECT a, b FROM t LIMIT 5",
		"SELECT `limit 1, 2` FROM t",
		"SELECT 1 /* LIMIT 1, 2 */",
		"SELECT a FROM t LIMIT",
		"SELECT a FROM t LIMIT 1,",
		"SELECT a FROM t LIMIT 1, ",
		"",
	}
	for _, stmt := range kept {
		want, _ := ForCopy(stmt)
		if got, _ := ForCopyOpts(stmt, on); got != want {
			t.Errorf("ForCopyOpts(%q) = %q; want it as ForCopy writes it, %q", stmt, got, want)
		}
	}
	// Off, the default (the routed ladder and sql-compare): left alone.
	if got, _ := ForCopy("SELECT a FROM t LIMIT 10, 5"); got != "SELECT a FROM t LIMIT 10, 5" {
		t.Errorf("ForCopy rewrote the LIMIT: %q", got)
	}
	// With the other rewrites in the same statement.
	got, _ := ForCopyOpts("SELECT a FROM t STRAIGHT_JOIN u ON u.id = t.id WHERE b LIKE 'x%' LIMIT 4, 2", CopyOptions{LimitOffsetComma: true, LikeIgnoresCase: true})
	if want := `SELECT a FROM t JOIN u ON u.id = t.id WHERE b ILIKE 'x%' LIMIT 2 OFFSET 4`; got != want {
		t.Errorf("with STRAIGHT_JOIN and LIKE: %q, want %q", got, want)
	}
}

// What is still MySQL's LIMIT offset, count in the text sent to the copy is
// refused with how to write it: the copy fails on it with its own parser
// error ("errhint NOT IMPLEMENTED" on DuckDB 1.5.6).
func TestUnroutedLimitVeto(t *testing.T) {
	refused := []string{
		"SELECT a FROM t LIMIT 10, 5",
		"SELECT a FROM t limit\n1\n,2",
		"SELECT a FROM t LIMIT 1 /* c */ , 2",
		"SELECT a FROM t LIMIT -1, 2",
		"SELECT a FROM t LIMIT NULL, 2",
		"SELECT a FROM t LIMIT '1', 2",
		"SELECT a FROM t LIMIT ?, ?",
		"SELECT a FROM t LIMIT 1, 18446744073709551615",
		"SELECT a FROM t LIMIT 1.5, 2",
		`SELECT "a" FROM "t" LIMIT 1, 2`,
		"SELECT a FROM t WHERE s = 'C:\\' LIMIT 1, 2",
	}
	for _, stmt := range refused {
		r, ok := UnroutedLimitVeto(stmt)
		if !ok || !strings.Contains(r.Message(), "LIMIT count OFFSET offset") {
			t.Errorf("%q: %+v, %v; want it refused with the copy's spelling", stmt, r, ok)
		}
	}
	answered := []string{
		"SELECT a FROM t LIMIT 5 OFFSET 10",
		"SELECT a, b FROM t LIMIT 5",
		"SELECT a FROM t LIMIT 5 UNION ALL SELECT a, b FROM u",
		"SELECT 'LIMIT 1, 2'",
		`SELECT "limit 1, 2" FROM t`,
		"SELECT 1 -- LIMIT 1, 2",
		"SELECT 1 /* LIMIT 1, 2 */",
		"SELECT t.limit, b FROM t",
		"",
	}
	for _, stmt := range answered {
		if r, ok := UnroutedLimitVeto(stmt); ok {
			t.Errorf("%q refused: %+v", stmt, r)
		}
	}
}

// MySQL refuses a LIMIT in an IN, ALL, ANY or SOME subquery with error 1235
// ("This version of MySQL doesn't yet support 'LIMIT & IN/ALL/ANY/SOME
// subquery'"), and the copy answers it. Measured on MySQL 8.0.46: the LIMIT
// of the subquery's own query, also after a UNION in it, inside a second pair
// of parentheses, or after a WITH; not one in a derived table or a CTE inside
// it, nor a scalar subquery, nor EXISTS.
func TestUnroutedVetoLimitInSetSubquery(t *testing.T) {
	refused := []string{
		"SELECT a FROM t WHERE a IN (SELECT a FROM u LIMIT 2)",
		"SELECT a FROM t WHERE a NOT IN (SELECT a FROM u ORDER BY a LIMIT 2)",
		"select a from t where a in\n(select a from u limit 2)",
		"SELECT a FROM t WHERE a IN ((SELECT a FROM u LIMIT 2))",
		"SELECT a FROM t WHERE a IN (SELECT * FROM (SELECT a FROM u) d LIMIT 2)",
		"SELECT a FROM t WHERE a = ANY (SELECT a FROM u LIMIT 2)",
		"SELECT a FROM t WHERE a = SOME (SELECT a FROM u LIMIT 2)",
		"SELECT a FROM t WHERE a > ALL (SELECT a FROM u LIMIT 2)",
		"SELECT a FROM t WHERE a <>ALL(SELECT a FROM u LIMIT 2)",
		"SELECT a FROM t WHERE (a, b) IN (SELECT a, b FROM u LIMIT 2)",
		"SELECT a FROM t WHERE a IN (SELECT a FROM u UNION SELECT a FROM v LIMIT 2)",
		"SELECT a FROM t WHERE a IN (SELECT a FROM u WHERE b IN (1, 2) LIMIT 2)",
		"SELECT a FROM t WHERE a IN (WITH c AS (SELECT a FROM u) SELECT a FROM c LIMIT 3)",
		"SELECT a IN (SELECT a FROM u LIMIT 1) FROM t",
		"SELECT a FROM t WHERE a IN (SELECT a FROM u) AND b IN (SELECT b FROM v LIMIT 1)",
		"SELECT a FROM t WHERE a IN (SELECT a FROM u LIMIT 1, 2)",
		"SELECT a FROM t WHERE `a` IN (SELECT `a` FROM `u` LIMIT 2)",
		"SELECT a AS `x-- y` FROM t WHERE a IN (SELECT a FROM u LIMIT 2)", // a -- inside a name cuts nothing
	}
	for _, stmt := range refused {
		r, ok := UnroutedVeto(stmt)
		if !ok || !strings.Contains(r.Message(), "1235") || !strings.Contains(r.Message(), "derived table") {
			t.Errorf("%q: %+v, %v; want the 1235 refusal", stmt, r, ok)
		}
		if strings.Contains(r.Message(), "Engine Routing") {
			t.Errorf("%q: %q offers Engine Routing, where MySQL refuses it too", stmt, r.Message())
		}
	}
	answered := []string{
		"SELECT a FROM t WHERE a IN (SELECT a FROM (SELECT a FROM u LIMIT 2) d)",
		"SELECT a FROM t WHERE a IN (WITH c AS (SELECT a FROM u LIMIT 3) SELECT a FROM c)",
		"SELECT a FROM t WHERE a IN (SELECT a FROM u WHERE b = (SELECT 1 LIMIT 1))",
		"SELECT a FROM t WHERE a = (SELECT a FROM u LIMIT 1)",
		"SELECT a FROM t WHERE EXISTS (SELECT a FROM u LIMIT 1)",
		"SELECT a FROM t WHERE a IN (SELECT a FROM u) LIMIT 2",
		"SELECT a FROM t WHERE a IN (1, 2) LIMIT 2",
		"SELECT a FROM t UNION ALL (SELECT a FROM u LIMIT 2)",
		"SELECT a FROM t UNION ALL(SELECT a FROM u LIMIT 2)",
		"SELECT a FROM t WHERE s = 'IN (SELECT a LIMIT 1)'",
		"SELECT a FROM t WHERE a IN (SELECT a FROM u) -- IN (SELECT a LIMIT 1)",
		"SELECT POSITION('a' IN s) FROM t LIMIT 1",
		"SELECT ANY_VALUE(a) FROM t LIMIT 1",
		"SELECT `in`(a) FROM t LIMIT 1",
	}
	for _, stmt := range answered {
		if r, ok := UnroutedVeto(stmt); ok {
			t.Errorf("%q refused: %+v", stmt, r)
		}
	}
}
