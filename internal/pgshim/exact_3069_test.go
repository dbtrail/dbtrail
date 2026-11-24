package pgshim

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// The screen's rules of #3069, #3071 and #3072. What a test says
// PostgreSQL 16 and the copy answer was measured by internal/pgcompare
// (screenExactness), over the table these types are of.

// exactTypes are the copy's types for pgcompare's pgcmp_exact, as the copy
// names them.
var exactTypes = map[string][]string{
	"id": {"INTEGER"}, "rl": {"FLOAT"}, "dbl": {"DOUBLE"}, "amt": {"DECIMAL(10,2)"}, "amt4": {"DECIMAL(10,4)"},
	"whole": {"INTEGER"}, "word": {"VARCHAR"}, "day": {"DATE"}, "stamp": {"TIMESTAMP"}, "clock": {"TIME"},
	"seen": {"TIMESTAMP WITH TIME ZONE"},
}

// checkExactness holds the screen to both sides of a rule: every statement of
// toSource is vetoed with a reason that holds its text, and every statement
// of stays is left to the copy, by the screen that knows the types and by the
// one that runs before them (screenUntyped), which may not veto what the
// typed one lets through.
func checkExactness(t *testing.T, types map[string][]string, toSource map[string]string, stays []string) {
	t.Helper()
	for q, want := range toSource {
		if why := CopyScreen(q, types); !strings.Contains(why, want) {
			t.Errorf("%s:\n  got %q, want it sent to the source for %q", q, why, want)
		}
	}
	for _, q := range stays {
		if why := CopyScreen(q, types); why != "" {
			t.Errorf("%s:\n  sent to the source (%s), want the copy", q, why)
		}
		toks, lexWhy := lexSQL(RewriteForCopy(q))
		if lexWhy != "" {
			t.Errorf("%s: not read: %s", q, lexWhy)
			continue
		}
		if why := screenUntyped(toks); why != "" {
			t.Errorf("%s:\n  vetoed without its types (%s), so it never reaches the plan", q, why)
		}
	}
}

// --- #3071 ---------------------------------------------------------------------

func TestCopyScreen_substringStart(t *testing.T) {
	const x = "pgcmp_exact"
	// substring('apple', -1, 3) is 'a' on PostgreSQL and 'e' on the copy.
	checkExactness(t, exactTypes, map[string]string{
		`SELECT substring('apple', -1, 3)`:                          substringVeto,
		`SELECT id, substring(word, -1, 3) FROM ` + x:               substringVeto,
		`SELECT id, substring(word, -2) FROM ` + x:                  substringVeto,
		`SELECT id, substring(word, 0, 3) FROM ` + x:                substringVeto,
		`SELECT id, substring(word, 00) FROM ` + x:                  substringVeto,
		`SELECT id, substring(word FROM -2 FOR 4) FROM ` + x:        substringVeto,
		`SELECT id, substring(word FROM 0) FROM ` + x:               substringVeto,
		`SELECT id, substring(word, whole, 2) FROM ` + x:            substringVeto,
		`SELECT id, substring(word, 1 - whole, 3) FROM ` + x:        substringVeto,
		`SELECT id, substring(word, 2, whole) FROM ` + x:            substringVeto,
		`SELECT id, substring(word, 2, -1) FROM ` + x:               substringVeto,
		`SELECT id, substring(word, 2, 0) FROM ` + x:                substringVeto,
		`SELECT id, substring(word, 1.5, 2) FROM ` + x:              substringVeto,
		`SELECT id, substring(word, (2), 2) FROM ` + x:              substringVeto,
		`SELECT id, substring(word, $1, 2) FROM ` + x:               substringVeto,
		`SELECT id, substring(word, '2', 2) FROM ` + x:              substringVeto,
		`SELECT id, substring(word FROM whole FOR 2) FROM ` + x:     substringVeto,
		`SELECT id, substring(word FROM 2 FOR -1) FROM ` + x:        substringVeto,
		`SELECT id, substring(word FOR 2) FROM ` + x:                substringVeto,
		`SELECT id, substring(word FROM 'a.') FROM ` + x:            substringVeto,
		`SELECT id, substring(word) FROM ` + x:                      substringVeto,
		`SELECT id, substring(word, 1, 2, 3) FROM ` + x:             substringVeto,
		`SELECT id, SUBSTRING(word , - 1 , 3) FROM ` + x:            substringVeto,
		`SELECT id, substring(substring(word, -1), 1, 2) FROM ` + x: substringVeto,
		// Never the copy's, whatever they are given.
		`SELECT id, left(word, 2) FROM ` + x:                                "the function left()",
		`SELECT id, right(word, -2) FROM ` + x:                              "the function right()",
		`SELECT id, overlay(word placing 'XX' from 2 for 1) FROM ` + x + ``: "the function overlay()",
	}, []string{
		`SELECT id, substring(word, 1, 3) FROM ` + x,
		`SELECT id, substring(word, 2) FROM ` + x,
		`SELECT id, substring(word, 4, 100) FROM ` + x,
		`SELECT id, substring(word FROM 2 FOR 3) FROM ` + x,
		`SELECT id, substring(word from 3) FROM ` + x,
		`SELECT id, SUBSTRING ( word , 01 , 3 ) FROM ` + x,
		`SELECT substring('apple', 2, 3), substring('apple' FROM 5)`,
		`SELECT id, substring(substring(word, 2), 1, 2) FROM ` + x,
		`SELECT id, substring(x.word, 1, 3) FROM ` + x + ` x`,
		`SELECT id, substring(coalesce(word, '-'), 1, 3) FROM ` + x,
	})
}

func TestSubstringOK(t *testing.T) {
	// A call with no closing parenthesis has no arguments to judge.
	if substringOK(nil) {
		t.Error("no arguments: taken as measured")
	}
	if substringOK([][]rtToken{{}, {{rtNumber, "1"}}}) {
		t.Error("an empty first argument: taken as measured")
	}
}

// --- #3072 ---------------------------------------------------------------------

func TestStrictTyped(t *testing.T) {
	strict := map[string][]string{
		"date":      {"2026-01-05", "1999-12-31", "0026-01-05", "9999-12-31"},
		"timestamp": {"2026-01-05", "2026-01-05 10:30:00", "2026-01-05T10:30:00", "2026-01-05 10:30", "2026-01-05 23:59:59.999999", "2026-01-05 00:00:00.1"},
		"time":      {"10:30:00", "10:30", "00:00:00", "23:59:59.999999", "23:59:59.5"},
		// An interval is not judged.
		"interval": {"1 day", "24:00:00", "26-01-05", ""},
	}
	for typ, texts := range strict {
		for _, text := range texts {
			if !strictTyped(typ, text) {
				t.Errorf("%s '%s': not taken as the strict form", typ, text)
			}
		}
	}
	// PostgreSQL 16 reads '26-01-05' as 2026-01-05, rounds a seventh decimal
	// and takes 24:00:00 as a time; the copy reads the year 26, cuts the
	// decimal, and answers midnight.
	loose := map[string][]string{
		"date": {"26-01-05", "2026-1-5", "2026-01-5", "20260105", "2026/01/05", "01-05-2026", "Jan 5 2026", "2026-01-05 10:30:00",
			" 2026-01-05", "2026-01-05 ", "epoch", "today", "infinity", "", "12026-01-05", "2026-01-05 BC"},
		"timestamp": {"26-01-05 10:30:00", "2026-01-05 10:30:00.1234567", "2026-01-05 24:00:00", "2026-01-05 10:60:00", "2026-01-05 10:30:60",
			"2026-01-05 1:30:00", "2026-01-05  10:30:00", "2026-01-05 10", "2026-01-05 10:30:00.", "now", "", "2026-01-05 10:30:00 AM"},
		"time": {"24:00:00", "24:00", "10:30:00.1234567", "1:30:00", "10:60:00", "23:59:60", "10", "103000", "10:30 PM", "", "allballs"},
	}
	for typ, texts := range loose {
		for _, text := range texts {
			if strictTyped(typ, text) {
				t.Errorf("%s '%s': taken as the strict form", typ, text)
			}
		}
	}
}

func TestCopyScreen_strictLiterals(t *testing.T) {
	const x = "pgcmp_exact"
	checkExactness(t, exactTypes, map[string]string{
		`SELECT '26-01-05'::date`:                                                   looseTypedVeto,
		`SELECT DATE '26-01-05'`:                                                    looseTypedVeto,
		`SELECT date '26-01-05'`:                                                    looseTypedVeto,
		`SELECT CAST('26-01-05' AS date)`:                                           looseTypedVeto,
		`SELECT TIMESTAMP '2026-01-05 10:30:00.1234567'`:                            looseTypedVeto,
		`SELECT TIME '24:00:00'`:                                                    looseTypedVeto,
		`SELECT TIME '10:30:00.1234567'`:                                            looseTypedVeto,
		`SELECT TIMESTAMP '2026-01-05 24:00:00'`:                                    looseTypedVeto,
		`SELECT DATE 'today', DATE '2026-01-05'`:                                    looseTypedVeto,
		`SELECT DATE '2026-01-05', DATE '2026-1-5'`:                                 looseTypedVeto,
		`SELECT id FROM ` + x + ` WHERE day >= DATE '26-01-05'`:                     looseTypedVeto,
		`SELECT id FROM ` + x + ` WHERE day IN (DATE '2026-01-05', '26-1-5'::date)`: looseTypedVeto,
		// A string beside a date, timestamp or time column is a literal of
		// the column's type.
		`SELECT id FROM ` + x + ` WHERE day = '26-01-05'`:                          looseTypedVeto,
		`SELECT id FROM ` + x + ` WHERE '26-01-05' = day`:                          looseTypedVeto,
		`SELECT id FROM ` + x + ` x WHERE x.day <> '26-01-05'`:                     looseTypedVeto,
		`SELECT id FROM ` + x + ` x WHERE '26-01-05' < x.day`:                      looseTypedVeto,
		`SELECT id FROM ` + x + ` WHERE day IN ('26-01-05', '2026-02-10')`:         looseTypedVeto,
		`SELECT id FROM ` + x + ` WHERE day NOT IN ('2026-02-10', '26-01-05')`:     looseTypedVeto,
		`SELECT id FROM ` + x + ` WHERE day BETWEEN '26-01-01' AND '2026-12-31'`:   looseTypedVeto,
		`SELECT id FROM ` + x + ` WHERE day BETWEEN '2026-01-01' AND '26-12-31'`:   looseTypedVeto,
		`SELECT id FROM ` + x + ` WHERE stamp = '2026-01-05 10:30:00.1234567'`:     looseTypedVeto,
		`SELECT id FROM ` + x + ` WHERE clock < '24:00:00'`:                        looseTypedVeto,
		`SELECT id FROM ` + x + ` WHERE day = '2026-01-05 10:30:00'`:               looseTypedVeto,
		`SELECT id FROM ` + x + ` WHERE day = '2026-01-05' AND stamp > '26-01-05'`: looseTypedVeto,
		`SELECT TIMESTAMP '2026-03-02 10:00:00+02'`:                                zonedTypedVeto,
		`SELECT id FROM ` + x + ` WHERE stamp = '2026-03-02 10:00:00+02'`:          zonedCompareVeto,
	}, []string{
		`SELECT DATE '2026-01-05', '2026-01-05'::date, CAST('2026-01-05' AS date)`,
		`SELECT TIMESTAMP '2026-01-05 10:30:00.123456', TIMESTAMP '2026-01-05 23:59:59', TIMESTAMP '2026-01-05', TIMESTAMP '2026-01-05T10:30:00'`,
		`SELECT TIMESTAMP '2026-01-05 10:30', TIME '10:30'`,
		`SELECT TIME '23:59:59.999999', TIME '00:00:00'`,
		`SELECT id FROM ` + x + ` WHERE day = '2026-01-05' OR day IN ('2026-02-10', '2026-12-31')`,
		`SELECT id FROM ` + x + ` WHERE day BETWEEN DATE '2026-01-01' AND '2026-01-31'`,
		`SELECT id FROM ` + x + ` WHERE stamp >= '2026-01-05' AND stamp < '2026-01-06 00:00:00' AND stamp <> TIMESTAMP '2026-01-05 10:30:00.123456'`,
		`SELECT id FROM ` + x + ` WHERE clock < '12:00:00' OR clock = TIME '23:59:59'`,
		`SELECT id FROM ` + x + ` x WHERE x.day >= '2026-01-01' AND x.stamp NOT IN ('2026-01-05 10:30:00')`,
		// An interval, and a string beside a column of another type, are not
		// date or time literals.
		`SELECT id FROM ` + x + ` WHERE stamp > TIMESTAMP '2026-01-05 00:00:00' - INTERVAL '26-01 05'`,
		`SELECT id FROM ` + x + ` WHERE word = '26-01-05' OR word IN ('24:00:00')`,
		`SELECT id FROM ` + x + ` WHERE whole = '5'`,
		`SELECT id, '26-01-05' AS label FROM ` + x,
		// A column with a time zone reads its strings by its own rules.
		`SELECT id FROM ` + x + ` WHERE seen = '2026-01-05 10:30:00+00'`,
	})
	// A column of that name that is a date in one table and text in another
	// is not known to take the string as a date.
	mixed := map[string][]string{"day": {"DATE", "VARCHAR"}, "id": {"INTEGER"}}
	if why := CopyScreen(`SELECT id FROM t WHERE day = '26-01-05'`, mixed); why != "" {
		t.Errorf("a name that is a date and text: sent to the source (%s)", why)
	}
}

// --- what kind of number a column is ------------------------------------------

func TestTypeNum(t *testing.T) {
	for ty, want := range map[string]numValue{
		"INTEGER": {kind: numScaled}, "BIGINT": {kind: numScaled}, "SMALLINT": {kind: numScaled},
		"DECIMAL(10,2)": {kind: numScaled, scale: 2}, "decimal(38, 0)": {kind: numScaled}, "NUMERIC(12,4)": {kind: numScaled, scale: 4},
		unsizedNumeric: {kind: numAnyScale}, "DECIMAL": {kind: numAnyScale},
		"DOUBLE": {kind: numDouble}, "FLOAT": {kind: numReal}, "REAL": {kind: numReal}, "FLOAT4": {kind: numReal},
		"VARCHAR": {kind: numNot}, "DATE": {kind: numNot}, "BOOLEAN": {kind: numNot}, "TIMESTAMP": {kind: numNot}, relType: {kind: numNot},
	} {
		if got := typeNum(ty); got != want {
			t.Errorf("typeNum(%q) = %+v, want %+v", ty, got, want)
		}
	}
}

// --- #3069 ---------------------------------------------------------------------

func TestCopyScreen_realArithmetic(t *testing.T) {
	const x = "pgcmp_exact"
	// r + 1 with r = -0.1 is 0.8999999985098839 on PostgreSQL and 0.9 on the
	// copy; sum(r) is 4.4 there and 4.399999998509884 here.
	checkExactness(t, exactTypes, map[string]string{
		`SELECT id, rl + 1 FROM ` + x:                                            realArithmeticVeto,
		`SELECT id, 1 + rl FROM ` + x:                                            realArithmeticVeto,
		`SELECT id, rl - 1, rl FROM ` + x:                                        realArithmeticVeto,
		`SELECT id, rl * 2 FROM ` + x:                                            realArithmeticVeto,
		`SELECT id, rl / 2 FROM ` + x:                                            realArithmeticVeto,
		`SELECT id, rl * whole FROM ` + x:                                        realArithmeticVeto,
		`SELECT id, whole * rl FROM ` + x:                                        realArithmeticVeto,
		`SELECT id, rl + amt FROM ` + x:                                          realArithmeticVeto,
		`SELECT id, rl + dbl FROM ` + x:                                          realArithmeticVeto,
		`SELECT id, x.rl + 1 FROM ` + x + ` x`:                                   realArithmeticVeto,
		`SELECT id, 2 * x.rl FROM ` + x + ` x`:                                   realArithmeticVeto,
		`SELECT id, "rl" + 1 FROM ` + x:                                          realArithmeticVeto,
		`SELECT id, (rl) + 1 FROM ` + x:                                          realArithmeticVeto,
		`SELECT id, 1 - (rl) FROM ` + x:                                          realArithmeticVeto,
		`SELECT id, abs(rl) * 2 FROM ` + x:                                       realArithmeticVeto,
		`SELECT id, 2 * abs(rl) FROM ` + x:                                       realArithmeticVeto,
		`SELECT id, coalesce(rl, 0) + 1 FROM ` + x:                               realArithmeticVeto,
		`SELECT max(rl) - min(rl) FROM ` + x:                                     realArithmeticVeto,
		`SELECT id, -rl FROM ` + x:                                               realArithmeticVeto,
		`SELECT id FROM ` + x + ` WHERE rl * 2 > 1`:                              realArithmeticVeto,
		`SELECT id FROM ` + x + ` ORDER BY rl + whole`:                           realArithmeticVeto,
		`SELECT s.id, s.v + 1 FROM (SELECT id, rl AS v FROM ` + x + `) s`:        realArithmeticVeto,
		`WITH s AS (SELECT id, rl v FROM ` + x + `) SELECT id, v * 2 FROM s`:     realArithmeticVeto,
		`SELECT id, dbl + 1 + rl FROM ` + x:                                      realArithmeticVeto,
		`SELECT sum(rl) FROM ` + x:                                               realSumVeto,
		`SELECT SUM(x.rl) FROM ` + x + ` x`:                                      realSumVeto,
		`SELECT sum(DISTINCT rl) FROM ` + x:                                      realSumVeto,
		`SELECT sum(abs(rl)) FROM ` + x:                                          realSumVeto,
		`SELECT sum(rl) FILTER (WHERE id > 1) FROM ` + x:                         realSumVeto,
		`SELECT id, sum(rl) OVER (ORDER BY id) FROM ` + x:                        realSumVeto,
		`SELECT sum(CASE WHEN id > 1 THEN rl END) FROM ` + x:                     realSumVeto,
		`SELECT sum(v) FROM (SELECT rl AS v FROM ` + x + `) s`:                   realSumVeto,
		`SELECT whole, sum(rl) FROM ` + x + ` GROUP BY whole HAVING sum(rl) > 1`: realSumVeto,
	}, []string{
		`SELECT id, rl, dbl FROM ` + x,
		`SELECT min(rl), max(rl), count(rl), count(DISTINCT rl) FROM ` + x,
		`SELECT id, abs(rl) FROM ` + x,
		`SELECT id FROM ` + x + ` WHERE rl > 0.1 OR rl = 0.1`,
		`SELECT id FROM ` + x + ` WHERE rl < dbl OR rl = dbl`,
		`SELECT id FROM ` + x + ` WHERE rl > -1 AND rl BETWEEN -1 AND 3`,
		`SELECT id FROM ` + x + ` WHERE rl IS NOT NULL ORDER BY rl DESC, id`,
		`SELECT rl, count(*) FROM ` + x + ` GROUP BY rl`,
		// A double precision, a whole number and a numeric compute the same
		// on both sides, beside a real that is only read.
		`SELECT id, dbl + 1, dbl * 2, dbl / 2, dbl - whole, 1 + dbl FROM ` + x,
		`SELECT sum(dbl), sum(whole), sum(amt) FROM ` + x,
		`SELECT id, whole + 1, whole * 2, amt + 1, amt * 2, amt - whole FROM ` + x,
		`SELECT id, dbl + amt, dbl * whole FROM ` + x,
		`SELECT id, rl, whole + 1, sum(dbl) OVER (ORDER BY id), count(*) OVER () FROM ` + x,
		`SELECT id, rl, CASE WHEN whole > 0 THEN -1 ELSE 1 END FROM ` + x,
		`SELECT s.id, s.v + 1 FROM (SELECT id, dbl AS v FROM ` + x + `) s`,
		`SELECT count(*), sum(whole) + 1 FROM ` + x + ` WHERE rl > 0`,
	})

	// A copy with no real column: nothing is one, whatever the names.
	noReal := map[string][]string{"id": {"INTEGER"}, "rl": {"DOUBLE"}, "v": {"DECIMAL(10,2)"}}
	for _, q := range []string{`SELECT rl + 1, sum(rl), v * 2, sum(unknown) FROM t`} {
		if why := CopyScreen(q, noReal); why != "" {
			t.Errorf("%s: sent to the source (%s) with no real column in the copy", q, why)
		}
	}
	// A name that is a real in one table and something else in another may
	// be the real.
	either := map[string][]string{"v": {"INTEGER", "FLOAT"}, "id": {"INTEGER"}}
	if why := CopyScreen(`SELECT v + 1 FROM t`, either); why != realArithmeticVeto {
		t.Errorf("a name that is a real in one table: %q", why)
	}
	// Types not known: as before, arithmetic stays.
	if why := CopyScreen(`SELECT a + 1, sum(a) FROM t`, nil); why != "" {
		t.Errorf("no types: sent to the source (%s)", why)
	}
}

// --- the reader's bounds ---------------------------------------------------------

// chained is a statement of n names, each defined from the one before it
// used twice (a2 is a1 + a1), read at the end by tail: without a memory of
// what each name was read as, reading the last costs 2^n readings of the
// first.
func chained(n int, column, tail string) string {
	from := "(SELECT " + column + " + " + column + " AS a1 FROM pgcmp_exact) s1"
	for i := 2; i <= n; i++ {
		from = fmt.Sprintf("(SELECT a%d + a%d AS a%d FROM %s) s%d", i-1, i-1, i, from, i)
	}
	return "SELECT " + fmt.Sprintf(tail, n) + " FROM " + from
}

// screenWithin runs the screen and fails the test when it takes longer than
// a statement of this size may.
func screenWithin(t *testing.T, name, q string, types map[string][]string) string {
	t.Helper()
	done := make(chan string, 1)
	go func() { done <- CopyScreen(q, types) }()
	select {
	case why := <-done:
		return why
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: the screen did not answer in 5 s", name)
		return ""
	}
}

func TestNumReader_bounded(t *testing.T) {
	// Forty names in a chain: each is read once, and the last is what the
	// first is, a real.
	if why := screenWithin(t, "40 names over a real", chained(40, "rl", "a%d * 2"), exactTypes); why != realArithmeticVeto {
		t.Errorf("40 chained names over a real, multiplied: %q", why)
	}
	if why := screenWithin(t, "40 names over a real, summed", chained(40, "rl", "sum(a%d)"), exactTypes); why != realSumVeto {
		t.Errorf("40 chained names over a real, summed: %q", why)
	}
	// A double precision all the way: nothing to send to the source for,
	// and still read once each.
	if why := screenWithin(t, "40 names over a double precision", chained(40, "dbl", "a%d * 2"), exactTypes); why != "" {
		t.Errorf("40 chained names over a double precision: sent to the source (%s)", why)
	}
	// A chain deeper than the reader follows is not judged: the source
	// answers.
	if why := screenWithin(t, "300 names", chained(300, "dbl", "a%d * 2"), exactTypes); why != numReadVeto {
		t.Errorf("300 chained names: %q, want the reader's own veto", why)
	}
	// Names that define each other end: each is read inside the other as a
	// column, of which there is none.
	loop := `SELECT a * 2, b * 2 FROM (SELECT b + 1 AS a, a + 1 AS b FROM pgcmp_exact) s`
	if why := screenWithin(t, "two names in a circle", loop, exactTypes); why == numReadVeto {
		t.Errorf("two names in a circle: %q", why)
	}
	// Parentheses nested by the thousand, each level computed with: the
	// work is bounded, and the statement goes to the source for its size.
	deep := "SELECT " + strings.Repeat("(", 20000) + "dbl" + strings.Repeat(" + 1)", 20000) + " FROM pgcmp_exact"
	if why := screenWithin(t, "20,000 parentheses", deep, exactTypes); why == "" {
		t.Error("20,000 nested parentheses computed with: left to the copy")
	}

	// A megabyte of one word, or of calls that never close, beside a real
	// column: nothing looks for a parenthesis' other end by walking the
	// statement again, so the screen answers at once. (What it answers is
	// the source: the statements are not SQL.)
	for name, piece := range map[string]string{
		"case":       "case ",
		"a call":     "foo( ",
		"sum":        "sum( ",
		"closings":   ") + ",
		"openings":   "( + ",
		"coalesce":   "coalesce( ",
		"a real":     "rl + ( ",
		"then a sum": "- sum( rl ",
	} {
		q := "SELECT " + strings.Repeat(piece, (1<<20)/len(piece)) + " FROM pgcmp_exact"
		start := time.Now()
		toks, why := lexSQL(q)
		if why != "" {
			t.Fatalf("%s: not read: %s", name, why)
		}
		lexed := time.Since(start)
		start = time.Now()
		done := make(chan string, 1)
		go func() { done <- realUse(toks, exactTypes) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("a megabyte of %q: the rule on real columns did not answer in 5 s (read in %v)", piece, lexed)
		}
		if took := time.Since(start); took > time.Second {
			t.Errorf("a megabyte of %q: the rule on real columns took %v", piece, took)
		}
	}
	// The matching itself.
	toks, _ := lexSQL("a ( b [ c ] ( ) ) ) ( d")
	got := matchParens(toks)
	for i := range toks {
		if toks[i].kind == rtOp && (toks[i].text == "(" || toks[i].text == "[") && got[i] >= 0 && got[got[i]] != i {
			t.Errorf("token %d opens and its other end %d does not point back", i, got[i])
		}
	}
	if got[1] != 8 || got[8] != 1 || got[3] != 5 || got[6] != 7 || got[9] != -1 || got[10] != -1 || got[0] != -1 {
		t.Errorf("matchParens: %v", got)
	}

	// The memory itself: a name is worked out once.
	toks, _ = lexSQL(chained(40, "rl", "a%d * 2"))
	r := newNumReader(toks, exactTypes, matchParens(toks))
	if v := r.name("a40"); v.kind != numReal || r.over {
		t.Errorf("a40: %+v, over=%v", v, r.over)
	}
	first := r.steps
	if first > 2000 {
		t.Errorf("reading 40 chained names cost %d steps", first)
	}
	if r.name("a40"); r.steps != first {
		t.Errorf("reading a40 again cost %d more steps", r.steps-first)
	}
	// A name read inside its own definition is the column of that name, and
	// what is kept is what it stands for.
	toks, _ = lexSQL(`SELECT t.rl * 2 FROM (SELECT max(rl) AS rl FROM pgcmp_exact) t`)
	r = newNumReader(toks, exactTypes, matchParens(toks))
	if v := r.name("rl"); v.kind != numReal {
		t.Errorf("max(rl) AS rl: %+v", v)
	}
}

// substr is substring with commas (#3219): the copy answers it under the same
// rule. PostgreSQL has no substr(x FROM n).
func TestCopyScreen_substr(t *testing.T) {
	types := map[string][]string{"id": {"INTEGER"}, "word": {"VARCHAR"}, "whole": {"INTEGER"}, "code": {bpcharType}}
	for _, q := range []string{
		"SELECT id, substr(word, 2, 3) FROM t",
		"SELECT substr(word, 1) FROM t",
		"SELECT SUBSTR(word, 1, 1), count(*) FROM t GROUP BY 1",
		"SELECT substr('apple', 2, 3)",
		"SELECT substr(word, 1, 2147483647) FROM t",
		// A char(n) column cut by whole numbers: the copy's (#3219).
		"SELECT substr(code, 1, 1) FROM t",
		"SELECT o.id, substr(o.code, 1, 2) FROM t o",
		"SELECT substring(code, 2, 1), substring(code FROM 2 FOR 1), substring(code FROM 1) FROM t",
		"SELECT id FROM t WHERE substr(code, 1, 1) IN ('1', '2')",
		"SELECT substr(code, 1, 1), count(*) FROM t GROUP BY substr(code, 1, 1)",
	} {
		if veto := CopyScreen(q, types); veto != "" {
			t.Errorf("%s: sent to the source for %s", q, veto)
		}
	}
	for q, says := range map[string]string{
		"SELECT substr(word, -1, 3) FROM t":         substringVeto,
		"SELECT substr(word, 0, 3) FROM t":          substringVeto,
		"SELECT substr(word, 0) FROM t":             substringVeto,
		"SELECT substr(word, 2, 0) FROM t":          substringVeto,
		"SELECT substr(word, 2, -1) FROM t":         substringVeto,
		"SELECT substr(word, whole, 2) FROM t":      substringVeto,
		"SELECT substr(word, 1 - whole) FROM t":     substringVeto,
		"SELECT substr(word, 1, 3000000000) FROM t": substringVeto,
		"SELECT substr(word, 2147483648) FROM t":    substringVeto,
		"SELECT substr(word, 2.5) FROM t":           substringVeto,
		"SELECT substr(word) FROM t":                substringVeto,
		"SELECT substr(word FROM 2) FROM t":         substringVeto,
		"SELECT substr(word, 1, 2, 3) FROM t":       substringVeto,
		// Its answer is text, whatever it is given: sorted or compared by
		// order, by the source's collation.
		"SELECT id FROM t ORDER BY substr(id, 1, 1)":      "text sorted",
		"SELECT id FROM t WHERE substr(id, 1, 1) < 'b'":   "text compared",
		"SELECT id FROM t WHERE substr(word, 1, 1) > 'b'": "text compared",
		// A char(n) column cut otherwise than by whole numbers written in the
		// text, or used on its own: other rows on the copy.
		"SELECT substr(code, whole, 1) FROM t":             "trailing blanks",
		"SELECT substr(code, -1, 1) FROM t":                "trailing blanks",
		"SELECT substr(code || 'x', 1, 1) FROM t":          "trailing blanks",
		"SELECT substr(trim(code), 1, 1) FROM t":           "trailing blanks",
		"SELECT substr(word, 1, length(code)) FROM t":      "",
		"SELECT id FROM t WHERE code = 'ab'":               "trailing blanks",
		"SELECT id FROM t WHERE code LIKE 'a%'":            "trailing blanks",
		"SELECT id FROM t ORDER BY code":                   "trailing blanks",
		"SELECT code, count(*) FROM t GROUP BY code":       "",
		"SELECT a.id FROM t a JOIN t b ON a.code = b.code": "trailing blanks",
		"SELECT length(code) FROM t":                       "trailing blanks",
		"SELECT id FROM t WHERE substr(word, 1, 1) = code": "trailing blanks",
	} {
		if veto := CopyScreen(q, types); !strings.Contains(veto, says) {
			t.Errorf("%s: veto %q, want one that says %q", q, veto, says)
		}
	}
}
