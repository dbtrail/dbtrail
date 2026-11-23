package pgshim

import (
	"strings"
	"testing"
)

// The screen's rules of #3069, #3070, #3071 and #3072. What a test says
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
		`SELECT id, substr(word, 2, 3) FROM ` + x:                           "the function substr()",
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
		`SELECT '26-01-05'::date`:                                                looseTypedVeto,
		`SELECT DATE '26-01-05'`:                                                 looseTypedVeto,
		`SELECT date '26-01-05'`:                                                 looseTypedVeto,
		`SELECT CAST('26-01-05' AS date)`:                                        looseTypedVeto,
		`SELECT TIMESTAMP '2026-01-05 10:30:00.1234567'`:                         looseTypedVeto,
		`SELECT TIME '24:00:00'`:                                                 looseTypedVeto,
		`SELECT TIME '10:30:00.1234567'`:                                         looseTypedVeto,
		`SELECT TIMESTAMP '2026-01-05 24:00:00'`:                                 looseTypedVeto,
		`SELECT DATE 'today', DATE '2026-01-05'`:                                 looseTypedVeto,
		`SELECT DATE '2026-01-05', DATE '2026-1-5'`:                              looseTypedVeto,
		`SELECT id FROM ` + x + ` WHERE day >= DATE '26-01-05'`:                  looseTypedVeto,
		`SELECT id FROM ` + x + ` WHERE day IN (DATE '2026-01-05', '26-1-5'::date)`: looseTypedVeto,
		// A string beside a date, timestamp or time column is a literal of
		// the column's type.
		`SELECT id FROM ` + x + ` WHERE day = '26-01-05'`:                            looseTypedVeto,
		`SELECT id FROM ` + x + ` WHERE '26-01-05' = day`:                            looseTypedVeto,
		`SELECT id FROM ` + x + ` x WHERE x.day <> '26-01-05'`:                       looseTypedVeto,
		`SELECT id FROM ` + x + ` x WHERE '26-01-05' < x.day`:                        looseTypedVeto,
		`SELECT id FROM ` + x + ` WHERE day IN ('26-01-05', '2026-02-10')`:           looseTypedVeto,
		`SELECT id FROM ` + x + ` WHERE day NOT IN ('2026-02-10', '26-01-05')`:       looseTypedVeto,
		`SELECT id FROM ` + x + ` WHERE day BETWEEN '26-01-01' AND '2026-12-31'`:     looseTypedVeto,
		`SELECT id FROM ` + x + ` WHERE day BETWEEN '2026-01-01' AND '26-12-31'`:     looseTypedVeto,
		`SELECT id FROM ` + x + ` WHERE stamp = '2026-01-05 10:30:00.1234567'`:       looseTypedVeto,
		`SELECT id FROM ` + x + ` WHERE clock < '24:00:00'`:                          looseTypedVeto,
		`SELECT id FROM ` + x + ` WHERE day = '2026-01-05 10:30:00'`:                 looseTypedVeto,
		`SELECT id FROM ` + x + ` WHERE day = '2026-01-05' AND stamp > '26-01-05'`:   looseTypedVeto,
		`SELECT TIMESTAMP '2026-03-02 10:00:00+02'`:                                  zonedTypedVeto,
		`SELECT id FROM ` + x + ` WHERE stamp = '2026-03-02 10:00:00+02'`:            zonedCompareVeto,
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

// --- #3070 ---------------------------------------------------------------------

func TestCopyScreen_numericMix(t *testing.T) {
	const x = "pgcmp_exact"
	const mix = "different decimals"
	checkExactness(t, exactTypes, map[string]string{
		// CASE WHEN n > 3 THEN n ELSE 0 END is 0 on PostgreSQL, 0.00 here.
		`SELECT id, CASE WHEN amt > 3 THEN amt ELSE 0 END FROM ` + x:                                                  "CASE over numbers of " + mix,
		`SELECT id, case when amt > 3 then 0 else amt end FROM ` + x:                                                  "CASE over numbers of " + mix,
		`SELECT id, CASE whole WHEN 1 THEN amt WHEN 2 THEN amt4 END FROM ` + x:                                        "CASE over numbers of " + mix,
		`SELECT id, CASE WHEN id = 2 THEN whole ELSE amt END FROM ` + x:                                               "CASE over numbers of " + mix,
		`SELECT id, CASE WHEN amt > 3 THEN amt ELSE 0.0 END FROM ` + x:                                                "CASE over numbers of " + mix,
		`SELECT id, CASE WHEN amt > 3 THEN x.amt ELSE -1 END FROM ` + x + ` x`:                                        "CASE over numbers of " + mix,
		`SELECT id, CASE WHEN amt > 3 THEN (amt) ELSE (0) END FROM ` + x:                                              "CASE over numbers of " + mix,
		`SELECT id, CASE WHEN whole > 0 THEN amt * whole ELSE 0 END FROM ` + x:                                        "CASE over numbers of " + mix,
		`SELECT id, CASE WHEN whole > 0 THEN amt ELSE whole * 2 END FROM ` + x:                                        "CASE over numbers of " + mix,
		`SELECT id, CASE WHEN whole > 0 THEN amt + 1 ELSE amt4 + 1 END FROM ` + x:                                     "CASE over numbers of " + mix,
		`SELECT id, CASE WHEN whole > 0 THEN 1 ELSE CASE WHEN whole < 0 THEN amt ELSE 0 END END FROM ` + x:            "CASE over numbers of " + mix,
		`SELECT sum(CASE WHEN whole > 0 THEN amt ELSE 0 END) FROM ` + x:                                               "CASE over numbers of " + mix,
		`SELECT id FROM ` + x + ` WHERE CASE WHEN whole > 0 THEN amt ELSE 0 END > 1`:                                  "CASE over numbers of " + mix,
		`SELECT CASE WHEN true THEN 1.5 ELSE 0 END`:                                                                   "CASE over numbers of " + mix,
		`SELECT id, greatest(amt, amt4), least(amt, amt4) FROM ` + x:                                                  "greatest() over numbers of " + mix,
		`SELECT id, least(amt, 3) FROM ` + x:                                                                          "least() over numbers of " + mix,
		`SELECT id, greatest(amt, 3) FROM ` + x:                                                                       "greatest() over numbers of " + mix,
		`SELECT id, greatest(1, 2.5) FROM ` + x:                                                                       "greatest() over numbers of " + mix,
		`SELECT coalesce(max(amt), 0) FROM ` + x + ` WHERE id < 0`:                                                    "coalesce() over numbers of " + mix,
		`SELECT coalesce(sum(amt), 0) FROM ` + x:                                                                      "coalesce() over numbers of " + mix,
		`SELECT coalesce(sum(DISTINCT amt), 0) FROM ` + x:                                                             "coalesce() over numbers of " + mix,
		`SELECT id, coalesce(amt, 0) FROM ` + x:                                                                       "coalesce() over numbers of " + mix,
		`SELECT id, COALESCE(amt, whole) FROM ` + x:                                                                   "coalesce() over numbers of " + mix,
		`SELECT id, coalesce(amt, amt4, 0.00) FROM ` + x:                                                              "coalesce() over numbers of " + mix,
		`SELECT id, coalesce(nullif(amt, 0), 0) FROM ` + x:                                                            "coalesce() over numbers of " + mix,
		`SELECT id, coalesce(amt, count(*) OVER ()) FROM ` + x:                                                        "coalesce() over numbers of " + mix,
		`SELECT id, coalesce(amt, $1) FROM ` + x:                                                                      "coalesce() over numbers of " + mix,
		`SELECT id, coalesce(amt, '0') FROM ` + x:                                                                     "coalesce() over numbers of " + mix,
		`SELECT coalesce(t.total, 0) FROM (SELECT sum(amt) AS total FROM ` + x + ` WHERE id < 0) t`:                   "coalesce() over numbers of " + mix,
		`WITH t AS (SELECT whole, sum(amt) AS total FROM ` + x + ` GROUP BY whole) SELECT coalesce(total, 0) FROM t`:  "coalesce() over numbers of " + mix,
		`SELECT coalesce(v, 0) FROM (SELECT amt * 2 AS v FROM ` + x + `) t`:                                           "coalesce() over numbers of " + mix,
		`SELECT id, coalesce(amt, whole) FROM (SELECT id, CASE WHEN id < 5 THEN amt END AS amt, 7 AS whole FROM ` + x + `) s`: "coalesce() over numbers of " + mix,
	}, []string{
		`SELECT id, CASE WHEN amt > 3 THEN amt ELSE 0.00 END FROM ` + x,
		`SELECT id, coalesce(amt, 0.00), greatest(amt, 3.00), least(amt, 3.00) FROM ` + x,
		`SELECT id, coalesce(amt4, 0.0000), greatest(amt4, amt4) FROM ` + x,
		`SELECT id, CASE WHEN whole > 0 THEN whole ELSE 0 END, coalesce(whole, 0), greatest(whole, 2) FROM ` + x,
		`SELECT id, CASE WHEN amt > 3 THEN amt END, CASE WHEN amt > 3 THEN amt ELSE NULL END FROM ` + x,
		`SELECT id, coalesce(amt, NULL), coalesce(NULL, amt) FROM ` + x,
		`SELECT coalesce(max(amt), 0.00), coalesce(sum(amt), 0.00), coalesce(count(amt), 0) FROM ` + x + ` WHERE id < 0`,
		`SELECT coalesce(t.total, 0.00), coalesce(t.k, 0) FROM (SELECT sum(amt) AS total, count(*) AS k FROM ` + x + ` WHERE id < 0) t`,
		`WITH t AS (SELECT whole AS w, count(*) AS k, sum(whole) AS units FROM ` + x + ` GROUP BY whole) SELECT coalesce(t.k, 0), coalesce(units, 0) FROM t`,
		`SELECT sum(CASE WHEN whole > 0 THEN amt ELSE 0.00 END), sum(CASE WHEN whole > 0 THEN 1 ELSE 0 END) FROM ` + x,
		`SELECT count(CASE WHEN amt > 3 THEN 1 END), sum(CASE WHEN amt > 3 THEN whole ELSE 0 END) FROM ` + x,
		// A double precision or a real beside another number: a float on
		// both sides.
		`SELECT id, coalesce(dbl, 0), greatest(dbl, amt), CASE WHEN whole > 0 THEN dbl ELSE 0 END FROM ` + x,
		`SELECT id, coalesce(rl, 0), greatest(rl, 1), least(rl, dbl), CASE WHEN whole > 0 THEN rl ELSE 0 END FROM ` + x,
		`SELECT id, coalesce(rl, amt), greatest(rl, amt) FROM ` + x,
		// Not numbers.
		`SELECT id, CASE WHEN amt > 3 THEN 'big' ELSE 'small' END FROM ` + x,
		`SELECT id, coalesce(word, '-'), coalesce(day, DATE '2026-01-01') FROM ` + x,
		`SELECT id, CASE WHEN amt > 3.5 THEN 1 ELSE 0 END FROM ` + x,
		`SELECT id, coalesce(length(word), 0), coalesce(whole * 2, 0), coalesce(whole + 1, whole) FROM ` + x,
	})

	// Without the copy's types only what the statement writes is known.
	if why := CopyScreen(`SELECT CASE WHEN c THEN 1.5 ELSE 0 END FROM t`, nil); !strings.Contains(why, mix) {
		t.Errorf("literals of two scales, no types: %q", why)
	}
	if why := CopyScreen(`SELECT coalesce(a, 0.5) FROM t`, nil); !strings.Contains(why, mix) {
		t.Errorf("a column of unknown type beside 0.5, no types: %q", why)
	}
	if why := CopyScreen(`SELECT coalesce(a, 0), CASE WHEN a = 1 THEN a ELSE b END FROM t`, nil); why != "" {
		t.Errorf("nothing known to have decimals, no types: sent to the source (%s)", why)
	}
	// The screen before the types are known judges only the numbers written:
	// under some types a column beside 0.5 answers the same.
	for _, q := range []string{`SELECT coalesce(a, 0.5) FROM t`, `SELECT CASE WHEN c THEN a ELSE 0 END FROM t`} {
		toks, _ := lexSQL(q)
		if why := screenUntyped(toks); why != "" {
			t.Errorf("%s: vetoed without its types (%s)", q, why)
		}
	}
	if toks, _ := lexSQL(`SELECT CASE WHEN c THEN 1.5 ELSE 0 END FROM t`); !strings.Contains(screenUntyped(toks), mix) {
		t.Error("literals of two scales: not vetoed before the types are known")
	}

	// A name that is a numeric of one scale in one table and of another
	// elsewhere is of neither, and goes to the source beside any number.
	two := map[string][]string{"amount": {"DECIMAL(12,2)", "DECIMAL(10,4)"}, "price": {"DECIMAL(12,2)"}, "id": {"INTEGER"}}
	if why := CopyScreen(`SELECT coalesce(amount, 0.00) FROM t`, two); !strings.Contains(why, mix) {
		t.Errorf("a name of two scales: %q", why)
	}
	if why := CopyScreen(`SELECT coalesce(price, 0.00), coalesce(amount, NULL) FROM t`, two); why != "" {
		t.Errorf("one scale, and a value alone: sent to the source (%s)", why)
	}
	// A numeric declared without precision has no one number of decimals.
	free := map[string][]string{"free": {unsizedNumeric}, "id": {"INTEGER"}}
	if why := CopyScreen(`SELECT coalesce(free, 0) FROM t`, free); !strings.Contains(why, mix) {
		t.Errorf("a numeric without precision beside 0: %q", why)
	}
}

// A CASE or a COALESCE around a rewritten round is checked where it runs,
// value by value (packMixedScale): the screen leaves it to that check.
func TestCopyScreen_numericMixLeavesRounds(t *testing.T) {
	const x = "pgcmp_exact"
	for _, q := range []string{
		`SELECT id, CASE WHEN whole > 0 THEN round(amt, 1) ELSE 0 END FROM ` + x,
		`SELECT id, coalesce(round(amt, 1), 0) FROM ` + x,
	} {
		if why := CopyScreen(q, exactTypes); why != "" {
			t.Errorf("%s: sent to the source (%s), want the copy's own check", q, why)
		}
	}
	// A round in the condition is no value: the values are judged.
	q := `SELECT id, CASE WHEN round(amt, 1) > 3 THEN amt ELSE 0 END FROM ` + x
	if why := CopyScreen(q, exactTypes); !strings.Contains(why, "different decimals") {
		t.Errorf("%s: %q", q, why)
	}
}

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
		`SELECT id, rl + 1 FROM ` + x:                                         realArithmeticVeto,
		`SELECT id, 1 + rl FROM ` + x:                                         realArithmeticVeto,
		`SELECT id, rl - 1, rl FROM ` + x:                                     realArithmeticVeto,
		`SELECT id, rl * 2 FROM ` + x:                                         realArithmeticVeto,
		`SELECT id, rl / 2 FROM ` + x:                                         realArithmeticVeto,
		`SELECT id, rl * whole FROM ` + x:                                     realArithmeticVeto,
		`SELECT id, whole * rl FROM ` + x:                                     realArithmeticVeto,
		`SELECT id, rl + amt FROM ` + x:                                       realArithmeticVeto,
		`SELECT id, rl + dbl FROM ` + x:                                       realArithmeticVeto,
		`SELECT id, x.rl + 1 FROM ` + x + ` x`:                                realArithmeticVeto,
		`SELECT id, 2 * x.rl FROM ` + x + ` x`:                                realArithmeticVeto,
		`SELECT id, "rl" + 1 FROM ` + x:                                       realArithmeticVeto,
		`SELECT id, (rl) + 1 FROM ` + x:                                       realArithmeticVeto,
		`SELECT id, 1 - (rl) FROM ` + x:                                       realArithmeticVeto,
		`SELECT id, abs(rl) * 2 FROM ` + x:                                    realArithmeticVeto,
		`SELECT id, 2 * abs(rl) FROM ` + x:                                    realArithmeticVeto,
		`SELECT id, coalesce(rl, 0) + 1 FROM ` + x:                            realArithmeticVeto,
		`SELECT max(rl) - min(rl) FROM ` + x:                                  realArithmeticVeto,
		`SELECT id, -rl FROM ` + x:                                            realArithmeticVeto,
		`SELECT id FROM ` + x + ` WHERE rl * 2 > 1`:                           realArithmeticVeto,
		`SELECT id FROM ` + x + ` ORDER BY rl + whole`:                        realArithmeticVeto,
		`SELECT s.id, s.v + 1 FROM (SELECT id, rl AS v FROM ` + x + `) s`:     realArithmeticVeto,
		`WITH s AS (SELECT id, rl v FROM ` + x + `) SELECT id, v * 2 FROM s`:  realArithmeticVeto,
		`SELECT id, dbl + 1 + rl FROM ` + x:                                   realArithmeticVeto,
		`SELECT sum(rl) FROM ` + x:                                            realSumVeto,
		`SELECT SUM(x.rl) FROM ` + x + ` x`:                                   realSumVeto,
		`SELECT sum(DISTINCT rl) FROM ` + x:                                   realSumVeto,
		`SELECT sum(abs(rl)) FROM ` + x:                                       realSumVeto,
		`SELECT sum(rl) FILTER (WHERE id > 1) FROM ` + x:                      realSumVeto,
		`SELECT id, sum(rl) OVER (ORDER BY id) FROM ` + x:                     realSumVeto,
		`SELECT sum(CASE WHEN id > 1 THEN rl END) FROM ` + x:                  realSumVeto,
		`SELECT sum(v) FROM (SELECT rl AS v FROM ` + x + `) s`:                realSumVeto,
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
