package readrouter

import (
	"strings"
	"testing"
	"time"
)

// An underscore inside a number: the copy takes it for a digit separator
// (1.5_5 is 1.55, 1e1_0 is 1e10), MySQL for the start of an alias (1.5 under
// the alias _5). Measured on MySQL 8.4, MariaDB 11.4 and the copy.
func TestVeto_underscoreInsideANumber(t *testing.T) {
	for stmt, want := range map[string]string{
		"SELECT 1.5_5":             vetoDigitWord,
		"SELECT 1e1_0":             vetoDigitWord,
		"SELECT .5_5":              vetoDigitWord,
		"SELECT 0.0_1":             vetoDigitWord,
		"SELECT 1.5e1_0":           vetoDigitWord,
		"SELECT 1.5_5 FROM orders": vetoDigitWord,
		"SELECT 1.5_x":             vetoDigitWord, // the same on both: kept back with the rest
		"SELECT a.b_1, c_2 FROM a": "",
		"SELECT t.col_1e5 FROM t":  "",
		"SELECT 1.55, 1e10":        "",
	} {
		if got := Veto(stmt); got != want {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, want)
		}
	}
}

// A decimal number that ends in a bare e: MySQL refuses the statement, the
// copy answers the number under the alias e.
func TestVeto_decimalThatEndsInABareE(t *testing.T) {
	for stmt, want := range map[string]string{
		"SELECT 1.5e FROM t":      vetoDigitWord,
		"SELECT 1.5E":             vetoDigitWord,
		"SELECT .5e":              vetoDigitWord,
		"SELECT 1.e":              vetoDigitWord,
		"SELECT 1.e FROM t":       vetoDigitWord,
		"SELECT 1.5ex":            vetoDigitWord,
		"SELECT 1.5e+x FROM t":    vetoDigitWord,
		"SELECT 1.5e-1":           "",
		"SELECT 1.5e+1":           "",
		"SELECT 1.5e5x":           "", // 1.5e5 AS x on both
		"SELECT 1.e2":             "",
		"SELECT 1.E+2":            "",
		"SELECT 1.5abc":           "",
		"SELECT t1.e FROM t1":     "", // a column named e
		"SELECT t.e, t.e2 FROM t": "",
		"SELECT x1.end FROM x1":   "",
		"SELECT 1.5 e FROM t":     "",
		"SELECT 1 .e FROM t":      "",
	} {
		if got := Veto(stmt); got != want {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, want)
		}
	}
}

// INTERVAL '1:30' MINUTE_SECOND is one minute and thirty seconds on MySQL.
// The copy has no two-part units: it reads INTERVAL '1:30' (an hour and a
// half) and takes MINUTE_SECOND for the column's alias.
func TestVeto_intervalWithATwoPartUnit(t *testing.T) {
	for stmt, want := range map[string]string{
		"SELECT DATE '2026-01-01' + INTERVAL '1:30' MINUTE_SECOND":      vetoIntervalUnit,
		"SELECT ts + interval '1:30' minute_second FROM t":              vetoIntervalUnit,
		"SELECT ts - INTERVAL'1 2'DAY_HOUR FROM t":                      vetoIntervalUnit,
		"SELECT ts + INTERVAL\n'1.5'\nSECOND_MICROSECOND FROM t":        vetoIntervalUnit,
		"SELECT DATE_ADD(ts, INTERVAL '1:30' HOUR_MINUTE) FROM t":       vetoIntervalUnit,
		"SELECT id FROM t WHERE ts > x - INTERVAL '0:30' MINUTE_SECOND": vetoIntervalUnit,
		"SELECT ts + INTERVAL ? MINUTE_SECOND FROM t":                   vetoIntervalUnit, // a prepared statement's template: the argument arrives quoted
		"SELECT ts + INTERVAL '1' DAY FROM t":                           "",
		"SELECT ts + INTERVAL 1 DAY FROM t":                             "",
		"SELECT ts + INTERVAL ? DAY FROM t":                             "",
		"SELECT ts + INTERVAL 1 DAY_HOUR FROM t":                        "", // the copy refuses it
		"SELECT ts + INTERVAL '1' DAY day_one FROM t":                   "",
		"SELECT ts + INTERVAL '1' DAY AS day_one FROM t":                "",
		"SELECT interval_days, 'x' my_alias FROM t":                     "",
		"SELECT 'INTERVAL ''1:30'' MINUTE_SECOND'":                      "",
		"SELECT 1 /* INTERVAL '1:30' MINUTE_SECOND */":                  "",
	} {
		if got := Veto(stmt); got != want {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, want)
		}
	}
}

// A date inside a call, a subquery or a CASE is still a date when the whole
// group is added to: GREATEST(DATE '2026-02-01', DATE '2026-01-01') + 1 is
// 20260202 on MySQL and 2026-02-02 on the copy. And AVG of a date is a
// number on MySQL and a date and time on the copy.
func TestVeto_dateInsideAGroup(t *testing.T) {
	for stmt, want := range map[string]string{
		"SELECT GREATEST(DATE '2026-02-01', DATE '2026-01-01') + 1":            vetoDateArithmetic,
		"SELECT 1 + GREATEST(DATE '2026-02-01', DATE '2026-01-01')":            vetoDateArithmetic,
		"SELECT -GREATEST(DATE '2026-02-01', DATE '2026-01-01')":               vetoDateArithmetic,
		"SELECT LEAST(DATE '2026-02-01', DATE '2026-01-01') - 1":               vetoDateArithmetic,
		"SELECT COALESCE(DATE(ts), DATE '2026-01-01') + 1 FROM t":              vetoDateArithmetic,
		"SELECT IFNULL(DATE(ts), x) + 1 FROM t":                                vetoDateArithmetic,
		"SELECT IF(a, DATE '2026-01-01', DATE '2026-01-02') + 1 FROM t":        vetoDateArithmetic,
		"SELECT NULLIF(CAST(ts AS DATE), x)\n-\n1 FROM t":                      vetoDateArithmetic,
		"SELECT COALESCE(a, GREATEST(b, DATE(ts))) + 1 FROM t":                 vetoDateArithmetic, // two groups out
		"SELECT COALESCE(a, GREATEST(b, DATE(ts)) + 1) FROM t":                 vetoDateArithmetic,
		"SELECT MAX(DATE(ts)) + 1 FROM t":                                      vetoDateArithmetic,
		"SELECT MAX(DATE(ts)) - MIN(DATE(ts)) FROM t":                          vetoDateArithmetic,
		"SELECT MAX(DATE(ts)) OVER () - 1 FROM t":                              vetoDateArithmetic,
		"SELECT MAX(DATE(ts)) over (PARTITION BY a ORDER BY b) + 1 FROM t":     vetoDateArithmetic,
		"SELECT MAX(DATE(ts)) OVER w - 1 FROM t WINDOW w AS (PARTITION BY a)":  vetoDateArithmetic,
		"SELECT (SELECT DATE(ts) FROM t WHERE id = 1) + 1":                     vetoDateArithmetic,
		"SELECT 1 + (SELECT MAX(DATE(ts)) FROM t)":                             vetoDateArithmetic,
		"SELECT CASE WHEN a THEN DATE(ts) END + 1 FROM t":                      vetoDateArithmetic,
		"SELECT 1 + CASE WHEN a THEN DATE(ts) END FROM t":                      vetoDateArithmetic,
		"SELECT case when a then DATE '2026-01-01' else d end\n- 1 FROM t":     vetoDateArithmetic,
		"SELECT CASE WHEN a THEN CASE WHEN b THEN DATE(ts) END END + 1 FROM t": vetoDateArithmetic,
		"SELECT COALESCE(CASE WHEN a THEN DATE(ts) END, d) + 1 FROM t":         vetoDateArithmetic,
		"SELECT AVG(DATE(ts)) FROM t":                                          vetoDateArithmetic,
		"SELECT avg ( CAST(ts AS DATE) ) FROM t":                               vetoDateArithmetic,
		"SELECT AVG(DATE '2026-01-01')":                                        vetoDateArithmetic,
		"SELECT AVG(GREATEST(DATE(a), DATE(b))) FROM t":                        vetoDateArithmetic,
		"SELECT AVG(DISTINCT DATE(ts)) FROM t":                                 "DISTINCT inside an aggregate (not folded by the copy's collation)",
		"SELECT MAX(CAST(? AS DATE)) + ? FROM t":                               vetoDateArithmetic, // a prepared statement's template
		// Over-vetoes, on purpose: the group holds a date, whatever it
		// returns.
		"SELECT SUM(IF(d >= DATE '2026-01-03', amount, 0)) - 1 FROM t":        vetoDateArithmetic,
		"SELECT CASE WHEN d > DATE '2026-01-03' THEN 1 ELSE 0 END + 1 FROM t": vetoDateArithmetic,
		"SELECT x + YEAR(DATE(ts)) FROM t":                                    vetoDateArithmetic,
		"SELECT (SELECT count(*) FROM t WHERE d < DATE '2026-01-03') - 1":     vetoDateArithmetic,
		"SELECT AVG(IF(d >= DATE '2026-01-03', amount, NULL)) FROM t":         vetoDateArithmetic,
		// A + or - elsewhere in the statement, and groups with no arithmetic
		// around them.
		"SELECT id FROM orders WHERE created_on >= DATE '2026-01-01' AND id + 1 > 2":                           "",
		"SELECT SUM(a) - SUM(b) FROM t WHERE d >= DATE '2026-01-01'":                                           "",
		"SELECT (a + b) - 1 FROM t WHERE (d >= DATE '2026-01-01') AND x - 1 > 0":                               "",
		"SELECT x - 1 FROM t WHERE x - 1 > 0 AND (d >= DATE '2026-01-01')":                                     "",
		"SELECT AVG(amount), MIN(DATE(ts)), MAX(DATE(ts)) FROM t":                                              "",
		"SELECT AVG(amount) + 1 FROM t WHERE d = DATE '2026-01-01'":                                            "",
		"SELECT CASE WHEN d > DATE '2026-01-03' THEN 1 ELSE 0 END, a + 1 FROM t":                               "",
		"SELECT CASE WHEN a THEN 1 END + 1, DATE(ts) FROM t":                                                   "",
		"SELECT GREATEST(DATE '2026-02-01', DATE '2026-01-01') + INTERVAL 1 DAY":                               "",
		"SELECT COUNT(*) FROM t WHERE d IN (DATE '2026-01-01', DATE '2026-01-02')":                             "",
		"SELECT a - 1 FROM (SELECT a FROM t WHERE d > DATE '2026-01-01') x":                                    "",
		"SELECT t.end - 1, DATE(ts) FROM t":                                                                    "", // a column named end
		"SELECT DATE_FORMAT(DATE(ts), '%Y') FROM t ORDER BY 1":                                                 "",
		"SELECT CONCAT(DATE '2026-01-01', 'x'), a - 1 FROM t":                                                  "",
		"SELECT xavg(DATE(ts)), avg_d(DATE(ts)) FROM t":                                                        "",
		"SELECT $date(ts) + 1, édate(ts) - 1 FROM t":                                                           "",                 // other names
		"SELECT CASE WHEN t.end THEN DATE(ts) END + 1 FROM t":                                                  vetoDateArithmetic, // t.end is a column
		"SELECT 1 + g(f(CASE WHEN a THEN DATE(ts)), b)":                                                        vetoDateArithmetic, // a CASE left open ends with its parentheses
		"SELECT f(DATE(ts), end) - 1 FROM t WHERE a = (CASE WHEN b THEN 1 END)":                                vetoDateArithmetic, // a stray END closes nothing
		"SELECT (CASE WHEN a THEN 1 ELSE (DATE(ts)) END) FROM t WHERE x - 1 > 0":                               "",
		"SELECT CAST(ts AS DATETIME (6)) - 1, CAST(ts AS DATETIME(6) ) - 1 FROM t":                             vetoDateArithmetic,
		"SELECT CAST(ts AS DATEX) - 1, CAST(ts AS xDATE) + 1, CAST(ts ASDATE) + 1, CAST(tsas date) + 1 FROM t": "",
		"SELECT MAX(DATE(ts)) OVERT - 1 FROM t":                                                                "",
		"SELECT MAX(DATE(ts)) OVER (PARTITION BY a) x, b - 1 FROM t":                                           "",
		"SELECT GREATEST(DATE(a), b) + intervals FROM t":                                                       vetoDateArithmetic,
		"SELECT GREATEST(DATE '2026-02-01', (a":                                                                "",
		"SELECT CASE WHEN a THEN DATE(ts) + ":                                                                  vetoDateArithmetic,
		"SELECT ) + 1, DATE(ts) FROM t":                                                                        "",
	} {
		if got := Veto(stmt); got != want {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, want)
		}
	}
}

// Veto runs on every statement, in the process that captures, and nothing
// caps a statement's length before it: the time these checks take must grow
// with the statement's length and no faster. Reading a group again for each
// match inside it took 12.9 s on 8,000 nested CASTs (112 KB).
//
// The shape checks are timed on their own, at 2,000 and at 8,000 levels:
// four times the text may take about four times as long, and sixteen times
// as long is the quadratic curve. A wall-clock bound on the whole of Veto
// would not hold under the race detector, where the pattern list alone
// (linear too) takes seconds on 100 KB.
func TestVeto_deepNestingIsLinear(t *testing.T) {
	shapes := map[string]func(depth int) string{
		"cast": func(d int) string {
			return "SELECT " + strings.Repeat("cast(", d) + "x" + strings.Repeat(" as date)", d) + " FROM t"
		},
		"cast char": func(d int) string {
			return "SELECT " + strings.Repeat("cast(", d) + "x" + strings.Repeat(" as char)", d) + " FROM t"
		},
		"date": func(d int) string {
			return "SELECT " + strings.Repeat("date(", d) + "x" + strings.Repeat(")", d) + " FROM t"
		},
		"parentheses": func(d int) string {
			return "SELECT " + strings.Repeat("(", d) + "DATE '2026-01-01'" + strings.Repeat(")", d) + " FROM t"
		},
		"case": func(d int) string {
			return "SELECT " + strings.Repeat("case when a then ", d) + "DATE(ts)" + strings.Repeat(" end", d) + " FROM t"
		},
		"siblings": func(d int) string {
			return "SELECT f(" + strings.Repeat("(", d) + strings.Repeat("DATE(ts), ", d) + "1" + strings.Repeat(")", d) + ") FROM t"
		},
		"windows": func(d int) string {
			return "SELECT " + strings.Repeat("max(", d) + "DATE(ts)" + strings.Repeat(") over ()", d) + " FROM t"
		},
		"unclosed": func(d int) string { return "SELECT " + strings.Repeat("cast(date(", d) },
		"spaces": func(d int) string {
			return "SELECT cast(x as date" + strings.Repeat(" ", 10*d) + ") , " + strings.Repeat("( ", d) + "DATE(ts)" + strings.Repeat(" )", d)
		},
		"digits":    func(d int) string { return "SELECT " + strings.Repeat("1.5e5, ", 2*d) + "1" },
		"intervals": func(d int) string { return "SELECT " + strings.Repeat("d + interval '1' day, ", d) + "1 FROM t" },
	}
	for name, build := range shapes {
		if got := shapeVeto(scan(build(8000)).blankedCopy); got != "" {
			t.Errorf("%s: vetoed as %q, want none", name, got)
		}
		small, large := shapeVetoTime(build(2000)), shapeVetoTime(build(8000))
		// Linear is 4; the allowance is for a clock this short.
		if large > 9*small+20*time.Millisecond {
			t.Errorf("%s: the shape checks took %s on 2,000 and %s on 8,000: faster than linear growth", name, small, large)
		}
	}
	// The same shapes with arithmetic at the far end are still found.
	const depth = 2000
	for name, stmt := range map[string]string{
		"cast":        "SELECT " + strings.Repeat("cast(", depth) + "x" + strings.Repeat(" as date)", depth) + " + 1",
		"parentheses": "SELECT 1 + " + strings.Repeat("(", depth) + "DATE '2026-01-01'" + strings.Repeat(")", depth),
		"case":        "SELECT " + strings.Repeat("case when a then ", depth) + "DATE(ts)" + strings.Repeat(" end", depth) + " - 1",
		"windows":     "SELECT " + strings.Repeat("max(", depth) + "DATE(ts)" + strings.Repeat(") over ()", depth) + " - 1",
	} {
		if got := Veto(stmt); got != vetoDateArithmetic {
			t.Errorf("%s with arithmetic: Veto = %q, want the date veto", name, got)
		}
	}
}

// shapeVetoTime is the faster of two runs of the shape checks over stmt.
func shapeVetoTime(stmt string) time.Duration {
	text := scan(stmt).blankedCopy
	best := time.Duration(0)
	for range 2 {
		start := time.Now()
		shapeVeto(text)
		if d := time.Since(start); best == 0 || d < best {
			best = d
		}
	}
	return best
}
