package readrouter

import (
	"strings"
	"testing"
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
		"SELECT ts + INTERVAL '90' MINUTE_SECOND FROM t":           vetoIntervalUnit,
		"SELECT ts + interval '90' minute_second FROM t":           vetoIntervalUnit,
		"SELECT ts - INTERVAL'1'DAY_HOUR FROM t":                   vetoIntervalUnit,
		"SELECT ts + INTERVAL\n'15'\nSECOND_MICROSECOND FROM t":    vetoIntervalUnit,
		"SELECT DATE_ADD(ts, INTERVAL '130' HOUR_MINUTE) FROM t":   vetoIntervalUnit,
		"SELECT DATE '2026-01-01' + INTERVAL '1:30' MINUTE_SECOND": vetoIntervalAmount, // named first: the amount is not a whole number
		"SELECT ts + INTERVAL '1' DAY FROM t":                      "",
		"SELECT ts + INTERVAL 1 DAY FROM t":                        "",
		"SELECT ts + INTERVAL 1 DAY_HOUR FROM t":                   "", // the copy refuses it
		"SELECT ts + INTERVAL '1' DAY day_one FROM t":              "",
		"SELECT ts + INTERVAL '1' DAY AS day_one FROM t":           "",
		"SELECT interval_days, 'x' my_alias FROM t":                "",
		"SELECT 'INTERVAL ''90'' MINUTE_SECOND'":                   "",
		"SELECT 1 /* INTERVAL '90' MINUTE_SECOND */":               "",
	} {
		if got := Veto(stmt); got != want {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, want)
		}
	}
}

// An amount in parentheses is an expression: MySQL rounds it (INTERVAL (1.5)
// DAY is two days) and the copy cuts it (one day). A placeholder is kept
// back too: a text bound to it is rounded by MySQL 8.4 ('1.5' is two days)
// and reaches the copy as the quoted '1.5', one day.
func TestVeto_intervalWithAnExpressionOrAPlaceholder(t *testing.T) {
	for stmt, want := range map[string]string{
		"SELECT d + INTERVAL (1.5) DAY FROM t":                       vetoIntervalExpr,
		"SELECT d + INTERVAL (a/2) DAY FROM t":                       vetoIntervalExpr,
		"SELECT ts + interval(0.5) minute FROM t":                    vetoIntervalExpr,
		"SELECT DATE_ADD(d, INTERVAL (1.5) DAY) FROM t":              vetoIntervalExpr,
		"SELECT d + INTERVAL ('1:30') MINUTE_SECOND FROM t":          vetoIntervalExpr,
		"SELECT d + INTERVAL /* n */ (1.5) DAY FROM t":               vetoIntervalExpr,
		"SELECT d + INTERVAL\n\t( 1 ) DAY FROM t":                    vetoIntervalExpr,
		"SELECT d + INTERVAL -- n\n (1.5) DAY FROM t":                vetoIntervalExpr,
		"SELECT d + INTERVAL (?) DAY FROM t":                         vetoIntervalExpr,
		"SELECT d + INTERVAL ? DAY FROM t":                           vetoIntervalExpr,
		"SELECT d + INTERVAL ? MINUTE_SECOND FROM t":                 vetoIntervalExpr,
		"SELECT id FROM t WHERE d > ? - interval\n? day":             vetoIntervalExpr,
		"SELECT d + INTERVAL 1 DAY FROM t WHERE (a) = ?":             "",
		"SELECT d + INTERVAL a DAY FROM t":                           "", // the copy refuses it
		"SELECT intervals (1), xinterval(2), t.interval_ (3) FROM t": "",
		"SELECT 'INTERVAL (1.5) DAY', 1 /* INTERVAL ? DAY */":        "",
	} {
		if got := Veto(stmt); got != want {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, want)
		}
	}
}

// A quoted amount is cut at the first character that is not a digit on
// MySQL ('1e2' is 1, '1.5e1' is 1) and read as a number on the copy (100,
// 15). Only a whole number, signed or not, means the same on both.
func TestVeto_intervalWithAQuotedAmountThatIsNotAWholeNumber(t *testing.T) {
	for stmt, want := range map[string]string{
		"SELECT d + INTERVAL '1e2' DAY FROM t":             vetoIntervalAmount,
		"SELECT d + interval '1.5e1' day FROM t":           vetoIntervalAmount,
		"SELECT d + INTERVAL '1E1' HOUR FROM t":            vetoIntervalAmount,
		"SELECT d + INTERVAL'1e2'DAY FROM t":               vetoIntervalAmount,
		"SELECT d + INTERVAL\n '1e2' DAY FROM t":           vetoIntervalAmount,
		"SELECT d + INTERVAL /* n */ '1e2' DAY FROM t":     vetoIntervalAmount,
		"SELECT d + INTERVAL -- n\n '1e2' DAY FROM t":      vetoIntervalAmount,
		"SELECT DATE_ADD(d, INTERVAL '1e2' DAY) FROM t":    vetoIntervalAmount,
		"SELECT d + INTERVAL '1.5' DAY FROM t":             vetoIntervalAmount, // the same day on both: kept back with the rest
		"SELECT d + INTERVAL '1.5' SECOND FROM t":          vetoIntervalAmount,
		"SELECT d + INTERVAL '' DAY FROM t":                vetoIntervalAmount,
		"SELECT d + INTERVAL '1''2' DAY FROM t":            vetoIntervalAmount,
		"SELECT d + INTERVAL '1 2' DAY FROM t":             vetoIntervalAmount,
		"SELECT d + INTERVAL '1'\n'e2' DAY FROM t":         vetoIntervalAmount, // two literals, one amount
		"SELECT d + INTERVAL '+' DAY FROM t":               vetoIntervalAmount,
		"SELECT d + INTERVAL \"1e2\" DAY FROM t":           vetoDoubleQuoted,
		"SELECT d + INTERVAL '1' DAY FROM t":               "",
		"SELECT d + INTERVAL '12' MONTH FROM t":            "",
		"SELECT d + INTERVAL '-2' DAY FROM t":              "",
		"SELECT d + INTERVAL '+2' DAY FROM t":              "",
		"SELECT d + INTERVAL ' 2 ' DAY FROM t":             "",
		"SELECT d + INTERVAL '02' DAY FROM t":              "",
		"SELECT d + INTERVAL 1 DAY, '1e2' FROM t":          "",
		"SELECT d + INTERVAL 1 DAY '1e2' FROM t":           "", // an alias
		"SELECT xinterval '1e2', t.interval_ '1e2' FROM t": "",
		"SELECT `interval` '1e2' FROM t":                   vetoNameString,
		"SELECT interval, '1e2' FROM t":                    "",
		"SELECT 'interval' '1e2'":                          "",
		"SELECT d FROM t WHERE x = 'interval ''1e2'' day'": "",
	} {
		if got := Veto(stmt); got != want {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, want)
		}
		// The rewrite for the copy refuses what the veto names.
		if _, refusal := ForCopy(stmt); want == vetoIntervalAmount && refusal != want {
			t.Errorf("ForCopy(%q) refusal = %q, want %q", stmt, refusal, want)
		}
	}
}

// AVG of a time is a number on MySQL (100000.0000 for ten o'clock) and a
// time on the copy. The copy refuses + and - on a time, so naming TIME as a
// shape keeps nothing else back that the copy would have answered.
func TestVeto_avgOfATime(t *testing.T) {
	for stmt, want := range map[string]string{
		"SELECT AVG(TIME '10:00:00')":                          vetoDateArithmetic,
		"SELECT AVG(CAST(ts AS TIME)) FROM t":                  vetoDateArithmetic,
		"SELECT avg(cast(ts as time(3))) FROM t":               vetoDateArithmetic,
		"SELECT AVG(GREATEST(TIME '10:00:00', x)) FROM t":      vetoDateArithmetic,
		"SELECT TIME '10:00:00' + 1":                           vetoDateArithmetic, // the copy refuses it: kept back at no cost
		"SELECT CAST(ts AS TIME) - 1 FROM t":                   vetoDateArithmetic,
		"SELECT TIME '10:00:00' + INTERVAL 1 HOUR":             "",
		"SELECT MAX(TIME '10:00:00'), CAST(ts AS CHAR) FROM t": "",
		"SELECT AVG(amount), runtime '10' FROM t":              "",
		"SELECT AVG(TIME(ts)) FROM t":                          "", // the copy refuses TIME(...)
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
// caps a statement's length before it: the work these checks do must grow
// with the statement's length and no faster. Reading a group again for each
// match inside it took 12.9 s on 8,000 nested CASTs (112 KB).
//
// The work is counted, not timed: every byte of the text and every group
// the checks look at goes through shapeText, which counts it. Four times
// the nesting is four times the text and may be about four times the work
// (the search for a window's parenthesis adds a logarithm); sixteen times
// is the quadratic curve.
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
		why, small := shapeVetoWork(scan(build(2000)).blankedCopy)
		if why != "" {
			t.Errorf("%s: vetoed as %q, want none", name, why)
		}
		_, large := shapeVetoWork(scan(build(8000)).blankedCopy)
		ratio := float64(large) / float64(small)
		t.Logf("%-12s %9d steps at 2,000, %9d at 8,000: x%.2f", name, small, large, ratio)
		if ratio > 4.5 {
			t.Errorf("%s: %d steps at 2,000 levels and %d at 8,000 (x%.2f): the work grows faster than the text", name, small, large, ratio)
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
