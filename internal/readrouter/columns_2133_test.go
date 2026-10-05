package readrouter

import (
	"strings"
	"testing"
)

// #2133. Every statement below was measured on MySQL 8.4.9, MariaDB 11.4 and
// the copy; consoleapp's TestIntegrationSQLCompareTemporalColumns pins the
// answers themselves.

// A DATE, DATETIME or TIMESTAMP column next to + or -, or under AVG, is a
// number built from its digits on MySQL and a date on the copy. The table
// has created_on (DATE) and seen_at (DATETIME).
func TestColumnVeto_arithmeticOnADateColumn(t *testing.T) {
	dates := []string{"created_on", "seen_at"}
	for stmt, kept := range map[string]bool{
		"SELECT created_on + 1 FROM orders":                                   true,
		"SELECT created_on - 1 FROM orders":                                   true,
		"SELECT created_on + 0 FROM orders":                                   true,
		"SELECT created_on+1 FROM orders":                                     true,
		"SELECT 1 + created_on FROM orders":                                   true,
		"SELECT CREATED_ON + 1 FROM orders":                                   true,
		"SELECT `created_on` + 1 FROM `orders`":                               true,
		"SELECT `created_on`+1 FROM `orders`":                                 true,
		"SELECT `Created_On` - 1 FROM `orders`":                               true,
		"SELECT o.created_on + 1 FROM orders o":                               true,
		"SELECT 1 + o.created_on FROM orders o":                               true,
		"SELECT 1 + `o`.`created_on` FROM orders o":                           true,
		"SELECT 1 + shop.orders.created_on FROM shop.orders":                  true,
		"SELECT 1 + `shop` . `orders` . `created_on` FROM shop.orders":        true,
		"SELECT (created_on) + 1 FROM orders":                                 true,
		"SELECT GREATEST(created_on, created_on) + 1 FROM orders":             true,
		"SELECT LAST_DAY(created_on) + 1 FROM orders":                         true,
		"SELECT MAX(created_on) - MIN(created_on) FROM orders":                true,
		"SELECT seen_at - seen_at FROM orders":                                true,
		"SELECT AVG(created_on) FROM orders":                                  true,
		"SELECT avg( seen_at ) FROM orders":                                   true,
		"SELECT AVG(COALESCE(created_on, seen_at)) FROM orders":               true,
		"SELECT created_on + 0, COUNT(*) FROM orders GROUP BY created_on + 0": true,
		"SELECT id FROM orders ORDER BY created_on + 0":                       true,
		"SELECT created_on + ? FROM orders":                                   true, // a prepared statement's template
		"SELECT CASE WHEN id = 1 THEN created_on END + 1 FROM orders":         true,
		"SELECT MAX(created_on) OVER () - 1 FROM orders":                      true,
		"SELECT -created_on FROM orders":                                      true, // the copy refuses it: kept back at no cost
		// A group that holds the column is added to: kept back, for nothing
		// when what the group returns is a number.
		"SELECT YEAR(created_on) + 1 FROM orders": true,
		// An alias of the date can be used from outside a subquery or a WITH,
		// where the text does not say it is one.
		"SELECT d + 1 FROM (SELECT created_on AS d FROM orders) x":                            true,
		"SELECT d + 1 FROM ( select DATE(seen_at) AS d FROM orders) x":                        true,
		"WITH c AS (SELECT created_on AS d FROM orders) SELECT d + 1 FROM c":                  true,
		"SELECT (SELECT MAX(created_on) FROM orders) + 1":                                     true,
		"SELECT id - 1 FROM orders WHERE id IN (SELECT id FROM orders WHERE created_on > '')": true, // for nothing
		// What both sides answer alike.
		"SELECT created_on FROM orders":                                                               false,
		"SELECT id, created_on, seen_at FROM orders ORDER BY created_on DESC":                         false,
		"SELECT id FROM orders WHERE created_on >= '' AND created_on < ''":                            false,
		"SELECT id FROM orders WHERE created_on BETWEEN ? AND ?":                                      false,
		"SELECT MIN(created_on), MAX(seen_at), COUNT(*) FROM orders":                                  false,
		"SELECT created_on, COUNT(*) FROM orders GROUP BY created_on":                                 false,
		"SELECT amount + tax FROM orders WHERE created_on >= ''":                                      false,
		"SELECT amount - 1, created_on FROM orders":                                                   false,
		"SELECT id + 1 FROM orders ORDER BY seen_at":                                                  false,
		"SELECT SUM(amount) - SUM(tax), MAX(created_on) FROM orders":                                  false,
		"SELECT YEAR(created_on), MONTH(created_on), SUM(amount) FROM orders GROUP BY 1, 2":           false,
		"SELECT created_on + INTERVAL 1 DAY FROM orders":                                              false, // the same day, shown as a date and time by the copy: listed in the docs
		"SELECT id FROM orders WHERE created_on > seen_at - INTERVAL 7 DAY":                           false,
		"SELECT id FROM orders WHERE created_on - INTERVAL 1 DAY = ''":                                false,
		"SELECT DATE_ADD(created_on, INTERVAL 1 DAY) FROM orders":                                     false,
		"SELECT id FROM orders WHERE id IN (SELECT id FROM orders WHERE created_on > '')":             false, // a subquery and no + or -
		"SELECT id FROM (SELECT id, created_on FROM orders) x WHERE created_on > '' - INTERVAL 1 DAY": false,
		"SELECT COUNT(*) FROM orders WHERE created_on IS NOT NULL":                                    false,
		// Another word that holds the name is not the name.
		"SELECT created_on_utc + 1, xcreated_on - 1 FROM orders": false,
		"SELECT `created_on x` + 1 FROM orders":                  false,
		// No column of the table named at all.
		"SELECT id + 1 FROM orders": false,
		"SELECT 1":                  false,
	} {
		got := ColumnVeto(Shape(stmt), dates, nil)
		if (got != "") != kept {
			t.Errorf("ColumnVeto(%q) = %q, want kept on the source: %v", stmt, got, kept)
		}
		if kept && !strings.Contains(got, "created_on") && !strings.Contains(got, "seen_at") {
			t.Errorf("ColumnVeto(%q) = %q: the reason does not name the column", stmt, got)
		}
	}
}

// A quoted column can be called what the group and sign checks take for a
// keyword: it is read as a name, not as that word.
func TestColumnVeto_columnNamedLikeAKeyword(t *testing.T) {
	dates := []string{"end", "interval", "over", "avg", "case"}
	for stmt, kept := range map[string]bool{
		"SELECT CASE WHEN a THEN `end` ELSE b END + 1 FROM t":     true,
		"SELECT 1 + CASE WHEN a THEN `end` ELSE b END FROM t":     true,
		"SELECT CASE WHEN a THEN `case` ELSE b END + 1 FROM t":    true,
		"SELECT `end` + 1 FROM t":                                 true,
		"SELECT start + `interval` FROM t":                        true,
		"SELECT MAX(`over`) OVER () + 1 FROM t":                   true,
		"SELECT AVG(`avg`) FROM t":                                true,
		"SELECT CASE WHEN a THEN `end` ELSE b END, id + 1 FROM t": false,
		// The bare keyword is taken for the column too: kept back for nothing.
		"SELECT CASE WHEN a THEN 1 ELSE 2 END + 1, `end` FROM t":   true,
		"SELECT `end`, id FROM t WHERE `over` > '' ORDER BY `avg`": false,
	} {
		if got := ColumnVeto(Shape(stmt), dates, nil); (got != "") != kept {
			t.Errorf("ColumnVeto(%q) = %q, want kept on the source: %v", stmt, got, kept)
		}
	}
}

// What stands in a string or a comment is not the statement: the shape has
// neither, so a date written there is not arithmetic and a sign there is no
// operator.
func TestColumnVeto_stringsAndCommentsAreNotRead(t *testing.T) {
	for _, stmt := range []string{
		"SELECT 'created_on + 1' FROM orders",
		"SELECT id /* created_on + 1 */ FROM orders",
		"SELECT id FROM orders -- created_on + 1\n",
		"SELECT created_on, '+' FROM orders",
		"SELECT created_on /* + */ FROM orders",
	} {
		if got := ColumnVeto(Shape(stmt), []string{"created_on"}, []string{"tm"}); got != "" {
			t.Errorf("ColumnVeto(%q) = %q, want none", stmt, got)
		}
	}
}

// A TIME is text on the copy and a YEAR a plain number: tm >= '9:00:00'
// compares letters there, and yr = 26 is not the year 2026. A statement that
// names one at all is the source's.
func TestColumnVeto_aTimeOrYearColumnNamed(t *testing.T) {
	whole := []string{"tm", "yr"}
	for stmt, kept := range map[string]bool{
		"SELECT id FROM ev WHERE tm >= '9:00:00'": true,
		"SELECT id FROM ev WHERE yr = 26":         true,
		"SELECT tm FROM ev":                       true,
		"SELECT `TM` FROM ev":                     true,
		"SELECT e.yr FROM ev e":                   true,
		"SELECT id FROM ev ORDER BY tm":           true,
		"SELECT id, amount + 1 FROM ev":           false,
		"SELECT tmx, yr2, `tm yr` FROM ev":        false,
		"SELECT 'tm', id /* yr */ FROM ev":        false,
	} {
		if got := ColumnVeto(Shape(stmt), nil, whole); (got != "") != kept {
			t.Errorf("ColumnVeto(%q) = %q, want kept on the source: %v", stmt, got, kept)
		}
	}
}

// Not known is never read as "nothing to find": a statement that cannot be
// searched, a column whose name cannot be looked for and a shape that was
// not handed over each keep the statement on the source, and a table with no
// such column asks nothing.
func TestColumnVeto_whatCannotBeSearchedIsRefused(t *testing.T) {
	dates := []string{"created_on"}
	if got := ColumnVeto(Shape("SELECT id FROM orders WHERE créated_on > 1"), dates, nil); !strings.Contains(got, "outside ASCII") {
		t.Errorf("a name outside ASCII: %q, want refused", got)
	}
	// Outside ASCII inside a string is not a name: the shape has blanked it.
	if got := ColumnVeto(Shape("SELECT id FROM orders WHERE city = 'Bogotá'"), dates, nil); got != "" {
		t.Errorf("a string outside ASCII: %q, want none", got)
	}
	for _, name := range []string{"fecha de alta", "día", "a`b", `a"b`, ""} {
		if got := ColumnVeto(Shape("SELECT id FROM orders"), []string{name}, nil); !strings.Contains(got, "cannot be looked for") {
			t.Errorf("a date column named %q: %q, want every statement refused", name, got)
		}
		if got := ColumnVeto(Shape("SELECT id FROM orders"), nil, []string{name}); !strings.Contains(got, "cannot be looked for") {
			t.Errorf("a TIME column named %q: %q, want every statement refused", name, got)
		}
	}
	if got := ColumnVeto("", dates, nil); got == "" {
		t.Error("no shape and a date column: answered, want refused")
	}
	if got := ColumnVeto("", nil, nil); got != "" {
		t.Errorf("no shape and no such column: %q, want none", got)
	}
	if got := ColumnVeto(Shape("SELECT créated_on + 1 FROM orders"), nil, nil); got != "" {
		t.Errorf("a table with no such column: %q, want none", got)
	}
}

// Bit operators and functions work on 64 unsigned bits on MySQL and on
// signed numbers on the copy.
func TestVeto_bitOperators(t *testing.T) {
	for stmt, want := range map[string]string{
		"SELECT n | 0 FROM t":                      vetoBitOperator,
		"SELECT n & 255 FROM t":                    vetoBitOperator,
		"SELECT n >> 1 FROM t":                     vetoBitOperator,
		"SELECT -1|0":                              vetoBitOperator,
		"SELECT id FROM t WHERE flags & 4 = 4":     vetoBitOperator,
		"SELECT id FROM t WHERE n | 0 > 0":         vetoBitOperator,
		"SELECT BIT_COUNT(n) FROM t":               vetoBitOperator,
		"SELECT bit_or (n) FROM t":                 vetoBitOperator,
		"SELECT BIT_AND(id) FROM t WHERE id > 9":   vetoBitOperator,
		"SELECT BIT_XOR(id) FROM t":                vetoBitOperator,
		"SELECT 1 && 1":                            vetoBitOperator, // the copy refuses it: kept back at no cost
		"SELECT id << 1 FROM t":                    "",              // where it would differ the copy refuses
		"SELECT '|', '&', '>>' FROM t":             "",
		"SELECT id /* a | b */ FROM t":             "",
		"SELECT bit_count, bit_or FROM t":          "", // columns
		"SELECT rabbit_count(n), `bit_and` FROM t": "",
		"SELECT id FROM t WHERE a > 1 AND b >= 2":  "",
	} {
		if got := Veto(stmt); got != want {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, want)
		}
	}
}

// CAST to DATETIME or TIME: a fraction of a second is rounded on MySQL, cut
// on MariaDB and kept on the copy.
func TestVeto_castToDatetimeOrTime(t *testing.T) {
	for stmt, want := range map[string]string{
		"SELECT CAST('2026-01-01 10:00:00.6' AS DATETIME)": vetoCastDatetime,
		"SELECT CAST(ts6 AS DATETIME) FROM t":              vetoCastDatetime,
		"SELECT cast(x as datetime ( 3 ) ) FROM t":         vetoCastDatetime,
		"SELECT CAST(x AS TIME) FROM t":                    vetoCastDatetime,
		"SELECT CAST(x AS\nTIME(1)) FROM t":                vetoCastDatetime,
		"SELECT CAST(x AS DATE) FROM t":                    "",
		"SELECT CAST(x AS CHAR) FROM t":                    "",
		"SELECT created_at AS time, v FROM t":              "", // an alias
		"SELECT created_at AS datetime FROM t":             "",
		"SELECT (SELECT 1) AS time FROM t":                 "",
		"SELECT x AS timeline, y AS datetimes FROM t":      "",
	} {
		if got := Veto(stmt); got != want {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, want)
		}
	}
}

// A string that starts with a two-digit year is the year 2026 on MySQL and
// the year 26 on the copy, as a typed literal, under a cast and against a
// column.
func TestVeto_twoDigitYear(t *testing.T) {
	for stmt, want := range map[string]string{
		"SELECT DATE '26-01-15'":                           vetoTwoDigitYear,
		"SELECT TIMESTAMP '26-01-15 10:00:00'":             vetoTwoDigitYear,
		"SELECT CAST('26-01-15' AS DATE)":                  vetoTwoDigitYear,
		"SELECT DATE('26-1-5')":                            vetoTwoDigitYear,
		"SELECT id FROM t WHERE d = '26-01-15'":            vetoTwoDigitYear,
		"SELECT id FROM t WHERE d = '26/01/15'":            vetoTwoDigitYear,
		"SELECT id FROM t WHERE d >= ' 99-12-31 23:59:59'": vetoTwoDigitYear,
		"SELECT id FROM t WHERE d = '2026-01-15'":          "",
		"SELECT id FROM t WHERE d = '1-2-3'":               "", // the year 1 on both
		"SELECT id FROM t WHERE d = '26.01.15'":            "", // the copy refuses it
		"SELECT id FROM t WHERE d = '260115'":              "", // and this
		"SELECT id FROM t WHERE code = '12-34'":            "",
		"SELECT id FROM t WHERE code = '12-AB-34'":         "",
		"SELECT id FROM t WHERE tm = '10:00:00'":           "",
		"SELECT id FROM t WHERE ip = '10.0.0.1'":           "",
		"SELECT id FROM t WHERE v = '26-'":                 "",
		"SELECT id FROM t /* '26-01-15' */":                "",
	} {
		if got := Veto(stmt); got != want {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, want)
		}
		if _, refusal := ForCopy(stmt); (refusal != "") != (want != "") {
			t.Errorf("ForCopy(%q) refusal = %q, want one: %v", stmt, refusal, want != "")
		}
	}
	for s, want := range map[string]bool{
		"26-01-15": true, "26/1/5": true, "  26-01-15 10:00:00": true, "00-1-1": true,
		"2026-01-15": false, "1-2-3": false, "26-01": false, "26-01-": false, "26-111-1": false, "": false, "26": false, "ab-01-15": false, "26.01.15": false,
	} {
		if got := TwoDigitYear(s); got != want {
			t.Errorf("TwoDigitYear(%q) = %v, want %v", s, got, want)
		}
	}
}

// The column rule reads every statement the copy is about to answer, in the
// process that captures: its work may grow with the statement's length and
// no faster. Counted, as TestVeto_deepNestingIsLinear counts the text
// checks.
func TestColumnVeto_isLinear(t *testing.T) {
	dates, whole := []string{"created_on", "d"}, []string{"tm"}
	shapes := map[string]func(n int) string{
		"nested": func(n int) string {
			return "SELECT " + strings.Repeat("f(", n) + "created_on" + strings.Repeat(")", n) + " FROM t"
		},
		"siblings": func(n int) string {
			return "SELECT g(" + strings.Repeat("(", n) + strings.Repeat("created_on, ", n) + "1" + strings.Repeat(")", n) + ") FROM t"
		},
		"qualifiers": func(n int) string { return "SELECT " + strings.Repeat("created_on.", n) + "created_on FROM t" },
		"quoted":     func(n int) string { return "SELECT " + strings.Repeat("`d`.`d`, ", n) + "1 FROM t" },
		"windows": func(n int) string {
			return "SELECT " + strings.Repeat("max(", n) + "d" + strings.Repeat(") over ()", n) + " FROM t"
		},
		"case": func(n int) string {
			return "SELECT " + strings.Repeat("case when a then ", n) + "d" + strings.Repeat(" end", n) + " FROM t"
		},
		"intervals": func(n int) string {
			return "SELECT " + strings.Repeat("d + interval 1 day, ", n) + "1 FROM (SELECT d FROM t) x"
		},
		"spaces": func(n int) string {
			return "SELECT d" + strings.Repeat(" ", 10*n) + ", " + strings.Repeat("( ", n) + "d" + strings.Repeat(" )", n) + " FROM t"
		},
		"unclosed": func(n int) string { return "SELECT " + strings.Repeat("max(d, (", n) },
	}
	for name, build := range shapes {
		why, small := columnVetoWork(Shape(build(2000)), dates, whole)
		if why != "" {
			t.Errorf("%s: kept back as %q, want none", name, why)
		}
		_, large := columnVetoWork(Shape(build(8000)), dates, whole)
		ratio := float64(large) / float64(small)
		t.Logf("%-12s %9d steps at 2,000, %9d at 8,000: x%.2f", name, small, large, ratio)
		if small == 0 || ratio > 4.5 {
			t.Errorf("%s: %d steps at 2,000 and %d at 8,000 (x%.2f): the work grows faster than the text", name, small, large, ratio)
		}
	}
	const depth = 2000
	for name, stmt := range map[string]string{
		"nested":  "SELECT " + strings.Repeat("f(", depth) + "created_on" + strings.Repeat(")", depth) + " + 1 FROM t",
		"windows": "SELECT " + strings.Repeat("max(", depth) + "d" + strings.Repeat(") over ()", depth) + " - 1 FROM t",
		"case":    "SELECT 1 + " + strings.Repeat("case when a then ", depth) + "d" + strings.Repeat(" end", depth) + " FROM t",
	} {
		if got := ColumnVeto(Shape(stmt), dates, whole); got == "" {
			t.Errorf("%s with arithmetic at the far end: not kept back", name)
		}
	}
}
