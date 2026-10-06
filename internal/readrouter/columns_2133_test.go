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
		// when what the group returns is a number, unless the function is
		// one known to return a number whatever it is given.
		"SELECT IFNULL(created_on, seen_at) + 1 FROM orders":         true,
		"SELECT TO_DAYS(created_on) - 1 FROM orders":                 true, // the copy refuses it
		"SELECT MEDIAN(created_on) OVER () FROM orders":              true, // MariaDB's: a number there, a date and time on the copy
		"SELECT MAX(YEAR(created_on) + created_on) FROM orders":      true,
		"SELECT YEAR(COALESCE(created_on, seen_at) + 1) FROM orders": true,
		// An alias of the date can be used from outside a subquery or a WITH,
		// where the text does not say it is one.
		"SELECT d + 1 FROM (SELECT created_on AS d FROM orders) x":                            true,
		"SELECT d + 1 FROM ( select DATE(seen_at) AS d FROM orders) x":                        true,
		"WITH c AS (SELECT created_on AS d FROM orders) SELECT d + 1 FROM c":                  true,
		"SELECT (SELECT MAX(created_on) FROM orders) + 1":                                     true,
		"SELECT id - 1 FROM orders WHERE id IN (SELECT id FROM orders WHERE created_on > '')": true, // for nothing
		"SELECT x -1 FROM (SELECT created_on AS x FROM orders) q":                             true, // x minus one, not minus-one
		"SELECT x - -1 FROM (SELECT created_on AS x FROM orders) q":                           true,
		"SELECT YEAR(d) + x FROM (SELECT created_on AS x, d FROM orders) q":                   true,
		"SELECT COALESCE(x, y) - 1 FROM (SELECT created_on AS x, y FROM orders) q":            true,
		"SELECT (x) + 1 FROM (SELECT created_on AS x FROM orders) q":                          true,
		// After a dot a reserved word is a name: q.limit -1 is a column minus one.
		"SELECT q.limit -1 FROM (SELECT created_on AS `limit` FROM orders) q":             true,
		"SELECT q.in -1 FROM (SELECT created_on AS `in` FROM orders) q":                   true,
		"SELECT q.then -1 FROM (SELECT created_on AS `then` FROM orders) q":               true,
		"SELECT q.by-1 FROM (SELECT created_on AS `by` FROM orders) q":                    true,
		"SELECT q.mod -1 FROM (SELECT created_on AS `mod` FROM orders) q":                 true,
		"SELECT q.case-1 FROM (SELECT created_on AS `case` FROM orders) q":                true,
		"SELECT q.all - 1 FROM (SELECT created_on AS `all` FROM orders) q":                true,
		"SELECT q . limit -1 FROM (SELECT created_on AS `limit` FROM orders) q":           true,
		"WITH x AS (SELECT created_on AS `select` FROM orders) SELECT x.select -1 FROM x": true,
		"SELECT `limit` -1 FROM (SELECT created_on AS `limit` FROM orders) q":             true,
		// Arithmetic on the date inside a call that returns a number is still arithmetic on the date.
		"SELECT YEAR(created_on + 1) FROM orders":                              true,
		"SELECT DAY(created_on + 1) FROM orders":                               true,
		"SELECT SUM(created_on - seen_at) FROM orders":                         true,
		"SELECT COUNT(DISTINCT created_on + 1) FROM orders":                    true,
		"SELECT EXTRACT(DAY FROM created_on - seen_at) FROM orders":            true,
		"SELECT 1 - year FROM (SELECT created_on AS year FROM orders) q":       true, // a name, not a call of YEAR
		"SELECT MEDIAN(x) OVER () FROM (SELECT created_on AS x FROM orders) q": true,
		"SELECT offset - 1 FROM (SELECT created_on AS offset FROM orders) q":   true, // offset is not a reserved word
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
		// A call that is a number on both sides whatever it is given.
		"SELECT YEAR(created_on) + 1 FROM orders":                                                false,
		"SELECT MAX(YEAR(created_on)) + 1 FROM orders":                                           false,
		"SELECT YEAR(created_on) * 100 + MONTH(created_on) FROM orders":                          false,
		"SELECT EXTRACT(YEAR FROM created_on) - 2000 FROM orders":                                false,
		"SELECT COUNT(*) - COUNT(seen_at), HOUR(seen_at) + 1 FROM orders":                        false,
		"SELECT SUM(CASE WHEN created_on >= '' THEN amount END) - SUM(amount) FROM orders":       false,
		"SELECT DAY(created_on) - DAYOFYEAR(seen_at), QUARTER(created_on) + 1 FROM orders":       false,
		"SELECT status FROM orders GROUP BY YEAR(created_on) - YEAR(seen_at)":                    false,
		"SELECT status, MAX(created_on) FROM orders GROUP BY status HAVING SUM(a) - SUM(b) > -1": false,
		"SELECT id FROM orders WHERE created_on >= '' ORDER BY amount DESC LIMIT 10":             false,
		// The sign of a number is not arithmetic, with a subquery or not.
		"SELECT id FROM orders WHERE id IN (SELECT id FROM orders WHERE created_on > '' AND amount > -1)":    false,
		"SELECT id FROM (SELECT id, created_on FROM orders) x WHERE a BETWEEN -5 AND +5 AND b * 1e-2 > (-1)": false,
		"SELECT CASE WHEN a THEN -1 ELSE - 2 END, f(a, -3) FROM (SELECT a, created_on FROM orders) x":        false,
		"SELECT YEAR(d) + 1, SUM(a) - COUNT(*) FROM (SELECT created_on AS d, a FROM orders) x":               false,
		// Another word that holds the name is not the name.
		"SELECT created_on_utc + 1, xcreated_on - 1 FROM orders": false,
		"SELECT `created_on x` + 1 FROM orders":                  false,
		// No column of the table named at all.
		"SELECT id + 1 FROM orders": false,
		"SELECT 1":                  false,
	} {
		got := ColumnVeto(Shape(stmt), dates, nil, false)
		if (got != "") != kept {
			t.Errorf("ColumnVeto(%q) = %q, want kept on the source: %v", stmt, got, kept)
		}
		if kept && !strings.Contains(got, "created_on") && !strings.Contains(got, "seen_at") {
			t.Errorf("ColumnVeto(%q) = %q: the reason does not name the column", stmt, got)
		}
	}
}

// A date can reach a + or an AVG under another name: an alias used where
// MySQL takes one for its expression, an alias given inside a subquery, a
// column list over a star. And a TIME or YEAR column can be reached with no
// name at all, through a star.
func TestColumnVeto_aDateUnderAnotherName(t *testing.T) {
	dates, whole := []string{"d", "d2"}, []string{"tm"}
	for stmt, kept := range map[string]bool{
		// An alias in GROUP BY, HAVING and ORDER BY.
		"SELECT d AS x, d2 AS y FROM t GROUP BY x, y HAVING x - y > 5": true,
		"SELECT d AS x, d2 AS y FROM t ORDER BY x - y":                 true,
		"SELECT d x, COUNT(*) FROM t GROUP BY x + 0":                   true,
		"SELECT status, MAX(d) FROM t GROUP BY status ORDER BY n - 1":  true, // for nothing
		"SELECT a - b, d FROM t GROUP BY d ORDER BY d":                 false,
		"SELECT a + 1 FROM t WHERE d >= '' ORDER BY d DESC":            false,
		"SELECT d FROM t ORDER BY d - INTERVAL 1 DAY":                  false,
		// AVG of an alias from a subquery or a WITH.
		"SELECT AVG(x) FROM (SELECT d AS x FROM t) q":           true,
		"WITH q AS (SELECT d AS x FROM t) SELECT AVG(x) FROM q": true,
		"SELECT AVG(n) FROM (SELECT n, d FROM t) q":             true, // for nothing
		"SELECT AVG(n), MAX(d) FROM t":                          false,
		// A column list over a star: the date's name is nowhere.
		"SELECT z - y FROM (SELECT * FROM t) AS q(x, y, z)":          true,
		"WITH q(i, x, y) AS (SELECT * FROM t) SELECT x - y FROM q":   true,
		"SELECT b - a FROM (TABLE t) q(i, a, b)":                     true,
		"SELECT AVG(x) FROM (SELECT t.* FROM t) q(i, x)":             true,
		"SELECT x FROM (SELECT * FROM t) AS q(x, y, z)":              false,
		"SELECT a - 1 FROM (SELECT a, COUNT(*) FROM t GROUP BY a) q": false, // no star and no date named
		"SELECT COUNT(*) - 1 FROM t WHERE a IN (SELECT 2 * 3)":       false, // count(*) and a product are not stars
	} {
		if got := ColumnVeto(Shape(stmt), dates, nil, false); (got != "") != kept {
			t.Errorf("ColumnVeto(%q) = %q, want kept on the source: %v", stmt, got, kept)
		}
	}
	// A star with no space after SELECT, and a bare TABLE t, are stars too.
	for _, stmt := range []string{
		"WITH q(i, a, b) AS (SELECT*FROM t) SELECT a - 1 FROM q",
		"WITH q(i, a, b) AS (select*from t) SELECT AVG(a) FROM q",
		"WITH q(i, a, b) AS (SELECT ALL*FROM t) SELECT a - b FROM q",
		"WITH q(i, a, b) AS (SELECT DISTINCT*FROM t) SELECT a - b FROM q",
		"WITH q(i, a, b) AS (TABLE t) SELECT a - b FROM q",
		"SELECT a - b FROM (SELECT x.*FROM t x) q(i, a, b)",
	} {
		if got := ColumnVeto(Shape(stmt), dates, nil, false); got == "" {
			t.Errorf("ColumnVeto(%q): not kept back", stmt)
		}
	}
	// A star the caller's parse saw and the text search would not.
	const hidden = "WITH q(i, a, b) AS (SELECT COLUMNS(c) FROM t) SELECT a - b FROM q"
	if got := ColumnVeto(Shape(hidden), dates, nil, false); got != "" {
		t.Errorf("ColumnVeto(%q) with no star known: %q, want none", hidden, got)
	}
	if got := ColumnVeto(Shape(hidden), dates, nil, true); got == "" {
		t.Errorf("ColumnVeto(%q) with a star the parse saw: not kept back", hidden)
	}
	// A star over a table with a TIME or YEAR column reaches the column
	// without its name: the copy sorts and compares a TIME as text.
	for stmt, star := range map[string]bool{
		"SELECT * FROM t ORDER BY 2":                              false,
		"SELECT*FROM t ORDER BY 2 LIMIT 1":                        false,
		"SELECT t.* FROM t":                                       false,
		"SELECT a, * FROM t":                                      false,
		"TABLE t":                                                 false,
		"SELECT MAX(b) FROM (SELECT * FROM t) q(a, b)":            false,
		"SELECT b FROM (SELECT * FROM t) q(a, b) WHERE b >= ''":   false,
		"SELECT 1 FROM x WHERE '' IN (SELECT * FROM t)":           false,
		"SELECT c FROM (SELECT * FROM t) q(a, b, c) WHERE c = 26": false,
		"SELECT COLUMNS(c) FROM t":                                true, // a star only the caller's parse sees
	} {
		if got := ColumnVeto(Shape(stmt), nil, whole, star); !strings.Contains(got, "star over a table with a TIME or YEAR") {
			t.Errorf("a TIME column: ColumnVeto(%q) = %q, want kept on the source for its star", stmt, got)
		}
	}
	for _, stmt := range []string{"SELECT COUNT(*), a * 2 FROM t", "SELECT a FROM t WHERE b IN (1 , 2)", "SELECT a, b FROM t ORDER BY 2"} {
		if got := ColumnVeto(Shape(stmt), nil, whole, false); got != "" {
			t.Errorf("a TIME column and no star: ColumnVeto(%q) = %q, want none", stmt, got)
		}
	}
	// A date column whose own name starts with $, quoted or not.
	for _, stmt := range []string{"SELECT $d + 1 FROM t", "SELECT `$d` + 1 FROM t"} {
		if got := ColumnVeto(Shape(stmt), []string{"$d"}, nil, false); got == "" {
			t.Errorf("ColumnVeto(%q) with a column named $d: not kept back", stmt)
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
		if got := ColumnVeto(Shape(stmt), dates, nil, false); (got != "") != kept {
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
		if got := ColumnVeto(Shape(stmt), []string{"created_on"}, []string{"tm"}, false); got != "" {
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
		if got := ColumnVeto(Shape(stmt), nil, whole, false); (got != "") != kept {
			t.Errorf("ColumnVeto(%q) = %q, want kept on the source: %v", stmt, got, kept)
		}
	}
}

// Names are matched the way MySQL compares the names of columns: the same
// letters up to case, accents kept, in whatever script. A name outside ASCII
// that is not a column keeps nothing back.
func TestColumnVeto_namesOutsideASCII(t *testing.T) {
	dates := []string{"año", "fecha_creación", "created_at", "İd", "kind"}
	for stmt, kept := range map[string]bool{
		"SELECT año + 1 FROM t":                                            true,
		"SELECT AÑO + 1 FROM t":                                            true,
		"SELECT `Año`+1 FROM t":                                            true,
		"SELECT t.fecha_creación - 1 FROM t":                               true,
		"SELECT FECHA_CREACIÓN - 1 FROM t":                                 true,
		"SELECT AVG(`fecha_creación`) FROM t":                              true,
		"SELECT \u212aind + 1 FROM t":                                      true,  // a Kelvin sign for the k
		"SELECT k\u0131nd + 1 FROM t":                                      true,  // a dotless i, which a server that compares without case takes for i
		"SELECT ano + 1, fecha_creacion - 1 FROM t":                        false, // no accent is dropped
		"SELECT total AS `número` FROM t WHERE created_at >= ''":           false,
		"SELECT total AS número, precio + 1 FROM t WHERE created_at >= ''": false,
		"SELECT `año`, `fecha_creación` FROM t ORDER BY `año`":             false,
		"SELECT 名前 + 1 FROM t WHERE created_at >= ''":                      false,
	} {
		if got := ColumnVeto(Shape(stmt), dates, nil, false); (got != "") != kept {
			t.Errorf("ColumnVeto(%q) = %q, want kept on the source: %v", stmt, got, kept)
		}
	}
	if got := ColumnVeto(Shape("SELECT hora FROM t WHERE DÍA = 1"), nil, []string{"día"}, false); got == "" {
		t.Error("a YEAR column named día, written DÍA: not kept back")
	}
}

// Not known is never read as "nothing to find": a statement that is not
// valid UTF-8, a column whose name cannot be looked for and a shape that was
// not handed over each keep the statement on the source, and a table with no
// such column asks nothing.
func TestColumnVeto_whatCannotBeSearchedIsRefused(t *testing.T) {
	dates := []string{"created_on"}
	if got := ColumnVeto("SELECT id FROM orders WHERE a\xff > 1", dates, nil, false); !strings.Contains(got, "not valid UTF-8") {
		t.Errorf("a statement that is not UTF-8: %q, want refused", got)
	}
	for _, name := range []string{"fecha de alta", "a`b", `a"b`, "a.b", "d\xff", ""} {
		if got := ColumnVeto(Shape("SELECT id FROM orders"), []string{name}, nil, false); !strings.Contains(got, "cannot be looked for") {
			t.Errorf("a date column named %q: %q, want every statement refused", name, got)
		}
		if got := ColumnVeto(Shape("SELECT id FROM orders"), nil, []string{name}, false); !strings.Contains(got, "cannot be looked for") {
			t.Errorf("a TIME column named %q: %q, want every statement refused", name, got)
		}
	}
	if got := ColumnVeto("", dates, nil, false); got == "" {
		t.Error("no shape and a date column: answered, want refused")
	}
	if got := ColumnVeto("", nil, nil, false); got != "" {
		t.Errorf("no shape and no such column: %q, want none", got)
	}
	if got := ColumnVeto("SELECT a\xff + 1 FROM orders", nil, nil, false); got != "" {
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
		// The days of the week are numbered another way on the copy, and its
		// microseconds hold the seconds.
		"SELECT DAYOFWEEK(d) FROM t":                     vetoDayNumbering,
		"SELECT weekday (d) FROM t":                      vetoDayNumbering,
		"SELECT MICROSECOND(ts) FROM t":                  vetoDayNumbering,
		"SELECT EXTRACT( MICROSECOND FROM ts) FROM t":    vetoDayNumbering,
		"SELECT EXTRACT(SECOND FROM ts), weekday FROM t": "",
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
		"SELECT id FROM t WHERE d = '26 01 15'":            vetoTwoDigitYear,
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
		"26-01-15": true, "26/1/5": true, "26 01 15": true, "\v26-01-15": true, "  26-01-15 10:00:00": true, "00-1-1": true,
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
		"signs": func(n int) string {
			return "SELECT d, " + strings.Repeat("sum(a) - sum(b), a > -1, year(x) + 1, ", n) + "1 FROM (SELECT d FROM t) x ORDER BY 1"
		},
		"long words": func(n int) string {
			return "SELECT d, " + strings.Repeat("a", 10*n) + strings.Repeat(" * -1", n) + " FROM (SELECT d FROM t) x"
		},
		"many names": func(n int) string {
			return "SELECT " + strings.Repeat("`d`, created_on, t.d, año, ", n) + "1 FROM t ORDER BY 1"
		},
	}
	for name, build := range shapes {
		why, small := columnVetoWork(Shape(build(2000)), dates, whole, false)
		if why != "" {
			t.Errorf("%s: kept back as %q, want none", name, why)
		}
		_, large := columnVetoWork(Shape(build(8000)), dates, whole, false)
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
		if got := ColumnVeto(Shape(stmt), dates, whole, false); got == "" {
			t.Errorf("%s with arithmetic at the far end: not kept back", name)
		}
	}
}
