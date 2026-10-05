package readrouter

import "testing"

// Statements that pass EXPLAIN on the source and that the copy answers too,
// with another value (#2122). Measured on MySQL 8.4.9, MariaDB 11.4 and the
// copy; the fixtures in consoleapp pin the answers themselves.

// `0x10` is one byte on MySQL and the number 0 under the alias x10 on the
// copy; a name that starts with a digit (`2fa`) is the number 2 under the
// alias fa there.
func TestVeto_wordThatStartsWithADigit(t *testing.T) {
	for stmt, want := range map[string]string{
		"SELECT 0x10":                                vetoDigitWord,
		"SELECT 0x4142, 2":                           vetoDigitWord,
		"SELECT id FROM t WHERE k = 0xDEADBEEF":      vetoDigitWord,
		"SELECT 0X10":                                vetoDigitWord, // a name on MySQL (the x must be lower case), still a number and an alias on the copy
		"SELECT 0x":                                  vetoDigitWord,
		"SELECT 0xZZ FROM t":                         vetoDigitWord,
		"SELECT 0b101":                               vetoDigitWord,
		"SELECT 0B101 FROM t":                        vetoDigitWord,
		"SELECT _binary 0x41":                        vetoDigitWord,
		"SELECT 0x10 + 0":                            vetoDigitWord,
		"SELECT 2fa FROM users":                      vetoDigitWord,
		"SELECT id,2fa FROM users":                   vetoDigitWord,
		"SELECT (10x) FROM t":                        vetoDigitWord,
		"SELECT 1_000":                               vetoDigitWord, // a name on MySQL, 1000 on the copy
		"SELECT 1e FROM t":                           vetoDigitWord, // a name on MySQL, 1 AS e on the copy
		"SELECT 1e\nFROM t":                          vetoDigitWord,
		"SELECT 1e":                                  vetoDigitWord,
		"SELECT 1ex FROM t":                          vetoDigitWord,
		"SELECT 1e-x FROM t":                         vetoDigitWord,
		"SELECT 1é FROM t":                           vetoDigitWord,
		"SELECT 1$ FROM t":                           vetoDigitWord,
		"SELECT id FROM t WHERE id = ? AND k = 0x01": vetoDigitWord, // a prepared statement's template
		"select 3d_model from parts":                 vetoDigitWord,
		// Numbers, which both sides read alike.
		"SELECT 10":                           "",
		"SELECT 1e5":                          "",
		"SELECT 1E2":                          "",
		"SELECT 1e+2":                         "",
		"SELECT 1e-2, 3":                      "",
		"SELECT 1.5e2":                        "",
		"SELECT .5e1":                         "",
		"SELECT 1.":                           "",
		"SELECT 1.e2":                         "",
		"SELECT 1.5abc":                       "", // 1.5 AS abc on both
		"SELECT 007 FROM t":                   "",
		"SELECT 1+2e3":                        "",
		"SELECT id FROM t LIMIT 10 OFFSET 20": "",
		"SELECT id FROM t WHERE id IN (1,2,3) AND x = ?": "",
		// A digit that does not start a word.
		"SELECT t0x1 FROM t":               "",
		"SELECT a.x FROM a":                "",
		"SELECT x10, b101 FROM t":          "",
		"SELECT col_0x10 FROM t":           "",
		"SELECT é0x10 FROM t":              "",
		"SELECT $0x10 FROM t":              "",
		"SELECT utf8mb4_0900_ai_ci FROM t": "",
		// Quoted names, string literals and comments are not words.
		"SELECT `2fa` FROM users":          "",
		"SELECT `0x10` FROM t":             "",
		"SELECT `t`.`2fa`, `10x` FROM `t`": "",
		"SELECT 'a 0x10 b' FROM t":         "",
		"SELECT '0x10', '2fa'":             "",
		"SELECT 1 /* 0x10 */ FROM t":       "",
		"SELECT 1 -- 0x10\n FROM t":        "",
		// After a dot the copy refuses the statement (t.2fa), or both read a
		// decimal number.
		"SELECT t.2fa FROM t": "",
	} {
		if got := Veto(stmt); got != want {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, want)
		}
	}
}

// x'41' is a hexadecimal string and b'1' a bit string on MySQL, and the
// texts x41 and b1 on the copy; e'x' is the column e under the alias x on
// MySQL and an escaped string on the copy.
func TestVeto_letterAgainstAString(t *testing.T) {
	for stmt, want := range map[string]string{
		"SELECT x'41'":                           vetoPrefixString,
		"SELECT X'41'":                           vetoPrefixString,
		"SELECT b'1'":                            vetoPrefixString,
		"SELECT B'101' + 0":                      vetoPrefixString,
		"SELECT e'x' FROM (SELECT 1 AS e) t":     vetoPrefixString,
		"SELECT E'x' FROM (SELECT 1 AS e) t":     vetoPrefixString,
		"SELECT id FROM t WHERE k = x'70616964'": vetoPrefixString,
		"SELECT id FROM t WHERE k=X''":           vetoPrefixString,
		"SELECT(x'41')":                          vetoPrefixString,
		"SELECT t.e'x' FROM t":                   vetoPrefixString,
		"SELECT x'41' FROM t WHERE id = ?":       vetoPrefixString,
		"SELECT x\"41\"":                         vetoDoubleQuoted, // already kept on MySQL
		"SELECT `t`x'41' FROM t":                 vetoNameString,   // #2130
		// With white space or a comment between, the copy refuses the
		// statement (no type is named x, b or e) and MySQL answers.
		"SELECT x 'a' FROM (SELECT 1 AS x) t":    "",
		"SELECT b\n'a' FROM (SELECT 1 AS b) t":   "",
		"SELECT x/**/'a' FROM (SELECT 1 AS x) t": "",
		// A longer word, a name, and a letter that is not a prefix on both.
		"SELECT max'a'": "",
		"SELECT xx'41'": "",
		"SELECT _x'41'": "",
		"SELECT n'a'":   "", // 'a' on both
		"SELECT N'a'":   "",
		"SELECT b.id FROM orders b WHERE b.status = 'paid'": "",
		"SELECT x, b, e FROM t WHERE x = 'a'":               "",
		"SELECT 'x' 'b'":                                    "",
		"SELECT 'a''x''b'":                                  "",
		"SELECT 'it''s x'":                                  "",
		"SELECT 1 /* x'41' */":                              "",
		"SELECT 1 -- b'1'\n":                                "",
	} {
		if got := Veto(stmt); got != want {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, want)
		}
	}
}

// ~ is bitwise NOT on MySQL, over 64 unsigned bits (~1 is
// 18446744073709551614); on the copy it is -2, and between two operands a
// regular expression match.
func TestVeto_tilde(t *testing.T) {
	for stmt, want := range map[string]string{
		"SELECT ~1":                              vetoTilde,
		"SELECT ~id FROM orders":                 vetoTilde,
		"SELECT id FROM orders WHERE flags = ~0": vetoTilde,
		"SELECT - ~ 1":                           vetoTilde,
		"SELECT ~?":                              vetoTilde, // a prepared statement's template
		"SELECT ~`id` FROM `orders`":             vetoTilde,
		"SELECT '~'":                             "",
		"SELECT 1 /* ~ */":                       "",
		"SELECT `a~b` FROM t":                    "",
		"SELECT id FROM t WHERE p = '~/x'":       "",
	} {
		if got := Veto(stmt); got != want {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, want)
		}
	}
}

// A date plus or minus something is a number built from its digits on MySQL
// (DATE '2026-01-01' + 1 is 20260102) and a date, a count of days or an
// interval on the copy.
func TestVeto_dateArithmetic(t *testing.T) {
	for stmt, want := range map[string]string{
		"SELECT DATE '2026-01-01' + 1":                                             vetoDateArithmetic,
		"SELECT DATE '2026-01-01' - 1":                                             vetoDateArithmetic,
		"SELECT DATE '2026-01-01' + 0":                                             vetoDateArithmetic,
		"SELECT DATE '2026-01-01'+1":                                               vetoDateArithmetic,
		"SELECT DATE '2026-01-01' -1":                                              vetoDateArithmetic,
		"select date\n'2026-01-01'\n+\n1":                                          vetoDateArithmetic,
		"SELECT 1 + DATE '2026-01-01'":                                             vetoDateArithmetic,
		"SELECT id + DATE '2026-01-01' FROM orders":                                vetoDateArithmetic,
		"SELECT (DATE '2026-01-01') + 1":                                           vetoDateArithmetic,
		"SELECT ( ( DATE '2026-01-01' ) ) - 1":                                     vetoDateArithmetic,
		"SELECT 1 + (DATE '2026-01-01')":                                           vetoDateArithmetic,
		"SELECT DATE '2026-02-01' - DATE '2026-01-31'":                             vetoDateArithmetic, // 70 on MySQL, 1 on the copy
		"SELECT created_on - DATE '2025-12-31' FROM orders":                        vetoDateArithmetic,
		"SELECT max(created_on) - DATE '2025-12-31' FROM orders":                   vetoDateArithmetic,
		"SELECT TIMESTAMP '2026-01-01 10:00:00' - TIMESTAMP '2026-01-01 09:00:00'": vetoDateArithmetic,
		"SELECT TIMESTAMP '2026-01-02 00:00:00' - x FROM t":                        vetoDateArithmetic,
		"SELECT DATE('2026-01-01') + 1":                                            vetoDateArithmetic,
		"SELECT DATE(created_on) + 1 FROM orders":                                  vetoDateArithmetic,
		"SELECT DATE(created_on) - DATE('2025-12-31') FROM orders":                 vetoDateArithmetic,
		"SELECT DATE(GREATEST(a, b)) - 1 FROM t":                                   vetoDateArithmetic,
		"SELECT 7 - DATE (created_on) FROM orders":                                 vetoDateArithmetic,
		"SELECT CAST('2026-01-01' AS DATE) + 1":                                    vetoDateArithmetic,
		"SELECT CAST(created_on AS DATE) - 1 FROM orders":                          vetoDateArithmetic,
		"SELECT cast( x as date ) + 1 FROM t":                                      vetoDateArithmetic,
		"SELECT CAST(a AS DATETIME) - CAST(b AS DATETIME) FROM t":                  vetoDateArithmetic,
		"SELECT CAST(a AS DATETIME(6)) - 1 FROM t":                                 vetoDateArithmetic,
		"SELECT CAST(? AS DATE) + 1":                                               vetoDateArithmetic, // a prepared statement's template
		"SELECT DATE ? + 1":                                                        "",                 // not a statement on MySQL
		"SELECT id FROM orders WHERE created_on > DATE '2026-01-01' + ?":           vetoDateArithmetic,
		"SELECT id FROM orders WHERE DATE(created_on) + 1 > 20260102":              vetoDateArithmetic,
		"SELECT DATE '2026-01-01' + interval_days FROM t":                          vetoDateArithmetic, // a column, not INTERVAL
		"SELECT DATE '2026-01-01' + INTERVAL 1 DAY + 1":                            "",                 // the copy refuses it
		// Over-vetoes, on purpose: the parentheses of a call are not told from
		// grouping ones, and INTERVAL written first is not looked for.
		"SELECT YEAR(DATE '2026-01-01') + 1":        vetoDateArithmetic,
		"SELECT MONTH(DATE(created_on)) - 1 FROM t": vetoDateArithmetic,
		"SELECT INTERVAL 1 DAY + DATE '2026-01-01'": vetoDateArithmetic,
		// A date and an interval: the same day on both.
		"SELECT DATE '2026-01-01' + INTERVAL 1 DAY":                                    "",
		"SELECT DATE '2026-01-01' - interval 1 month":                                  "",
		"SELECT DATE '2026-01-01'+INTERVAL 1 DAY":                                      "",
		"SELECT DATE '2026-01-01' -\n INTERVAL 1 DAY":                                  "",
		"SELECT (DATE '2026-01-01') + INTERVAL 1 DAY":                                  "",
		"SELECT DATE(created_on) + INTERVAL 1 DAY FROM orders":                         "",
		"SELECT CAST(x AS DATE) - INTERVAL 7 DAY FROM t":                               "",
		"SELECT id FROM orders WHERE created_on >= DATE '2026-01-08' - INTERVAL 7 DAY": "",
		// A date with no arithmetic next to it.
		"SELECT DATE '2026-01-01'":                                                               "",
		"SELECT id FROM orders WHERE created_on = DATE '2026-01-01'":                             "",
		"SELECT id FROM orders WHERE created_on BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'": "",
		"SELECT DATE(created_on), sum(amount * 2) - 1 FROM orders GROUP BY DATE(created_on)":     "",
		"SELECT CAST(created_on AS DATE), amount + 1 FROM orders":                                "",
		"SELECT amount - 1, DATE '2026-01-01' FROM orders":                                       "",
		"SELECT amount + 1 AS date, created_on AS date2 FROM orders":                             "",
		"SELECT created_on AS date FROM orders WHERE amount + 1 > 2":                             "",
		"SELECT YEAR(created_on) + 1, MONTH(created_on) - 1 FROM orders":                         "",
		"SELECT CAST(a AS DECIMAL(10,2)) + 1, CAST(b AS CHAR) FROM t":                            "",
		"SELECT CAST(DATE(a) AS CHAR) FROM t":                                                    "",
		"SELECT DATE '2026-01-01' = x + 1 FROM t":                                                "",
		"SELECT update_date + 1, to_date - 1 FROM t":                                             "", // columns: nothing in the text says they are dates
		"SELECT created_on + 1 FROM orders":                                                      "",
		"SELECT `date` + 1 FROM t":                                                               "",
		"SELECT 'DATE ''2026-01-01'' + 1'":                                                       "",
		"SELECT 1 /* DATE '2026-01-01' + 1 */":                                                   "",
		"SELECT DATE(":                                                                           "",
		"SELECT CAST(x AS DATE":                                                                  "",
	} {
		if got := Veto(stmt); got != want {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, want)
		}
	}
}

// A NUL byte is still not a veto: no statement was found that both sides
// answer. Outside a string or a comment both servers refuse it; inside a
// string or a /* */ comment the copy refuses it; at the very end, or in a
// trailing line comment, both answer the same.
func TestVeto_nulByteIsNotAVeto(t *testing.T) {
	for _, stmt := range []string{"SELECT 1\x00+1", "SELECT 'a\x00b'", "SELECT 1 /* \x00 */ + 1", "SELECT 1 -- x\x00\n+1", "SELECT 1\x00"} {
		if got := Veto(stmt); got != "" {
			t.Errorf("Veto(%q) = %q, want none", stmt, got)
		}
	}
}

// The new vetoes are named after the ones that were already there.
func TestVeto_2122_olderVetoesAreNamedFirst(t *testing.T) {
	for stmt, want := range map[string]string{
		"SELECT NOW() + 0x10":  "NOW/CURDATE/CURTIME/CURRENT_TIMESTAMP",
		"SELECT CURDATE() + 1": "NOW/CURDATE/CURTIME/CURRENT_TIMESTAMP",
		"SELECT x'41' || ~1":   "|| (string concatenation on the copy, logical OR on MySQL)",
		"SELECT x'4\\1'":       vetoBackslash,
	} {
		if got := Veto(stmt); got != want {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, want)
		}
	}
}
