package readrouter

import (
	"strings"
	"testing"
)

// #2918: LENGTH and OCTET_LENGTH count bytes in the column's character set
// on MySQL, BIT_LENGTH eight times as many, CHAR_LENGTH and CHARACTER_LENGTH
// characters. The copy holds every text as UTF-8: over a column whose bytes
// are that UTF-8 (utf8mb4, utf8mb3, ascii) it gives MySQL's numbers, with
// LENGTH and OCTET_LENGTH written as its strlen (ForCopyOpts). LengthVeto
// keeps the statement on MySQL unless every call's one argument is such a
// column.
func TestLengthVeto(t *testing.T) {
	safe := []string{"u", "c", "m3", "año", "length"}
	other := []string{"l", "bn", "js", "n", "id"}
	for stmt, want := range map[string]bool{
		// Answered by the copy.
		"SELECT LENGTH(u), OCTET_LENGTH(u), BIT_LENGTH(u), CHAR_LENGTH(u), CHARACTER_LENGTH(u) FROM t": false,
		"SELECT length(c), char_length(m3) FROM t":                                                     false,
		"SELECT Length(U) FROM t":                                                                   false, // a column's name in another case
		"SELECT LENGTH (u), CHAR_LENGTH\v(c) FROM t":                                                false, // white space MySQL takes before the parenthesis
		"SELECT LENGTH(`u`), LENGTH(t.u), LENGTH(`db`.`t`.`u`), LENGTH((u)) FROM t":                 false,
		"SELECT LENGTH( u ) FROM t":                                                                 false,
		"SELECT LENGTH(año) FROM t":                                                                 false,
		"SELECT LENGTH(length) FROM t":                                                              false, // a column called length
		"SELECT SUM(LENGTH(u)), MAX(CHAR_LENGTH(c)), MIN(BIT_LENGTH(m3)) FROM t":                    false,
		"SELECT k % 100 AS bucket, COUNT(*), SUM(LENGTH(c)) FROM t GROUP BY bucket ORDER BY bucket": false, // the call is before GROUP BY
		"SELECT id FROM t WHERE LENGTH(u) > 100":                                                    false,
		"SELECT u FROM t":                                                                           false,
		"SELECT length FROM t":                                                                      false,
		"SELECT * FROM t WHERE u = 'length(l)'":                                                     false, // inside a string
		"SELECT mylength(l), length_of(l), $length(l), xchar_length(l) FROM t":                      false, // other functions
		// Kept on MySQL.
		"SELECT LENGTH(l) FROM t":                                      true, // latin1: one byte a letter
		"SELECT CHAR_LENGTH(l) FROM t":                                 true,
		"SELECT LENGTH(bn), LENGTH(js), LENGTH(n), LENGTH(id) FROM t":  true,
		"SELECT LENGTH(u), LENGTH(l) FROM t":                           true,
		"SELECT LENGTH(unknown_col) FROM t":                            true, // not known
		"SELECT LENGTH(ano) FROM t":                                    true, // accents are kept in names
		"SELECT LENGTH(LOWER(u)) FROM t":                               true, // an expression
		"SELECT LENGTH(CONCAT(u, c)) FROM t":                           true,
		"SELECT LENGTH(u || c) FROM t":                                 true,
		"SELECT LENGTH('abc')":                                         true, // a literal
		"SELECT LENGTH(?)":                                             true, // a placeholder
		"SELECT LENGTH(123)":                                           true,
		"SELECT LENGTH(u, c) FROM t":                                   true,
		"SELECT LENGTH() FROM t":                                       true,
		"SELECT LENGTH(u FROM t":                                       true, // no closing parenthesis
		"SELECT t.length(u) FROM t":                                    true, // a stored function on MySQL
		"SELECT db . length(u) FROM t":                                 true,
		"SELECT `length`(u) FROM t":                                    true, // the built-in function, quoted
		"SELECT LENGTH(u) FROM (SELECT l AS u FROM t) q":               true, // a name the statement defines
		"WITH q AS (SELECT l AS u FROM t) SELECT LENGTH(u) FROM q":     true,
		"SELECT LENGTH(u) FROM t WHERE id IN (SELECT id FROM s)":       true,
		"SELECT l AS u FROM t ORDER BY LENGTH(u)":                      true, // MySQL reads a select-list alias there
		"SELECT l AS u FROM t GROUP BY l HAVING CHAR_LENGTH(u) > 1":    true,
		"SELECT k, COUNT(*) FROM t GROUP BY k ORDER BY SUM(LENGTH(u))": true,
		"SELECT LENGTH/**/(l) FROM t":                                  true, // a comment before the parenthesis: still the call
		"SELECT LENGTH -- x\n(l) FROM t":                               true,
		"SELECT LENGTH#x\n(l) FROM t":                                  true,
		"SELECT LENGTH/**/(u) FROM t":                                  false,
	} {
		got := LengthVeto(Shape(stmt), safe, other)
		if (got != "") != want {
			t.Errorf("LengthVeto(%q) = %q, want vetoed %v", stmt, got, want)
		}
	}
	// The reason says which call kept the statement on MySQL.
	for stmt, in := range map[string][2]string{
		"SELECT LENGTH(u), LENGTH(l) FROM t":     {"LENGTH(l)", "LENGTH(u)"},
		"SELECT CHAR_LENGTH(u), LENGTH(n) FROM t": {"LENGTH(n)", "CHAR_LENGTH(u)"},
		"SELECT LENGTH(LOWER(u)) FROM t":         {"LENGTH(LOWER(u))", "zzz"},
		"SELECT LENGTH/**/(l) FROM t":            {"LENGTH(l)", "zzz"},
	} {
		got := LengthVeto(Shape(stmt), safe, other)
		if !strings.HasPrefix(got, VetoLength) || !strings.Contains(got, in[0]) || strings.Contains(got, in[1]) {
			t.Errorf("LengthVeto(%q) = %q, want it to name %s and not %s", stmt, got, in[0], in[1])
		}
	}
	// A name that is utf8mb4 in one table and latin1 in another.
	if LengthVeto(Shape("SELECT LENGTH(u) FROM a JOIN b USING (k)"), []string{"u"}, []string{"U"}) == "" {
		t.Error("u is latin1 in one of the tables, yet the copy answers")
	}
	// Nothing known: every length call stays on MySQL.
	if LengthVeto(Shape("SELECT LENGTH(u) FROM t"), nil, nil) == "" {
		t.Error("with no types known the copy answers")
	}
	// No call: nothing to ask.
	if got := LengthVeto(Shape("SELECT u FROM t"), nil, nil); got != "" {
		t.Errorf("no length call, nothing known: %q", got)
	}
}

// Veto does not keep the length functions on MySQL by their text: the
// columns' character sets decide (LengthVeto, asked where they are known).
func TestVeto_leavesLengthToTheTypes(t *testing.T) {
	for _, stmt := range []string{
		"SELECT LENGTH(c), OCTET_LENGTH(c), BIT_LENGTH(c), CHAR_LENGTH(c), CHARACTER_LENGTH(c) FROM t",
		"SELECT k % 100 AS bucket, COUNT(*), SUM(LENGTH(c)) FROM sbtest1 GROUP BY bucket ORDER BY bucket",
	} {
		if v := VetoOpts(stmt, CopyOptions{MySQLLength: true}); v != "" {
			t.Errorf("VetoOpts(%q) = %q", stmt, v)
		}
	}
}

// Under any connection collation a length call of a column is a number the
// collation cannot change (#2918): the statement is collation free when
// nothing else in it takes the collation.
func TestCollationFree_lengthFunctions(t *testing.T) {
	for _, stmt := range []string{
		"SELECT k % 100 AS bucket, COUNT(*), SUM(LENGTH(c)) FROM sbtest1 GROUP BY bucket ORDER BY bucket",
		"SELECT LENGTH(c), OCTET_LENGTH(c), BIT_LENGTH(c), CHAR_LENGTH(c), CHARACTER_LENGTH(c) FROM t",
		"SELECT id FROM t WHERE LENGTH(c) > 100 ORDER BY id",
		"SELECT length (c) FROM t",
	} {
		if !CollationFree(stmt) {
			t.Errorf("CollationFree(%q) = false, want true", stmt)
		}
	}
	for _, stmt := range []string{
		"SELECT LENGTH('é') FROM t",          // a literal: the connection's character set
		"SELECT LENGTH(CONCAT(c, s)) FROM t", // text a function makes
		"SELECT LENGTH(c) FROM t WHERE c = 'x'",
		"SELECT t.length(c) FROM t", // a stored function
		"SELECT xlength(c) FROM t",
		"SELECT LENGTHB(c) FROM t", // MariaDB's, not measured
	} {
		if CollationFree(stmt) {
			t.Errorf("CollationFree(%q) = true, want false", stmt)
		}
	}
}

// Under MySQLLength the copy is sent LENGTH and OCTET_LENGTH as strlen, its
// count of bytes: its own length counts characters ('é' is 1 there and 2 on
// MySQL), and its octet_length takes no text. Only the calls are rewritten.
func TestForCopyOpts_lengthAsStrlen(t *testing.T) {
	on := CopyOptions{MySQLLength: true}
	for stmt, want := range map[string]string{
		"SELECT LENGTH(c), OCTET_LENGTH(c) FROM t":                         "SELECT strlen(c), strlen(c) FROM t",
		"SELECT length (c), Octet_Length\n(c) FROM t":                      "SELECT strlen (c), strlen\n(c) FROM t",
		"SELECT LENGTH/* x */(c) FROM t":                                   "SELECT strlen/* x */(c) FROM t",
		"SELECT SUM(LENGTH(c)) FROM t WHERE LENGTH(c) > 2":                 "SELECT SUM(strlen(c)) FROM t WHERE strlen(c) > 2",
		"SELECT LENGTH(LENGTH(c)) FROM t":                                  "SELECT strlen(strlen(c)) FROM t",
		"SELECT CHAR_LENGTH(c), CHARACTER_LENGTH(c), BIT_LENGTH(c) FROM t": "SELECT CHAR_LENGTH(c), CHARACTER_LENGTH(c), BIT_LENGTH(c) FROM t",
		"SELECT length, `length` FROM t ORDER BY length":                   `SELECT length, "length" FROM t ORDER BY length`,
		"SELECT t.length FROM t":                                           "SELECT t.length FROM t",
		"SELECT t.length(c) FROM t":                                        "SELECT t.length(c) FROM t",
		"SELECT length.x FROM length":                                      "SELECT length.x FROM length",
		"SELECT 'LENGTH(c)' FROM t":                                        "SELECT 'LENGTH(c)' FROM t",
		"SELECT c FROM t -- LENGTH(c)\n":                                   "SELECT c FROM t -- LENGTH(c)\n",
		"SELECT mylength(c), length_x(c) FROM t":                           "SELECT mylength(c), length_x(c) FROM t",
	} {
		got, why := ForCopyOpts(stmt, on)
		if why != "" || got != want {
			t.Errorf("ForCopyOpts(%q) = %q, %q; want %q", stmt, got, why, want)
		}
	}
	// Off (the port without routing, ForCopy): sent as written.
	for _, opts := range []CopyOptions{{}, {LimitOffsetComma: true, MySQLDateFunctions: true}} {
		if got, _ := ForCopyOpts("SELECT LENGTH(c) FROM t", opts); got != "SELECT LENGTH(c) FROM t" {
			t.Errorf("ForCopyOpts without MySQLLength = %q", got)
		}
	}
	// With the other rewrites in the same statement.
	got, why := ForCopyOpts("SELECT LENGTH(c) FROM t WHERE d > DATE_SUB(NOW(), INTERVAL 1 DAY) AND c LIKE 'a%'",
		CopyOptions{MySQLLength: true, LikeIgnoresCase: true, MySQLDateSub: true, MySQLClockKeywords: true})
	if why != "" || !strings.HasPrefix(got, "SELECT strlen(c) FROM t WHERE") || !strings.Contains(got, "ILIKE") {
		t.Errorf("with other rewrites: %q, %q", got, why)
	}
}
