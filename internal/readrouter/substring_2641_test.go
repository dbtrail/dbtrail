package readrouter

import "testing"

// #2641: SUBSTRING, SUBSTR and MID answer differently on the copy when the
// position is 0, when a negative position reaches before the start of the
// text, and when the length is negative (measured on MySQL 8.0.46 and
// DuckDB 1.5.6: SUBSTRING('abc', 0, 2) is empty on MySQL and 'a' on the
// copy, and so are SUBSTRING('abc', -4, 2) and SUBSTRING('abc', 2, -1)).
// The copy may answer only when every position is a positive whole number
// or -1 written in the statement and every length a whole number written
// there.
func TestVeto_substringPosition(t *testing.T) {
	vetoed := []string{
		"SELECT SUBSTRING(name, 0, 2) FROM t",
		"SELECT SUBSTRING(name, 0) FROM t",
		"SELECT substr(name, 0, 2) FROM t",
		"SELECT Mid(name, 0, 2) FROM t",
		"SELECT SUBSTRING (name, 0, 2) FROM t", // a space before the parenthesis
		"SELECT SUBSTRING\v(name, 0, 2) FROM t",
		"SELECT SUBSTRING(\n name ,\n 0 ,\n 2 ) FROM t",
		// Negative: the same as MySQL only while the text is long enough,
		// which the statement does not say. -1 is the exception (below).
		"SELECT SUBSTRING(name, -2) FROM t", // over 'a': '' on MySQL, 'a' on the copy
		"SELECT SUBSTRING(name, -3, 2) FROM t",
		"SELECT SUBSTRING(name, -10) FROM t",
		"SELECT SUBSTRING(name, -01) FROM t",
		// A negative length.
		"SELECT SUBSTRING(name, 2, -1) FROM t",
		"SELECT SUBSTRING(name, 1, -1) FROM t",
		// Not a positive whole number written in the statement.
		"SELECT SUBSTRING(name, pos, 2) FROM t",
		"SELECT SUBSTRING(name, ?, 2) FROM t",
		"SELECT SUBSTRING(name, 2, ?) FROM t",
		"SELECT SUBSTRING(name, 1 + 1, 2) FROM t",
		"SELECT SUBSTRING(name, 1.5, 1) FROM t", // MySQL rounds it to 2
		"SELECT SUBSTRING(name, 1e0, 1) FROM t",
		"SELECT SUBSTRING(name, +1, 1) FROM t",
		"SELECT SUBSTRING(name, 2, len) FROM t",
		"SELECT SUBSTRING(name, (SELECT 0), 2) FROM t",
		// The FROM ... FOR form reads the same way.
		"SELECT SUBSTRING(name FROM 0 FOR 2) FROM t",
		"SELECT SUBSTRING(name FROM 0) FROM t",
		"SELECT SUBSTRING(name FROM -2) FROM t",
		"SELECT SUBSTRING(name FROM 2 FOR -1) FROM t",
		"SELECT SUBSTRING(name from pos for 2) FROM t",
		// Nested: the outer call's position is 0.
		"SELECT SUBSTRING(SUBSTRING(name, 1, 3), 0, 2) FROM t",
		// The inner call's position is 0.
		"SELECT SUBSTRING(SUBSTRING(name, 0, 3), 1, 2) FROM t",
		// One call fine, another not.
		"SELECT SUBSTRING(a, 1, 2), SUBSTR(b, 0) FROM t",
		"SELECT * FROM t WHERE MID(code, 0, 1) = 'x'",
		// Not one of the two shapes MySQL takes.
		"SELECT SUBSTRING(name) FROM t",
		"SELECT SUBSTRING(name, 1, 2, 3) FROM t",
		"SELECT SUBSTRING(name, 1",
	}
	for _, stmt := range vetoed {
		if got := Veto(stmt); got != vetoSubstring {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, vetoSubstring)
		}
	}
	clean := []string{
		"SELECT SUBSTRING(name, 1, 2) FROM t",
		"SELECT SUBSTRING(name, 2) FROM t",
		"SELECT SUBSTRING(name, 4) FROM t", // past the end: '' on both
		"SELECT SUBSTRING(name, 5, 1) FROM t",
		"SELECT SUBSTRING(name, 2, 0) FROM t", // length 0: '' on both
		"SELECT substr(name, 3, 5), mid(name, 1, 1) FROM t",
		"SELECT SUBSTRING(name FROM 2 FOR 1) FROM t",
		"SELECT SUBSTRING(name FROM 2) FROM t",
		// -1 is the last character on both, and '' over '' on both.
		"SELECT SUBSTRING(name, -1) FROM t",
		"SELECT SUBSTRING(name, - 1, 1) FROM t",
		"SELECT SUBSTRING(name FROM -1) FROM t",
		"SELECT ascii(substring(c_state from  1  for  1)) FROM t",
		"SELECT SUBSTRING(UPPER(name), 1, 2) FROM t",
		"SELECT SUBSTRING(COALESCE(a, b), 1, 2) FROM t", // commas inside the first argument
		"SELECT SUBSTRING(f(x, 0), 1) FROM t",           // the 0 is f's
		"SELECT SUBSTRING(SUBSTRING(name, 2), 1, 2) FROM t",
		"SELECT SUBSTRING((name), (1), (2)) FROM t",
		"SELECT SUBSTRING_INDEX(name, ',', 0) FROM t",       // another function
		"SELECT * FROM t WHERE note = 'substring(a, 0, 2)'", // inside a string literal
		"SELECT `substring`, mid, substr FROM t",            // columns of those names, no call
		"SELECT t.mid FROM t",
		"SELECT xsubstr(name, 0, 2), my_mid(name, 0), año_mid(name, 0) FROM t", // other functions
		"SELECT name FROM t /* SUBSTRING(name, 0, 2) */",
	}
	for _, stmt := range clean {
		if got := Veto(stmt); got != "" {
			t.Errorf("Veto(%q) = %q, want none", stmt, got)
		}
	}
	// A quoted name is an error on MySQL and an answer on the copy: kept on
	// MySQL by the veto on a backtick name before a parenthesis.
	if got := Veto("SELECT `substring`(name, 0, 2) FROM t"); got == "" {
		t.Error("`substring`(name, 0, 2) goes to the copy")
	}
	// A statement another veto keeps on MySQL is still named by that one.
	if got := Veto("SELECT CONCAT(a, b), SUBSTRING(name, 0, 2) FROM t"); got != vetoNullArg {
		t.Errorf("CONCAT and SUBSTRING: %q, want the CONCAT veto first", got)
	}
}
