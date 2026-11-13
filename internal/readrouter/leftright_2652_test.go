package readrouter

import "testing"

// #2652: LEFT and RIGHT with a negative length are empty on MySQL and the
// text without its last (LEFT) or first (RIGHT) characters on the copy
// (measured on MySQL 8.0.46 and DuckDB 1.5.6: LEFT('abc', -1) is empty and
// 'ab', RIGHT('abc', -1) empty and 'bc', the same over a multi-byte text).
// A length of 0 or more, past the end too, and NULL agree. The copy may
// answer only when every length is a whole number written in the statement.
func TestVeto_leftRightLength(t *testing.T) {
	vetoed := []string{
		"SELECT LEFT('abc', -1)",
		"SELECT RIGHT('abc', -1)",
		"SELECT LEFT(name, -1) FROM t",
		"SELECT right(name, -3) FROM t",
		"SELECT Left(name, - 1) FROM t",
		"SELECT LEFT(name, (-1)) FROM t",
		"SELECT LEFT(name, '-1') FROM t", // read as -1 on both sides
		"SELECT LEFT(name, '2') FROM t",  // the same on both, but blanked like '-1'
		"SELECT LEFT (name, -1) FROM t",  // a space before the parenthesis
		"SELECT LEFT\v(name, -1) FROM t",
		"SELECT LEFT(\n name ,\n -1 ) FROM t",
		// Not a whole number written in the statement.
		"SELECT LEFT(name, len) FROM t",
		"SELECT RIGHT(name, t.len) FROM t",
		"SELECT LEFT(name, ?) FROM t",
		"SELECT LEFT(name, 1 + 1) FROM t",
		"SELECT LEFT(name, -0) FROM t",
		"SELECT LEFT(name, +2) FROM t",
		"SELECT LEFT(name, 1.5) FROM t",
		"SELECT LEFT(name, 1e0) FROM t",
		"SELECT LEFT(name, TRUE) FROM t",
		"SELECT LEFT(name, NULL) FROM t",
		"SELECT LEFT(name, (SELECT 1)) FROM t",
		"SELECT LEFT(name, CHAR_LENGTH(name) - 1) FROM t",
		// Nested: the outer call's length, then the inner call's.
		"SELECT LEFT(LEFT(name, 3), -1) FROM t",
		"SELECT RIGHT(LEFT(name, -1), 2) FROM t",
		// One call fine, another not.
		"SELECT LEFT(a, 2), RIGHT(b, -1) FROM t",
		"SELECT * FROM t WHERE LEFT(code, -1) = 'x'",
		"SELECT * FROM a LEFT JOIN b ON a.id = b.id WHERE RIGHT(b.code, -1) = ''",
		// A qualified name: a stored function on MySQL.
		"SELECT db.left(name, -1) FROM t",
		// Not the shape MySQL takes.
		"SELECT LEFT(name) FROM t",
		"SELECT LEFT(name, 1, 2) FROM t",
		"SELECT LEFT() FROM t",
		"SELECT LEFT(name, 1",
	}
	for _, stmt := range vetoed {
		if got := Veto(stmt); got != vetoLeftRight {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, vetoLeftRight)
		}
	}
	clean := []string{
		"SELECT LEFT(name, 2) FROM t",
		"SELECT RIGHT(name, 2) FROM t",
		"SELECT LEFT(name, 0), RIGHT(name, 0) FROM t", // '' on both
		"SELECT LEFT(name, 100) FROM t",               // past the end: the whole text on both
		"SELECT LEFT(name, 01) FROM t",
		"SELECT LEFT(name, (2)) FROM t",
		"SELECT left(name,2), right(name,3) FROM t",
		"SELECT LEFT(UPPER(name), 2) FROM t",
		"SELECT LEFT(COALESCE(a, b), 2) FROM t", // commas inside the first argument
		"SELECT LEFT(f(x, -1), 2) FROM t",       // the -1 is f's
		"SELECT LEFT(RIGHT(name, 3), 2) FROM t",
		"SELECT * FROM t WHERE LEFT(code, 2) = 'ab'",
		// LEFT and RIGHT as join keywords, before a parenthesis too.
		"SELECT * FROM a LEFT JOIN (SELECT id FROM b) d ON a.id = d.id",
		"SELECT * FROM a RIGHT JOIN (b) ON a.id = b.id",
		"SELECT * FROM a LEFT OUTER JOIN (b, c) ON a.id = b.id",
		"SELECT * FROM a NATURAL LEFT JOIN b",
		"SELECT * FROM a NATURAL RIGHT OUTER JOIN b",
		// Columns of those names, no call.
		"SELECT `left`, `right` FROM t",
		"SELECT t.left, t.right FROM t",
		// Other functions.
		"SELECT xleft(name, -1), my_right(name, -1), año_left(name, -1), left2(name, -1) FROM t",
		"SELECT LTRIM(name), RTRIM(name) FROM t",
		// Inside a string literal or a comment.
		"SELECT * FROM t WHERE note = 'left(a, -1)'",
		"SELECT name FROM t /* RIGHT(name, -1) */",
		"SELECT name FROM t -- LEFT(name, -1)\n",
	}
	for _, stmt := range clean {
		if got := Veto(stmt); got != "" {
			t.Errorf("Veto(%q) = %q, want none", stmt, got)
		}
	}
	// A quoted name is a stored function on MySQL and the built-in on the
	// copy: kept on MySQL by the veto on a backtick name before a
	// parenthesis.
	if got := Veto("SELECT `left`(name, 2) FROM t"); got != vetoNameCall {
		t.Errorf("`left`(name, 2): %q, want %q", got, vetoNameCall)
	}
	// A statement another veto keeps on MySQL is still named by that one.
	if got := Veto("SELECT CONCAT(a, b), LEFT(name, -1) FROM t"); got != vetoNullArg {
		t.Errorf("CONCAT and LEFT: %q, want the CONCAT veto first", got)
	}
	if got := Veto("SELECT SUBSTRING(name, 0, 2), LEFT(name, -1) FROM t"); got != vetoSubstring {
		t.Errorf("SUBSTRING and LEFT: %q, want the SUBSTRING veto first", got)
	}
}

// #2652: a space character outside ASCII (a no-break space, an ideographic
// space, a zero-width space and the others below) outside string literals,
// quoted names and comments is part of a name on MySQL and white space on
// the copy. Over a table with columns a, b and a<U+00A0>b, SELECT a<U+00A0>b
// FROM t is 30 (that column) on MySQL 8.0.46 and 10 (column a under the
// alias b) on DuckDB 1.5.6, with no error on either side. A statement that
// holds one stays on MySQL.
func TestVeto_spaceOutsideASCII(t *testing.T) {
	// Every character the copy took for white space in SELECT 1<c>+ 2,
	// abs<c>(-1), 'a'<c>AS x and SELECT<c>1, each an error on MySQL.
	for _, r := range []rune{0x00A0, 0x2000, 0x2001, 0x2002, 0x2003, 0x2004, 0x2005, 0x2006, 0x2007, 0x2008, 0x2009, 0x200A, 0x200B, 0x202F, 0x205F, 0x2060, 0x3000, 0xFEFF} {
		c := string(r)
		for _, stmt := range []string{
			"SELECT 1" + c + "+ 2",
			"SELECT abs" + c + "(-1)",
			"SELECT 'a'" + c + "AS x",
			"SELECT" + c + "1",
			"SELECT name FROM t" + c,
		} {
			if got := Veto(stmt); got != vetoSpaceOutsideASCII {
				t.Errorf("Veto(%q) = %q, want %q", stmt, got, vetoSpaceOutsideASCII)
			}
		}
	}
	vetoed := []string{
		// One column on MySQL, a column and its alias on the copy.
		"SELECT a\u00a0b FROM t",
		"SELECT a\u3000, b FROM t",
		// The issue's cases: an error on MySQL, and every name-based veto
		// reads the character as a space.
		"SELECT ROUND\u00a0(2.5e0)",
		"SELECT LEFT\u00a0('abc', -1)",
		"SELECT SUBSTRING\u00a0('abc', 0, 2)",
		"SELECT CONCAT\u3000('a', NULL)",
		"SELECT name FROM t WHERE a\u00a0=\u00a01",
		"SELECT `a`\u00a0FROM `t`",
		"SELECT name FROM t WHERE note = 'x'\u00a0AND id = 1",
		// Inside a string literal, where the copy finds the quotes another
		// way: a quote inside a /* */ comment (the copy does not skip those
		// before it turns the character into a space), or a $. SELECT /* '
		// */ 'a<U+00A0>b' is 'a b' on the copy.
		"SELECT /* ' */ 'a\u00a0b'",
		"SELECT id FROM t /* it's */ WHERE name = 'x\u00a0y' /* ' */",
		"SELECT /* \" */ 'a\u00a0b'",
		"SELECT c$ FROM t WHERE note = 'a\u3000b'",
		"SELECT `c$` FROM t WHERE note = 'a\u00a0b'",
	}
	for _, stmt := range vetoed {
		if got := Veto(stmt); got != vetoSpaceOutsideASCII {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, vetoSpaceOutsideASCII)
		}
	}
	clean := []string{
		// Inside a string literal, a quoted name or a comment the
		// character is the same text on both sides.
		"SELECT * FROM t WHERE note = 'a\u00a0b'",
		"SELECT `a\u00a0b` FROM t",
		"SELECT `a\u3000b` FROM `t`",
		"SELECT name FROM t /* \u00a0 */",
		"SELECT name FROM t -- \u00a0\n",
		// Other characters outside ASCII are part of a name on both.
		"SELECT año FROM t",
		"SELECT می‌خواهم FROM t", // a zero-width non-joiner inside a Persian name
		"SELECT name FROM t WHERE note = '\u200b'",
		// A comment without a quote, or a -- comment with one: the copy
		// finds the same strings as MySQL.
		"SELECT /* note */ 'a\u00a0b'",
		"SELECT -- '\n 'a\u00a0b'",
	}
	for _, stmt := range clean {
		if got := Veto(stmt); got != "" {
			t.Errorf("Veto(%q) = %q, want none", stmt, got)
		}
	}
	// Characters both sides refuse are left to the copy's refusal, which
	// sends the statement to MySQL anyway: they are not white space there.
	for _, r := range []rune{0x0085, 0x1680, 0x180E, 0x2028, 0x2029, 0x200C, 0x200D} {
		if got := Veto("SELECT 1" + string(r) + "+ 2"); got == vetoSpaceOutsideASCII {
			t.Errorf("U+%04X: %q, a character the copy does not take for a space", r, got)
		}
	}
	// A refusal of the rewrite is named first: it is the more precise reason.
	if got := Veto("SELECT `text`\u00a0'Label' FROM `t`"); got != vetoNameString {
		t.Errorf("name and string: %q, want %q", got, vetoNameString)
	}
	// The plain port without routing still gets the rewrite: the copy's
	// meaning is what that port answers with, and the character changes
	// nothing in how the names are quoted.
	text, why := ForCopy("SELECT `a`\u00a0FROM `t`")
	if why != "" || text != "SELECT \"a\"\u00a0FROM \"t\"" {
		t.Errorf("ForCopy = %q, %q; want the names rewritten and no refusal", text, why)
	}
}
