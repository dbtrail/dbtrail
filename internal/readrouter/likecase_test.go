package readrouter

import "testing"

// A server's switch "LIKE ignores case on the copy" (CopyOptions.
// LikeIgnoresCase): MySQL compares LIKE without case under its default
// collations, the copy with it. Off, LIKE keeps the statement on MySQL, as it
// always has. On, the statement may go to the copy and LIKE is written ILIKE
// there, the copy's LIKE without case; REGEXP and RLIKE stay on MySQL either
// way.
func TestVetoOpts_like(t *testing.T) {
	on := CopyOptions{LikeIgnoresCase: true}
	cases := []struct {
		stmt     string
		off, onV bool // vetoed with the switch off / on
	}{
		{"SELECT count(*) FROM t WHERE name LIKE 'PR%'", true, false},
		{"SELECT count(*) FROM t WHERE name NOT LIKE '%b'", true, false},
		{"SELECT count(*) FROM t WHERE name like 'a!_%' ESCAPE '!'", true, false},
		{"SELECT count(*) FROM t WHERE name REGEXP '^a'", true, true},
		{"SELECT count(*) FROM t WHERE name RLIKE '^a'", true, true},
		{"SELECT count(*) FROM t WHERE name LIKE 'a' AND b REGEXP 'x'", true, true},
		// Another veto still applies with the switch on.
		{"SELECT GROUP_CONCAT(name) FROM t WHERE name LIKE 'a%'", true, true},
		// The pattern must be written in the statement or be a placeholder:
		// one that comes from a column or an expression can hold a
		// backslash, an escape on MySQL and a character on the copy.
		{"SELECT count(*) FROM t WHERE name LIKE other_col", true, true},
		{"SELECT count(*) FROM t WHERE name LIKE CONCAT('%', c, '%')", true, true},
		{"SELECT count(*) FROM t WHERE name LIKE (SELECT p FROM q)", true, true},
		{"SELECT count(*) FROM t WHERE name LIKE ?", true, false},
		{"SELECT count(*) FROM t WHERE name LIKE 'a%' AND b LIKE c", true, true},
		{"SELECT count(*) FROM t WHERE name LIKE _utf8mb4'a%'", true, true},
		// SOUNDS LIKE is not LIKE, and the copy has no SOUNDS.
		{"SELECT count(*) FROM t WHERE name SOUNDS LIKE 'a'", true, true},
		// Not the keyword: nothing to veto either way.
		{"SELECT count(*) FROM t WHERE note = 'LIKE'", false, false},
		{"SELECT likes FROM t", false, false},
	}
	for _, c := range cases {
		if got := Veto(c.stmt) != ""; got != c.off {
			t.Errorf("Veto(%q) vetoed=%v, want %v", c.stmt, got, c.off)
		}
		if got := VetoOpts(c.stmt, on) != ""; got != c.onV {
			t.Errorf("VetoOpts(%q, on) vetoed=%v (%q), want %v", c.stmt, got, VetoOpts(c.stmt, on), c.onV)
		}
	}
}

func TestForCopyOpts_likeIgnoresCase(t *testing.T) {
	on := CopyOptions{LikeIgnoresCase: true}
	cases := map[string]string{
		"SELECT count(*) FROM t WHERE name LIKE 'PR%'":               "SELECT count(*) FROM t WHERE name ILIKE 'PR%'",
		"SELECT count(*) FROM t WHERE name like 'PR%'":               "SELECT count(*) FROM t WHERE name ILIKE 'PR%'",
		"SELECT count(*) FROM t WHERE name NOT LIKE '%b'":            "SELECT count(*) FROM t WHERE name NOT ILIKE '%b'",
		"SELECT count(*) FROM t WHERE name LIKE 'a!_%' ESCAPE '!'":   "SELECT count(*) FROM t WHERE name ILIKE 'a!_%' ESCAPE '!'",
		"SELECT count(*) FROM t WHERE `name` LIKE 'a%'":              `SELECT count(*) FROM t WHERE "name" ILIKE 'a%'`,
		"SELECT count(*) FROM t WHERE name LIKE ?":                   "SELECT count(*) FROM t WHERE name ILIKE ?",
		"SELECT count(*) FROM a STRAIGHT_JOIN b WHERE a.n LIKE 'x%'": "SELECT count(*) FROM a CROSS JOIN b WHERE a.n ILIKE 'x%'",
		// Not the keyword: a string, a comment, a quoted name, a longer
		// word, a name next to a dot.
		"SELECT count(*) FROM t WHERE note = 'x LIKE y'": "SELECT count(*) FROM t WHERE note = 'x LIKE y'",
		"SELECT count(*) FROM t /* LIKE */ WHERE 1 = 1":  "SELECT count(*) FROM t /* LIKE */ WHERE 1 = 1",
		"SELECT `like`, likes, unlike FROM t":            `SELECT "like", likes, unlike FROM t`,
		"SELECT t.like FROM t":                           "SELECT t.like FROM t",
	}
	for stmt, want := range cases {
		got, why := ForCopyOpts(stmt, on)
		if why != "" {
			t.Errorf("ForCopyOpts(%q) refused: %s", stmt, why)
			continue
		}
		if got != want {
			t.Errorf("ForCopyOpts(%q)\n got %q\nwant %q", stmt, got, want)
		}
	}
	// Off, LIKE is written as the client wrote it (the veto keeps such a
	// statement off the copy; a copy-only port runs the client's text).
	if got, _ := ForCopy("SELECT 1 FROM t WHERE n LIKE 'a%'"); got != "SELECT 1 FROM t WHERE n LIKE 'a%'" {
		t.Errorf("ForCopy rewrote LIKE with the switch off: %q", got)
	}
}
