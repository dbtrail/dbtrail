package readrouter

import "testing"

// A `#` starts a comment on MySQL and a column-position reference on the
// copy: `SELECT #2<newline> alpha FROM ln` is column alpha on MySQL (1.50,
// measured on MySQL 8.4 and MariaDB 11.4) and, on DuckDB, the SECOND column
// under the name alpha (26). The statement passes EXPLAIN on the source, so
// only a veto keeps it there (#2111).
func TestVeto_hashComment(t *testing.T) {
	const reason = "# starts a comment on MySQL and a column-position reference on the copy"
	for stmt, want := range map[string]string{
		"SELECT #2\n alpha FROM ln":                 reason,
		"SELECT id, #1\n zeta FROM ln WHERE id > 0": reason,
		"SELECT id FROM ln # the rest is a comment": reason,
		"SELECT id FROM ln #c\n WHERE id = 1":       reason,
		"#1\nSELECT id FROM ln":                     reason,
		"SELECT id FROM ln WHERE note = '#2'":       "",
		"SELECT id FROM ln WHERE note = 'a''#b'":    "",
		"SELECT id FROM ln /* #2 */ WHERE id = 1":   "",
		"SELECT id FROM ln -- #2\n WHERE id = 1":    "",
		"SELECT id FROM ln WHERE id = 1":            "",
		"SELECT `a#b` FROM ln":                      "", // inside a quoted name it is part of the name
		"SELECT id FROM ln WHERE note = \"#\"":      "double-quoted string literal",
		"SELECT GROUP_CONCAT(id) FROM ln # c":       "GROUP_CONCAT", // the table's vetoes are named first
	} {
		if got := Veto(stmt); got != want {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, want)
		}
	}
}

// What the rest of the package reads from a scrubbed statement is as it was:
// the comment is gone, as MySQL reads the statement.
func TestScrub_hashCommentIsRemoved(t *testing.T) {
	if got := Scrub("SELECT id #, count(*) FOR UPDATE\n FROM ln"); got != "SELECT id \n FROM ln" {
		t.Errorf("Scrub = %q", got)
	}
}
