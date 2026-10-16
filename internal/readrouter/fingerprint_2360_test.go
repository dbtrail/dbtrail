package readrouter

import (
	"strings"
	"testing"
)

// TestFingerprint pins what one row of the router's per-statement table
// groups (#2360): statements that differ only in their values share a
// fingerprint, statements that read different things do not.
func TestFingerprint(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"", ""},
		{"   \n\t ", ""},
		// Numbers of every spelling are values.
		{"SELECT * FROM t WHERE id = 12345", "select * from t where id = ?"},
		{"SELECT * FROM t WHERE id = -5", "select * from t where id = ?"},
		{"SELECT * FROM t WHERE x > 1.5e3 AND y < .25 AND z = 0x1F AND w = 0b101", "select * from t where x > ? and y < ? and z = ? and w = ?"},
		{"SELECT * FROM t WHERE x = 1E-3", "select * from t where x = ?"},
		// A minus between two values is arithmetic, not a sign.
		{"SELECT a - 1 FROM t", "select a - ? from t"},
		{"SELECT a-1 FROM t", "select a - ? from t"},
		// Strings, with or without a character set or hex prefix, are values.
		{"SELECT * FROM t WHERE s = 'abc' OR s = \"d\" OR s = x'1F' OR s = _utf8mb4'e' OR s = N'f'", "select * from t where s = ? or s = ? or s = ? or s = ? or s = ?"},
		// Digits inside a name are the name.
		{"SELECT col2 FROM t1 JOIN db3.t4 ON t1.id = t4.id", "select col2 from t1 join db3.t4 on t1.id = t4.id"},
		{"SELECT * FROM 1t", "select * from 1t"},
		{"SELECT `Col 2` FROM `T1`", "select `Col 2` from `T1`"},
		{"SELECT @v1, @@session.sql_mode", "select @v1, @@session.sql_mode"},
		// A placeholder and a literal are the same value.
		{"SELECT * FROM t WHERE id = ?", "select * from t where id = ?"},
		// Lists of values of any length are one row.
		{"SELECT * FROM t WHERE id IN (1, 2, 3)", "select * from t where id in(?)"},
		{"SELECT * FROM t WHERE id in(?,?)", "select * from t where id in(?)"},
		{"SELECT * FROM t WHERE id NOT IN ('a')", "select * from t where id not in(?)"},
		{"INSERT INTO t VALUES (1, 'a'), (2, 'b')", "insert into t values(?, ?)"},
		{"INSERT INTO t (a, b) VALUES (?, ?)", "insert into t(a, b) values(?, ?)"},
		// A word that ends in "in" or "values" is not a list.
		{"SELECT min(1, 2), coin(3, 4) FROM t", "select min(?, ?), coin(?, ?) from t"},
		{"INSERT INTO t VALUES (1) ON DUPLICATE KEY UPDATE a = VALUES(a)", "insert into t values(?) on duplicate key update a = values(a)"},
		// A list with something other than values in it is kept as written.
		{"SELECT * FROM t WHERE id IN (SELECT id FROM u)", "select * from t where id in(select id from u)"},
		{"SELECT * FROM t WHERE id IN (a, 1)", "select * from t where id in(a, ?)"},
		// LIMIT and OFFSET are values.
		{"SELECT * FROM t LIMIT 10", "select * from t limit ?"},
		{"SELECT * FROM t LIMIT 20 OFFSET 40", "select * from t limit ? offset ?"},
		// Case, white space, comments, line breaks and a closing ; do not count.
		{"select *\n  from   t\twhere ID=1;", "select * from t where id = ?"},
		{"SELECT /* hint */ * FROM t -- tail\nWHERE id = 1", "select * from t where id = ?"},
		// Operators of more than one character stay whole.
		{"SELECT * FROM t WHERE a>=1 AND b<>2 AND c!=3 AND d<=>NULL AND j->>'$.k' = 4", "select * from t where a >= ? and b <> ? and c != ? and d <=> null and j ->> ? = ?"},
		{"SELECT COUNT(*), SUM(x) FROM t GROUP BY s", "select count(*), sum(x) from t group by s"},
	} {
		if got := Fingerprint(tc.in); got != tc.want {
			t.Errorf("Fingerprint(%q)\n got  %q\n want %q", tc.in, got, tc.want)
		}
	}
}

// TestFingerprint_sameRow pins pairs that must land on one row.
func TestFingerprint_sameRow(t *testing.T) {
	for _, p := range [][2]string{
		{"SELECT * FROM t WHERE id = 1", "SELECT * FROM t WHERE id = 99999"},
		{"SELECT * FROM t WHERE id = 1", "select * from t where id = ?"},
		{"SELECT * FROM t WHERE id IN (1,2)", "SELECT * FROM t WHERE id IN (1,2,3,4,5)"},
		{"SELECT * FROM t LIMIT 10", "SELECT * FROM t LIMIT 20"},
		{"SELECT * FROM t WHERE s = 'a'", "SELECT * FROM t WHERE s = 'it''s'"},
	} {
		if a, b := Fingerprint(p[0]), Fingerprint(p[1]); a != b {
			t.Errorf("%q and %q: %q != %q", p[0], p[1], a, b)
		}
	}
	for _, p := range [][2]string{
		{"SELECT * FROM t1", "SELECT * FROM t2"},
		{"SELECT a FROM t", "SELECT b FROM t"},
		{"SELECT `A` FROM t", "SELECT `a` FROM t"},
	} {
		if a, b := Fingerprint(p[0]), Fingerprint(p[1]); a == b {
			t.Errorf("%q and %q share %q", p[0], p[1], a)
		}
	}
}

// TestFingerprint_leavesShapeAlone: routing reads Shape (ColumnVeto), so the
// fingerprint must not change it.
func TestFingerprint_leavesShapeAlone(t *testing.T) {
	stmt := "SELECT a FROM t WHERE id = 12345 AND s = 'x'"
	before := Shape(stmt)
	_ = Fingerprint(stmt)
	if Shape(stmt) != before || !strings.Contains(before, "12345") {
		t.Fatalf("Shape(%q) = %q, want the number kept", stmt, Shape(stmt))
	}
}
