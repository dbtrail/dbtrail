package readrouter

import "testing"

// #2836: a statement whose answer the connection's collation cannot change
// (no string literal, no text a function makes) against one where it can.
func TestCollationFree_2836(t *testing.T) {
	free := []string{
		"SELECT side, dt FROM ev ORDER BY k",
		"SELECT MIN(side), COUNT(*) FROM ev WHERE k > 0",
		"SELECT MIN(side), k, COUNT(*) FROM ev GROUP BY k ORDER BY k",
		"SELECT side, s FROM ev ORDER BY k",
		"SELECT s, COUNT(*) FROM t GROUP BY s ORDER BY MIN(id)",
		"SELECT DISTINCT s FROM t ORDER BY s",
		"SELECT MIN(s), MAX(s), MIN(g), MAX(g) FROM t",
		"SELECT id FROM t WHERE s = g ORDER BY id",
		"SELECT id FROM t WHERE g IN (b, s) ORDER BY id",
		"SELECT id, CASE WHEN n > 5 THEN s ELSE g END AS x FROM t ORDER BY x, id",
		"SELECT id FROM t WHERE s LIKE g ORDER BY id",
		"SELECT id FROM t WHERE n IN (1, 2) AND (k > 3 OR k < 1)",
		"SELECT YEAR(d), MONTH(d), COUNT(*) FROM t GROUP BY YEAR(d), MONTH(d)",
		"SELECT id FROM t WHERE d > CURDATE() - INTERVAL 30 DAY",
		"SELECT id FROM t WHERE dt >= NOW() - INTERVAL 1 HOUR",
		"SELECT DATEDIFF(d, d2), TIMESTAMPDIFF(DAY, d, d2) FROM t",
		"SELECT ROUND(AVG(n), 2), SUM(n) FROM t",
		"SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id)",
		"SELECT x FROM (SELECT id AS x FROM t) q ORDER BY x",
		"SELECT `count(x)` FROM t",             // a name in backticks
		"SELECT COUNT (*) FROM t",              // a space before the parenthesis
		"SELECT COUNT\v(*) FROM t",             // a vertical tab
		"SELECT id FROM t /* 'a' CONCAT(x) */", // inside a comment
		"SELECT id, ROW_NUMBER FROM t",         // a column named like a function
		"SELECT SUM(n) OVER (PARTITION BY k) FROM t",
	}
	for _, stmt := range free {
		if !CollationFree(stmt) {
			t.Errorf("CollationFree(%q) = false, want true", stmt)
		}
	}
	notFree := []string{
		"SELECT '10:47' < '1045'",
		"SELECT id FROM t WHERE s = 'SS'",
		"SELECT id FROM t WHERE d > '2026-01-05'", // a literal, even beside a date
		"SELECT id FROM t WHERE s LIKE 'a%'",
		"SELECT id FROM t WHERE s IN ('a', 'b')",
		"SELECT CASE WHEN n > 1 THEN 'x' ELSE s END FROM t",
		"SELECT \"x\" FROM t",             // a double-quoted string
		"SELECT id FROM t WHERE s = ?",    // a placeholder, perhaps text
		"SELECT id FROM t WHERE b = 0x41", // a hex number: a binary string
		"SELECT id FROM t WHERE b = 0b1",  // a bit number
		"SELECT CONCAT(s, g) AS x FROM t ORDER BY x",
		"SELECT CONCAT(n) < CONCAT(id) FROM t",
		"SELECT LOWER(s) FROM t ORDER BY 1",
		"SELECT DATE_FORMAT(d, '%b') FROM t",
		"SELECT MONTHNAME(d) AS m FROM t ORDER BY m",
		"SELECT DISTINCT MONTHNAME(d) FROM t",
		"SELECT CAST(n AS CHAR) AS x FROM t ORDER BY x",
		"SELECT IFNULL(s, g) FROM t",
		"SELECT COALESCE(s, b) FROM t",
		"SELECT GREATEST(s, g) FROM t",
		"SELECT db.min(s) FROM t", // a stored function
		"SELECT xmin(s) FROM t",   // another function
		"SELECT N'x' FROM t",      // a national string
		"SELECT _utf8mb4'x' FROM t",
		"SELECT x'41' FROM t",
		"SELECT n FROM a UNION ALL SELECT tm FROM b ORDER BY 1", // a time beside a number, as text
		"(SELECT s FROM a) UNION (SELECT n FROM b)",
	}
	for _, stmt := range notFree {
		if CollationFree(stmt) {
			t.Errorf("CollationFree(%q) = true, want false", stmt)
		}
	}
}
