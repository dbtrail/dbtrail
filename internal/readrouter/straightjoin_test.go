package readrouter

import "testing"

// STRAIGHT_JOIN is MySQL's join that reads the left table first. The copy
// does not have the keyword, so the router writes it as the join it means:
// with ON or USING an inner JOIN, without them a CROSS JOIN (MySQL then
// filters in WHERE), and as a SELECT modifier it is dropped (it only orders
// MySQL's plan).
func TestForCopy_straightJoin(t *testing.T) {
	cases := map[string]string{
		// The join operator, with and without a condition.
		"SELECT * FROM a STRAIGHT_JOIN b ON a.x = b.y":              "SELECT * FROM a JOIN b ON a.x = b.y",
		"SELECT * FROM a STRAIGHT_JOIN b ON (a.x = b.y)":            "SELECT * FROM a JOIN b ON (a.x = b.y)",
		"SELECT * FROM a STRAIGHT_JOIN b USING (id)":                "SELECT * FROM a JOIN b USING (id)",
		"SELECT * FROM a STRAIGHT_JOIN b WHERE a.x = b.y":           "SELECT * FROM a CROSS JOIN b WHERE a.x = b.y",
		"SELECT * FROM a STRAIGHT_JOIN b":                           "SELECT * FROM a CROSS JOIN b",
		"select * from a straight_join b where a.x = b.y":           "select * from a CROSS JOIN b where a.x = b.y",
		"SELECT * FROM a Straight_Join b ON a.x = b.y":              "SELECT * FROM a JOIN b ON a.x = b.y",
		"SELECT * FROM a\n\tSTRAIGHT_JOIN\n\tb\nON a.x = b.y":       "SELECT * FROM a\n\tJOIN\n\tb\nON a.x = b.y",
		"SELECT * FROM a t1 STRAIGHT_JOIN b AS on_time WHERE 1 = 1": "SELECT * FROM a t1 CROSS JOIN b AS on_time WHERE 1 = 1",
		// A chain, the way TPC-style queries pin a join order.
		"SELECT 1 FROM a STRAIGHT_JOIN b STRAIGHT_JOIN c WHERE a.x = b.x AND b.y = c.y": "SELECT 1 FROM a CROSS JOIN b CROSS JOIN c WHERE a.x = b.x AND b.y = c.y",
		"SELECT 1 FROM a STRAIGHT_JOIN b ON a.x = b.x STRAIGHT_JOIN c WHERE b.y = c.y":  "SELECT 1 FROM a JOIN b ON a.x = b.x CROSS JOIN c WHERE b.y = c.y",
		// What ends the right side before any ON: another join, a comma, a
		// clause, the group it sits in.
		"SELECT 1 FROM a STRAIGHT_JOIN b LEFT JOIN c ON c.k = b.k":         "SELECT 1 FROM a CROSS JOIN b LEFT JOIN c ON c.k = b.k",
		"SELECT 1 FROM a STRAIGHT_JOIN b JOIN c ON c.k = b.k":              "SELECT 1 FROM a CROSS JOIN b JOIN c ON c.k = b.k",
		"SELECT 1 FROM a STRAIGHT_JOIN b, c WHERE a.x = c.x":               "SELECT 1 FROM a CROSS JOIN b, c WHERE a.x = c.x",
		"SELECT 1 FROM (a STRAIGHT_JOIN b) JOIN c ON c.k = b.k":            "SELECT 1 FROM (a CROSS JOIN b) JOIN c ON c.k = b.k",
		"SELECT 1 FROM a STRAIGHT_JOIN b GROUP BY a.x":                     "SELECT 1 FROM a CROSS JOIN b GROUP BY a.x",
		"SELECT 1 FROM a STRAIGHT_JOIN b ORDER BY 1 LIMIT 5":               "SELECT 1 FROM a CROSS JOIN b ORDER BY 1 LIMIT 5",
		"SELECT 1 FROM a STRAIGHT_JOIN b UNION SELECT 2":                   "SELECT 1 FROM a CROSS JOIN b UNION SELECT 2",
		"SELECT 1 FROM a STRAIGHT_JOIN b;":                                 "SELECT 1 FROM a CROSS JOIN b;",
		"SELECT (SELECT 1 FROM a STRAIGHT_JOIN b) FROM c":                  "SELECT (SELECT 1 FROM a CROSS JOIN b) FROM c",
		"SELECT 1 FROM c WHERE c.x IN (SELECT a.x FROM a STRAIGHT_JOIN b)": "SELECT 1 FROM c WHERE c.x IN (SELECT a.x FROM a CROSS JOIN b)",
		// A derived table on the right: its own WHERE is not the outer one.
		"SELECT 1 FROM a STRAIGHT_JOIN (SELECT x FROM c WHERE z = 1) d ON d.x = a.x": "SELECT 1 FROM a JOIN (SELECT x FROM c WHERE z = 1) d ON d.x = a.x",
		// The SELECT modifier.
		"SELECT STRAIGHT_JOIN a.x FROM a JOIN b ON a.x = b.x":        "SELECT  a.x FROM a JOIN b ON a.x = b.x",
		"select straight_join a.x from a, b where a.x = b.x":         "select  a.x from a, b where a.x = b.x",
		"SELECT DISTINCT STRAIGHT_JOIN x FROM a":                     "SELECT DISTINCT  x FROM a",
		"SELECT ALL STRAIGHT_JOIN x FROM a":                          "SELECT ALL  x FROM a",
		"SELECT x FROM a WHERE x IN (SELECT STRAIGHT_JOIN y FROM b)": "SELECT x FROM a WHERE x IN (SELECT  y FROM b)",
		// Backtick-quoted names around it are rewritten as before.
		"SELECT `a`.`x` FROM `a` STRAIGHT_JOIN `b` ON `a`.`x` = `b`.`y`": `SELECT "a"."x" FROM "a" JOIN "b" ON "a"."x" = "b"."y"`,
		"SELECT 1 FROM `a`STRAIGHT_JOIN`b` WHERE 1 = 1":                  `SELECT 1 FROM "a"CROSS JOIN"b" WHERE 1 = 1`,
		// Not the keyword: inside a string, a comment, a quoted name, or a
		// longer word.
		"SELECT 1 FROM a WHERE note = 'STRAIGHT_JOIN'":      "SELECT 1 FROM a WHERE note = 'STRAIGHT_JOIN'",
		"SELECT 1 FROM a /* STRAIGHT_JOIN b */ WHERE 1 = 1": "SELECT 1 FROM a /* STRAIGHT_JOIN b */ WHERE 1 = 1",
		"SELECT 1 FROM a -- straight_join b\nWHERE 1 = 1":   "SELECT 1 FROM a -- straight_join b\nWHERE 1 = 1",
		"SELECT `straight_join` FROM a":                     `SELECT "straight_join" FROM a`,
		"SELECT my_straight_join, straight_joins FROM a":    "SELECT my_straight_join, straight_joins FROM a",
		// A comment between the right side and its ON does not hide the ON.
		"SELECT 1 FROM a STRAIGHT_JOIN b /* why */ ON a.x = b.x": "SELECT 1 FROM a JOIN b /* why */ ON a.x = b.x",
		"SELECT 1 FROM a STRAIGHT_JOIN b /* ON */ WHERE 1 = 1":   "SELECT 1 FROM a CROSS JOIN b /* ON */ WHERE 1 = 1",
		"SELECT 1 FROM a STRAIGHT_JOIN b WHERE note = ' ON '":    "SELECT 1 FROM a CROSS JOIN b WHERE note = ' ON '",
	}
	for stmt, want := range cases {
		got, why := ForCopy(stmt)
		if why != "" {
			t.Errorf("ForCopy(%q) refused: %s", stmt, why)
			continue
		}
		if got != want {
			t.Errorf("ForCopy(%q)\n got %q\nwant %q", stmt, got, want)
		}
	}
}

// A prepared statement is rewritten in the pieces between its placeholders
// (shim copyParts). A piece can end before the ON of its STRAIGHT_JOIN, and
// then the piece reads as a join with no condition. That pick is wrong, and
// it is safe: the copy refuses CROSS JOIN ... ON as a syntax error, so the
// statement runs on MySQL. It never turns into another query.
func TestForCopy_straightJoinInAPreparedPiece(t *testing.T) {
	got, why := ForCopy("SELECT 1 FROM a STRAIGHT_JOIN (SELECT x FROM c WHERE z = ")
	if why != "" || got != "SELECT 1 FROM a CROSS JOIN (SELECT x FROM c WHERE z = " {
		t.Errorf("piece: got %q refused %q", got, why)
	}
}
