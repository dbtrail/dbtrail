package readrouter

import "testing"

// #2641: ROUND of a half rounds to even on MySQL over a FLOAT or a DOUBLE
// (ROUND(2.5e0) is 2, ROUND(0.125e0, 2) is 0.12) and away from zero on the
// copy (3 and 0.13). Over a whole number or an exact DECIMAL both round away
// from zero and print the same. RoundVeto keeps the statement on MySQL unless
// every ROUND's first argument is known to be one of those.
func TestRoundVeto(t *testing.T) {
	safe := []string{"total", "amount", "qty", "id", "año"}
	other := []string{"price", "ratio", "note"}
	for stmt, want := range map[string]bool{
		// Answered by the copy. The README's example first.
		"SELECT c.tier, count(*) AS orders, round(sum(o.total), 2) AS revenue\nFROM orders o JOIN customers c ON c.id = o.customer_id\nGROUP BY c.tier ORDER BY revenue DESC;": false,
		"SELECT ROUND(total) FROM t":                                                    false,
		"SELECT ROUND(total, 2) FROM t":                                                 false,
		"SELECT ROUND(total, -1) FROM t":                                                false,
		"SELECT ROUND(total, - 1) FROM t":                                               false,
		"SELECT ROUND(total, 0) FROM t":                                                 false,
		"SELECT round ( total , 2 ) FROM t":                                             false, // spaces
		"SELECT ROUND\v(total, 2) FROM t":                                               false,
		"SELECT Round(TOTAL, 2) FROM t":                                                 false, // a column's name in another case
		"SELECT ROUND(`total`, 2) FROM `t`":                                             false,
		"SELECT ROUND(t.total, 2) FROM t":                                               false,
		"SELECT ROUND(`shop`.`t`.`total`, 2) FROM t":                                    false,
		"SELECT ROUND((total), 2) FROM t":                                               false,
		"SELECT ROUND(SUM(total), 2), ROUND(MAX(qty)), ROUND(min(amount), 1) FROM t":    false,
		"SELECT ROUND(sum( `total` ), 2) FROM t":                                        false,
		"SELECT ROUND(año, 2) FROM t":                                                   false,
		"SELECT ROUND(2.5), ROUND(-2.5), ROUND(2.675, 2), ROUND(25, -1)":                false, // decimal literals: exact on both
		"SELECT ROUND(.5)":                                                              false,
		"SELECT total FROM t":                                                           false,
		"SELECT round FROM t":                                                           false, // a column called round
		"SELECT `round`, rounded FROM t":                                                false,
		"SELECT myround(price), my_round(price), $round(price), round_to(price) FROM t": false, // other functions
		"SELECT * FROM t WHERE note = 'round(price)'":                                   false, // inside a string
		"SELECT * FROM t ORDER BY total":                                                false,
		"SELECT ROUND(total, 2) FROM t ORDER BY id":                                     false, // the call is before ORDER BY
		// Kept on MySQL.
		"SELECT ROUND(price) FROM t":                           true, // a DOUBLE
		"SELECT ROUND(price, 2) FROM t":                        true,
		"SELECT ROUND(ratio, 2) FROM t":                        true, // a FLOAT
		"SELECT ROUND(t.price, 2) FROM t":                      true,
		"SELECT `round`(price) FROM t":                         true, // MySQL calls the built-in function
		"SELECT ROUND (price) FROM t":                          true,
		"SELECT ROUND(note) FROM t":                            true, // text: MySQL reads it as a DOUBLE
		"SELECT ROUND(unknown_col, 2) FROM t":                  true, // not known to be exact
		"SELECT ROUND(ano, 2) FROM t":                          true, // accents are kept in names
		"SELECT ROUND(SUM(price), 2) FROM t":                   true,
		"SELECT ROUND(AVG(total), 2) FROM t":                   true, // a DOUBLE on the copy
		"SELECT ROUND(SUM(qty * total), 2) FROM t":             true, // an expression
		"SELECT ROUND(total / 3, 2) FROM t":                    true,
		"SELECT ROUND(total * 1.0) FROM t":                     true,
		"SELECT ROUND(CASE WHEN id > 1 THEN total END) FROM t": true,
		"SELECT ROUND(ROUND(total, 2), 1) FROM t":              true, // a call: its type is not read
		"SELECT ROUND(COUNT(*)) FROM t":                        true,
		"SELECT ROUND(SUM(DISTINCT total)) FROM t":             true,
		"SELECT ROUND(2.5e0)":                                  true, // a DOUBLE literal
		"SELECT ROUND(25E-1)":                                  true,
		"SELECT ROUND(?)":                                      true,
		"SELECT ROUND(total, ?) FROM t":                        true, // digits not written in the statement
		"SELECT ROUND(total, id) FROM t":                       true,
		"SELECT ROUND(total, 1.5) FROM t":                      true,
		"SELECT ROUND(total, 2, 3) FROM t":                     true,
		"SELECT ROUND() FROM t":                                true,
		"SELECT ROUND(total FROM t":                            true, // no closing parenthesis
		"SELECT ROUND(total), ROUND(price) FROM t":             true,
		"SELECT ROUND(price) FROM t WHERE ROUND(total) > 1":    true,
		// A name defined inside the statement can hold anything.
		"SELECT ROUND(total) FROM (SELECT price AS total FROM t) q":           true,
		"WITH q AS (SELECT price AS total FROM t) SELECT ROUND(total) FROM q": true,
		"SELECT ROUND(total) FROM t WHERE id IN (SELECT id FROM u)":           true,
		// MySQL reads a select-list alias there.
		"SELECT price AS total FROM t ORDER BY ROUND(total)":                  true,
		"SELECT price AS total FROM t GROUP BY price HAVING ROUND(total) > 1": true,
	} {
		got := RoundVeto(Shape(stmt), safe, other)
		if (got != "") != want {
			t.Errorf("RoundVeto(%q) = %q, want vetoed %v", stmt, got, want)
		}
	}
	// A name that is exact in one table and a DOUBLE in another.
	if RoundVeto(Shape("SELECT ROUND(total) FROM a JOIN b USING (k)"), []string{"total"}, []string{"TOTAL"}) == "" {
		t.Error("total is a DOUBLE in one of the tables, yet the copy answers")
	}
	// Nothing known: every ROUND of a column stays on MySQL.
	if RoundVeto(Shape("SELECT ROUND(total, 2) FROM t"), nil, nil) == "" {
		t.Error("with no types known the copy answers")
	}
}

// Veto does not keep ROUND on MySQL by its text: the types decide
// (RoundVeto, asked where they are known).
func TestVeto_leavesRoundToTheTypes(t *testing.T) {
	for _, stmt := range []string{
		"SELECT ROUND(price) FROM t",
		"SELECT c.tier, count(*) AS orders, round(sum(o.total), 2) AS revenue FROM orders o JOIN customers c ON c.id = o.customer_id GROUP BY c.tier ORDER BY revenue DESC",
	} {
		if v := Veto(stmt); v != "" {
			t.Errorf("Veto(%q) = %q", stmt, v)
		}
	}
}
