package readrouter

import "testing"

// A statement that asks MySQL to join its tables in the order written
// (STRAIGHT_JOIN, as a join or as a SELECT modifier) asks the copy the same:
// its planner reads the views' row counts as guesses and can pick an order
// whose intermediate results do not fit anywhere (a CH-benCHmark Q5 that
// filled 351 GB of spill), where the written order answers in seconds.
// Read like the rewrite reads it: not in a comment or a string literal, and
// not as a name after or before a dot.
func TestKeepsJoinOrder(t *testing.T) {
	q5 := "SELECT n_name, sum(ol_amount) AS revenue FROM REGION STRAIGHT_JOIN NATION STRAIGHT_JOIN  SUPPLIER STRAIGHT_JOIN STOCK " +
		"STRAIGHT_JOIN ORDER_LINE STRAIGHT_JOIN OORDER STRAIGHT_JOIN CUSTOMER WHERE c_id = o_c_id AND c_w_id = o_w_id AND " +
		"c_d_id = o_d_id AND ol_o_id = o_id AND ol_w_id = o_w_id AND ol_d_id=o_d_id AND ol_w_id = s_w_id AND ol_i_id = s_i_id AND " +
		"MOD((s_w_id * s_i_id), 10000) = su_suppkey AND ascii(substring(c_state from  1  for  1)) = su_nationkey AND " +
		"su_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'Europe' AND o_entry_d >= '2007-01-02 00:00:00.000000' " +
		"GROUP BY n_name ORDER BY revenue DESC"
	q21 := "SELECT su_name, count(*) AS numwait FROM NATION STRAIGHT_JOIN SUPPLIER STRAIGHT_JOIN STOCK STRAIGHT_JOIN ORDER_LINE l1 " +
		"STRAIGHT_JOIN OORDER WHERE ol_o_id = o_id AND ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_w_id = s_w_id AND ol_i_id = s_i_id AND " +
		"mod((s_w_id * s_i_id),10000) = su_suppkey AND l1.ol_delivery_d > o_entry_d AND NOT EXISTS (SELECT * FROM ORDER_LINE l2 " +
		"WHERE l2.ol_o_id = l1.ol_o_id AND l2.ol_w_id = l1.ol_w_id AND l2.ol_d_id = l1.ol_d_id AND l2.ol_delivery_d > l1.ol_delivery_d) " +
		"AND su_nationkey = n_nationkey AND n_name = 'Germany' GROUP BY su_name ORDER BY numwait DESC, su_name"
	cases := []struct {
		name, stmt string
		want       bool
	}{
		// Every join fixed: the written order is the query's.
		{"a join with ON", "SELECT * FROM a STRAIGHT_JOIN b ON a.id = b.id", true},
		{"a join with the condition in WHERE", "SELECT * FROM a STRAIGHT_JOIN b WHERE a.id = b.id", true},
		{"lowercase", "select * from a straight_join b on a.id = b.id", true},
		{"mixed case", "SELECT * FROM a Straight_Join b ON a.id = b.id", true},
		{"with a placeholder", "SELECT * FROM a STRAIGHT_JOIN b ON a.id = b.id WHERE a.x = ?", true},
		{"a function's arguments", "SELECT concat(a.x, b.y), mod(a.id, 10) FROM a STRAIGHT_JOIN b WHERE a.id = b.id", true},
		{"a subquery of one table", "SELECT * FROM a STRAIGHT_JOIN b ON a.id = b.id WHERE NOT EXISTS (SELECT 1 FROM c WHERE c.id = a.id)", true},
		{"CH-benCHmark Q5", q5, true},
		{"CH-benCHmark Q21", q21, true},
		// A join the client left to the planner: the copy's planner keeps
		// it, so the order is not fixed for the rest either.
		{"only in a subquery", "SELECT * FROM a, b WHERE a.x = b.x AND a.id IN (SELECT s.id FROM s STRAIGHT_JOIN t ON s.id = t.id)", false},
		{"mixed with a comma", "SELECT * FROM a STRAIGHT_JOIN b ON a.id = b.id, c WHERE c.y = a.y", false},
		{"mixed with a JOIN", "SELECT * FROM a STRAIGHT_JOIN b ON a.id = b.id JOIN c ON c.y = a.y", false},
		{"mixed with a LEFT JOIN", "SELECT * FROM a STRAIGHT_JOIN b ON a.id = b.id LEFT JOIN c ON c.y = a.y", false},
		{"a modifier over a list", "SELECT STRAIGHT_JOIN * FROM a, b WHERE a.id = b.id", false},
		{"a subquery with a comma", "SELECT * FROM a STRAIGHT_JOIN b ON a.id = b.id WHERE a.x IN (SELECT c.x FROM c, d WHERE c.y = d.y)", false},
		// No STRAIGHT_JOIN at all.
		{"none", "SELECT * FROM a JOIN b ON a.id = b.id", false},
		{"empty", "", false},
		{"in a comment", "SELECT * FROM a /* STRAIGHT_JOIN */ JOIN b ON a.id = b.id", false},
		{"in a line comment", "SELECT * FROM a JOIN b ON a.id = b.id -- STRAIGHT_JOIN\n", false},
		{"in a string", "SELECT * FROM a WHERE note = 'STRAIGHT_JOIN'", false},
		{"a table named after a dot", "SELECT * FROM db.straight_join", false},
		{"a table named before a dot", "SELECT straight_join.x FROM `straight_join`", false},
		{"a quoted name", "SELECT * FROM `straight_join`", false},
		{"part of a longer word", "SELECT straight_joins FROM a", false},
		// A column named join after a dot is not a JOIN, nor is a comma
		// inside a quoted name.
		{"a column named join", "SELECT a.`join`, b.join FROM a STRAIGHT_JOIN b ON a.id = b.id", true},
		{"a comma in a quoted name", "SELECT * FROM `a,b` STRAIGHT_JOIN c ON c.id = `a,b`.id", true},
	}
	for _, c := range cases {
		if got := KeepsJoinOrder(c.stmt); got != c.want {
			t.Errorf("%s: KeepsJoinOrder(%q) = %v, want %v", c.name, c.stmt, got, c.want)
		}
	}
}
