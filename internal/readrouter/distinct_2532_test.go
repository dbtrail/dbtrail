package readrouter

import "testing"

// #2532: an aggregate with DISTINCT over a number or a date counts the same
// values on MySQL and on the copy; over text the copy does not fold case
// there, so 'Paid' and 'paid' are two. DistinctVeto keeps the statement on
// MySQL unless every DISTINCT argument is a bare column known to be a
// number or a date.
func TestDistinctVeto(t *testing.T) {
	safe := []string{"ol_w_id", "ol_i_id", "amount", "id", "created_on", "año"}
	other := []string{"status", "note", "i_name"}
	for stmt, want := range map[string]bool{
		// Answered by the copy.
		"SELECT COUNT(DISTINCT ol_w_id) FROM order_line":                                                                false,
		"SELECT COUNT(DISTINCT ol.ol_w_id) FROM order_line ol":                                                          false,
		"SELECT COUNT(DISTINCT shop.order_line.ol_w_id) FROM order_line":                                                false,
		"SELECT count( distinct\n `ol_w_id` ) FROM order_line":                                                          false,
		"SELECT COUNT(DISTINCT OL_W_ID) FROM order_line":                                                                false,
		"SELECT COUNT(DISTINCT(ol_i_id)) FROM order_line":                                                               false,
		"SELECT COUNT(DISTINCT ( ol_i_id )) FROM order_line":                                                            false,
		"SELECT SUM(DISTINCT amount), AVG(DISTINCT amount), MIN(DISTINCT id), MAX(DISTINCT created_on) FROM t":          false,
		"SELECT COUNT(DISTINCT ol_w_id), COUNT(DISTINCT ol_i_id) FROM order_line":                                       false,
		"SELECT COUNT(DISTINCT ol_w_id, ol_i_id) FROM order_line":                                                       false,
		"SELECT COUNT(DISTINCT año) FROM t":                                                                             false,
		"SELECT i_name, COUNT(DISTINCT ol_w_id) FROM order_line JOIN item ON i_id = ol_i_id GROUP BY i_name ORDER BY 2": false,
		"SELECT COUNT(DISTINCTROW status) FROM t":                 true,
		"SELECT COUNT(DISTINCTROW ol_w_id) FROM t":                false,
		"SELECT DISTINCT status FROM t":                                                                                 false, // not inside an aggregate: the collation folds it
		"SELECT COUNT(*) FROM t":                                                                                        false,
		"SELECT status FROM t WHERE note = 'count(distinct status)'":                                                    false, // inside a string
		// Kept on MySQL.
		"SELECT COUNT(DISTINCT status) FROM t":                        true,
		"SELECT COUNT(DISTINCT t.status) FROM t":                      true,
		"SELECT COUNT(DISTINCT ol_w_id, status) FROM t":               true,
		"SELECT COUNT(DISTINCT ol_w_id), COUNT(DISTINCT note) FROM t": true,
		"SELECT COUNT(DISTINCT unknown_col) FROM t":                   true, // not known to be a number
		"SELECT COUNT(DISTINCT ano) FROM t":                           true, // accents are kept in names
		"SELECT COUNT(DISTINCT id + 1) FROM t":                        true,
		"SELECT COUNT(DISTINCT LOWER(id)) FROM t":                     true,
		"SELECT COUNT(DISTINCT CASE WHEN id > 1 THEN id END) FROM t":  true,
		"SELECT COUNT(DISTINCT '') FROM t":                            true,
		"SELECT COUNT(DISTINCT CONCAT(id, amount)) FROM t":            true,
		"SELECT COUNT(DISTINCT ol_w_id FROM t":                        true, // no closing parenthesis
		"SELECT GROUP_CONCAT(DISTINCT id) FROM t":                     true,
		// A name defined inside the statement can hold anything.
		"SELECT COUNT(DISTINCT id) FROM (SELECT status AS id FROM t) q":           true,
		"SELECT COUNT(DISTINCT id) FROM (SELECT status id FROM t) q":              true,
		"WITH q AS (SELECT status AS id FROM t) SELECT COUNT(DISTINCT id) FROM q": true,
		"SELECT COUNT(DISTINCT id) FROM t WHERE id IN (SELECT id FROM u)":         true,
		// MySQL reads a select-list alias there.
		"SELECT status AS id FROM t GROUP BY status HAVING COUNT(DISTINCT id) > 1": true,
		"SELECT status AS id FROM t GROUP BY status ORDER BY COUNT(DISTINCT id)":   true,
	} {
		got := DistinctVeto(Shape(stmt), safe, other)
		if (got != "") != want {
			t.Errorf("DistinctVeto(%q) = %q, want vetoed %v", stmt, got, want)
		}
	}
	// A name that is a number in one table and text in another.
	if DistinctVeto(Shape("SELECT COUNT(DISTINCT id) FROM a JOIN b USING (k)"), []string{"id"}, []string{"ID"}) == "" {
		t.Error("id is text in one of the tables, yet the copy answers")
	}
	// Nothing known: every DISTINCT aggregate stays on MySQL.
	if DistinctVeto(Shape("SELECT COUNT(DISTINCT id) FROM t"), nil, nil) == "" {
		t.Error("with no types known the copy answers")
	}
}

// Veto no longer keeps a DISTINCT aggregate on MySQL by its text alone: the
// types decide (DistinctVeto, asked where they are known).
func TestVeto_leavesDistinctToTheTypes(t *testing.T) {
	if v := Veto("SELECT COUNT(DISTINCT ol_w_id) FROM order_line"); v != "" {
		t.Errorf("Veto = %q", v)
	}
}
