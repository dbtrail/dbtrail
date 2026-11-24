//go:build integration

package pgcompare

import "github.com/dbtrail/dbtrail/internal/sqlcompare"

// aliasScopeShapes are names given in the select list of a table in FROM or
// of a WITH, read by the WHERE around it (CH-benCHmark's Q2), measured on
// PostgreSQL 16 and on the copy. The lines marked stay are ones the port's
// screen must leave to the copy (checkRouting).
func aliasScopeShapes() []expectation {
	o, c := itOrders, itCustomers
	stay := func(stmt string) expectation { return expectation{stmt: stmt, verdict: sqlcompare.Equal, stay: true} }
	return []expectation{
		// Q2's shape: each row with its customer's smallest quantity.
		stay(`SELECT o.id, o.qty FROM ` + o + ` o, (SELECT customer_id AS m_c, min(qty) AS m_q FROM ` + o + ` GROUP BY customer_id) m WHERE o.customer_id = m_c AND o.qty = m_q ORDER BY o.id`),
		// Answered the same, and still the source's: the screen of mod() does
		// not read the type of a name given inside (aliastypes.go).
		{stmt: `SELECT id FROM (SELECT id, qty AS q FROM ` + o + `) s WHERE mod(q, 2) = 0 ORDER BY id`, verdict: sqlcompare.Equal},
		stay(`SELECT id, q FROM (SELECT id, qty q FROM ` + o + `) s WHERE q > 2 ORDER BY id`),
		stay(`WITH w AS (SELECT customer_id AS cid, sum(qty) AS units FROM ` + o + ` GROUP BY customer_id) SELECT c.id, units FROM ` + c + ` c, w WHERE c.id = cid AND units > 5 ORDER BY c.id`),
		stay(`SELECT id FROM (SELECT id FROM (SELECT id, qty AS q FROM ` + o + `) i WHERE q > 0) s WHERE id > 1 ORDER BY id`),
		// The same statement's name: PostgreSQL refuses it, the copy would
		// answer.
		{stmt: `SELECT id, qty AS q FROM ` + o + ` WHERE q > 2`, verdict: sqlcompare.SourceError, why: "a name of the select list in the same statement's WHERE"},
		{stmt: `SELECT id, qty AS q FROM ` + o + ` WHERE id IN (SELECT id FROM ` + o + `) AND q > 2`, verdict: sqlcompare.SourceError, why: "a name of the select list in the same statement's WHERE, after a subquery"},
	}
}
