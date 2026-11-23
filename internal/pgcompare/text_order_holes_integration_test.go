//go:build integration

package pgcompare

import "github.com/dbtrail/dbtrail/internal/sqlcompare"

// textOrderHoles are statements that sort or compare text and that the
// port's screen read as having no text in them, measured on PostgreSQL 16
// (collation en_US.utf8, not C) and on the copy. PostgreSQL sorts text by
// the database's collation (Ana, bruno, Carla, Dario), the copy by bytes
// (Ana, Carla, Dario, bruno): each DIFFERENT line is one the screen must
// send to the source (checkRouting), and each line marked stay a common
// statement beside it that the copy answers the same and must keep.
func textOrderHoles() []expectation {
	c, o, pe := itCustomers, itOrders, itPeople
	stay := func(stmt string) expectation { return expectation{stmt: stmt, verdict: sqlcompare.Equal, stay: true} }
	differs := func(kind, stmt string) expectation {
		return expectation{stmt: stmt, verdict: sqlcompare.Different, kind: kind, why: "text sorted or compared by the source's collation"}
	}
	return []expectation{
		// A name the statement gives a value, that is also the name of the
		// text column it stands for.
		differs("order", `SELECT name AS name FROM `+c+` ORDER BY name`),
		differs("order", `SELECT x.name AS name FROM `+c+` x ORDER BY x.name`),
		// These rows happen to agree (Ana is first either way): sent all
		// the same, min() of text is by order.
		{stmt: `SELECT min(name) AS name FROM ` + c, verdict: sqlcompare.Equal},
		differs("order", `WITH s AS (SELECT name AS name FROM `+c+`) SELECT name FROM s ORDER BY name`),
		differs("order", `SELECT s.name FROM (SELECT name AS name, id FROM `+c+`) s ORDER BY s.name`),
		{stmt: `SELECT id FROM (SELECT name AS name, id FROM ` + c + `) s WHERE name > 'B'`, verdict: sqlcompare.Equal},
		// The same name over a column that is not text, and a name of its
		// own: the copy's.
		stay(`SELECT id AS id, signup AS signup FROM `+c+` ORDER BY signup NULLS LAST, id`),
		stay(`SELECT o.qty AS qty, o.amount AS amount FROM `+o+` o ORDER BY amount DESC NULLS LAST, qty`),
		stay(`SELECT customer_id AS customer_id, sum(qty) AS qty FROM `+o+` GROUP BY customer_id ORDER BY qty DESC, customer_id`),
		stay(`SELECT customer_id, sum(amount) AS total FROM `+o+` GROUP BY customer_id ORDER BY total DESC NULLS LAST`),
		stay(`SELECT name AS name, id FROM `+c+` ORDER BY id`),

		// A position behind a star.
		differs("order", `SELECT * FROM `+c+` ORDER BY 2`),
		differs("order", `SELECT * FROM `+c+` WHERE id > 0 ORDER BY 2 DESC, 1`),
		differs("order", `SELECT * FROM `+pe+` p ORDER BY 3`),
		differs("order", `SELECT * FROM (SELECT * FROM `+c+`) s ORDER BY 2`),
		differs("order", `SELECT x.*, 1 AS one FROM `+c+` x ORDER BY 2`),
		stay(`SELECT * FROM `+c+` ORDER BY 1`),
		stay(`SELECT * FROM `+c+` ORDER BY 5 NULLS LAST, 1 DESC`),
		stay(`SELECT * FROM `+pe+` WHERE age > 10 ORDER BY 4, 1`),
		stay(`SELECT * FROM (SELECT id, qty, placed_at FROM `+o+`) s ORDER BY 3 NULLS LAST, 1`),
		stay(`SELECT customer_id, count(*) FROM `+o+` GROUP BY 1 ORDER BY 1`),
		stay(`SELECT customer_id, sum(qty) AS units FROM `+o+` GROUP BY 1 ORDER BY 2 DESC NULLS LAST, 1`),

		// A text column named like a word of ORDER BY.
		differs("order", `SELECT first, last FROM `+pe+` ORDER BY last`),
		differs("order", `SELECT first, last FROM `+pe+` ORDER BY first DESC`),
		differs("order", `SELECT id FROM `+pe+` ORDER BY last NULLS LAST, id`),
		differs("order", `SELECT p.id FROM `+pe+` p ORDER BY p.first`),
		differs("rows", `SELECT min(first), max(first), min(last), max(last) FROM `+pe),
		differs("rows", `SELECT id FROM `+pe+` WHERE last > 'C' ORDER BY id`),
		stay(`SELECT id, first, last FROM `+pe+` ORDER BY id DESC NULLS LAST`),
		stay(`SELECT id, first FROM `+pe+` ORDER BY age DESC NULLS FIRST, id ASC`),
		stay(`SELECT age, count(*) FROM `+pe+` GROUP BY age ORDER BY count(*) DESC, age`),
		stay(`SELECT id FROM `+pe+` WHERE first = 'Ana' OR last IN ('Ana', 'bruno') ORDER BY id`),
	}
}
