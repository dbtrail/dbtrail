//go:build integration

package pgcompare

import "github.com/dbtrail/dbtrail/internal/sqlcompare"

// reportShapes are the report shapes of #3219, measured on PostgreSQL 16 and
// on the copy, in a session in UTC: date_trunc of a timestamp with time
// zone, and a sort key that is a sum or a count in a statement that also
// reads text. Each line marked stay is one the port's screen must leave to
// the copy (checkRouting).
func reportShapes() []expectation {
	o, c := itOrders, itCustomers
	stay := func(stmt string) expectation { return expectation{stmt: stmt, verdict: sqlcompare.Equal, stay: true} }
	return []expectation{
		// A series by day, month and year over a timestamptz: PostgreSQL
		// answers a timestamp with time zone at the start of the day in the
		// session's zone, and so does the copy, in UTC.
		stay(`SELECT date_trunc('day', seen_at) AS day, count(*) AS n, sum(qty) FROM ` + o + ` GROUP BY 1 ORDER BY 1`),
		stay(`SELECT date_trunc('month', o.seen_at) AS m, count(*) FROM ` + o + ` o GROUP BY 1 ORDER BY 1`),
		stay(`SELECT date_trunc('year', seen_at), date_trunc('day', seen_at), id FROM ` + o + ` ORDER BY id`),
		stay(`SELECT date_trunc('day', o.seen_at) AS day, c.id, count(*) FROM ` + o + ` o JOIN ` + c + ` c ON c.id = o.customer_id WHERE o.seen_at >= '2026-02-01 00:00:00+00' GROUP BY 1, 2 ORDER BY 1, 2`),
		// The column written with its table, for a timestamp too.
		stay(`SELECT date_trunc('day', o.placed_at) AS day, count(*) FROM ` + o + ` o GROUP BY 1 ORDER BY 1`),
		// Still the source's: a date (PostgreSQL answers a timestamp with
		// time zone, the copy a timestamp) and a field below the day.
		{stmt: `SELECT date_trunc('month', signup) FROM ` + c, verdict: sqlcompare.Different, kind: "rows", why: "#2770: date_trunc of a date"},
		{stmt: `SELECT id, date_trunc('hour', seen_at) FROM ` + o + ` ORDER BY id`, verdict: sqlcompare.Equal},

		// A sort key that is a sum or a count is never text, whatever text
		// the statement reads elsewhere.
		stay(`SELECT c.name, sum(o.qty) AS units FROM ` + o + ` o JOIN ` + c + ` c ON c.id = o.customer_id GROUP BY c.name ORDER BY sum(o.qty) DESC`),
		stay(`SELECT status, count(*) FROM ` + o + ` WHERE status IS NOT NULL GROUP BY status ORDER BY count(*) DESC, sum(id)`),
		stay(`SELECT id, status, rank() OVER (PARTITION BY customer_id ORDER BY sum(qty) DESC, id) AS r FROM ` + o + ` GROUP BY id, status, customer_id ORDER BY id`),
		// The shape of the issue: a ranking in a subquery, read by its
		// name, joined to a table whose text is only shown.
		stay(`SELECT t.customer_id, t.r, t.id, c.name, t.units
		    FROM (SELECT customer_id, id, sum(qty) AS units, rank() OVER (PARTITION BY customer_id ORDER BY sum(qty) DESC, id) AS r
		          FROM ` + o + ` WHERE customer_id BETWEEN 1 AND 4 GROUP BY customer_id, id) t
		    JOIN ` + c + ` c ON c.id = t.customer_id WHERE t.r <= 2 ORDER BY t.customer_id, t.r`),
		// Still the source's: a key that is text, or may be.
		{stmt: `SELECT name FROM ` + c + ` ORDER BY min(name) OVER ()`, verdict: sqlcompare.Equal},
		{stmt: `SELECT c.name, sum(o.qty) FROM ` + o + ` o JOIN ` + c + ` c ON c.id = o.customer_id GROUP BY c.name ORDER BY c.name`, verdict: sqlcompare.Different, kind: "order", why: "#2770: text sorted by the source's collation"},
		{stmt: `SELECT name FROM ` + c + ` ORDER BY coalesce(city, name), id`, verdict: sqlcompare.Different, kind: "order", why: "#2770: text sorted by the source's collation"},
	}
}
