//go:build integration

package pgcompare

import "github.com/dbtrail/dbtrail/internal/sqlcompare"

// screenExactness are the statements of #3069, #3071 and #3072, measured on
// PostgreSQL 16 and on the copy: three things the copy answers otherwise,
// and beside each the forms it answers the same.
//
// A line that is DIFFERENT, or that the source fails, is one the port's
// screen must send to the source (checkRouting). A line marked stay is one
// the screen must leave to the copy: a rule drawn too wide would send it
// away, and the copy answers it the same.
func screenExactness() []expectation {
	x := itExact
	stay := func(stmt string) expectation { return expectation{stmt: stmt, verdict: sqlcompare.Equal, stay: true} }
	eq := func(stmt string) expectation { return expectation{stmt: stmt, verdict: sqlcompare.Equal} }
	differs := func(kind, why, stmt string) expectation {
		return expectation{stmt: stmt, verdict: sqlcompare.Different, kind: kind, why: why}
	}
	const (
		sub      = "#3071: the copy counts a start of zero or below otherwise"
		lit      = "#3072: the copy reads this date or time literal otherwise"
		leftSide = "the left side of a division is the whole product, and PostgreSQL divides it as what it is"
		asReal   = "#3069: PostgreSQL computes a real as a double precision, and sums it as a real"
	)
	return []expectation{
		// --- the main ORDER BY of text (#3219): the copy's, by bytes ----------
		// Known and accepted: 'Ana, bruno, Carla' on PostgreSQL is 'Ana, Carla,
		// bruno' here, and a LIMIT then keeps other rows.
		differs("order", "the copy sorts text by bytes: accepted for the main ORDER BY", `SELECT name FROM pgcmp_customers WHERE id < 4 ORDER BY name, id`),
		differs("text", "the copy sorts text by bytes, and the LIMIT keeps other rows: accepted", `SELECT name FROM pgcmp_customers ORDER BY name LIMIT 2`),
		// --- a division whose left side is a product ------------------------
		// PostgreSQL divides the whole product: sum of a bigint is a numeric
		// there (13.5) and a whole number on the copy (13).
		differs("rows", leftSide, `SELECT qty, sum(id) * qty / 2 FROM pgcmp_orders WHERE qty = 3 GROUP BY qty`),
		// A numeric on the left is the macro's quotient since #3238: for
		// these PostgreSQL's own text.
		stay(`SELECT qty, amount * qty / 2 FROM pgcmp_orders WHERE id = 1`),
		stay(`SELECT (amount + 1) * qty / 2 FROM pgcmp_orders WHERE id = 1`),
		differs("precision", leftSide, `WITH c(qty) AS (SELECT amount FROM pgcmp_orders WHERE id = 1) SELECT qty / 2 FROM c`),
		stay(`SELECT id, qty / 2, -qty / 2, 2 * qty / 3, qty * qty / 2, qty * id / 7, o.qty / 2, ratio * qty / 4 FROM pgcmp_orders o ORDER BY id`),
		// --- #3071: substring ---------------------------------------------
		differs("rows", sub, `SELECT substring('apple', -1, 3)`),
		differs("text", sub, `SELECT id, substring(word, -1, 3) FROM `+x+` ORDER BY id`),
		differs("text", sub, `SELECT id, substring(word FROM -2 FOR 4) FROM `+x+` ORDER BY id`),
		differs("text", sub, `SELECT id, substring(word, -2) FROM `+x+` ORDER BY id`),
		differs("text", sub, `SELECT id, substring(word, 1 - whole, 3) FROM `+x+` ORDER BY id`),
		// A start of zero agrees, and so does a start read from a column on
		// these rows (the one below zero cuts an empty text): the screen
		// sends both to the source all the same, since only a start written
		// as a positive number is known before the statement runs.
		eq(`SELECT id, substring(word, 0, 3) FROM ` + x + ` ORDER BY id`),
		eq(`SELECT id, substring(word, whole, 2) FROM ` + x + ` ORDER BY id`),
		{stmt: `SELECT id, substring(word, 2, -1) FROM ` + x + ` ORDER BY id`, verdict: sqlcompare.SourceError},
		stay(`SELECT id, substring(word, 1, 3), substring(word, 2), substring(word, 4, 100) FROM ` + x + ` ORDER BY id`),
		stay(`SELECT id, substring(word FROM 2 FOR 3), substring(word FROM 3) FROM ` + x + ` ORDER BY id`),
		stay(`SELECT substring('apple', 2, 3), substring('apple' FROM 5)`),
		// substr is substring with commas (#3219): the copy's under the same
		// rule, a start and a length that are positive whole numbers.
		differs("text", sub, `SELECT id, substr(word, -1, 3) FROM `+x+` ORDER BY id`),
		stay(`SELECT id, substr(word, 2, 3), substr(word, 1, 1), substr(word, 2), substr(word, 4, 100) FROM ` + x + ` ORDER BY id`),
		stay(`SELECT substr('apple', 2, 3), substr('apple', 5), substr('año', 2, 1)`),
		// left, right and overlay were never the copy's: the screen sends
		// each to the source, whatever it is given.
		eq(`SELECT id, left(word, 2), right(word, 2) FROM ` + x + ` ORDER BY id`),
		eq(`SELECT id, left(word, -2), right(word, -2) FROM ` + x + ` ORDER BY id`),
		{stmt: `SELECT id, overlay(word placing 'XX' from 2 for 1) FROM ` + x + ` ORDER BY id`, verdict: sqlcompare.NotOnCopy, says: "overlay", why: "the copy has no overlay()"},

		// --- #3072: date and time literals --------------------------------
		differs("rows", lit, `SELECT '26-01-05'::date`),
		differs("rows", lit, `SELECT DATE '26-01-05'`),
		differs("rows", lit, `SELECT CAST('26-01-05' AS date)`),
		differs("rows", lit, `SELECT TIMESTAMP '2026-01-05 10:30:00.1234567'`),
		differs("rows", lit, `SELECT TIME '24:00:00'`),
		differs("rows", lit, `SELECT id FROM `+x+` WHERE day = '26-01-05'`),
		differs("rows", lit, `SELECT id FROM `+x+` WHERE day IN ('26-01-05', '2026-02-10')`),
		differs("rows", lit, `SELECT id FROM `+x+` WHERE day BETWEEN '26-01-01' AND '26-12-31'`),
		differs("rows", lit, `SELECT id FROM `+x+` WHERE stamp = '2026-01-05 10:30:00.1234567'`),
		// Compared with a time column the copy answers the same; the screen
		// holds a string to the form of a literal all the same.
		eq(`SELECT id FROM ` + x + ` WHERE clock < '24:00:00'`),
		// Other spellings outside the strict form, as they measure.
		eq(`SELECT DATE '2026-1-5'`),
		eq(`SELECT DATE '2026-01-05 10:30:00'`),
		{stmt: `SELECT TIME '23:59:60'`, verdict: sqlcompare.NotOnCopy, says: "out of range", why: "a leap second is 24:00:00 on PostgreSQL; the copy refuses it"},
		eq(`SELECT DATE 'epoch'`),
		stay(`SELECT DATE '2026-01-05', '2026-01-05'::date, CAST('2026-01-05' AS date)`),
		stay(`SELECT TIMESTAMP '2026-01-05 10:30:00.123456', TIMESTAMP '2026-01-05 23:59:59', TIMESTAMP '2026-01-05', TIMESTAMP '2026-01-05T10:30:00'`),
		stay(`SELECT TIMESTAMP '2026-01-05 10:30', TIME '10:30'`),
		stay(`SELECT TIME '23:59:59.999999', TIME '00:00:00'`),
		stay(`SELECT id FROM ` + x + ` WHERE day = '2026-01-05' OR day IN ('2026-02-10', '2026-12-31')`),
		stay(`SELECT id FROM ` + x + ` WHERE day BETWEEN DATE '2026-01-01' AND '2026-01-31'`),
		stay(`SELECT id FROM ` + x + ` WHERE stamp >= '2026-01-05' AND stamp < '2026-01-06 00:00:00' AND stamp <> TIMESTAMP '2026-01-05 10:30:00.123456'`),
		stay(`SELECT id FROM ` + x + ` WHERE clock < '12:00:00' OR clock = TIME '23:59:59'`),

		// --- #3069: a real computed with, and summed ----------------------
		differs("precision", asReal, `SELECT id, rl + 1 FROM `+x+` ORDER BY id`),
		differs("precision", asReal, `SELECT sum(rl) FROM `+x+` WHERE id <= 3`),
		differs("precision", asReal, `SELECT id, rl * 2, rl / 2 FROM `+x+` ORDER BY id`),
		differs("precision", asReal, `SELECT id, rl * whole, rl + amt, rl - 1 FROM `+x+` ORDER BY id`),
		differs("precision", asReal, `SELECT id, (rl) + 1, abs(rl) * 2, 1 + rl FROM `+x+` ORDER BY id`),
		differs("precision", asReal, `SELECT s.id, s.v + 1 FROM (SELECT id, rl AS v FROM `+x+`) s ORDER BY s.id`),
		differs("precision", asReal, `SELECT id, sum(rl) OVER (ORDER BY id) FROM `+x+` ORDER BY id`),
		// A real with another real, with a double precision, or with its
		// sign turned, agrees on these rows. The rule is by the column's type
		// and sends them to the source with the rest.
		eq(`SELECT max(rl) - min(rl) FROM ` + x),
		eq(`SELECT id, rl + dbl FROM ` + x + ` ORDER BY id`),
		eq(`SELECT id, -rl FROM ` + x + ` ORDER BY id`),
		eq(`SELECT sum(rl) FROM ` + x),
		// Beside another value in a COALESCE or a GREATEST the answer is a
		// real or a double precision on both sides.
		stay(`SELECT id, coalesce(rl, 0), greatest(rl, 1), least(rl, dbl), CASE WHEN whole > 0 THEN rl ELSE 0 END FROM ` + x + ` ORDER BY id`),
		stay(`SELECT id, coalesce(rl, amt), greatest(rl, amt) FROM ` + x + ` ORDER BY id`),
		stay(`SELECT id, rl, dbl FROM ` + x + ` ORDER BY id`),
		stay(`SELECT min(rl), max(rl), count(rl) FROM ` + x),
		stay(`SELECT id, abs(rl) FROM ` + x + ` ORDER BY id`),
		stay(`SELECT id FROM ` + x + ` WHERE rl > 0.1 OR rl = 0.1 ORDER BY id`),
		stay(`SELECT id FROM ` + x + ` WHERE rl < dbl OR rl = dbl ORDER BY id`),
		stay(`SELECT id, dbl + 1, dbl * 2, dbl / 2, dbl - whole, 1 + dbl FROM ` + x + ` ORDER BY id`),
		stay(`SELECT sum(dbl), sum(whole), sum(amt) FROM ` + x),
		stay(`SELECT id, whole + 1, whole * 2, amt + 1, amt * 2, amt - whole FROM ` + x + ` ORDER BY id`),
		stay(`SELECT id, dbl + amt, dbl * whole FROM ` + x + ` ORDER BY id`),
	}
}
