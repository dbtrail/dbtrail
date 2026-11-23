//go:build integration

package pgcompare

import "github.com/dbtrail/dbtrail/internal/sqlcompare"

// screenExactness are the statements of #3069, #3070, #3071 and #3072,
// measured on PostgreSQL 16 and on the copy: four things the copy answers
// otherwise, and beside each the forms it answers the same.
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
		sub   = "#3071: the copy counts a start of zero or below otherwise"
		lit   = "#3072: the copy reads this date or time literal otherwise"
		scale = "#3070: PostgreSQL writes each value with its own decimals"
		asReal = "#3069: PostgreSQL computes a real as a double precision, and sums it as a real"
	)
	return []expectation{
		// --- #3071: substring ---------------------------------------------
		differs("rows", sub, `SELECT substring('apple', -1, 3)`),
		differs("rows", sub, `SELECT id, substring(word, -1, 3) FROM `+x+` ORDER BY id`),
		differs("rows", sub, `SELECT id, substring(word, 0, 3) FROM `+x+` ORDER BY id`),
		differs("rows", sub, `SELECT id, substring(word FROM -2 FOR 4) FROM `+x+` ORDER BY id`),
		differs("rows", sub, `SELECT id, substring(word, whole, 2) FROM `+x+` ORDER BY id`),
		differs("rows", sub, `SELECT id, substring(word, -2) FROM `+x+` ORDER BY id`),
		{stmt: `SELECT id, substring(word, 2, -1) FROM ` + x + ` ORDER BY id`, verdict: sqlcompare.SourceError},
		stay(`SELECT id, substring(word, 1, 3), substring(word, 2), substring(word, 4, 100) FROM `+x+` ORDER BY id`),
		stay(`SELECT id, substring(word FROM 2 FOR 3), substring(word FROM 3) FROM `+x+` ORDER BY id`),
		stay(`SELECT substring('apple', 2, 3), substring('apple' FROM 5)`),
		// substr, left, right and overlay were never the copy's: the screen
		// sends each to the source, whatever it is given.
		differs("rows", sub, `SELECT id, substr(word, -1, 3) FROM `+x+` ORDER BY id`),
		eq(`SELECT id, substr(word, 2, 3) FROM `+x+` ORDER BY id`),
		eq(`SELECT id, left(word, 2), right(word, 2) FROM `+x+` ORDER BY id`),
		eq(`SELECT id, left(word, -2), right(word, -2) FROM `+x+` ORDER BY id`),
		eq(`SELECT id, overlay(word placing 'XX' from 2 for 1) FROM `+x+` ORDER BY id`),

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
		differs("rows", lit, `SELECT id FROM `+x+` WHERE clock < '24:00:00'`),
		// Other spellings outside the strict form, as they measure.
		eq(`SELECT DATE '2026-1-5'`),
		eq(`SELECT DATE '2026-01-05 10:30:00'`),
		eq(`SELECT TIME '23:59:60'`),
		eq(`SELECT DATE 'epoch'`),
		stay(`SELECT DATE '2026-01-05', '2026-01-05'::date, CAST('2026-01-05' AS date)`),
		stay(`SELECT TIMESTAMP '2026-01-05 10:30:00.123456', TIMESTAMP '2026-01-05 23:59:59', TIMESTAMP '2026-01-05', TIMESTAMP '2026-01-05T10:30:00'`),
		stay(`SELECT TIMESTAMP '2026-01-05 10:30', TIME '10:30'`),
		stay(`SELECT TIME '23:59:59.999999', TIME '00:00:00'`),
		stay(`SELECT id FROM `+x+` WHERE day = '2026-01-05' OR day IN ('2026-02-10', '2026-12-31')`),
		stay(`SELECT id FROM `+x+` WHERE day BETWEEN DATE '2026-01-01' AND '2026-01-31'`),
		stay(`SELECT id FROM `+x+` WHERE stamp >= '2026-01-05' AND stamp < '2026-01-06 00:00:00' AND stamp <> TIMESTAMP '2026-01-05 10:30:00.123456'`),
		stay(`SELECT id FROM `+x+` WHERE clock < '12:00:00' OR clock = TIME '23:59:59'`),

		// --- #3070: numbers of different decimals in one column -----------
		differs("precision", scale, `SELECT id, CASE WHEN amt > 3 THEN amt ELSE 0 END FROM `+x+` ORDER BY id`),
		differs("precision", scale, `SELECT id, greatest(amt, amt4), least(amt, amt4) FROM `+x+` ORDER BY id`),
		differs("precision", scale, `SELECT coalesce(max(amt), 0) FROM `+x+` WHERE id < 0`),
		differs("precision", scale, `SELECT id, coalesce(amt, 0) FROM `+x+` ORDER BY id`),
		differs("precision", scale, `SELECT id, coalesce(amt, whole) FROM `+x+` ORDER BY id`),
		differs("precision", scale, `SELECT CASE WHEN true THEN 1.5 ELSE 0 END, CASE WHEN false THEN 1.5 ELSE 0 END`),
		differs("precision", scale, `SELECT id, CASE WHEN whole > 0 THEN amt * whole ELSE 0 END FROM `+x+` ORDER BY id`),
		differs("precision", scale, `SELECT coalesce(t.total, 0) FROM (SELECT sum(amt) AS total FROM `+x+` WHERE id < 0) t`),
		differs("precision", scale, `SELECT id, greatest(amt, 3) FROM `+x+` ORDER BY id`),
		differs("precision", scale, `SELECT sum(CASE WHEN whole > 0 THEN amt ELSE 0 END) FROM `+x+` WHERE id = 3`),
		stay(`SELECT id, CASE WHEN amt > 3 THEN amt ELSE 0.00 END FROM `+x+` ORDER BY id`),
		stay(`SELECT id, coalesce(amt, 0.00), greatest(amt, 3.00), least(amt, 3.00) FROM `+x+` ORDER BY id`),
		stay(`SELECT id, CASE WHEN whole > 0 THEN whole ELSE 0 END, coalesce(whole, 0), greatest(whole, 2) FROM `+x+` ORDER BY id`),
		stay(`SELECT id, CASE WHEN amt > 3 THEN amt END, CASE WHEN amt > 3 THEN amt ELSE NULL END FROM `+x+` ORDER BY id`),
		stay(`SELECT coalesce(max(amt), 0.00), coalesce(sum(amt), 0.00), coalesce(count(amt), 0) FROM `+x+` WHERE id < 0`),
		stay(`SELECT coalesce(t.total, 0.00), coalesce(t.k, 0) FROM (SELECT sum(amt) AS total, count(*) AS k FROM `+x+` WHERE id < 0) t`),
		stay(`SELECT sum(CASE WHEN whole > 0 THEN amt ELSE 0.00 END), sum(CASE WHEN whole > 0 THEN 1 ELSE 0 END) FROM `+x),
		stay(`SELECT id, coalesce(dbl, 0), greatest(dbl, amt), CASE WHEN whole > 0 THEN dbl ELSE 0 END FROM `+x+` ORDER BY id`),
		stay(`SELECT id, CASE WHEN amt > 3 THEN 'big' ELSE 'small' END FROM `+x+` ORDER BY id`),

		// --- #3069: a real computed with, and summed ----------------------
		differs("precision", asReal, `SELECT id, rl + 1 FROM `+x+` ORDER BY id`),
		differs("precision", asReal, `SELECT sum(rl) FROM `+x),
		differs("precision", asReal, `SELECT id, rl * 2, rl / 2 FROM `+x+` ORDER BY id`),
		differs("precision", asReal, `SELECT id, rl * whole, rl + amt, rl - 1 FROM `+x+` ORDER BY id`),
		differs("precision", asReal, `SELECT id, (rl) + 1, abs(rl) * 2, 1 + rl FROM `+x+` ORDER BY id`),
		differs("precision", asReal, `SELECT max(rl) - min(rl) FROM `+x),
		differs("precision", asReal, `SELECT s.id, s.v + 1 FROM (SELECT id, rl AS v FROM `+x+`) s ORDER BY s.id`),
		differs("precision", asReal, `SELECT id, sum(rl) OVER (ORDER BY id) FROM `+x+` ORDER BY id`),
		differs("precision", asReal, `SELECT id, coalesce(rl, 0), greatest(rl, 1) FROM `+x+` ORDER BY id`),
		differs("precision", asReal, `SELECT id, rl + dbl FROM `+x+` ORDER BY id`),
		eq(`SELECT id, -rl FROM `+x+` ORDER BY id`),
		stay(`SELECT id, rl, dbl FROM `+x+` ORDER BY id`),
		stay(`SELECT min(rl), max(rl), count(rl) FROM `+x),
		stay(`SELECT id, abs(rl) FROM `+x+` ORDER BY id`),
		stay(`SELECT id FROM `+x+` WHERE rl > 0.1 OR rl = 0.1 ORDER BY id`),
		stay(`SELECT id FROM `+x+` WHERE rl < dbl OR rl = dbl ORDER BY id`),
		stay(`SELECT id, dbl + 1, dbl * 2, dbl / 2, dbl - whole, 1 + dbl FROM `+x+` ORDER BY id`),
		stay(`SELECT sum(dbl), sum(whole), sum(amt) FROM `+x),
		stay(`SELECT id, whole + 1, whole * 2, amt + 1, amt * 2, amt - whole FROM `+x+` ORDER BY id`),
		stay(`SELECT id, dbl + amt, dbl * whole FROM `+x+` ORDER BY id`),
	}
}
