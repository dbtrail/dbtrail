//go:build integration

package pgcompare

import "github.com/dbtrail/dbtrail/internal/sqlcompare"

// zoneAndBetween are the statements of #3024, measured on PostgreSQL 16 and
// on the copy: a time zone written in a literal whose type holds none
// (timestamp, date, time), both bounds of a BETWEEN, and an operator that
// ends in + or -.
//
// PostgreSQL does not read the zone: TIMESTAMP '2026-03-02 10:00:00+02' is
// 10:00, in a session in UTC and in one in America/Bogota. The copy's engine
// (DuckDB 1.5.6) does the same with an offset glued to the seconds, refuses
// a zone's name and a time written without its seconds, and takes an offset
// PostgreSQL refuses (+16). The port's screen sends every one of them to the
// source (pgshim zonedTyped): checkRouting holds it to the lines below that
// are not EQUAL, and the EQUAL ones say what an engine that began to apply
// the offset would change.
func zoneAndBetween() []expectation {
	o, c := itOrders, itCustomers
	eq := func(stmt string) expectation { return expectation{stmt: stmt, verdict: sqlcompare.Equal} }
	refused := func(says, stmt string) expectation {
		return expectation{stmt: stmt, verdict: sqlcompare.NotOnCopy, says: says, why: "#3024: the copy refuses this spelling of a time zone"}
	}
	return []expectation{
		// No zone: both bounds of a BETWEEN, strings and typed literals.
		eq(`SELECT id FROM ` + o + ` WHERE placed_at BETWEEN '2026-03-02 10:00:00' AND '2026-03-03 10:00:00'`),
		eq(`SELECT id FROM ` + o + ` WHERE placed_at BETWEEN TIMESTAMP '2026-03-02 10:00:00' AND TIMESTAMP '2026-03-03 10:00:00'`),
		eq(`SELECT id FROM ` + o + ` WHERE placed_at NOT BETWEEN TIMESTAMP '2026-01-01 00:00:00' AND TIMESTAMP '2026-03-03 10:00:00'`),
		eq(`SELECT id FROM ` + o + ` WHERE qty NOT BETWEEN 3 AND 10`),
		eq(`SELECT name FROM ` + c + ` WHERE signup BETWEEN DATE '2025-12-01' AND DATE '2026-01-15'`),
		eq(`SELECT name FROM ` + c + ` WHERE signup BETWEEN '2025-12-01' AND '2026-01-15'`),
		// A typed literal with an offset: order 8 was placed at 10:00, and
		// is the answer to the first on both sides, to the second on neither.
		eq(`SELECT id FROM ` + o + ` WHERE placed_at = TIMESTAMP '2026-03-02 10:00:00+02'`),
		eq(`SELECT id FROM ` + o + ` WHERE placed_at = TIMESTAMP '2026-03-02 12:00:00+02'`),
		eq(`SELECT id FROM ` + o + ` WHERE placed_at = TIMESTAMP '2026-03-02 10:00:00Z'`),
		eq(`SELECT id FROM ` + o + ` WHERE placed_at = TIMESTAMP '2026-03-02T10:00:00+02:00'`),
		eq(`SELECT id FROM ` + o + ` WHERE placed_at = '2026-03-02 10:00:00+02'::timestamp`),
		eq(`SELECT id FROM ` + o + ` WHERE placed_at = CAST('2026-03-02 10:00:00+02' AS timestamp)`),
		eq(`SELECT id FROM ` + o + ` WHERE placed_at = '2026-03-02 10:00:00+02'`),
		eq(`SELECT id FROM ` + o + ` WHERE placed_at IN ('2026-03-02 10:00:00+02', '2026-03-03 12:00:00+02')`),
		eq(`SELECT id FROM ` + o + ` WHERE placed_at IN (TIMESTAMP '2026-03-02 10:00:00+02')`),
		eq(`SELECT TIMESTAMP '2026-03-02 10:00:00+02' AS t, TIMESTAMP '2026-03-02 23:30:00.25-05:30' AS u`),
		eq(`SELECT TIME '10:00:00+02' AS t`),
		eq(`SELECT name FROM ` + c + ` WHERE signup = DATE '2026-01-15 23:30:00-05'`),
		eq(`SELECT name FROM ` + c + ` WHERE signup = CAST('2026-01-15 23:30:00-05' AS date)`),
		eq(`SELECT name FROM ` + c + ` WHERE signup = '2026-01-15 23:30:00-05'::date`),
		eq(`SELECT name FROM ` + c + ` WHERE signup = '2026-01-15 23:30:00-05'`),
		eq(`SELECT DATE '2026-01-15 23:30:00-05' AS d, DATE '2026-01-15 01:30:00+05' AS e`),
		// A spelling the copy refuses: the source answers.
		refused("invalid timestamp field format", `SELECT id FROM `+o+` WHERE placed_at = TIMESTAMP '2026-03-02 10:00+02'`),
		refused("not UTC", `SELECT id FROM `+o+` WHERE placed_at = TIMESTAMP '2026-03-02 10:00:00 Europe/Paris'`),
		refused("not UTC", `SELECT id FROM `+o+` WHERE placed_at = TIMESTAMP '2026-03-02 10:00:00 +02'`),
		// An offset PostgreSQL refuses (time zone displacement out of range)
		// and the copy takes.
		{stmt: `SELECT id FROM ` + o + ` WHERE placed_at = TIMESTAMP '2026-03-02 10:00:00+16'`, verdict: sqlcompare.SourceError},
		// A zone in a bound of a BETWEEN.
		eq(`SELECT id FROM ` + o + ` WHERE placed_at BETWEEN '2026-03-02 10:00:00' AND '2026-03-03 10:00:00+02'`),
		eq(`SELECT id FROM ` + o + ` WHERE placed_at BETWEEN '2026-03-02 12:00:00+02' AND '2026-03-03 10:00:00'`),
		eq(`SELECT id FROM ` + o + ` WHERE placed_at NOT BETWEEN '2026-01-01 00:00:00' AND '2026-03-03 10:00:00+02'`),
		eq(`SELECT id FROM ` + o + ` WHERE placed_at BETWEEN TIMESTAMP '2026-03-02 10:00:00' AND TIMESTAMP '2026-03-03 10:00:00+02'`),
		eq(`SELECT id FROM ` + o + ` WHERE (placed_at BETWEEN '2026-03-02 10:00:00' AND '2026-03-03 10:00:00+02') AND qty BETWEEN 1 AND 100`),
		eq(`SELECT name FROM ` + c + ` WHERE signup BETWEEN '2025-12-01' AND '2026-01-15 23:30:00-05'`),
		eq(`SELECT id FROM ` + o + ` WHERE seen_at BETWEEN '2026-03-02 10:00:00+00' AND '2026-03-03 12:00:00+02'`),
		refused("not UTC", `SELECT id FROM `+o+` WHERE placed_at BETWEEN '2026-03-02 10:00:00' AND '2026-03-03 10:00:00 Europe/Paris'`),
		{stmt: `SELECT id FROM ` + o + ` WHERE placed_at BETWEEN '2026-03-02 10:00:00' AND '2026-03-03 10:00:00+16'`, verdict: sqlcompare.SourceError},
		// Text in the upper bound is compared by the source's collation (b
		// sorts before Carla there, after it byte by byte), and PostgreSQL
		// refuses a number between a number and text.
		{stmt: `SELECT name FROM ` + c + ` WHERE 'b' BETWEEN 'a' AND name`, verdict: sqlcompare.Different, kind: "rows", why: "#3024: text in the upper bound of a BETWEEN, compared by collation"},
		{stmt: `SELECT id FROM ` + o + ` WHERE qty BETWEEN 1 AND note`, verdict: sqlcompare.SourceError},
		{stmt: `SELECT id FROM ` + o + ` WHERE 5 BETWEEN 1 AND note`, verdict: sqlcompare.SourceError},
		// An operator that ends in + or - is cut before it, by PostgreSQL's
		// scanner and by the copy's: =- is = and -, and !=- is one operator
		// neither has.
		eq(`SELECT id FROM ` + o + ` WHERE qty =-5`),
		eq(`SELECT id, qty+-1, qty*-2 FROM ` + o + ` WHERE qty <>-5 AND qty >=-5`),
		{stmt: `SELECT id FROM ` + o + ` WHERE qty !=-5`, verdict: sqlcompare.SourceError},
	}
}
