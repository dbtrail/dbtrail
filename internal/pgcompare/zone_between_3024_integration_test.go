//go:build integration

package pgcompare

import "github.com/dbtrail/dbtrail/internal/sqlcompare"

// zoneAndBetween are the statements of #3024, measured on PostgreSQL 16 and
// on the copy: a time zone written in a literal that PostgreSQL does not
// read (timestamp, date and time have none), and both bounds of a BETWEEN.
// checkRouting holds the port's screen to each verdict.
func zoneAndBetween() []expectation {
	o, c := itOrders, itCustomers
	eq := func(stmt string) expectation { return expectation{stmt: stmt, verdict: sqlcompare.Equal} }
	differs := func(stmt string) expectation {
		return expectation{stmt: stmt, verdict: sqlcompare.Different, kind: "rows", why: "#3024: PostgreSQL does not read the zone, the copy applies it"}
	}
	return []expectation{
		// No zone: both bounds of a BETWEEN, strings and typed literals.
		eq(`SELECT id FROM ` + o + ` WHERE placed_at BETWEEN '2026-03-02 10:00:00' AND '2026-03-03 10:00:00'`),
		eq(`SELECT id FROM ` + o + ` WHERE placed_at BETWEEN TIMESTAMP '2026-03-02 10:00:00' AND TIMESTAMP '2026-03-03 10:00:00'`),
		eq(`SELECT id FROM ` + o + ` WHERE placed_at NOT BETWEEN TIMESTAMP '2026-01-01 00:00:00' AND TIMESTAMP '2026-03-03 10:00:00'`),
		eq(`SELECT id FROM ` + o + ` WHERE qty NOT BETWEEN 3 AND 10`),
		eq(`SELECT name FROM ` + c + ` WHERE signup BETWEEN DATE '2025-12-01' AND DATE '2026-01-15'`),
		// A typed literal with a zone.
		differs(`SELECT id FROM ` + o + ` WHERE placed_at = TIMESTAMP '2026-03-02 10:00:00+02'`),
		differs(`SELECT id FROM ` + o + ` WHERE placed_at = TIMESTAMP '2026-03-02 12:00:00+02'`),
		differs(`SELECT id FROM ` + o + ` WHERE placed_at = TIMESTAMP '2026-03-02 10:00+02'`),
		differs(`SELECT id FROM ` + o + ` WHERE placed_at = TIMESTAMP '2026-03-02 10:00:00 Europe/Paris'`),
		differs(`SELECT id FROM ` + o + ` WHERE placed_at = TIMESTAMP '2026-03-02 10:00:00Z'`),
		differs(`SELECT id FROM ` + o + ` WHERE placed_at = '2026-03-02 10:00:00+02'::timestamp`),
		differs(`SELECT id FROM ` + o + ` WHERE placed_at = CAST('2026-03-02 10:00:00+02' AS timestamp)`),
		differs(`SELECT id FROM ` + o + ` WHERE placed_at = '2026-03-02 10:00:00+02'`),
		differs(`SELECT id FROM ` + o + ` WHERE placed_at IN ('2026-03-02 10:00:00+02', '2026-03-03 12:00:00+02')`),
		differs(`SELECT id FROM ` + o + ` WHERE placed_at IN (TIMESTAMP '2026-03-02 10:00:00+02')`),
		differs(`SELECT TIMESTAMP '2026-03-02 10:00:00+02' AS t`),
		differs(`SELECT TIME '10:00:00+02' AS t`),
		differs(`SELECT name FROM ` + c + ` WHERE signup = DATE '2026-01-15 23:30:00-05'`),
		differs(`SELECT name FROM ` + c + ` WHERE signup = CAST('2026-01-15 23:30:00-05' AS date)`),
		differs(`SELECT name FROM ` + c + ` WHERE signup = '2026-01-15 23:30:00-05'::date`),
		differs(`SELECT name FROM ` + c + ` WHERE signup = '2026-01-15 23:30:00-05'`),
		differs(`SELECT DATE '2026-01-15 23:30:00-05' AS d, DATE '2026-01-15 01:30:00+05' AS e`),
		// A zone in a bound of a BETWEEN.
		differs(`SELECT id FROM ` + o + ` WHERE placed_at BETWEEN '2026-03-02 10:00:00' AND '2026-03-03 10:00:00+02'`),
		differs(`SELECT id FROM ` + o + ` WHERE placed_at BETWEEN '2026-03-02 12:00:00+02' AND '2026-03-03 10:00:00'`),
		differs(`SELECT id FROM ` + o + ` WHERE placed_at NOT BETWEEN '2026-01-01 00:00:00' AND '2026-03-03 10:00:00+02'`),
		differs(`SELECT id FROM ` + o + ` WHERE placed_at BETWEEN TIMESTAMP '2026-03-02 10:00:00' AND TIMESTAMP '2026-03-03 10:00:00+02'`),
		differs(`SELECT id FROM ` + o + ` WHERE placed_at BETWEEN '2026-03-02 10:00:00' AND '2026-03-03 10:00:00 Europe/Paris'`),
		differs(`SELECT id FROM ` + o + ` WHERE (placed_at BETWEEN '2026-03-02 10:00:00' AND '2026-03-03 10:00:00+02') AND qty BETWEEN 1 AND 100`),
		differs(`SELECT name FROM ` + c + ` WHERE signup BETWEEN '2025-12-01' AND '2026-01-15 23:30:00-05'`),
		differs(`SELECT id FROM ` + o + ` WHERE seen_at BETWEEN '2026-03-02 10:00:00+00' AND '2026-03-03 12:00:00+02'`),
		// The upper bound is text: PostgreSQL refuses a number between a
		// number and text.
		{stmt: `SELECT id FROM ` + o + ` WHERE qty BETWEEN 1 AND note`, verdict: sqlcompare.SourceError},
		{stmt: `SELECT id FROM ` + o + ` WHERE 5 BETWEEN 1 AND note`, verdict: sqlcompare.SourceError},
		// An operator that ends in + or - is cut before it, as PostgreSQL's
		// scanner cuts it: =- is = and -.
		eq(`SELECT id FROM ` + o + ` WHERE qty =-5`),
		eq(`SELECT id, qty+-1, qty*-2 FROM ` + o + ` WHERE qty <>-5 AND qty >=-5`),
		{stmt: `SELECT id FROM ` + o + ` WHERE qty !=-5`, verdict: sqlcompare.SourceError},
	}
}
