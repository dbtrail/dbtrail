package sqlsandbox

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// The copy's ROUND of a numeric division and of a numeric AVG for a
// PostgreSQL source (pgRoundMacros), as the PostgreSQL port writes them.
// Every expected text is PostgreSQL's: round(numeric, k) has exactly k
// decimals and rounds half away from zero. internal/pgcompare reads the same
// statements from PostgreSQL itself.
func TestPGRoundMacros(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	cases := []struct {
		name, expr, want, typ string
	}{
		{"exact", "_bintrail_pg_round_div(CAST(10.50 AS DECIMAL(12,2)), 3, 2, 0.01)", "3.50", "DECIMAL(38,2)"},
		{"a third", "_bintrail_pg_round_div(100.0 * 1, 3, 2, 0.01)", "33.33", "DECIMAL(38,2)"},
		{"two thirds", "_bintrail_pg_round_div(100.0 * 2, 3, 2, 0.01)", "66.67", "DECIMAL(38,2)"},
		{"a half rounds away from zero", "_bintrail_pg_round_div(CAST(2.675 AS DECIMAL(4,3)), 1, 2, 0.01)", "2.68", "DECIMAL(38,2)"},
		{"a negative half too", "_bintrail_pg_round_div(CAST(-2.675 AS DECIMAL(4,3)), 1, 2, 0.01)", "-2.68", "DECIMAL(38,2)"},
		{"just under a half", "_bintrail_pg_round_div(CAST(2.6749 AS DECIMAL(5,4)), 1, 2, 0.01)", "2.67", "DECIMAL(38,2)"},
		{"a negative divisor", "_bintrail_pg_round_div(CAST(1.00 AS DECIMAL(3,2)), -3, 2, 0.01)", "-0.33", "DECIMAL(38,2)"},
		{"the decimals PostgreSQL writes", "_bintrail_pg_round_div(CAST(5.00 AS DECIMAL(12,2)), 2, 3, 0.001)", "2.500", "DECIMAL(38,3)"},
		{"no decimals", "_bintrail_pg_round_div(CAST(7.5 AS DECIMAL(2,1)), 1, 0, 1)", "8", "DECIMAL(38,0)"},
		{"no decimals, negative", "_bintrail_pg_round_div(CAST(-7.5 AS DECIMAL(2,1)), 1, 0, 1)", "-8", "DECIMAL(38,0)"},
		{"zero over a numeric", "_bintrail_pg_round_div(CAST(0 AS DECIMAL(4,4)), 7, 2, 0.01)", "0.00", "DECIMAL(38,2)"},
		{"a numeric divisor", "_bintrail_pg_round_div(CAST(10 AS INTEGER), CAST(0.3 AS DECIMAL(2,1)), 4, 0.0001)", "33.3333", "DECIMAL(38,4)"},
		{"a NULL side", "_bintrail_pg_round_div(CAST(NULL AS DECIMAL(4,1)), 3, 2, 0.01)", "<nil>", "DECIMAL(38,2)"},
		{"NULLIF of zero", "_bintrail_pg_round_div(1.0, nullif(0, 0), 2, 0.01)", "<nil>", "DECIMAL(38,2)"},
		{"far from a half, many decimals", "_bintrail_pg_round_div(CAST(0.0049 AS DECIMAL(5,4)), 1, 2, 0.01)", "0.00", "DECIMAL(38,2)"},
		{"avg of whole numbers", "(SELECT _bintrail_pg_round_avg(x, 2, 0.01) FROM (VALUES (CAST(1 AS SMALLINT)), (2), (4)) v(x))", "2.33", "DECIMAL(38,2)"},
		{"100 times an avg of numerics", "(SELECT _bintrail_pg_round_avg(x, 4, 0.01) FROM (VALUES (CAST(0.1234 AS DECIMAL(4,4))), (0.0500)) v(x))", "8.67", "DECIMAL(38,2)"},
		{"avg, negative, no decimals", "(SELECT _bintrail_pg_round_avg(x, 0, 1) FROM (VALUES (-1), (-2)) v(x))", "-2", "DECIMAL(38,0)"},
		{"avg leaves NULLs out", "(SELECT _bintrail_pg_round_avg(x, 1, 0.1) FROM (VALUES (1), (NULL), (2)) v(x))", "1.5", "DECIMAL(38,1)"},
		{"avg of no row", "(SELECT _bintrail_pg_round_avg(x, 2, 0.01) FROM (VALUES (1)) v(x) WHERE x > 1)", "<nil>", "DECIMAL(38,2)"},
		// floor(a / b): the whole number below, a numeric with no decimals.
		{"floor", "_bintrail_pg_floor_div(CAST(10.50 AS DECIMAL(12,2)), 3)", "3", "DECIMAL(38,0)"},
		{"floor, negative", "_bintrail_pg_floor_div(CAST(-10.50 AS DECIMAL(12,2)), 3)", "-4", "DECIMAL(38,0)"},
		{"floor, negative divisor", "_bintrail_pg_floor_div(CAST(10.50 AS DECIMAL(12,2)), -3)", "-4", "DECIMAL(38,0)"},
		{"floor of a whole quotient", "_bintrail_pg_floor_div(CAST(5.00 AS DECIMAL(3,2)), 5)", "1", "DECIMAL(38,0)"},
		{"floor of a whole negative quotient", "_bintrail_pg_floor_div(CAST(-5.00 AS DECIMAL(3,2)), 5)", "-1", "DECIMAL(38,0)"},
		{"floor just under a whole number", "_bintrail_pg_floor_div(CAST(9.99 AS DECIMAL(3,2)), 1)", "9", "DECIMAL(38,0)"},
		{"floor of zero", "_bintrail_pg_floor_div(CAST(0.00 AS DECIMAL(3,2)), 7)", "0", "DECIMAL(38,0)"},
		{"floor of a NULL side", "_bintrail_pg_floor_div(CAST(NULL AS DECIMAL(3,2)), 7)", "<nil>", "DECIMAL(38,0)"},
		// extract(epoch from x): six decimals, as PostgreSQL 14 and later.
		{"epoch of a timestamptz", "_bintrail_pg_epoch(CAST('2026-11-18 10:00:00.5+00' AS TIMESTAMPTZ))", "1794996000.500000", "DECIMAL(38,6)"},
		{"epoch of a timestamp", "_bintrail_pg_epoch(CAST('2026-11-18 10:00:00' AS TIMESTAMP))", "1794996000.000000", "DECIMAL(38,6)"},
		{"epoch before 1970", "_bintrail_pg_epoch(CAST('1969-12-31 23:59:59.5' AS TIMESTAMP))", "-0.500000", "DECIMAL(38,6)"},
		{"epoch of NULL", "(SELECT _bintrail_pg_epoch(x) FROM (VALUES (CAST(NULL AS TIMESTAMP))) v(x))", "<nil>", "DECIMAL(38,6)"},
		// Ten-minute buckets, as the HTAP bench's reports write them.
		{"a ten-minute bucket", "CAST(_bintrail_pg_to_timestamp(_bintrail_pg_floor_div(_bintrail_pg_epoch(CAST('2026-11-18 10:07:30.25+00' AS TIMESTAMPTZ)), 600) * 600) AS VARCHAR)", "2026-11-18 10:00:00+00", "VARCHAR"},
		{"a bucket before 1970", "CAST(_bintrail_pg_to_timestamp(_bintrail_pg_floor_div(_bintrail_pg_epoch(CAST('1969-12-31 23:55:00+00' AS TIMESTAMPTZ)), 600) * 600) AS VARCHAR)", "1969-12-31 23:50:00+00", "VARCHAR"},
		// The same buckets as the port writes them since: from microseconds.
		{"a ten-minute bucket from microseconds", "CAST(_bintrail_pg_to_timestamp(_bintrail_pg_floor_epoch(CAST('2026-11-18 10:07:30.25+00' AS TIMESTAMPTZ), 600) * 600) AS VARCHAR)", "2026-11-18 10:00:00+00", "VARCHAR"},
		{"a bucket before 1970 from microseconds", "CAST(_bintrail_pg_to_timestamp(_bintrail_pg_floor_epoch(CAST('1969-12-31 23:55:00+00' AS TIMESTAMPTZ), 600) * 600) AS VARCHAR)", "1969-12-31 23:50:00+00", "VARCHAR"},
	}
	items := make([]string, len(cases))
	for i, c := range cases {
		items[i] = fmt.Sprintf("%s AS c%d", c.expr, i)
	}
	job := f.job("SELECT " + strings.Join(items, ", "))
	job.Session = Session{PostgresRules: true}
	res, err := r.Run(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 || len(res.Rows[0]) != len(cases) {
		t.Fatalf("%d rows, want one of %d cells", len(res.Rows), len(cases))
	}
	for i, c := range cases {
		if got := fmt.Sprint(res.Rows[0][i]); got != c.want {
			t.Errorf("%s: %s = %s, want %s", c.name, c.expr, got, c.want)
		}
		if typ := res.Columns[i].Type; typ != c.typ {
			t.Errorf("%s: typed %s, want %s", c.name, typ, c.typ)
		}
	}

	// What PostgreSQL would answer otherwise, or not at all, fails the
	// statement, which a forwarding account then sends to PostgreSQL.
	for _, c := range []struct{ name, expr, says string }{
		{"a zero divisor", "_bintrail_pg_round_div(1.0, 0, 2, 0.01)", "division by zero"},
		{"two whole numbers", "_bintrail_pg_round_div(7, 2, 2, 0.01)", "both sides of the division are whole numbers"},
		{"a float", "_bintrail_pg_round_div(CAST(1.5 AS DOUBLE), 2, 2, 0.01)", "not a numeric or a whole number"},
		{"1e2, a DOUBLE here", "_bintrail_pg_round_div(1e2, 3, 2, 0.01)", "not a numeric or a whole number"},
		{"avg of a float", "(SELECT _bintrail_pg_round_avg(x, 2, 0.01) FROM (VALUES (CAST(1.5 AS DOUBLE))) v(x))", "not a numeric or a whole number"},
		// 0.004999999999999999: PostgreSQL's first rounding could reach the half.
		{"just under a half, past PostgreSQL's scale", "_bintrail_pg_round_div(CAST(0.004999999999999999 AS DECIMAL(18,18)), 1, 2, 0.01)", "too close to a half"},
		// A quotient of 16 whole digits: PostgreSQL's scale does not reach past k.
		{"a quotient too large", "_bintrail_pg_round_div(CAST(1000000000000000.0 AS DECIMAL(17,1)), 1, 2, 0.01)", "too close to a half"},
		{"floor by zero", "_bintrail_pg_floor_div(1.0, 0)", "division by zero"},
		{"floor of two whole numbers", "_bintrail_pg_floor_div(7, 2)", "both sides of the division are whole numbers"},
		{"floor of a float", "_bintrail_pg_floor_div(CAST(1.5 AS DOUBLE), 2)", "not a numeric or a whole number"},
		// PostgreSQL's first rounding could reach 3, or -2.
		{"floor just under a whole number, past PostgreSQL's scale", "_bintrail_pg_floor_div(CAST(2.99999999999999999 AS DECIMAL(18,17)), 1)", "too close to a whole number"},
		{"floor just over a negative whole number", "_bintrail_pg_floor_div(CAST(-2.00000000000000001 AS DECIMAL(18,17)), 1)", "too close to a whole number"},
		{"epoch of a date", "_bintrail_pg_epoch(CAST('2026-11-18' AS DATE))", "not a timestamp"},
		{"epoch of an interval", "_bintrail_pg_epoch(INTERVAL 1 DAY)", "not a timestamp"},
		{"to_timestamp of a fraction", "_bintrail_pg_to_timestamp(1.5)", "not a whole number of seconds"},
		{"to_timestamp of a float", "_bintrail_pg_to_timestamp(CAST(15 AS DOUBLE))", "not a whole number of seconds"},
		{"too large for 128 bits", "_bintrail_pg_round_div(CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0)), CAST(0.3 AS DECIMAL(2,1)), 2, 0.01)", "Overflow"},
	} {
		job := f.job("SELECT " + c.expr)
		job.Session = Session{PostgresRules: true}
		res, err := r.Run(context.Background(), job)
		if err == nil || !strings.Contains(err.Error(), c.says) {
			t.Errorf("%s: %s gave %v, err %v; want an error that says %q", c.name, c.expr, res.Rows, err, c.says)
		}
	}

	// Grouped, as a report writes it, and headed round as PostgreSQL heads
	// an unnamed round(...).
	job = f.job("SELECT g, _bintrail_pg_round_div(100.0 * sum(x), count(*), 2, 0.01), _bintrail_pg_round_avg(x, 1, 0.1), _bintrail_pg_floor_div(1.0 * g, 2) FROM (VALUES (1, 1), (1, 2), (2, 5)) v(g, x) GROUP BY g ORDER BY g")
	job.Session = Session{PostgresRules: true, PostgresNames: true}
	res, err = r.Run(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(res.Rows); got != "[[1 150.00 1.5 0] [2 500.00 5.0 1]]" {
		t.Errorf("grouped: %s", got)
	}
	if len(res.Columns) != 4 || res.Columns[1].Name != "round" || res.Columns[2].Name != "round" || res.Columns[3].Name != "floor" {
		t.Errorf("headed %v, want round, round and floor", res.Columns)
	}

	// A session for MySQL has none of them.
	job = f.job("SELECT _bintrail_pg_round_div(1.0, 3, 2, 0.01)")
	if _, err := r.Run(context.Background(), job); err == nil {
		t.Error("a session without PostgresRules has the PostgreSQL ROUND")
	}
}
