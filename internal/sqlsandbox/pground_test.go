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
		// round(x, k) of a numeric or a whole number: exactly k decimals, as
		// PostgreSQL 16 writes them (measured: round(500.00, 2) is 500.00,
		// round(500, 2) is 500.00, round(12.50, 3) is 12.500).
		{"a whole value keeps its decimals", "_bintrail_pg_round_k(CAST(500 AS DECIMAL(10,2)), 2, 0.01)", "500.00", "DECIMAL(38,2)"},
		{"a whole number gets them", "_bintrail_pg_round_k(CAST(500 AS INTEGER), 2, 0.01)", "500.00", "DECIMAL(38,2)"},
		{"zero", "_bintrail_pg_round_k(0, 2, 0.01)", "0.00", "DECIMAL(38,2)"},
		{"zero with one decimal", "_bintrail_pg_round_k(CAST(0.0 AS DECIMAL(2,1)), 2, 0.01)", "0.00", "DECIMAL(38,2)"},
		{"more decimals than the column", "_bintrail_pg_round_k(CAST(12.50 AS DECIMAL(10,2)), 3, 0.001)", "12.500", "DECIMAL(38,3)"},
		{"fewer, half away from zero", "_bintrail_pg_round_k(CAST(-12.345 AS DECIMAL(5,3)), 2, 0.01)", "-12.35", "DECIMAL(38,2)"},
		{"a negative that rounds to zero", "_bintrail_pg_round_k(CAST(-0.004 AS DECIMAL(4,3)), 2, 0.01)", "0.00", "DECIMAL(38,2)"},
		{"no decimals", "_bintrail_pg_round_k(CAST(12.5 AS DECIMAL(3,1)), 0, 1)", "13", "DECIMAL(38,0)"},
		{"a sum's 38 digits", "_bintrail_pg_round_k(CAST(1234567890123456789012345678901234.67 AS DECIMAL(38,2)), 3, 0.001)", "1234567890123456789012345678901234.670", "DECIMAL(38,3)"},
		{"a bigint's sum, a numeric there", "_bintrail_pg_round_k(CAST(39 AS HUGEINT), 2, 0.01)", "39.00", "DECIMAL(38,2)"},
		{"NULL", "(SELECT _bintrail_pg_round_k(x, 2, 0.01) FROM (VALUES (CAST(NULL AS DECIMAL(10,2)))) v(x))", "<nil>", "DECIMAL(38,2)"},
		// A negative k: to tens, hundreds... with no decimals. The engine's
		// own round answers -5.0 to tens as 0 and overflows a bigint; the
		// macro answers as PostgreSQL does.
		{"to tens", "_bintrail_pg_round_neg(15, -1)", "20", "DECIMAL(38,0)"},
		{"to tens, negative", "_bintrail_pg_round_neg(-15, -1)", "-20", "DECIMAL(38,0)"},
		{"to tens, negative, odd", "_bintrail_pg_round_neg(-25, -1)", "-30", "DECIMAL(38,0)"},
		{"to tens, under the half", "_bintrail_pg_round_neg(CAST(14.99 AS DECIMAL(4,2)), -1)", "10", "DECIMAL(38,0)"},
		{"a half with a decimal", "_bintrail_pg_round_neg(CAST(-5.0 AS DECIMAL(2,1)), -1)", "-10", "DECIMAL(38,0)"},
		{"a half and a bit", "_bintrail_pg_round_neg(CAST(-15.5 AS DECIMAL(3,1)), -1)", "-20", "DECIMAL(38,0)"},
		{"just under a half", "_bintrail_pg_round_neg(CAST(-4.0 AS DECIMAL(2,1)), -1)", "0", "DECIMAL(38,0)"},
		{"the largest bigint", "_bintrail_pg_round_neg(CAST(9223372036854775807 AS BIGINT), -1)", "9223372036854775810", "DECIMAL(38,0)"},
		{"past every digit", "_bintrail_pg_round_neg(CAST(1234.5 AS DECIMAL(5,1)), -5)", "0", "DECIMAL(38,0)"},
		// round(x) of a numeric: no decimals, half away from zero.
		{"round of a numeric", "_bintrail_pg_round_whole(CAST(12.5 AS DECIMAL(3,1)))", "13", "DECIMAL(3,0)"},
		{"round of a negative numeric", "_bintrail_pg_round_whole(CAST(-12.5 AS DECIMAL(3,1)))", "-13", "DECIMAL(3,0)"},
		{"round of a small negative", "_bintrail_pg_round_whole(CAST(-0.4 AS DECIMAL(2,1)))", "0", "DECIMAL(2,0)"},
		{"round of a negative half", "_bintrail_pg_round_whole(CAST(-0.5 AS DECIMAL(2,1)))", "-1", "DECIMAL(2,0)"},
		// A CASE's values, each with the copy's one type: a value whose own
		// decimals are the type's is answered.
		{"a CASE value with the type's decimals", "_bintrail_pg_unpack(CASE WHEN 1 = 1 THEN _bintrail_pg_pack(CAST(1.50 AS DECIMAL(3,2))) ELSE _bintrail_pg_pack(0) END)", "1.50", "DECIMAL(12,2)"},
		{"a CASE value of NULL", "_bintrail_pg_unpack(CASE WHEN 1 = 1 THEN _bintrail_pg_pack(CAST(NULL AS DECIMAL(3,2))) ELSE _bintrail_pg_pack(0) END)", "<nil>", "DECIMAL(12,2)"},
		{"a CASE with no ELSE", "(SELECT _bintrail_pg_unpack(CASE WHEN x > 2 THEN _bintrail_pg_pack(x) END) FROM (VALUES (CAST(1.50 AS DECIMAL(3,2)))) v(x))", "<nil>", "DECIMAL(3,2)"},
		{"a COALESCE of the first", "_bintrail_pg_unpack(coalesce(_bintrail_pg_packn(CAST(1.50 AS DECIMAL(3,2))), _bintrail_pg_packn(0)))", "1.50", "DECIMAL(12,2)"},
		{"a COALESCE of whole numbers", "_bintrail_pg_unpack(coalesce(_bintrail_pg_packn(CAST(NULL AS DECIMAL(3,0))), _bintrail_pg_packn(7)))", "7", "DECIMAL(10,0)"},
		{"a CASE of floats", "_bintrail_pg_unpack(CASE WHEN 1 = 2 THEN _bintrail_pg_pack(CAST(1.50 AS DECIMAL(3,2))) ELSE _bintrail_pg_pack(CAST(2 AS DOUBLE)) END)", "2", "DOUBLE"},
		// avg of a float stays the engine's.
		{"avg of floats", "(SELECT _bintrail_pg_avg(x) FROM (VALUES (CAST(0.5 AS DOUBLE)), (0.25)) v(x))", "0.375", "DOUBLE"},
		// A division on its own (#3219): the exact quotient rounded once to 16
		// decimals. PostgreSQL writes 0.25000000000000000000,
		// 3.7500000000000000, 0.33333333333333333333, 0.66666666666666666667,
		// 0.10000000000000000000, 41152.263374485597 and
		// 0.00000300000000000000.
		{"numeric over numeric", "_bintrail_pg_div(CAST(1.00 AS DECIMAL(3,2)), CAST(4.00 AS DECIMAL(3,2)))", "0.2500000000000000", "DECIMAL(38,16)"},
		{"numeric over a whole number", "_bintrail_pg_div(CAST(7.50 AS DECIMAL(3,2)), 2)", "3.7500000000000000", "DECIMAL(38,16)"},
		{"a whole number over a numeric", "_bintrail_pg_div(1, CAST(3.0 AS DECIMAL(2,1)))", "0.3333333333333333", "DECIMAL(38,16)"},
		{"rounded up at the last decimal", "_bintrail_pg_div(2, CAST(3.0 AS DECIMAL(2,1)))", "0.6666666666666667", "DECIMAL(38,16)"},
		{"rounded away from zero", "_bintrail_pg_div(-2, CAST(3.0 AS DECIMAL(2,1)))", "-0.6666666666666667", "DECIMAL(38,16)"},
		{"a quotient that ends", "_bintrail_pg_div(CAST(0.3 AS DECIMAL(2,1)), 3)", "0.1000000000000000", "DECIMAL(38,16)"},
		{"a negative quotient", "_bintrail_pg_div(CAST(-7.50 AS DECIMAL(3,2)), 2)", "-3.7500000000000000", "DECIMAL(38,16)"},
		{"two negatives", "_bintrail_pg_div(CAST(-7.50 AS DECIMAL(3,2)), -2)", "3.7500000000000000", "DECIMAL(38,16)"},
		{"a large quotient", "_bintrail_pg_div(CAST(1000000.00 AS DECIMAL(12,2)), CAST(24.3 AS DECIMAL(3,1)))", "41152.2633744855967078", "DECIMAL(38,16)"},
		{"a small quotient", "_bintrail_pg_div(CAST(0.003 AS DECIMAL(4,3)), 1000)", "0.0000030000000000", "DECIMAL(38,16)"},
		// Past 2^53 without the point, which a double precision would not
		// hold, and a divisor of 18 decimals.
		{"a decimal past 2^53", "_bintrail_pg_div(CAST(12345678901234567.89 AS DECIMAL(20,2)), 1)", "12345678901234567.8900000000000000", "DECIMAL(38,16)"},
		{"a third of a decimal past 2^53", "_bintrail_pg_div(CAST(123456789012345678.91 AS DECIMAL(20,2)), 3)", "41152263004115226.3033333333333333", "DECIMAL(38,16)"},
		{"a long literal divisor", "_bintrail_pg_div(CAST(9.50 AS DECIMAL(12,2)), 3.123456789012345678)", "3.0415019773665422", "DECIMAL(38,16)"},
		{"the smallest quotient answered", "_bintrail_pg_div(CAST(1.000 AS DECIMAL(4,3)), 1000000)", "0.0000010000000000", "DECIMAL(38,16)"},
		{"zero over a numeric", "_bintrail_pg_div(CAST(0.00 AS DECIMAL(3,2)), 4)", "0.0000000000000000", "DECIMAL(38,16)"},
		{"a bigint over a numeric", "_bintrail_pg_div(CAST(9007199254740993 AS BIGINT), CAST(2.0 AS DECIMAL(2,1)))", "4503599627370496.5000000000000000", "DECIMAL(38,16)"},
		{"a numeric of 18 decimals", "_bintrail_pg_div(CAST(1.000000000000000001 AS DECIMAL(38,18)), 3)", "0.3333333333333333", "DECIMAL(38,16)"},
		{"NULL over zero", "_bintrail_pg_div(CAST(NULL AS DECIMAL(3,2)), 0)", "<nil>", "DECIMAL(38,16)"},
		{"over NULL", "_bintrail_pg_div(CAST(1.00 AS DECIMAL(3,2)), CAST(NULL AS INTEGER))", "<nil>", "DECIMAL(38,16)"},
		{"a sum over a count", "(SELECT _bintrail_pg_div(sum(x), count(*)) FROM (VALUES (CAST(5.00 AS DECIMAL(3,2))), (10.00)) v(x))", "7.5000000000000000", "DECIMAL(38,16)"},
		{"a chain of two", "_bintrail_pg_div(_bintrail_pg_div(CAST(9.00 AS DECIMAL(3,2)), 2), 3)", "1.5000000000000000", "DECIMAL(38,16)"},
		// avg of numerics and of whole numbers (#3219): the exact sum over the
		// count, on the same terms. PostgreSQL writes 7.5000000000000000,
		// 0.50000000000000000000, 3.3333333333333333 and -2.5000000000000000.
		{"avg of numerics", "(SELECT _bintrail_pg_avgn(x) FROM (VALUES (CAST(5.00 AS DECIMAL(3,2))), (10.00)) v(x))", "7.5000000000000000", "DECIMAL(38,16)"},
		{"avg of whole numbers", "(SELECT _bintrail_pg_avgn(x) FROM (VALUES (0), (1)) v(x))", "0.5000000000000000", "DECIMAL(38,16)"},
		{"avg of bigints", "(SELECT _bintrail_pg_avgn(x) FROM (VALUES (CAST(9007199254740993 AS BIGINT)), (9007199254740994)) v(x))", "9007199254740993.5000000000000000", "DECIMAL(38,16)"},
		{"avg that does not end", "(SELECT _bintrail_pg_avgn(x) FROM (VALUES (CAST(3.00 AS DECIMAL(3,2))), (3.00), (4.00)) v(x))", "3.3333333333333333", "DECIMAL(38,16)"},
		{"avg of negatives", "(SELECT _bintrail_pg_avgn(x) FROM (VALUES (CAST(-2.50 AS DECIMAL(3,2))), (-2.50)) v(x))", "-2.5000000000000000", "DECIMAL(38,16)"},
		{"avg of NULLs", "(SELECT _bintrail_pg_avgn(x) FROM (VALUES (CAST(NULL AS DECIMAL(3,2))), (NULL)) v(x))", "<nil>", "DECIMAL(38,16)"},
		{"avg skips NULL", "(SELECT _bintrail_pg_avgn(x) FROM (VALUES (CAST(NULL AS DECIMAL(3,2))), (4.00)) v(x))", "4.0000000000000000", "DECIMAL(38,16)"},
		{"avg of no rows", "(SELECT _bintrail_pg_avgn(x) FROM (VALUES (CAST(1.00 AS DECIMAL(3,2)))) v(x) WHERE x > 5)", "<nil>", "DECIMAL(38,16)"},
		{"avg of a division", "(SELECT _bintrail_pg_avgn(_bintrail_pg_div(x, 2)) FROM (VALUES (CAST(3.00 AS DECIMAL(3,2))), (5.00)) v(x))", "2.0000000000000000", "DECIMAL(38,16)"},
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
		// round(double precision, k) does not exist on PostgreSQL, and its
		// round(double precision) rounds a half to even; round(integer) is a
		// double precision there (9.007199254740992e+15 for 2^53 + 1).
		{"round to k of a float", "_bintrail_pg_round_k(CAST(2.5 AS DOUBLE), 2, 0.01)", "double precision"},
		{"round to tens of a float", "_bintrail_pg_round_neg(CAST(25 AS DOUBLE), -1)", "double precision"},
		{"round of a float", "_bintrail_pg_round_whole(CAST(2.5 AS DOUBLE))", "double precision"},
		{"round of a whole number", "_bintrail_pg_round_whole(5)", "whole number"},
		{"round of text", "_bintrail_pg_round_k('5', 2, 0.01)", "not a numeric or a whole number"},
		{"more digits than the copy holds", "_bintrail_pg_round_k(CAST(123456789012345678901234567890123456.78 AS DECIMAL(38,2)), 3, 0.001)", "out of range"},
		// A division on its own (#3219).
		{"a zero divisor of a division", "_bintrail_pg_div(1.0, 0)", "division by zero"},
		{"a zero numeric divisor", "_bintrail_pg_div(5, CAST(0.00 AS DECIMAL(3,2)))", "division by zero"},
		{"two whole numbers divided", "_bintrail_pg_div(7, 2)", "both sides of the division are whole numbers"},
		{"a bigint over a smallint", "_bintrail_pg_div(CAST(7 AS BIGINT), CAST(2 AS SMALLINT))", "both sides of the division are whole numbers"},
		{"a float divided", "_bintrail_pg_div(CAST(1.5 AS DOUBLE), 2.0)", "not a numeric or a whole number"},
		{"by a float", "_bintrail_pg_div(2.0, CAST(1.5 AS DOUBLE))", "not a numeric or a whole number"},
		{"text divided", "_bintrail_pg_div('6', 2.0)", "not a numeric or a whole number"},
		// Past 128 bits it fails, and the source answers: never another number.
		{"a numeric of 18 decimals past 128 bits", "_bintrail_pg_div(CAST(200 AS DECIMAL(38,18)), 3)", "Overflow"},
		{"a long literal divisor past 128 bits", "_bintrail_pg_div(CAST(9.50 AS DECIMAL(12,2)), 200.000000000000000000)", "Overflow"},
		{"a quotient under the floor", "_bintrail_pg_div(CAST(0.999 AS DECIMAL(4,3)), 1000000)", "under 0.000001"},
		{"a quotient that would round to zero", "_bintrail_pg_div(CAST(0.0000000001 AS DECIMAL(18,10)), 3000000)", "under 0.000001"},
		{"a negative quotient under the floor", "_bintrail_pg_div(CAST(-0.0000000001 AS DECIMAL(18,10)), 3)", "under 0.000001"},
		{"avg under the floor", "(SELECT _bintrail_pg_avgn(x) FROM (VALUES (CAST(0.0000001 AS DECIMAL(18,10))), (0.0000001)) v(x))", "under 0.000001"},
		{"a quotient past the copy's numeric", "_bintrail_pg_div(CAST(99999999999999999999999.0 AS DECIMAL(24,1)), CAST(0.1 AS DECIMAL(2,1)))", ""},
		{"avgn of floats", "(SELECT _bintrail_pg_avgn(x) FROM (VALUES (CAST(0.5 AS DOUBLE)), (0.25)) v(x))", "not a numeric or a whole number"},
		// avg of a numeric or a whole number: PostgreSQL writes as many
		// decimals as each value needs (500.0000000000000000, 0.50000000000000000000).
		{"avg of whole numbers", "(SELECT _bintrail_pg_avg(x) FROM (VALUES (0), (1)) v(x))", "avg()"},
		{"avg of numerics", "(SELECT _bintrail_pg_avg(x) FROM (VALUES (CAST(5.00 AS DECIMAL(3,2))), (10.00)) v(x))", "avg()"},
		// A CASE value whose own decimals are not the type's: PostgreSQL
		// writes 0 where the copy would write 0.00.
		{"a CASE value with other decimals", "_bintrail_pg_unpack(CASE WHEN 1 = 2 THEN _bintrail_pg_pack(CAST(1.50 AS DECIMAL(3,2))) ELSE _bintrail_pg_pack(0) END)", "different numbers of decimals"},
		{"a COALESCE of the second", "_bintrail_pg_unpack(coalesce(_bintrail_pg_packn(CAST(NULL AS DECIMAL(3,2))), _bintrail_pg_packn(0)))", "different numbers of decimals"},
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

	// Headed as PostgreSQL heads the call each macro stands for: a CASE by
	// its ELSE, or "case"; a COALESCE "coalesce".
	job = f.job(`SELECT _bintrail_pg_round_k(x, 2, 0.01), _bintrail_pg_round_neg(x, -1), _bintrail_pg_round_whole(x),
		_bintrail_pg_avg(y), _bintrail_pg_unpack(CASE WHEN x > 0 THEN _bintrail_pg_pack(x) ELSE _bintrail_pg_pack(x) END),
		_bintrail_pg_unpack(CASE WHEN x > 0 THEN _bintrail_pg_pack(x) ELSE _bintrail_pg_pack(1) END),
		_bintrail_pg_unpack(coalesce(_bintrail_pg_packn(x), _bintrail_pg_packn(x)))
		FROM (VALUES (CAST(1.5 AS DECIMAL(2,1)), CAST(1 AS DOUBLE))) v(x, y) GROUP BY x`)
	job.Session = Session{PostgresRules: true, PostgresNames: true}
	res, err = r.Run(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, c := range res.Columns {
		names = append(names, c.Name)
	}
	if got := strings.Join(names, ","); got != "round,round,round,avg,x,case,coalesce" {
		t.Errorf("headed %s", got)
	}

	// A session for MySQL has none of them.
	for _, macro := range []string{"_bintrail_pg_round_div(1.0, 3, 2, 0.01)", "_bintrail_pg_round_k(1.5, 2, 0.01)", "_bintrail_pg_round_whole(1.5)", "_bintrail_pg_unpack(_bintrail_pg_pack(1))"} {
		job = f.job("SELECT " + macro)
		if _, err := r.Run(context.Background(), job); err == nil {
			t.Errorf("a session without PostgresRules has %s", macro)
		}
	}
}
