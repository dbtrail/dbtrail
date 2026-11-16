package sqlsandbox

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// #2920: the copy's AVG of whole numbers (the routing port writes AVG(x) as
// _bintrail_avg and AVG(DISTINCT x) as _bintrail_avg_distinct) is MySQL's: a
// DECIMAL with four decimals, rounded half away from zero, exact past 2^53.
// Every expected text below was read from MySQL 8.0.46 and MariaDB 10.11.19
// over the same values (both gave the same text, typed NEWDECIMAL with 4
// decimals).
func TestAvgMacros_2920(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	// One statement for every case: a statement each costs minutes under
	// -race.
	cases := []struct {
		name, values, typ, call, want string
	}{
		{"1, 2, 4", "1, 2, 4", "INTEGER", "_bintrail_avg", "2.3333"},
		{"negative", "-1, -2", "INTEGER", "_bintrail_avg", "-1.5000"},
		{"one value", "7", "INTEGER", "_bintrail_avg", "7.0000"},
		{"zero", "0, 0", "INTEGER", "_bintrail_avg", "0.0000"},
		{"NULLs only", "NULL, NULL", "INTEGER", "_bintrail_avg", "<nil>"},
		{"NULLs left out", "1, NULL, 2", "INTEGER", "_bintrail_avg", "1.5000"},
		{"a half rounds up", "1" + strings.Repeat(", 0", 31), "INTEGER", "_bintrail_avg", "0.0313"},
		{"a negative half rounds away from zero", "-1" + strings.Repeat(", 0", 31), "INTEGER", "_bintrail_avg", "-0.0313"},
		{"just under a half", "1" + strings.Repeat(", 0", 32), "INTEGER", "_bintrail_avg", "0.0303"},
		{"BIGINT near 2^63", "9223372036854775807, 9223372036854775807, 9223372036854775806", "BIGINT", "_bintrail_avg", "9223372036854775806.6667"},
		{"BIGINT at -2^63", "-9223372036854775808, -9223372036854775808", "BIGINT", "_bintrail_avg", "-9223372036854775808.0000"},
		{"BIGINT both ends", "9223372036854775807, 9223372036854775807, 9223372036854775806, -9223372036854775808, -9223372036854775808", "BIGINT", "_bintrail_avg", "1844674407370955160.8000"},
		{"UNSIGNED near 2^64", "18446744073709551615, 18446744073709551615, 18446744073709551614", "UBIGINT", "_bintrail_avg", "18446744073709551614.6667"},
		{"UNSIGNED with zeros", "18446744073709551615, 18446744073709551615, 18446744073709551614, 0, 0", "UBIGINT", "_bintrail_avg", "11068046444225730968.8000"},
		{"TINYINT", "1, 2, 3", "TINYINT", "_bintrail_avg", "2.0000"},
		{"BOOLEAN", "true, false, true", "BOOLEAN", "_bintrail_avg", "0.6667"},
		{"DISTINCT", "1, 1, 2, 4, 4", "INTEGER", "_bintrail_avg_distinct", "2.3333"},
		{"DISTINCT NULLs only", "NULL", "INTEGER", "_bintrail_avg_distinct", "<nil>"},
		{"DISTINCT negative", "-1, -1, -2", "INTEGER", "_bintrail_avg_distinct", "-1.5000"},
	}
	var items []string
	for i, c := range cases {
		vals := strings.Split(c.values, ", ")
		for j, v := range vals {
			vals[j] = "(CAST(" + v + " AS " + c.typ + "))"
		}
		items = append(items, fmt.Sprintf("(SELECT %s(x) FROM (VALUES %s) v%d(x)) AS c%d", c.call, strings.Join(vals, ", "), i, i))
	}
	// And no row at all.
	items = append(items, "(SELECT _bintrail_avg(x) FROM (VALUES (CAST(1 AS INTEGER))) e(x) WHERE x > 1) AS empty")
	job := f.job("SELECT " + strings.Join(items, ", "))
	job.Session = Session{MySQLClock: true}
	res, err := r.Run(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 || len(res.Rows[0]) != len(cases)+1 {
		t.Fatalf("%d rows, want one of %d cells", len(res.Rows), len(cases)+1)
	}
	for i, c := range cases {
		if got := fmt.Sprint(res.Rows[0][i]); got != c.want {
			t.Errorf("%s: %s = %s, want %s", c.name, c.call, got, c.want)
		}
		if typ := res.Columns[i].Type; typ != "DECIMAL(38,4)" {
			t.Errorf("%s: typed %s, want DECIMAL(38,4) (MySQL's NEWDECIMAL with 4 decimals)", c.name, typ)
		}
	}
	if got := res.Rows[0][len(cases)]; got != nil {
		t.Errorf("no row: %v, want NULL", got)
	}

	// Grouped, as a statement writes it: each group its own average.
	job = f.job("SELECT g, _bintrail_avg(x) AS a, _bintrail_avg_distinct(x) AS d FROM (VALUES (1, 1), (1, 2), (1, 4), (1, 4), (2, -1), (2, -2), (3, NULL)) v(g, x) GROUP BY g ORDER BY g")
	job.Session = Session{MySQLClock: true}
	res, err = r.Run(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fmt.Sprint(res.Rows), "[[1 2.7500 2.3333] [2 -1.5000 -1.5000] [3 <nil> <nil>]]"; got != want {
		t.Errorf("grouped = %s, want %s", got, want)
	}
}

// The macros call one another with the parameters s, n and x: a table with
// columns of those names must not be read as the macro's own (#2920 review).
func TestAvgMacros_columnNames_2920(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	job := f.job("SELECT _bintrail_avg(x) AS ax, _bintrail_avg(s) AS a_s, _bintrail_avg(n) AS an, _bintrail_avg_distinct(n) AS dn, _bintrail_avg_distinct(s) AS ds, _bintrail_avg(x + 0) AS ex " +
		"FROM (VALUES (1, 10, -1), (2, 20, -2), (4, 10, -4)) v(s, n, x)")
	job.Session = Session{MySQLClock: true}
	res, err := r.Run(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fmt.Sprint(res.Rows), "[[-2.3333 2.3333 13.3333 15.0000 2.3333 -2.3333]]"; got != want {
		t.Errorf("columns named s, n and x: %s, want %s", got, want)
	}
}

// A sum or a product past what a 128-bit integer holds is an error on the
// copy and never a wrapped number: abs(sum) * 20000 passes 1.7e38 from a sum
// of about 8.5e33, and the sum itself at about 1.7e38. Each statement is its
// own run, since an error ends it. What a run's error then does to the
// client's statement is the router's (shim RouteReasonCopyRefused: the copy
// errored, MySQL answers); no BIGINT column reaches either sum in fewer than
// about 10^15 rows.
func TestAvgMacros_overflow_2920(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	run := func(values string) (any, error) {
		job := f.job("SELECT _bintrail_avg(x) FROM (VALUES " + values + ") v(x)")
		job.Session = Session{MySQLClock: true}
		res, err := r.Run(context.Background(), job)
		if err != nil {
			return nil, err
		}
		return res.Rows[0][0], nil
	}
	h := func(v string) string { return "(CAST(" + v + " AS HUGEINT))" }
	for _, c := range []struct{ name, values string }{
		{"the product: one value of 9e33", h("9000000000000000000000000000000000")},
		{"the sum: two values of 1e38", h("100000000000000000000000000000000000000") + ", " + h("100000000000000000000000000000000000000")},
		{"the sum: two values of -1e38", h("-100000000000000000000000000000000000000") + ", " + h("-100000000000000000000000000000000000000")},
		{"the product, negative: -9e33", h("-9000000000000000000000000000000000")},
	} {
		name := c.name
		got, err := run(c.values)
		if err == nil {
			t.Errorf("%s: answered %v, want an error", name, got)
		}
	}
	// Just under the product's limit the answer is exact.
	if got, err := run(h("8000000000000000000000000000000000")); err != nil || fmt.Sprint(got) != "8000000000000000000000000000000000.0000" {
		t.Errorf("8e33: %v, %v; want 8000000000000000000000000000000000.0000", got, err)
	}
}
