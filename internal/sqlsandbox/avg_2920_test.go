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
