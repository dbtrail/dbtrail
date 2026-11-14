package sqlsandbox

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// #2611: the copy's TIMESTAMPDIFF (the routing port writes TIMESTAMPDIFF as
// _bintrail_timestampdiff or _bintrail_timestampdiff_months) gives MySQL's
// whole units. Every number below was read from MySQL 8.0.46 over the same
// two DATETIME(6) values, both ways round (a BIGINT comes back as a json.Number, compared by its text): month ends (January 31 to February
// 28 is no month), February 29, a second short of a month, a fraction short of
// one, the years 1 and 9999 (microseconds past 2^53), NULL.
func TestTimestampDiffMacros_2611(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	run := func(zone, sql string, clock bool) ([][]any, error) {
		t.Helper()
		job := f.job(sql)
		job.Session = Session{TimeZone: zone, MySQLClock: clock}
		res, err := r.Run(context.Background(), job)
		return res.Rows, err
	}
	units := []struct {
		name, macro string
		per         int64
	}{
		{"MICROSECOND", "_bintrail_timestampdiff", 1},
		{"SECOND", "_bintrail_timestampdiff", 1000000},
		{"MINUTE", "_bintrail_timestampdiff", 60000000},
		{"HOUR", "_bintrail_timestampdiff", 3600000000},
		{"DAY", "_bintrail_timestampdiff", 86400000000},
		{"WEEK", "_bintrail_timestampdiff", 604800000000},
		{"MONTH", "_bintrail_timestampdiff_months", 1},
		{"QUARTER", "_bintrail_timestampdiff_months", 3},
		{"YEAR", "_bintrail_timestampdiff_months", 12},
	}
	// a, b, then MySQL's TIMESTAMPDIFF(unit, a, b) in the order of units;
	// TIMESTAMPDIFF(unit, b, a) was the same number negated on every row.
	rows := []struct {
		a, b string
		want []int64
	}{
		{"2026-01-31 00:00:00", "2026-02-28 00:00:00", []int64{2419200000000, 2419200, 40320, 672, 28, 4, 0, 0, 0}},
		{"2024-01-31 00:00:00", "2024-02-29 00:00:00", []int64{2505600000000, 2505600, 41760, 696, 29, 4, 0, 0, 0}},
		{"2024-02-29 00:00:00", "2025-02-28 00:00:00", []int64{31536000000000, 31536000, 525600, 8760, 365, 52, 11, 3, 0}},
		{"2024-02-29 00:00:00", "2028-02-29 00:00:00", []int64{126230400000000, 126230400, 2103840, 35064, 1461, 208, 48, 16, 4}},
		{"2026-01-15 10:00:00", "2026-02-15 09:59:59", []int64{2678399000000, 2678399, 44639, 743, 30, 4, 0, 0, 0}},
		{"2026-01-15 10:00:00.5", "2026-02-15 10:00:00.4", []int64{2678399900000, 2678399, 44639, 743, 30, 4, 0, 0, 0}},
		{"2026-03-01 00:00:00.9", "2026-03-01 00:00:02.1", []int64{1200000, 1, 0, 0, 0, 0, 0, 0, 0}},
		{"2026-03-31 00:00:00", "2026-06-30 00:00:00", []int64{7862400000000, 7862400, 131040, 2184, 91, 13, 2, 0, 0}},
		{"0001-01-01 00:00:00", "9999-12-31 23:59:59.999999", []int64{315537897599999999, 315537897599, 5258964959, 87649415, 3652058, 521722, 119987, 39995, 9998}},
		{"2026-01-01 00:00:00", "2026-01-08 00:00:00", []int64{604800000000, 604800, 10080, 168, 7, 1, 0, 0, 0}},
		{"2026-01-01 00:00:00", "2026-01-07 23:59:59", []int64{604799000000, 604799, 10079, 167, 6, 0, 0, 0, 0}},
		{"2026-02-28 12:00:00", "2026-03-31 11:00:00", []int64{2674800000000, 2674800, 44580, 743, 30, 4, 1, 0, 0}},
		{"2025-12-31 23:59:59", "2026-01-01 00:00:00", []int64{1000000, 1, 0, 0, 0, 0, 0, 0, 0}},
		{"2026-05-31 08:00:00", "2026-05-31 08:00:00", []int64{0, 0, 0, 0, 0, 0, 0, 0, 0}},
	}
	// One statement per row, every unit both ways round: a statement per
	// value costs minutes under -race.
	for _, row := range rows {
		var items, want []string
		for i, u := range units {
			for _, way := range []struct {
				x, y string
				want int64
			}{{row.a, row.b, row.want[i]}, {row.b, row.a, -row.want[i]}} {
				call := fmt.Sprintf("%s(TIMESTAMP '%s', TIMESTAMP '%s', %d)", u.macro, way.x, way.y, u.per)
				items = append(items, call, "typeof("+call+")")
				want = append(want, fmt.Sprint(way.want), "BIGINT")
			}
		}
		got, err := run("", "SELECT "+strings.Join(items, ", "), true)
		if err != nil {
			t.Errorf("%s to %s: %v", row.a, row.b, err)
			continue
		}
		if len(got) != 1 || len(got[0]) != len(want) {
			t.Errorf("%s to %s: %d rows, want one of %d cells", row.a, row.b, len(got), len(want))
			continue
		}
		for i, cell := range got[0] {
			if fmt.Sprint(cell) != want[i] {
				t.Errorf("%s = %v, want %s", items[i], cell, want[i])
			}
		}
	}
	for _, c := range []struct {
		zone, sql string
		want      [][]any
	}{
		// NULL on either side.
		{"", "SELECT _bintrail_timestampdiff(CAST(NULL AS TIMESTAMP), TIMESTAMP '2026-01-01 00:00:00', 86400000000)", [][]any{{nil}}},
		{"", "SELECT _bintrail_timestampdiff_months(TIMESTAMP '2026-01-01 00:00:00', CAST(NULL AS TIMESTAMP), 1)", [][]any{{nil}}},
		// A DATE is its midnight (MySQL: 29 hours, and January 31 to
		// February 28 no month).
		{"", "SELECT _bintrail_timestampdiff(DATE '2026-01-01', TIMESTAMP '2026-01-02 05:00:00', 3600000000)", [][]any{{"29"}}},
		{"", "SELECT _bintrail_timestampdiff_months(DATE '2026-01-31', DATE '2026-02-28', 1)", [][]any{{"0"}}},
		// A TIMESTAMP is read on the session's wall clock: from January 31
		// 20:00 UTC to February 28 20:00 UTC is no month in UTC and one five
		// hours east (February 1 01:00 to March 1 01:00), as on MySQL under
		// +05:00. The minutes are the same in both.
		{"", "SELECT _bintrail_timestampdiff_months(TIMESTAMPTZ '2026-01-31 20:00:00+00', TIMESTAMPTZ '2026-02-28 20:00:00+00', 1)", [][]any{{"0"}}},
		{"Etc/GMT-5", "SELECT _bintrail_timestampdiff_months(TIMESTAMPTZ '2026-01-31 20:00:00+00', TIMESTAMPTZ '2026-02-28 20:00:00+00', 1)", [][]any{{"1"}}},
		{"Etc/GMT-5", "SELECT _bintrail_timestampdiff(TIMESTAMPTZ '2026-01-31 20:00:00+00', TIMESTAMPTZ '2026-02-28 20:00:00+00', 60000000)", [][]any{{"40320"}}},
		// The year 1 on a TIMESTAMP WITH TIME ZONE, which year() would read
		// by era: the cast to the wall clock first.
		{"", "SELECT _bintrail_timestampdiff_months(TIMESTAMPTZ '0001-01-01 00:00:00+00', TIMESTAMPTZ '9999-12-31 23:59:59.999999+00', 12)", [][]any{{"9998"}}},
	} {
		got, err := run(c.zone, c.sql, true)
		if err != nil {
			t.Errorf("[%s] %s: %v", c.zone, c.sql, err)
			continue
		}
		if fmt.Sprint(got) != fmt.Sprint(c.want) {
			t.Errorf("[%s] %s = %#v, want %#v", c.zone, c.sql, got, c.want)
		}
	}
	// The SQL card's reader writes DuckDB: no such macro there. Nor has the
	// copy a timestampdiff of its own, so MySQL's call sent as written fails
	// instead of answering.
	if _, err := run("", "SELECT _bintrail_timestampdiff(TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-01-02 00:00:00', 1)", false); err == nil {
		t.Error("without MySQLClock: the macro answered")
	}
	for _, clock := range []bool{false, true} {
		if _, err := run("", "SELECT timestampdiff(DAY, TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-01-02 00:00:00')", clock); err == nil {
			t.Errorf("MySQLClock %v: the copy answered TIMESTAMPDIFF as written", clock)
		}
	}
}
