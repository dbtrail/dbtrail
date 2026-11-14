package sqlsandbox

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

// #2800: the copy's DATE_ADD (the routing port writes DATE_ADD, ADDDATE and
// d + INTERVAL n unit as _bintrail_date_add) is d + i, the value and type the
// copy gave each spelling before, to the last microsecond of 9999, and an
// error with the declined-value mark past it or before the year 1, where
// MySQL answers NULL. Both macros check the range in one place.
func TestDateAddMacro_2800(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	run := func(zone, sql string, clock bool) ([][]any, error) {
		t.Helper()
		job := f.job(sql)
		job.Session = Session{TimeZone: zone, MySQLClock: clock}
		res, err := r.Run(context.Background(), job)
		return res.Rows, err
	}
	for _, c := range []struct {
		zone, sql string
		want      [][]any
	}{
		{"", "SELECT _bintrail_date_add(TIMESTAMP '2026-01-31 10:00:00', INTERVAL 1 MONTH)", [][]any{{"2026-02-28T10:00:00Z"}}},
		{"", "SELECT _bintrail_date_add(TIMESTAMP '2024-02-29 00:00:00', INTERVAL 1 YEAR)", [][]any{{"2025-02-28T00:00:00Z"}}},
		{"", "SELECT _bintrail_date_add(TIMESTAMP '9999-12-31 23:59:58', INTERVAL 1 SECOND)", [][]any{{"9999-12-31T23:59:59Z"}}},
		{"", "SELECT _bintrail_date_add(DATE '9999-12-31', INTERVAL 86399 SECOND)", [][]any{{"9999-12-31T23:59:59Z"}}},
		{"", "SELECT _bintrail_date_add(TIMESTAMP '0001-01-02 00:00:00', INTERVAL '-1' DAY)", [][]any{{"0001-01-01T00:00:00Z"}}},
		{"", "SELECT _bintrail_date_add(CAST(NULL AS TIMESTAMP), INTERVAL 1 DAY)", [][]any{{nil}}},
		{"", "SELECT _bintrail_date_sub(TIMESTAMP '9999-12-31 23:59:59', INTERVAL '-0' SECOND)", [][]any{{"9999-12-31T23:59:59Z"}}},
		// A TIMESTAMP WITH TIME ZONE steps the wall clock of a fixed offset:
		// 18:00 UTC is 23:00 five hours east, and 59 minutes later still 9999.
		{"Etc/GMT-5", "SELECT _bintrail_date_add(TIMESTAMPTZ '9999-12-31 18:00:00+00', INTERVAL 59 MINUTE)", [][]any{{"9999-12-31T18:59:00Z"}}},
	} {
		got, err := run(c.zone, c.sql, true)
		if err != nil {
			t.Errorf("[%s] %s: %v", c.zone, c.sql, err)
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("[%s] %s = %#v, want %#v", c.zone, c.sql, got, c.want)
		}
	}
	// The same value and type as the copy's own date_add and +.
	for _, zone := range []string{"", "Etc/GMT-5"} {
		for _, x := range []string{"TIMESTAMP '2026-03-31 10:00:00'", "TIMESTAMPTZ '2026-03-31 23:30:00+00'", "DATE '2026-03-31'"} {
			for _, i := range []string{"INTERVAL 1 MONTH", "INTERVAL 2 HOUR", "INTERVAL '7' DAY"} {
				sql := "SELECT _bintrail_date_add(" + x + ", " + i + ") = date_add(" + x + ", " + i + ") AND " + x + " + " + i + " = date_add(" + x + ", " + i + "), " +
					"typeof(_bintrail_date_add(" + x + ", " + i + ")) = typeof(" + x + " + " + i + ")"
				got, err := run(zone, sql, true)
				if err != nil || !reflect.DeepEqual(got, [][]any{{true, true}}) {
					t.Errorf("[%s] %s = %#v, %v, want the copy's own value and type", zone, sql, got, err)
				}
			}
		}
	}
	// Outside the years 1 to 9999: declined, so MySQL answers.
	for _, c := range []struct{ zone, sql string }{
		{"", "SELECT _bintrail_date_add(TIMESTAMP '9999-12-31 23:59:59', INTERVAL 1 SECOND)"},
		{"", "SELECT _bintrail_date_add(TIMESTAMP '9999-12-31 00:00:00', INTERVAL 1 DAY)"},
		{"", "SELECT _bintrail_date_add(DATE '9999-12-31', INTERVAL 1 DAY)"},
		{"", "SELECT _bintrail_date_add(TIMESTAMP '0001-01-01 00:00:00', INTERVAL '-1' DAY)"},
		// Under UTC a DATETIME and a TIMESTAMP are TIMESTAMP WITH TIME ZONE
		// on the copy, where year() counts by era: the year 0 is 1 BC.
		{"", "SELECT _bintrail_date_add(TIMESTAMPTZ '0001-01-01 00:00:00+00', INTERVAL '-1' DAY)"},
		{"", "SELECT _bintrail_date_add(TIMESTAMPTZ '9999-12-31 23:59:59+00', INTERVAL 1 SECOND)"},
		// Five hours east, 18:00 UTC is 23:00: an hour later is past 9999
		// on the session's wall clock, though not in UTC.
		{"Etc/GMT-5", "SELECT _bintrail_date_add(TIMESTAMPTZ '9999-12-31 18:00:00+00', INTERVAL 61 MINUTE)"},
	} {
		if _, err := run(c.zone, c.sql, true); !DeclinedValue(err) {
			t.Errorf("[%s] %s: %v, want a declined value", c.zone, c.sql, err)
		}
	}
	// The SQL card's reader writes DuckDB: no such macro there.
	if _, err := run("", "SELECT _bintrail_date_add(TIMESTAMP '2026-03-31 10:00:00', INTERVAL 1 DAY)", false); err == nil {
		t.Error("without MySQLClock: the macro answered")
	}
	for _, m := range []string{dateAddMacro, dateSubMacro} {
		if strings.Count(m, valueDeclinedMark+"')") != 1 {
			t.Errorf("a macro's error message does not end with %q: %s", valueDeclinedMark, m)
		}
	}
}
