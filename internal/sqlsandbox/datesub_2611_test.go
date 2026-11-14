package sqlsandbox

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

// #2611: the copy's DATE_SUB (the routing port writes DATE_SUB and SUBDATE
// with an INTERVAL as _bintrail_date_sub) is the date minus the interval,
// with MySQL's values at month ends, February 29 and the turn of a year,
// NULL over NULL, and an error with the declined-value mark outside the years
// 1 to 9999, where MySQL answers NULL or a zero date. Defined only for a
// client that writes MySQL (Session.MySQLClock).
func TestDateSubMacro_2611(t *testing.T) {
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
		{"", "SELECT _bintrail_date_sub(TIMESTAMP '2026-03-31 10:00:00', INTERVAL 1 MONTH)", [][]any{{"2026-02-28T10:00:00Z"}}},
		{"", "SELECT _bintrail_date_sub(TIMESTAMP '2024-02-29 00:00:00', INTERVAL 1 YEAR)", [][]any{{"2023-02-28T00:00:00Z"}}},
		{"", "SELECT _bintrail_date_sub(TIMESTAMP '2024-03-31 00:00:00', INTERVAL 1 month)", [][]any{{"2024-02-29T00:00:00Z"}}},
		{"", "SELECT _bintrail_date_sub(TIMESTAMP '2026-05-31 23:59:59', INTERVAL 1 QUARTER)", [][]any{{"2026-02-28T23:59:59Z"}}},
		{"", "SELECT _bintrail_date_sub(TIMESTAMP '2026-01-01 00:00:00', INTERVAL 1 SECOND)", [][]any{{"2025-12-31T23:59:59Z"}}},
		{"", "SELECT _bintrail_date_sub(TIMESTAMP '2026-01-01 00:30:00', INTERVAL '7' DAY)", [][]any{{"2025-12-25T00:30:00Z"}}},
		{"", "SELECT _bintrail_date_sub(DATE '2026-01-01', INTERVAL 1 HOUR)", [][]any{{"2025-12-31T23:00:00Z"}}},
		{"", "SELECT _bintrail_date_sub(TIMESTAMP '1000-01-01 00:00:00', INTERVAL 1 DAY)", [][]any{{"0999-12-31T00:00:00Z"}}},
		{"", "SELECT _bintrail_date_sub(TIMESTAMP '0001-01-01 00:00:00', INTERVAL 0 DAY)", [][]any{{"0001-01-01T00:00:00Z"}}},
		{"", "SELECT _bintrail_date_sub(CAST(NULL AS TIMESTAMP), INTERVAL 1 DAY)", [][]any{{nil}}},
		// A TIMESTAMP steps the wall clock of a fixed-offset zone: the first
		// of the month five hours east is still the first a month earlier.
		{"Etc/GMT-5", "SELECT _bintrail_date_sub(TIMESTAMPTZ '2026-03-31 23:30:00+00', INTERVAL 1 MONTH)", [][]any{{"2026-02-28T23:30:00Z"}}},
		{"Etc/GMT-5", "SELECT _bintrail_date_sub(TIMESTAMPTZ '2026-03-31 19:00:00+00', INTERVAL 1 MONTH)", [][]any{{"2026-02-28T19:00:00Z"}}},
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
	// Outside the years 1 to 9999: declined, so MySQL answers.
	for _, sql := range []string{
		"SELECT _bintrail_date_sub(TIMESTAMP '0001-01-01 00:00:00', INTERVAL 1 DAY)",
		"SELECT _bintrail_date_sub(DATE '0001-01-01', INTERVAL 1 MONTH)",
		"SELECT _bintrail_date_sub(TIMESTAMP '2026-03-31 00:00:00', INTERVAL 1000000 DAY)",
		"SELECT _bintrail_date_sub(TIMESTAMP '9999-12-31 00:00:00', INTERVAL (-1) DAY)",
	} {
		_, err := run("", sql, true)
		if !DeclinedValue(err) {
			t.Errorf("%s: %v, want a declined value", sql, err)
		}
	}
	// The SQL card's reader writes DuckDB: no such macro there.
	if _, err := run("", "SELECT _bintrail_date_sub(TIMESTAMP '2026-03-31 10:00:00', INTERVAL 1 DAY)", false); err == nil {
		t.Error("without MySQLClock: the macro answered")
	}
	if strings.Count(dateSubMacro, valueDeclinedMark+"')") != 1 {
		t.Errorf("the macro's error message does not end with %q", valueDeclinedMark)
	}
}
