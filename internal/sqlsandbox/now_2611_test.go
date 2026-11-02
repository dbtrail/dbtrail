package sqlsandbox

import (
	"context"
	"reflect"
	"testing"
)

// #2611: NOW(), CURDATE(), UTC_TIMESTAMP() and UTC_DATE() on the copy are
// MySQL's: a date and time (or a date) with no fraction, the wall clock of
// the session's zone (or of UTC), for a client that writes MySQL
// (Session.MySQLClock). _bintrail_wall is the rule over a given instant, so a
// test can ask about any moment.
func TestNowMacros_2611(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	run := func(zone, sql string) ([][]any, error) {
		t.Helper()
		job := f.job(sql)
		job.Session = Session{TimeZone: zone, MySQLClock: true}
		res, err := r.Run(context.Background(), job)
		return res.Rows, err
	}
	for _, c := range []struct {
		zone, sql string
		want      [][]any
	}{
		{"", "SELECT typeof(now()), typeof(curdate()), typeof(utc_timestamp()), typeof(utc_date())",
			[][]any{{"TIMESTAMP", "DATE", "TIMESTAMP", "DATE"}}},
		{"", "SELECT now() = CAST(date_trunc('second', current_timestamp) AS TIMESTAMP), now() = utc_timestamp(), curdate() = utc_date()",
			[][]any{{true, true, true}}},
		{"", "SELECT _bintrail_wall(TIMESTAMPTZ '2026-11-02 17:00:01.9+00')", [][]any{{"2026-11-02T17:00:01Z"}}},
		{"Etc/GMT+5", "SELECT _bintrail_wall(TIMESTAMPTZ '2026-11-02 03:00:00+00')", [][]any{{"2026-11-01T22:00:00Z"}}},
		{"America/New_York", "SELECT _bintrail_wall(TIMESTAMPTZ '2026-11-02 17:00:01.9+00')", [][]any{{"2026-11-02T12:00:01Z"}}},
		{"America/New_York", "SELECT now() - INTERVAL 30 DAY < now(), curdate() <= CAST(now() AS DATE)", [][]any{{true, true}}},
		{"Etc/GMT+5", "SELECT CAST(utc_timestamp() AS VARCHAR) >= CAST(now() AS VARCHAR)", [][]any{{true}}},
	} {
		got, err := run(c.zone, c.sql)
		if err != nil {
			t.Errorf("[%s] %s: %v", c.zone, c.sql, err)
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("[%s] %s = %#v, want %#v", c.zone, c.sql, got, c.want)
		}
	}
	// The SQL card's reader writes DuckDB: its now() stays DuckDB's.
	job := f.job("SELECT typeof(now())")
	res, err := r.Run(context.Background(), job)
	if err != nil || !reflect.DeepEqual(res.Rows, [][]any{{"TIMESTAMP WITH TIME ZONE"}}) {
		t.Errorf("without MySQLClock: %v, %v; want DuckDB's own now()", res.Rows, err)
	}
}
