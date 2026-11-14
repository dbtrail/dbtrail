package sqlsandbox

import (
	"context"
	"testing"
)

func TestZZExplore2800(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	for _, zone := range []string{"", "Etc/GMT-5"} {
		for _, sql := range []string{
			"SELECT date_add(TIMESTAMPTZ '2026-03-31 23:30:00+00', INTERVAL 1 MONTH), TIMESTAMPTZ '2026-03-31 23:30:00+00' + INTERVAL 1 MONTH",
			"SELECT typeof(date_add(TIMESTAMPTZ '2026-03-31 23:30:00+00', INTERVAL 1 MONTH)), typeof(TIMESTAMPTZ '2026-03-31 23:30:00+00' + INTERVAL 1 MONTH)",
			"SELECT date_add(TIMESTAMP '2026-03-31 10:00:00', INTERVAL 1 MONTH), TIMESTAMP '2026-03-31 10:00:00' + INTERVAL 1 MONTH",
			"SELECT typeof(date_add(DATE '2026-03-31', INTERVAL 1 DAY)), typeof(DATE '2026-03-31' + INTERVAL 1 DAY)",
			"SELECT TIMESTAMP '9999-12-31 23:59:58' + INTERVAL 2 SECOND",
			"SELECT TIMESTAMP '0001-01-01 00:00:00' + INTERVAL -1 DAY",
			"SELECT TIMESTAMP '2026-03-31 10:00:00' + INTERVAL -1 DAY",
			"SELECT TIMESTAMP '2026-03-31 10:00:00' + INTERVAL '-1' DAY",
			"SELECT '2026-03-31 10:00:00' + INTERVAL 1 DAY",
			"SELECT typeof('2026-03-31' - INTERVAL 1 DAY)",
			"SELECT INTERVAL 1 DAY + TIMESTAMP '2026-03-31 10:00:00'",
			"SELECT TIMESTAMP '2026-03-31 10:00:00' + INTERVAL 1 DAY * 2",
			"SELECT now() + INTERVAL 1 DAY > now()",
			"SELECT adddate(TIMESTAMP '2026-03-31 10:00:00', INTERVAL 1 DAY)",
			"SELECT DATETIME '2026-03-31 10:00:00' + INTERVAL 1 DAY",
			"SELECT 1 - 1 + INTERVAL 1 DAY",
		} {
			job := f.job(sql)
			job.Session = Session{TimeZone: zone, MySQLClock: true}
			res, err := r.Run(context.Background(), job)
			t.Logf("[%s] %s => %v %v", zone, sql, res.Rows, err)
		}
	}
}
