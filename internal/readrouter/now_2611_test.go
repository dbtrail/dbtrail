package readrouter

import (
	"strings"
	"testing"
)

// #2611: NOW(), CURDATE(), UTC_TIMESTAMP() and UTC_DATE() reach the copy,
// which has them with MySQL's values (sqlsandbox's macros), as long as no
// arithmetic other than INTERVAL stands next to them: MySQL turns a date or a
// date and time next to + 1, * 2 or under AVG into the number its digits
// spell (CURDATE() + 1 is 20261103), where the copy adds a day.
func TestVeto_now_2611(t *testing.T) {
	for _, stmt := range []string{
		"SELECT NOW()",
		"SELECT now ( )",
		"SELECT CURDATE(), UTC_TIMESTAMP(), UTC_DATE()",
		"SELECT * FROM t WHERE ts >= NOW() - INTERVAL 30 DAY",
		"SELECT * FROM t WHERE ts >= NOW() - interval 30 day AND ts < NOW() + INTERVAL 1 HOUR",
		"SELECT * FROM t WHERE d = CURDATE()",
		"SELECT * FROM t WHERE ts BETWEEN CURDATE() - INTERVAL 7 DAY AND NOW()",
		"SELECT YEAR(NOW()), DATE(NOW()), MONTH(CURDATE()) FROM t",
		"SELECT YEAR(NOW()) - 1 FROM t",
		"SELECT * FROM t WHERE note = 'NOW() + 1'",
		"SELECT `now` FROM t",
		"SELECT * FROM t WHERE d >= CURDATE() - INTERVAL 30 DAY AND d < CURDATE() + INTERVAL 1 DAY",
		"SELECT * FROM t WHERE CURDATE() - INTERVAL 7 DAY <= d",
		"SELECT * FROM t WHERE d BETWEEN UTC_DATE() - INTERVAL 7 DAY AND UTC_DATE()",
		"SELECT NOW() - INTERVAL 1 DAY FROM t",
		"SELECT * FROM t WHERE d = CURDATE() AND ts < NOW()",
		"SELECT * FROM t WHERE t.d >= CURDATE() - INTERVAL 7 DAY ORDER BY t.d",
		"SELECT * FROM t WHERE (d = CURDATE() OR d = CURDATE() - INTERVAL 1 DAY)",
		"SELECT * FROM t WHERE d NOT BETWEEN CURDATE() - INTERVAL 7 DAY AND CURDATE() GROUP BY d",
		"SELECT COUNT(*) FROM t WHERE d = CURDATE() - INTERVAL 1 DAY - INTERVAL 2 HOUR",
		"SELECT IF(d = CURDATE(), 1, 0) FROM t", "SELECT CURDATE() AS today, COUNT(*) FROM t",
		"SELECT DAY(CURDATE()), QUARTER(UTC_DATE()) FROM t",
		"SELECT d, COUNT(*) FROM t WHERE d >= CURDATE() - INTERVAL 7 DAY GROUP BY d",
		"SELECT DISTINCT d FROM t WHERE d = CURDATE()", "SELECT ALL d FROM t WHERE d = CURDATE()",
		"SELECT id, d FROM t WHERE d = CURDATE() ORDER BY d",
	} {
		if got := Veto(stmt); got != "" {
			t.Errorf("Veto(%q) = %q, want the copy to answer it", stmt, got)
		}
	}
	for _, stmt := range []string{
		// Not given to the copy: a fraction, the keywords DuckDB reads as its
		// own, a time of day, the time a function runs at.
		"SELECT NOW(3)", "SELECT NOW( 6 )", "SELECT UTC_TIMESTAMP(3)",
		"SELECT CURRENT_TIMESTAMP", "SELECT CURRENT_TIMESTAMP()", "SELECT LOCALTIME", "SELECT LOCALTIMESTAMP",
		"SELECT CURRENT_DATE", "SELECT CURRENT_DATE()",
		"SELECT CURTIME()", "SELECT CURRENT_TIME", "SELECT UTC_TIME()", "SELECT SYSDATE()",
		// Arithmetic that makes them numbers on MySQL.
		"SELECT NOW() + 1", "SELECT CURDATE() - 1", "SELECT 1 + CURDATE()", "SELECT NOW() * 2",
		"SELECT NOW() / 2", "SELECT CURDATE() % 7", "SELECT NOW() DIV 2", "SELECT CURDATE() MOD 7",
		"SELECT (NOW()) + 1", "SELECT AVG(NOW()) FROM t", "SELECT SUM(CURDATE()) FROM t",
		"SELECT NOW() - ts FROM t", "SELECT ts - NOW() FROM t",
		// Under another name, past a derived table.
		"SELECT d + 1 FROM (SELECT CURDATE() AS d) x",
		// A date stepped by INTERVAL is a date on MySQL and a date and time on
		// the copy: only where it is compared does that not show.
		"SELECT CURDATE() - INTERVAL 7 DAY", "SELECT CONCAT(CURDATE() - INTERVAL 1 DAY, '')", "SELECT UTC_DATE() + INTERVAL 1 MONTH FROM t",
		// A date compared with a string: MySQL reads a time in it, the copy
		// drops it.
		"SELECT * FROM t WHERE CURDATE() = '2026-11-02 10:00:00'", "SELECT * FROM t WHERE '2026-11-02 12:00' > UTC_DATE()",
		"SELECT * FROM t WHERE CURDATE() IN ('2026-11-02', '2026-11-03')",
		// Without parentheses, which MySQL takes and the copy reads as a column.
		"SELECT UTC_DATE", "SELECT UTC_TIMESTAMP FROM t", "SELECT * FROM t WHERE d = UTC_DATE",
		// A date the copy is not known to answer as MySQL does: stepped
		// through another function or a group, picked among strings, built
		// into one, compared with a parameter or with what is not a column.
		"SELECT DATE_ADD(CURDATE(), INTERVAL 1 DAY)", "SELECT DATE_ADD(UTC_DATE(), INTERVAL 7 DAY) FROM t",
		"SELECT (CURDATE()) + INTERVAL 1 DAY", "SELECT IFNULL(CURDATE(), d) + INTERVAL 1 DAY FROM t",
		"SELECT DATE(NOW()) - INTERVAL 1 DAY", "SELECT CAST(NOW() AS DATE) - INTERVAL 1 DAY",
		"SELECT CURDATE() = CONCAT('2026-11-02', ' 10:00')", "SELECT GREATEST(CURDATE(), '2026-11-02 10:00')",
		"SELECT * FROM t WHERE CURDATE() = ?", "SELECT * FROM t WHERE ? = CURDATE()",
		"SELECT COALESCE(s, CURDATE()) FROM t", "SELECT CASE WHEN a THEN CURDATE() ELSE s END FROM t",
		"SELECT CURDATE() FROM t UNION SELECT s FROM u", "SELECT * FROM t WHERE d = CURDATE() + 0",
		"SELECT * FROM t WHERE \"d\" = CURDATE()",
		// Arithmetic after the INTERVAL: MySQL ends the step first, then
		// multiplies the date as a number; the copy multiplies the INTERVAL.
		"SELECT NOW() + INTERVAL 1 DAY * 2", "SELECT * FROM t WHERE d >= CURDATE() - INTERVAL 1 DAY * 7",
		"SELECT * FROM t WHERE ts >= NOW() - INTERVAL 1 DAY / 2", "SELECT NOW() - INTERVAL 1 DAY + 0",
		"SELECT NOW() - INTERVAL 1 DAY DIV 2", "SELECT * FROM t WHERE ts > NOW() - INTERVAL a DAY",
		"SELECT * FROM t WHERE CURDATE() BETWEEN '2026-11-02 10:00' AND d", "SELECT * FROM t WHERE CURDATE() IN (d, '2026-11-02 10:00')",
		"SELECT 2 * UTC_TIMESTAMP()", "SELECT 7 MOD CURDATE()",
		// A name the statement gives may stand for text: a derived table's
		// column, a WITH's, a select item read back in HAVING or ORDER BY.
		"SELECT * FROM (SELECT note AS d FROM ev) x WHERE d = CURDATE()",
		"SELECT * FROM (SELECT CONCAT(d, ' 10:00') AS d FROM ev) x WHERE d = CURDATE()",
		"WITH x AS (SELECT CONCAT(d, ' 10:00') AS d FROM ev) SELECT * FROM x WHERE d = CURDATE()",
		"SELECT note AS d FROM ev HAVING d = CURDATE()", "SELECT note AS d FROM ev WHERE d = CURDATE()",
		"SELECT note d FROM ev WHERE d = CURDATE()",
		"SELECT CURDATE() AS x FROM ev HAVING x = '2026-11-02 10:00'", "SELECT CURDATE() AS x, s FROM t ORDER BY x = s",
	} {
		if got := Veto(stmt); got == "" {
			t.Errorf("Veto(%q) = none, want MySQL to answer it", stmt)
		}
	}
}

// DateComparedColumns names the columns CURDATE() and UTC_DATE() are
// compared with, for the caller that knows their types: the copy answers only
// where each is a date, as MySQL compares a date with a string holding a time
// as a date and time and the copy drops the time.
func TestDateComparedColumns_2611(t *testing.T) {
	for stmt, want := range map[string]string{
		"SELECT * FROM t WHERE t.d >= CURDATE() - INTERVAL 7 DAY AND UTC_DATE() > e": "d,e",
		"SELECT * FROM t WHERE d BETWEEN CURDATE() - INTERVAL 7 DAY AND CURDATE()":   "d",
		"SELECT YEAR(CURDATE()), NOW() FROM t WHERE ts < NOW()":                      "",
	} {
		if got := strings.Join(DateComparedColumns(stmt), ","); got != want {
			t.Errorf("DateComparedColumns(%q) = %q, want %q", stmt, got, want)
		}
	}
}

// CallsNow says a statement calls one of the four the copy answers, for the
// checks that apply to those statements alone.
func TestCallsNow_2611(t *testing.T) {
	for stmt, want := range map[string]bool{
		"SELECT * FROM t WHERE ts >= NOW() - INTERVAL 1 DAY": true,
		"SELECT curdate ( )":                   true,
		"SELECT UTC_DATE()":                    true,
		"SELECT * FROM t WHERE note = 'NOW()'": false,
		"SELECT `now` FROM t":                  false,
		"SELECT 1 /* NOW() */":                 false,
		"SELECT NOW(/**/) - INTERVAL 1 DAY":    true,
	} {
		if got := CallsNow(stmt); got != want {
			t.Errorf("CallsNow(%q) = %v, want %v", stmt, got, want)
		}
	}
}
