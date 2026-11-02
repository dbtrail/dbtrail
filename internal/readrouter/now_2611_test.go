package readrouter

import "testing"

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
	} {
		if got := Veto(stmt); got == "" {
			t.Errorf("Veto(%q) = none, want MySQL to answer it", stmt)
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
	} {
		if got := CallsNow(stmt); got != want {
			t.Errorf("CallsNow(%q) = %v, want %v", stmt, got, want)
		}
	}
}
