package readrouter

import (
	"strings"
	"testing"
)

// routedOpts is what the routing port passes since #2611: the clock keywords
// written as the calls, and DATE_FORMAT and DATEDIFF written as the copy's
// calls that answer the same.
var routedOpts = CopyOptions{MySQLClockKeywords: true, MySQLDateFunctions: true}

// #2611 under routing: a DATEDIFF the copy is sent as date_diff no longer
// keeps the statement on MySQL; TIMESTAMPDIFF and TIMEDIFF still do, and so
// does every DATEDIFF the rewrite would leave as written. CURDATE() and
// UTC_DATE() may now be a whole argument of DATEDIFF or the date of
// DATE_FORMAT: cast to a date, or written as text, it is the same date on
// both sides.
func TestVeto_dateFunctionsRouted_2611(t *testing.T) {
	for _, stmt := range []string{
		"SELECT DATEDIFF(d, d2) FROM t",
		"SELECT datediff ( d , d2 ) FROM t",
		"SELECT DateDiff(t.d, `d2`) FROM t",
		"SELECT id FROM t WHERE DATEDIFF(d, d2) > 30",
		"SELECT DATEDIFF(d /* the end */, d2) FROM t",
		"SELECT DATEDIFF(CURDATE(), d) FROM t",
		"SELECT DATEDIFF(d, CURDATE()) FROM t",
		"SELECT DATEDIFF(d, CURRENT_DATE) FROM t",
		"SELECT DATEDIFF(UTC_DATE(), d) FROM t",
		"SELECT DATEDIFF(NOW(), ts) FROM t",
		"SELECT DATE_FORMAT(CURDATE(), '%Y') FROM t",
		"SELECT DATE_FORMAT(CURRENT_DATE, '%Y-%m') FROM t",
		"SELECT DATE_FORMAT(NOW(), '%Y-%m') FROM t",
		"SELECT DATE_FORMAT(dt, '%Y-%m') AS month, COUNT(*) FROM orders WHERE dt >= NOW() - INTERVAL 30 DAY GROUP BY month ORDER BY month",
		"SELECT DATEDIFF(d, d2) FROM t WHERE k > ?",
		// The words inside a string or a comment call nothing.
		"SELECT 'DATEDIFF(a, b, c)' FROM t", "SELECT 1 /* TIMESTAMPDIFF(DAY, a, b) */",
	} {
		if got := VetoOpts(stmt, routedOpts); got != "" {
			t.Errorf("VetoOpts(%q) = %q, want the copy to answer it", stmt, got)
		}
	}
	for _, stmt := range []string{
		// Not the two-argument DATEDIFF the copy is sent as date_diff.
		"SELECT DATEDIFF(d) FROM t",
		"SELECT DATEDIFF('day', d, d2) FROM t",
		"SELECT DATEDIFF(d, ) FROM t",
		"SELECT DATEDIFF(, d) FROM t",
		"SELECT DATEDIFF() FROM t",
		// A stored function of a schema, not the built-in one.
		"SELECT db.datediff(d, d2) FROM t",
		// The other two: still MySQL's.
		"SELECT TIMESTAMPDIFF(DAY, d2, d) FROM t",
		"SELECT TIMEDIFF(dt, ts) FROM t",
		"SELECT DATEDIFF(d, d2), TIMESTAMPDIFF(DAY, d2, d) FROM t",
		// CURDATE() stepped, or an argument of something else, is still MySQL's.
		"SELECT DATEDIFF(CURDATE() - INTERVAL 1 DAY, d) FROM t",
		"SELECT DATE_FORMAT(CURDATE() + INTERVAL 1 DAY, '%Y') FROM t",
		"SELECT DATE_FORMAT('%Y', CURDATE()) FROM t",
		"SELECT db.datediff(CURDATE(), d) FROM t",
		"SELECT DATEDIFF(COALESCE(CURDATE(), d), d) FROM t",
		// Arithmetic that makes the date a number, as before.
		"SELECT DATEDIFF(CURDATE() - 1, d) FROM t",
	} {
		if got := VetoOpts(stmt, routedOpts); got == "" {
			t.Errorf("VetoOpts(%q) = \"\", want it kept on MySQL", stmt)
		}
	}
	// Without the option (the sqlcompare harness, Veto) DATEDIFF stays MySQL's.
	for _, stmt := range []string{"SELECT DATEDIFF(d, d2) FROM t", "SELECT DATEDIFF(CURDATE(), d) FROM t"} {
		if got := VetoOpts(stmt, clockOpts); got == "" {
			t.Errorf("VetoOpts(%q) without MySQLDateFunctions = \"\", want it kept on MySQL", stmt)
		}
	}
}

// The routed rewrite is the one the port without routing has (#2628).
func TestForCopyOpts_dateFunctionsRouted_2611(t *testing.T) {
	for stmt, want := range map[string]string{
		"SELECT DATE_FORMAT(dt, '%Y-%m') AS month FROM t GROUP BY month ORDER BY month": "SELECT strftime(dt, '%Y-%m') AS month FROM t GROUP BY month ORDER BY month",
		"SELECT DATEDIFF(CURRENT_DATE, d) FROM t":                                       "SELECT date_diff('day', CAST(d AS DATE), CAST(curdate() AS DATE)) FROM t",
		"SELECT DATE_FORMAT(d, '%D') FROM t":                                            "SELECT DATE_FORMAT(d, '%D') FROM t",
	} {
		got, why := ForCopyOpts(stmt, routedOpts)
		if why != "" || got != want {
			t.Errorf("ForCopyOpts(%q) = %q, %q; want %q", stmt, got, why, want)
		}
	}
}

// DateFunctionVeto reads, once the columns of the tables a statement reads
// are known, whether every DATE_FORMAT and DATEDIFF takes a date the copy
// reads as MySQL does: a DATE, DATETIME or TIMESTAMP column, or one of the
// four clock calls. A string, a text column, a placeholder or an expression
// is MySQL's: MySQL reads text into a date its own way (an offset in it, a
// date without separators), and the copy refuses some and reads others
// otherwise.
func TestDateFunctionVeto_2611(t *testing.T) {
	dates := []string{"d", "d2", "dt", "ts", "Fecha", "now"}
	other := []string{"id", "s", "k"}
	for _, stmt := range []string{
		"SELECT id FROM t",
		"SELECT DATE_FORMAT(d, '%Y') FROM t",
		"SELECT date_format ( t.dt , '%Y-%m' ) FROM t",
		"SELECT DATE_FORMAT(db.t.ts, '%H') FROM t",
		"SELECT DATE_FORMAT(\"d\", '%Y') FROM t",
		"SELECT DATE_FORMAT(fecha, '%Y') FROM t",
		"SELECT DATE_FORMAT((d), '%Y') FROM t",
		"SELECT DATE_FORMAT(now, '%Y') FROM t",
		"SELECT DATE_FORMAT(NOW(), '%Y'), DATE_FORMAT(curdate( ), '%Y'), DATE_FORMAT(UTC_TIMESTAMP(), '%Y'), DATE_FORMAT(utc_date(), '%Y') FROM t",
		"SELECT DATEDIFF(d, d2), DATEDIFF(dt, ts), DATEDIFF(CURDATE(), d), DATEDIFF(ts, NOW()) FROM t",
		"SELECT DATE_FORMAT(dt, '%Y-%m') AS month, COUNT(*) FROM t GROUP BY month ORDER BY month",
		"SELECT DATE_FORMAT(dt, '%Y-%m'), COUNT(*) FROM t GROUP BY DATE_FORMAT(dt, '%Y-%m') ORDER BY DATE_FORMAT(dt, '%Y-%m')",
		"SELECT DATE_FORMAT(d, ?) FROM t",
		// A string where a value stands is no name.
		Shape("SELECT DATE_FORMAT(d, '%Y') FROM t WHERE s LIKE 'a%' OR s = 'b' OR s IN ('c', 'd') OR CASE WHEN k THEN 'e' ELSE 'f' END = 'g'"),
		// The shape writes CURRENT_DATE as the call, padded.
		Shape("SELECT DATEDIFF(CURRENT_DATE, d) FROM t"),
	} {
		if got := DateFunctionVeto(stmt, dates, other); got != "" {
			t.Errorf("DateFunctionVeto(%q) = %q, want the copy to answer it", stmt, got)
		}
	}
	for _, stmt := range []string{
		"SELECT DATE_FORMAT(s, '%Y') FROM t",
		"SELECT DATE_FORMAT('', '%Y') FROM t",
		"SELECT DATE_FORMAT(?, '%Y') FROM t",
		"SELECT DATE_FORMAT(NULL, '%Y') FROM t",
		"SELECT DATE_FORMAT(20260115, '%Y') FROM t",
		"SELECT DATE_FORMAT(d + INTERVAL 1 DAY, '%Y') FROM t",
		"SELECT DATE_FORMAT(COALESCE(d, dt), '%Y') FROM t",
		"SELECT DATE_FORMAT(NOW(3), '%Y') FROM t",
		"SELECT DATE_FORMAT(unknown_col, '%Y') FROM t",
		"SELECT DATE_FORMAT(d) FROM t",
		"SELECT DATE_FORMAT(d, '%Y', 'x') FROM t",
		"SELECT DATEDIFF(d, s) FROM t",
		"SELECT DATEDIFF(s, d) FROM t",
		"SELECT DATEDIFF(d, '') FROM t",
		"SELECT DATEDIFF(?, d) FROM t",
		"SELECT DATEDIFF(DATE_FORMAT(d, '%Y-%m-01'), d) FROM t",
		"SELECT DATEDIFF(d) FROM t",
		"SELECT db.date_format(d, '%Y') FROM t",
		// A name the statement gives may stand for text.
		"SELECT s AS d, DATE_FORMAT(d, '%Y') FROM t",
		"SELECT s d2, DATEDIFF(d, d2) FROM t",
		// The same names in quotes, or written as strings.
		Shape("SELECT id, s AS `d` FROM t ORDER BY DATE_FORMAT(d, '%Y')"),
		Shape("SELECT id, s AS 'd' FROM t ORDER BY DATE_FORMAT(d, '%Y')"),
		Shape("SELECT id, s 'd' FROM t ORDER BY DATE_FORMAT(d, '%Y')"),
		"SELECT DATE_FORMAT(d, '%Y') FROM (SELECT s AS d FROM t) x",
		"WITH x AS (SELECT s AS d FROM t) SELECT DATE_FORMAT(d, '%Y') FROM x",
		"SELECT DATE_FORMAT(d, '%Y') FROM t WHERE id IN (SELECT id FROM u)",
	} {
		if got := DateFunctionVeto(stmt, dates, other); got == "" {
			t.Errorf("DateFunctionVeto(%q) = \"\", want it kept on MySQL", stmt)
		}
	}
	// A name in both lists (two tables, two types) is not a date.
	if got := DateFunctionVeto("SELECT DATE_FORMAT(d, '%Y') FROM t JOIN u", []string{"d"}, []string{"d"}); got == "" {
		t.Error("a column that is a date in one table and text in another let through")
	}
	// The shape is what the console hands over: its method is the same.
	if got := ShapeOf("SELECT DATE_FORMAT(s, '%Y') FROM t").DateFunctionVeto(dates, other); !strings.Contains(got, "DATE_FORMAT") {
		t.Errorf("StatementShape.DateFunctionVeto = %q, want the veto", got)
	}
}
