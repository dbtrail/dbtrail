package readrouter

import (
	"strings"
	"testing"
)

// routedSubOpts is what the routing port passes since DATE_SUB reached the
// copy (#2611).
var routedSubOpts = CopyOptions{MySQLClockKeywords: true, MySQLDateFunctions: true, MySQLDateSub: true}

// #2611: under routing DATE_SUB(d, INTERVAL n unit) and SUBDATE(d, INTERVAL n
// unit) are sent to the copy as its macro over the same two arguments, as the
// client wrote them; anything else is sent as written, and the copy refuses
// it.
func TestForCopy_dateSub_2611(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"SELECT DATE_SUB(dt, INTERVAL 1 DAY) FROM t", "SELECT _bintrail_date_sub(dt, INTERVAL 1 DAY) FROM t"},
		{"SELECT date_sub ( dt , interval 1 day ) FROM t", "SELECT _bintrail_date_sub(dt, interval 1 day) FROM t"},
		{"SELECT SubDate(`dt`, INTERVAL '7' DAY) FROM t", `SELECT _bintrail_date_sub("dt", INTERVAL '7' DAY) FROM t`},
		{"SELECT DATE_SUB(dt,\n\tINTERVAL 2 HOUR) FROM t", "SELECT _bintrail_date_sub(dt, INTERVAL 2 HOUR) FROM t"},
		{"SELECT DATE_SUB(dt /* the time */, INTERVAL 1 MONTH) FROM t", "SELECT _bintrail_date_sub(dt /* the time */, INTERVAL 1 MONTH) FROM t"},
		{"SELECT DATE_SUB(dt -- the time\n, INTERVAL 1 MONTH) FROM t", "SELECT _bintrail_date_sub(dt -- the time\n, INTERVAL 1 MONTH) FROM t"},
		{"SELECT DATE_SUB(NOW(), INTERVAL 30 DAY) FROM t", "SELECT _bintrail_date_sub(NOW(), INTERVAL 30 DAY) FROM t"},
		{"SELECT DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 30 DAY) FROM t", "SELECT _bintrail_date_sub(now(), INTERVAL 30 DAY) FROM t"},
		// Inside another call, and inside itself: each written once.
		{"SELECT DATE_FORMAT(DATE_SUB(dt, INTERVAL 1 DAY), '%Y') FROM t", "SELECT strftime(_bintrail_date_sub(dt, INTERVAL 1 DAY), '%Y') FROM t"},
		{"SELECT DATE_SUB(DATE_SUB(dt, INTERVAL 1 DAY), INTERVAL 1 MONTH) FROM t", "SELECT _bintrail_date_sub(_bintrail_date_sub(dt, INTERVAL 1 DAY), INTERVAL 1 MONTH) FROM t"},
		{"SELECT id FROM t WHERE dt >= DATE_SUB(NOW(), INTERVAL 7 DAY) AND k > ?", "SELECT id FROM t WHERE dt >= _bintrail_date_sub(NOW(), INTERVAL 7 DAY) AND k > ?"},
		// Not the INTERVAL form, not two arguments, a stored function, a name:
		// sent as written.
		{"SELECT SUBDATE(d, 1) FROM t", "SELECT SUBDATE(d, 1) FROM t"},
		{"SELECT DATE_SUB(d) FROM t", "SELECT DATE_SUB(d) FROM t"},
		{"SELECT DATE_SUB(d, INTERVAL 1 DAY, 2) FROM t", "SELECT DATE_SUB(d, INTERVAL 1 DAY, 2) FROM t"},
		{"SELECT DATE_SUB(, INTERVAL 1 DAY) FROM t", "SELECT DATE_SUB(, INTERVAL 1 DAY) FROM t"},
		{"SELECT DATE_SUB('day', d, d2) FROM t", "SELECT DATE_SUB('day', d, d2) FROM t"},
		{"SELECT DATE_SUB(d, intervals) FROM t", "SELECT DATE_SUB(d, intervals) FROM t"},
		{"SELECT db.date_sub(d, INTERVAL 1 DAY) FROM t", "SELECT db.date_sub(d, INTERVAL 1 DAY) FROM t"},
		{"SELECT t.date_sub FROM t", "SELECT t.date_sub FROM t"},
		{"SELECT date_subs(d, INTERVAL 1 DAY) FROM t", "SELECT date_subs(d, INTERVAL 1 DAY) FROM t"},
		{"SELECT 'DATE_SUB(d, INTERVAL 1 DAY)' FROM t", "SELECT 'DATE_SUB(d, INTERVAL 1 DAY)' FROM t"},
		{"SELECT 1 /* DATE_SUB(d, INTERVAL 1 DAY) */ FROM t", "SELECT 1 /* DATE_SUB(d, INTERVAL 1 DAY) */ FROM t"},
	} {
		got, why := ForCopyOpts(c.in, routedSubOpts)
		if why != "" || got != c.want {
			t.Errorf("ForCopyOpts(%q) = %q, %q; want %q", c.in, got, why, c.want)
		}
	}
	// Without the option (the port without routing, Veto), as written.
	const stmt = "SELECT DATE_SUB(dt, INTERVAL 1 DAY) FROM t"
	if got, _ := ForCopyOpts(stmt, CopyOptions{MySQLDateFunctions: true, MySQLClockKeywords: true}); got != stmt {
		t.Errorf("without MySQLDateSub: %q, want it as written", got)
	}
}

// #2611: which DATE_SUB calls the copy answers once the column types are
// known. d is a DATE, dt a DATETIME and ts a TIMESTAMP without fractional
// seconds; dt6 has them, note is text, both is a DATE in one table and a
// DATETIME in another.
func TestDateSubVeto_2611(t *testing.T) {
	dates := []string{"d", "both"}
	times := []string{"dt", "ts", "both"}
	other := []string{"id", "dt6", "note", "k"}
	for _, stmt := range []string{
		"SELECT DATE_SUB(dt, INTERVAL 1 DAY) FROM t",
		"SELECT DATE_SUB(ts, INTERVAL 1 MONTH) FROM t",
		"SELECT date_sub(dt, interval 1 year), SUBDATE(ts, INTERVAL 2 QUARTER), DATE_SUB(dt, INTERVAL 3 Week) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL 90 MINUTE), DATE_SUB(dt, INTERVAL 3601 SECOND), DATE_SUB(ts, INTERVAL 25 HOUR) FROM t",
		"SELECT DATE_SUB(`dt`, INTERVAL 1 DAY), DATE_SUB(t.dt, INTERVAL 1 DAY), DATE_SUB((dt), INTERVAL 1 DAY) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL '7' DAY) FROM t",
		// A quoted amount with a sign is the same number on both (scan
		// refuses one that is not a whole number).
		"SELECT DATE_SUB(dt, INTERVAL '-1' DAY) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL 0 DAY) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL 999999999 SECOND) FROM t",
		"SELECT id FROM t WHERE dt >= DATE_SUB(NOW(), INTERVAL 30 DAY)",
		"SELECT id FROM t WHERE ts BETWEEN DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 HOUR) AND NOW()",
		"SELECT id FROM t WHERE d >= DATE_SUB(NOW(), INTERVAL 7 DAY) AND k > ?",
		// A DATE stepped by less than a day is a date and time on both.
		"SELECT DATE_SUB(d, INTERVAL 1 HOUR), DATE_SUB(d, INTERVAL 30 MINUTE), DATE_SUB(d, INTERVAL 1 SECOND) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL 1 DAY) AS since, COUNT(*) FROM t GROUP BY since ORDER BY since",
		// No call: nothing to read.
		"SELECT d FROM t",
		"SELECT 1 FROM t WHERE note = ''",
	} {
		if got := DateSubVeto(Shape(stmt), dates, times, other); got != "" {
			t.Errorf("DateSubVeto(%q) = %q, want the copy to answer it", stmt, got)
		}
	}
	for _, stmt := range []string{
		// A DATE stepped by a day or more: a DATE on MySQL, a date and time
		// on the copy. CURDATE() and UTC_DATE() too.
		"SELECT DATE_SUB(d, INTERVAL 1 DAY) FROM t",
		"SELECT DATE_SUB(d, INTERVAL 1 MONTH) FROM t",
		"SELECT SUBDATE(d, INTERVAL 1 week) FROM t",
		"SELECT DATE_SUB(CURDATE(), INTERVAL 1 HOUR) FROM t",
		"SELECT DATE_SUB(UTC_DATE(), INTERVAL 1 DAY) FROM t",
		"SELECT DATE_SUB(both, INTERVAL 1 HOUR) FROM t",
		// A fraction MySQL prints by type and the copy by value.
		"SELECT DATE_SUB(dt6, INTERVAL 1 DAY) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL 1 MICROSECOND) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL 1 SECOND_MICROSECOND) FROM t",
		"SELECT DATE_SUB(NOW(3), INTERVAL 1 DAY) FROM t",
		// Units of two parts and MySQL's other spellings.
		"SELECT DATE_SUB(dt, INTERVAL 1 DAY_HOUR) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL '1:30' HOUR_MINUTE) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL 1 SQL_TSI_DAY) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL 1 DAYS) FROM t",
		// Amounts other than a whole number written in the statement.
		"SELECT DATE_SUB(dt, INTERVAL -1 DAY) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL - 1 DAY) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL 1.5 DAY) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL 1e2 DAY) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL (1) DAY) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL 1 + 1 DAY) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL ? DAY) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL k DAY) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL NULL DAY) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL 1000000000 SECOND) FROM t",
		// What is stepped: text, a literal, a placeholder, an expression, a
		// column not known, NULL.
		"SELECT DATE_SUB(note, INTERVAL 1 DAY) FROM t",
		"SELECT DATE_SUB('2026-03-31', INTERVAL 1 DAY) FROM t",
		"SELECT DATE_SUB(?, INTERVAL 1 DAY) FROM t",
		"SELECT DATE_SUB(dt + INTERVAL 1 DAY, INTERVAL 1 MONTH) FROM t",
		"SELECT DATE_SUB(DATE_SUB(dt, INTERVAL 1 DAY), INTERVAL 1 MONTH) FROM t",
		"SELECT DATE_SUB(missing, INTERVAL 1 DAY) FROM t",
		"SELECT DATE_SUB(NULL, INTERVAL 1 DAY) FROM t",
		// Not MySQL's two-argument call.
		"SELECT DATE_SUB(dt) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL 1 DAY, 2) FROM t",
		"SELECT SUBDATE(dt, 1) FROM t",
		"SELECT db.date_sub(dt, INTERVAL 1 DAY) FROM t",
		// A name the statement may give itself.
		"SELECT note AS dt, DATE_SUB(dt, INTERVAL 1 DAY) FROM t",
		"SELECT note 'dt', DATE_SUB(dt, INTERVAL 1 DAY) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL 1 DAY) FROM (SELECT note AS dt FROM t) x",
		"WITH x AS (SELECT dt FROM t) SELECT DATE_SUB(dt, INTERVAL 1 DAY) FROM x",
		// One call the copy answers and one it does not.
		"SELECT DATE_SUB(dt, INTERVAL 1 DAY), DATE_SUB(d, INTERVAL 1 DAY) FROM t",
	} {
		if got := DateSubVeto(Shape(stmt), dates, times, other); got != VetoDateSub {
			t.Errorf("DateSubVeto(%q) = %q, want it kept on MySQL", stmt, got)
		}
	}
	// A table whose columns are not known: a column is never read as a
	// date, a clock call still is.
	if got := ShapeOf("SELECT DATE_SUB(dt, INTERVAL 1 DAY) FROM t").DateSubVeto(nil, nil, nil); got == "" {
		t.Error("no column types: answered by the copy")
	}
	if got := ShapeOf("SELECT DATE_SUB(NOW(), INTERVAL 1 DAY) FROM t").DateSubVeto(nil, nil, nil); got != "" {
		t.Errorf("NOW() with no column types: %q", got)
	}
}

// #2611: what stands around a DATE_SUB the copy answers is the existing
// checks'. Each of these is a number on MySQL (the digits of the date and
// time) and may be an interval or a date and time on the copy; one of the
// router's checks must keep it on MySQL. The call itself is one DateSubVeto
// lets through. SUM, * and / the copy refuses over a date and time, which
// sends the statement to MySQL; the integration test runs those.
func TestDateSubSurroundings_2611(t *testing.T) {
	dates := []string{"d", "dt", "ts"}
	for _, stmt := range []string{
		"SELECT DATE_SUB(dt, INTERVAL 1 DAY) + 0 FROM t",
		"SELECT DATE_SUB(dt, INTERVAL 1 DAY) - 1 FROM t",
		"SELECT DATE_SUB(dt, INTERVAL 1 DAY) - dt FROM t",
		"SELECT dt - DATE_SUB(dt, INTERVAL 1 DAY) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL 1 DAY) - DATE_SUB(ts, INTERVAL 1 DAY) FROM t",
		"SELECT AVG(DATE_SUB(dt, INTERVAL 1 DAY)) FROM t",
		"SELECT -DATE_SUB(dt, INTERVAL 1 DAY) FROM t",
		"SELECT DATE_SUB(NOW(), INTERVAL 1 DAY) + 0 FROM t",
		"SELECT DATE_SUB(NOW(), INTERVAL 1 DAY) - DATE_SUB(NOW(), INTERVAL 2 DAY) FROM t",
		"SELECT AVG(DATE_SUB(NOW(), INTERVAL 1 DAY)) FROM t",
		"SELECT 1 + DATE_SUB(NOW(), INTERVAL 1 DAY) FROM t",
		"SELECT DATE_SUB(NOW(), INTERVAL 1 DAY) DIV 2 FROM t",
	} {
		shape := Shape(stmt)
		if DateSubVeto(shape, []string{"d"}, []string{"dt", "ts"}, nil) != "" {
			t.Errorf("%q: the call itself is kept back; this test is about what stands around it", stmt)
			continue
		}
		if VetoOpts(stmt, routedSubOpts) == "" && ColumnVeto(shape, dates, nil, false) == "" {
			t.Errorf("%q: answered by the copy, want it kept on MySQL", stmt)
		}
	}
}

// #2611: DATE_SUB is not on the veto list: a DATE_SUB the copy is sent as
// written is refused by it, and one it is sent as the macro is the
// console's to read (DateSubVeto). CURDATE() stepped through it is still
// MySQL's (dateUses).
func TestVeto_dateSub_2611(t *testing.T) {
	for _, stmt := range []string{
		"SELECT DATE_SUB(dt, INTERVAL 1 DAY) FROM t",
		"SELECT id FROM t WHERE dt >= DATE_SUB(NOW(), INTERVAL 30 DAY)",
		"SELECT id FROM t WHERE dt >= DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 30 DAY)",
	} {
		if got := VetoOpts(stmt, routedSubOpts); got != "" {
			t.Errorf("VetoOpts(%q) = %q, want the copy to answer it", stmt, got)
		}
	}
	for _, stmt := range []string{
		"SELECT DATE_SUB(CURDATE(), INTERVAL 1 DAY) FROM t",
		"SELECT id FROM t WHERE d >= DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)",
		"SELECT DATE_SUB(UTC_DATE(), INTERVAL 1 DAY) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL ? DAY) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL (1.5) DAY) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL '1.5' DAY) FROM t",
		"SELECT DATE_SUB(dt, INTERVAL '1 2' DAY) FROM t",
		// A quoted name before a parenthesis: refused by the scan, as any.
		"SELECT `date_sub`(dt, INTERVAL 1 DAY) FROM t",
	} {
		if got := VetoOpts(stmt, routedSubOpts); got == "" {
			t.Errorf("VetoOpts(%q) = \"\", want it kept on MySQL", stmt)
		}
	}
	if !strings.Contains(VetoDateSub, "DATE_SUB") {
		t.Error("the reason does not name the function")
	}
}
