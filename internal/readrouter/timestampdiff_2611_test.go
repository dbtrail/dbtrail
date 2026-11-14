package readrouter

import (
	"testing"
)

// routedDiffOpts is what the routing port passes since TIMESTAMPDIFF reached
// the copy (#2611).
var routedDiffOpts = CopyOptions{MySQLClockKeywords: true, MySQLDateFunctions: true, MySQLDateSub: true, MySQLTimestampDiff: true}

// #2611: under routing TIMESTAMPDIFF(unit, a, b) is sent to the copy as one of
// its two macros over a and b as the client wrote them, the unit as its length
// (microseconds, or months); anything else is sent as written, and the veto
// keeps it on MySQL.
func TestForCopy_timestampDiff_2611(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"SELECT TIMESTAMPDIFF(DAY, a, b) FROM t", "SELECT _bintrail_timestampdiff(a, b, 86400000000) FROM t"},
		{"SELECT TIMESTAMPDIFF(MICROSECOND, a, b), TIMESTAMPDIFF(SECOND, a, b), TIMESTAMPDIFF(MINUTE, a, b) FROM t",
			"SELECT _bintrail_timestampdiff(a, b, 1), _bintrail_timestampdiff(a, b, 1000000), _bintrail_timestampdiff(a, b, 60000000) FROM t"},
		{"SELECT TIMESTAMPDIFF(HOUR, a, b), TIMESTAMPDIFF(WEEK, a, b) FROM t",
			"SELECT _bintrail_timestampdiff(a, b, 3600000000), _bintrail_timestampdiff(a, b, 604800000000) FROM t"},
		{"SELECT timestampdiff ( month , a , b ), TimestampDiff(Quarter, a, b), TIMESTAMPDIFF(year, a, b) FROM t",
			"SELECT _bintrail_timestampdiff_months(a, b, 1), _bintrail_timestampdiff_months(a, b, 3), _bintrail_timestampdiff_months(a, b, 12) FROM t"},
		{"SELECT TIMESTAMPDIFF(DAY, `a`, t.`b`) FROM t", `SELECT _bintrail_timestampdiff("a", t."b", 86400000000) FROM t`},
		{"SELECT TIMESTAMPDIFF(DAY,\n\ta,\n\tb) FROM t", "SELECT _bintrail_timestampdiff(a, b, 86400000000) FROM t"},
		// The unit is read as the veto reads it: a comment beside it is a
		// space.
		{"SELECT TIMESTAMPDIFF(/* c */ DAY, a, b) FROM t", "SELECT _bintrail_timestampdiff(a, b, 86400000000) FROM t"},
		{"SELECT TIMESTAMPDIFF(DAY -- the unit\n, a, b) FROM t", "SELECT _bintrail_timestampdiff(a, b, 86400000000) FROM t"},
		{"SELECT TIMESTAMPDIFF(DAY, a /* from */, b -- to\n) FROM t", "SELECT _bintrail_timestampdiff(a /* from */, b -- to\n, 86400000000) FROM t"},
		{"SELECT TIMESTAMPDIFF(DAY, a, CURRENT_TIMESTAMP) FROM t", "SELECT _bintrail_timestampdiff(a, now(), 86400000000) FROM t"},
		{"SELECT TIMESTAMPDIFF\v(DAY, a, b) FROM t", "SELECT _bintrail_timestampdiff(a, b, 86400000000) FROM t"},
		// Inside another call and around one: each written once.
		{"SELECT TIMESTAMPDIFF(DAY, DATE_SUB(a, INTERVAL 1 DAY), b) FROM t", "SELECT _bintrail_timestampdiff(_bintrail_date_sub(a, INTERVAL 1 DAY), b, 86400000000) FROM t"},
		{"SELECT id FROM t WHERE TIMESTAMPDIFF(DAY, a, NOW()) > ? ORDER BY TIMESTAMPDIFF(HOUR, a, b)",
			"SELECT id FROM t WHERE _bintrail_timestampdiff(a, NOW(), 86400000000) > ? ORDER BY _bintrail_timestampdiff(a, b, 3600000000)"},
		// Not a unit of one word this reads, not three arguments, a stored
		// function, a name, a comment before the parenthesis: as written.
		{"SELECT TIMESTAMPDIFF(SQL_TSI_DAY, a, b) FROM t", "SELECT TIMESTAMPDIFF(SQL_TSI_DAY, a, b) FROM t"},
		{"SELECT TIMESTAMPDIFF('DAY', a, b) FROM t", "SELECT TIMESTAMPDIFF('DAY', a, b) FROM t"},
		{"SELECT TIMESTAMPDIFF(DAYS, a, b) FROM t", "SELECT TIMESTAMPDIFF(DAYS, a, b) FROM t"},
		{"SELECT TIMESTAMPDIFF(DAY_HOUR, a, b) FROM t", "SELECT TIMESTAMPDIFF(DAY_HOUR, a, b) FROM t"},
		{"SELECT TIMESTAMPDIFF(DAY HOUR, a, b) FROM t", "SELECT TIMESTAMPDIFF(DAY HOUR, a, b) FROM t"},
		{"SELECT TIMESTAMPDIFF(u, a, b) FROM t", "SELECT TIMESTAMPDIFF(u, a, b) FROM t"},
		{"SELECT TIMESTAMPDIFF(DAY, a) FROM t", "SELECT TIMESTAMPDIFF(DAY, a) FROM t"},
		{"SELECT TIMESTAMPDIFF(DAY, a, b, c) FROM t", "SELECT TIMESTAMPDIFF(DAY, a, b, c) FROM t"},
		{"SELECT TIMESTAMPDIFF(DAY, , b) FROM t", "SELECT TIMESTAMPDIFF(DAY, , b) FROM t"},
		{"SELECT TIMESTAMPDIFF(DAY, a, ) FROM t", "SELECT TIMESTAMPDIFF(DAY, a, ) FROM t"},
		{"SELECT TIMESTAMPDIFF(, a, b) FROM t", "SELECT TIMESTAMPDIFF(, a, b) FROM t"},
		{"SELECT db.timestampdiff(DAY, a, b) FROM t", "SELECT db.timestampdiff(DAY, a, b) FROM t"},
		{"SELECT t.timestampdiff FROM t", "SELECT t.timestampdiff FROM t"},
		{"SELECT timestampdiffs(DAY, a, b) FROM t", "SELECT timestampdiffs(DAY, a, b) FROM t"},
		{"SELECT 'TIMESTAMPDIFF(DAY, a, b)' FROM t", "SELECT 'TIMESTAMPDIFF(DAY, a, b)' FROM t"},
		{"SELECT 1 /* TIMESTAMPDIFF(DAY, a, b) */ FROM t", "SELECT 1 /* TIMESTAMPDIFF(DAY, a, b) */ FROM t"},
		{"SELECT TIMESTAMPDIFF /* c */ (DAY, a, b) FROM t", "SELECT TIMESTAMPDIFF /* c */ (DAY, a, b) FROM t"},
	} {
		got, why := ForCopyOpts(c.in, routedDiffOpts)
		if why != "" || got != c.want {
			t.Errorf("ForCopyOpts(%q) = %q, %q; want %q", c.in, got, why, c.want)
		}
	}
	// Without the option (the port without routing, Veto), as written.
	const stmt = "SELECT TIMESTAMPDIFF(DAY, a, b) FROM t"
	if got, _ := ForCopyOpts(stmt, routedSubOpts); got != stmt {
		t.Errorf("without MySQLTimestampDiff: %q, want it as written", got)
	}
}

// #2611: the veto list keeps TIMESTAMPDIFF on MySQL unless the option is on
// and every call is one the copy is sent as its macro; DATEDIFF stays MySQL's
// without MySQLDateFunctions, and TIMEDIFF always.
func TestVetoOpts_timestampDiff_2611(t *testing.T) {
	both := CopyOptions{MySQLDateFunctions: true, MySQLTimestampDiff: true}
	diffOnly := CopyOptions{MySQLTimestampDiff: true}
	funcsOnly := CopyOptions{MySQLDateFunctions: true}
	for _, c := range []struct {
		o    CopyOptions
		stmt string
		want string
	}{
		{both, "SELECT TIMESTAMPDIFF(DAY, a, b) FROM t", ""},
		{both, "SELECT DATEDIFF(a, b), TIMESTAMPDIFF(MONTH, a, b) FROM t", ""},
		{both, "SELECT TIMESTAMPDIFF\v(DAY, a, b) FROM t", ""},
		{both, "SELECT TIMESTAMPDIFF(/* c */ YEAR, a, b) FROM t", ""},
		{both, "SELECT TIMEDIFF(a, b) FROM t", vetoDateDiff},
		{both, "SELECT TIMESTAMPDIFF(DAY, a, b), TIMEDIFF(a, b) FROM t", vetoDateDiff},
		{both, "SELECT TIMESTAMPDIFF(SQL_TSI_DAY, a, b) FROM t", vetoDateDiff},
		{both, "SELECT TIMESTAMPDIFF(DAY, a, b), TIMESTAMPDIFF(DAYS, a, b) FROM t", vetoDateDiff},
		{both, "SELECT TIMESTAMPDIFF(DAY, a) FROM t", vetoDateDiff},
		{both, "SELECT db.timestampdiff(DAY, a, b) FROM t", vetoDateDiff},
		{both, "SELECT TIMESTAMPDIFF /* c */ (DAY, a, b) FROM t", vetoDateDiff},
		{both, "SELECT TIMESTAMPDIFF\v/* c */(DAY, a, b) FROM t", vetoDateDiff},
		{diffOnly, "SELECT TIMESTAMPDIFF(DAY, a, b) FROM t", ""},
		{diffOnly, "SELECT DATEDIFF(a, b) FROM t", vetoDateDiff},
		{diffOnly, "SELECT TIMEDIFF(a, b) FROM t", vetoDateDiff},
		{funcsOnly, "SELECT TIMESTAMPDIFF(DAY, a, b) FROM t", vetoDateDiff},
		{funcsOnly, "SELECT DATEDIFF(a, b) FROM t", ""},
		{CopyOptions{}, "SELECT TIMESTAMPDIFF(DAY, a, b) FROM t", vetoDateDiff},
		{CopyOptions{}, "SELECT DATEDIFF(a, b) FROM t", vetoDateDiff},
		// A client that writes one of the copy's macros itself: no such
		// function on MySQL, which refuses the statement.
		{routedDiffOpts, "SELECT _bintrail_timestampdiff(a, b, 1) FROM t", vetoCopyMacro},
		{routedDiffOpts, "SELECT _BINTRAIL_TIMESTAMPDIFF_MONTHS (a, b, 1) FROM t", vetoCopyMacro},
		{routedDiffOpts, "SELECT _bintrail_date_sub(a, INTERVAL 1 DAY) FROM t", vetoCopyMacro},
		{CopyOptions{}, "SELECT 1 FROM t WHERE x = _bintrail_wall(a)", vetoCopyMacro},
		// A name, not a call: the copy's macros are not in its way.
		{routedDiffOpts, "SELECT dt._bintrail_timestampdiff(dt2, 1) FROM t", vetoCopyMacro},
		{routedDiffOpts, "SELECT _bintrail_x, t._bintrail_y, `_bintrail_z` FROM t", ""},
		{both, "SELECT TIMESTAMPDIFF(DAY, a, b), TIMEDIFF\v(a, b) FROM t", vetoDateDiff},
	} {
		if got := VetoOpts(c.stmt, c.o); got != c.want {
			t.Errorf("VetoOpts(%q, %+v) = %q, want %q", c.stmt, c.o, got, c.want)
		}
	}
}

// #2611: which TIMESTAMPDIFF calls the copy answers once the column types are
// known. d is a DATE, dt a DATETIME and ts a TIMESTAMP without fractional
// seconds, both a DATE in one table and a DATETIME in another; dt6 has a
// fraction, note is text.
func TestTimestampDiffVeto_2611(t *testing.T) {
	dates := []string{"d", "dt", "ts", "both"}
	other := []string{"id", "dt6", "note", "k"}
	for _, stmt := range []string{
		"SELECT TIMESTAMPDIFF(DAY, dt, ts) FROM t",
		"SELECT TIMESTAMPDIFF(MONTH, d, NOW()), TIMESTAMPDIFF(YEAR, d, CURDATE()) FROM t",
		"SELECT TIMESTAMPDIFF(MINUTE, UTC_TIMESTAMP(), dt), TIMESTAMPDIFF(DAY, UTC_DATE(), d) FROM t",
		"SELECT TIMESTAMPDIFF(MICROSECOND, `dt`, t.ts), TIMESTAMPDIFF(SECOND, (dt), ((ts))) FROM t",
		"SELECT TIMESTAMPDIFF(/* c */ WEEK, dt, ts), timestampdiff(quarter, d, both) FROM t",
		"SELECT TIMESTAMPDIFF(HOUR, d, dt) FROM t",
		"SELECT id FROM t WHERE TIMESTAMPDIFF(DAY, dt, NOW()) > 30",
		"SELECT id FROM t WHERE TIMESTAMPDIFF(DAY, dt, NOW()) > ? ORDER BY TIMESTAMPDIFF(HOUR, dt, ts)",
		"SELECT TIMESTAMPDIFF(DAY, dt, ts) AS days, COUNT(*) FROM t GROUP BY days ORDER BY days",
		"SELECT TIMESTAMPDIFF(DAY, dt, CURRENT_TIMESTAMP) FROM t",
		// No call: nothing to read.
		"SELECT d FROM t",
		"SELECT 1 FROM t WHERE note = ''",
	} {
		if got := TimestampDiffVeto(Shape(stmt), dates, other); got != "" {
			t.Errorf("TimestampDiffVeto(%q) = %q, want the copy to answer it", stmt, got)
		}
	}
	for _, stmt := range []string{
		// Not a whole-second date column or a clock call: text MySQL reads
		// its own way, a fraction, a number, a literal, an expression.
		"SELECT TIMESTAMPDIFF(DAY, note, NOW()) FROM t",
		"SELECT TIMESTAMPDIFF(DAY, dt6, NOW()) FROM t",
		"SELECT TIMESTAMPDIFF(DAY, id, dt) FROM t",
		"SELECT TIMESTAMPDIFF(DAY, x, dt) FROM t",
		"SELECT TIMESTAMPDIFF(DAY, '2026-01-01', dt) FROM t",
		"SELECT TIMESTAMPDIFF(DAY, dt, ?) FROM t",
		"SELECT TIMESTAMPDIFF(DAY, dt, ts + INTERVAL 1 DAY) FROM t",
		"SELECT TIMESTAMPDIFF(DAY, DATE(dt), ts) FROM t",
		"SELECT TIMESTAMPDIFF(DAY, dt, NOW(3)) FROM t",
		"SELECT TIMESTAMPDIFF(DAY, dt, TIMESTAMPDIFF(DAY, dt, ts)) FROM t",
		"SELECT TIMESTAMPDIFF\v(DAY, note, NOW()) FROM t",
		// A unit this does not rewrite, or other than three arguments.
		"SELECT TIMESTAMPDIFF(SQL_TSI_DAY, dt, ts) FROM t",
		"SELECT TIMESTAMPDIFF(DAY, dt) FROM t",
		// A name the statement may give itself.
		"SELECT dt AS x, TIMESTAMPDIFF(DAY, x, NOW()) FROM t",
		"SELECT TIMESTAMPDIFF(DAY, dt, ts) AS dt FROM t ORDER BY dt",
		"SELECT TIMESTAMPDIFF(DAY, dt, ts) 'n' FROM t",
		"SELECT TIMESTAMPDIFF(DAY, dt, ts) FROM (SELECT note AS dt, ts FROM t) q",
		"WITH q AS (SELECT dt, ts FROM t) SELECT TIMESTAMPDIFF(DAY, dt, ts) FROM q",
		"SELECT db.timestampdiff(DAY, dt, ts) FROM t",
	} {
		if got := TimestampDiffVeto(Shape(stmt), dates, other); got != VetoTimestampDiff {
			t.Errorf("TimestampDiffVeto(%q) = %q, want %q", stmt, got, VetoTimestampDiff)
		}
	}
	// A name in dates and in other counts as other.
	if got := TimestampDiffVeto(Shape("SELECT TIMESTAMPDIFF(DAY, dt, ts) FROM t"), dates, []string{"dt"}); got != VetoTimestampDiff {
		t.Errorf("a name in both lists: %q, want %q", got, VetoTimestampDiff)
	}
}

// #2611: CURDATE() and UTC_DATE() are whole arguments of TIMESTAMPDIFF's two
// dates, where the copy reads them as the same date (dateUses); not its unit.
func TestDateUses_timestampDiff_2611(t *testing.T) {
	for _, c := range []struct {
		stmt string
		ok   bool
	}{
		{"select timestampdiff(year, d, curdate()) from t", true},
		{"select timestampdiff(day, utc_date(), d) from t", true},
		{"select timestampdiff(day, curdate() + interval 1 day, d) from t", false},
		{"select db.timestampdiff(day, curdate(), d) from t", false},
		{"select timestampdiff(curdate(), d, d) from t", false},
	} {
		if _, ok := dateUses(c.stmt); ok != c.ok {
			t.Errorf("dateUses(%q) ok = %v, want %v", c.stmt, ok, c.ok)
		}
	}
}
