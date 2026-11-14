package readrouter

import (
	"regexp"
	"testing"
)

// dateAddOpts are the routing port's options (shim's copyOpts).
var dateAddOpts = CopyOptions{MySQLClockKeywords: true, MySQLDateFunctions: true, MySQLDateSub: true, MySQLDateAdd: true}

// #2800: under routing, a date stepped by an INTERVAL with DATE_ADD,
// ADDDATE, + or - is sent to the copy inside its macros, which decline a
// result outside the years 1 to 9999 (NULL on MySQL). A step the rewrite
// cannot wrap with certainty stays on MySQL.
func TestForCopyDateAdd_2800(t *testing.T) {
	for stmt, want := range map[string]string{
		"SELECT dt + INTERVAL 1 DAY FROM t":                   "SELECT _bintrail_date_add(dt, INTERVAL 1 DAY) FROM t",
		"SELECT dt - INTERVAL 1 DAY FROM t":                   "SELECT _bintrail_date_sub(dt, INTERVAL 1 DAY) FROM t",
		"SELECT dt+INTERVAL 1 DAY FROM t":                     "SELECT _bintrail_date_add(dt, INTERVAL 1 DAY) FROM t",
		"select dt + interval '7' day from t":                 "select _bintrail_date_add(dt, interval '7' day) from t",
		"SELECT DATE_ADD(dt, INTERVAL 1 DAY) FROM t":          "SELECT _bintrail_date_add(dt, INTERVAL 1 DAY) FROM t",
		"SELECT adddate(dt, interval 1 day) FROM t":           "SELECT _bintrail_date_add(dt, interval 1 day) FROM t",
		"SELECT `t`.`dt` + INTERVAL 1 DAY FROM `t`":           `SELECT _bintrail_date_add("t"."dt", INTERVAL 1 DAY) FROM "t"`,
		"SELECT `interval` + INTERVAL 1 DAY FROM t":           `SELECT _bintrail_date_add("interval", INTERVAL 1 DAY) FROM t`,
		"SELECT db.t.dt + INTERVAL 1 DAY FROM db.t":           "SELECT _bintrail_date_add(db.t.dt, INTERVAL 1 DAY) FROM db.t",
		"SELECT dt + INTERVAL 1 DAY + INTERVAL 2 HOUR FROM t": "SELECT _bintrail_date_add(_bintrail_date_add(dt, INTERVAL 1 DAY), INTERVAL 2 HOUR) FROM t",
		"SELECT dt + INTERVAL 1 DAY - INTERVAL 2 HOUR FROM t": "SELECT _bintrail_date_sub(_bintrail_date_add(dt, INTERVAL 1 DAY), INTERVAL 2 HOUR) FROM t",
		"SELECT (dt + INTERVAL 1 DAY) FROM t":                 "SELECT (_bintrail_date_add(dt, INTERVAL 1 DAY)) FROM t",
		"SELECT (dt) + INTERVAL 1 DAY FROM t":                 "SELECT _bintrail_date_add((dt), INTERVAL 1 DAY) FROM t",
		"SELECT DATE(dt) + INTERVAL 1 DAY FROM t":             "SELECT _bintrail_date_add(DATE(dt), INTERVAL 1 DAY) FROM t",
		"SELECT DATE_ADD(dt + INTERVAL 1 DAY, INTERVAL 1 HOUR) FROM t": "SELECT _bintrail_date_add(_bintrail_date_add(dt, INTERVAL 1 DAY), INTERVAL 1 HOUR) FROM t",
		"SELECT DATE_ADD(DATE_ADD(dt, INTERVAL 1 DAY), INTERVAL 1 HOUR) FROM t": "SELECT _bintrail_date_add(_bintrail_date_add(dt, INTERVAL 1 DAY), INTERVAL 1 HOUR) FROM t",
		"SELECT DATE_ADD(dt, INTERVAL 1 DAY) + INTERVAL 1 HOUR FROM t":          "SELECT _bintrail_date_add(_bintrail_date_add(dt, INTERVAL 1 DAY), INTERVAL 1 HOUR) FROM t",
		"SELECT dt /* c */ + /* d */ INTERVAL 1 DAY FROM t":                     "SELECT _bintrail_date_add(dt, INTERVAL 1 DAY) FROM t",
		"SELECT dt + INTERVAL /* c */ 1 DAY FROM t":                             "SELECT _bintrail_date_add(dt, INTERVAL /* c */ 1 DAY) FROM t",
		"SELECT CASE WHEN a THEN dt + INTERVAL 1 DAY ELSE dt END FROM t":        "SELECT CASE WHEN a THEN _bintrail_date_add(dt, INTERVAL 1 DAY) ELSE dt END FROM t",
		"SELECT COUNT(DISTINCT dt + INTERVAL 1 DAY) FROM t":                     "SELECT COUNT(DISTINCT _bintrail_date_add(dt, INTERVAL 1 DAY)) FROM t",
		"SELECT a FROM t WHERE dt + INTERVAL 1 DAY > ? AND NOT dt - INTERVAL 1 DAY < dt2": "SELECT a FROM t WHERE _bintrail_date_add(dt, INTERVAL 1 DAY) > ? AND NOT _bintrail_date_sub(dt, INTERVAL 1 DAY) < dt2",
		"SELECT a FROM t WHERE x BETWEEN dt - INTERVAL 1 DAY AND dt + INTERVAL 1 DAY":     "SELECT a FROM t WHERE x BETWEEN _bintrail_date_sub(dt, INTERVAL 1 DAY) AND _bintrail_date_add(dt, INTERVAL 1 DAY)",
		"SELECT a FROM t WHERE created_on > seen_at - INTERVAL 7 DAY ORDER BY dt + INTERVAL 1 DAY": "SELECT a FROM t WHERE created_on > _bintrail_date_sub(seen_at, INTERVAL 7 DAY) ORDER BY _bintrail_date_add(dt, INTERVAL 1 DAY)",
		"SELECT TIMESTAMP '9999-12-31 00:00:00' + INTERVAL 1 DAY":                                  "SELECT _bintrail_date_add(TIMESTAMP '9999-12-31 00:00:00', INTERVAL 1 DAY)",
		"SELECT DATE '9999-12-31' - INTERVAL 1 DAY":                                                "SELECT _bintrail_date_sub(DATE '9999-12-31', INTERVAL 1 DAY)",
		"SELECT dt + INTERVAL 1.5 DAY FROM t":                                                      "SELECT _bintrail_date_add(dt, INTERVAL 1.5 DAY) FROM t",
		// NOW() and the other clocks stepped by less than a thousand
		// years are inside the range whatever the clock says: sent as
		// written, as before.
		"SELECT a FROM t WHERE dt >= NOW() - INTERVAL 30 DAY":                      "SELECT a FROM t WHERE dt >= NOW() - INTERVAL 30 DAY",
		"SELECT a FROM t WHERE d >= CURRENT_DATE - INTERVAL 30 DAY":                "SELECT a FROM t WHERE d >= curdate() - INTERVAL 30 DAY",
		"SELECT a FROM t WHERE ts BETWEEN UTC_TIMESTAMP() - INTERVAL '7' DAY AND UTC_TIMESTAMP() + INTERVAL 1000 YEAR": "SELECT a FROM t WHERE ts BETWEEN UTC_TIMESTAMP() - INTERVAL '7' DAY AND UTC_TIMESTAMP() + INTERVAL 1000 YEAR",
		"SELECT NOW() + INTERVAL 1001 YEAR":                                        "SELECT _bintrail_date_add(NOW(), INTERVAL 1001 YEAR)",
		"SELECT NOW() + INTERVAL 1 MICROSECOND":                                    "SELECT _bintrail_date_add(NOW(), INTERVAL 1 MICROSECOND)",
		// Not a step: the word is a name, or an argument.
		"SELECT t.interval + 1 FROM t":                       "SELECT t.interval + 1 FROM t",
		"SELECT DATE_SUB(dt, INTERVAL 1 DAY) FROM t":         "SELECT _bintrail_date_sub(dt, INTERVAL 1 DAY) FROM t",
		"SELECT 'dt + INTERVAL 1 DAY' FROM t":                "SELECT 'dt + INTERVAL 1 DAY' FROM t",
		"SELECT dt FROM t -- + INTERVAL 1 DAY\n":              "SELECT dt FROM t -- + INTERVAL 1 DAY\n",
	} {
		got, refusal := ForCopyOpts(stmt, dateAddOpts)
		if refusal != "" || got != want {
			t.Errorf("ForCopyOpts(%q)\n  = %q, %q\n  want %q", stmt, got, refusal, want)
		}
		if v := VetoOpts(stmt, dateAddOpts); v == VetoDateAdd {
			t.Errorf("VetoOpts(%q) = %q, but the rewrite wraps it", stmt, v)
		}
	}
	// Without the option nothing changes: the port without routing has no
	// such macros.
	for _, stmt := range []string{"SELECT dt + INTERVAL 1 DAY FROM t", "SELECT DATE_ADD(dt, INTERVAL 1 DAY) FROM t"} {
		if got, refusal := ForCopyOpts(stmt, CopyOptions{MySQLDateSub: true}); got != stmt || refusal != "" {
			t.Errorf("without MySQLDateAdd ForCopyOpts(%q) = %q, %q", stmt, got, refusal)
		}
		if v := VetoOpts(stmt, CopyOptions{}); v == VetoDateAdd {
			t.Errorf("without MySQLDateAdd VetoOpts(%q) = %q", stmt, v)
		}
	}
}

// A step the rewrite cannot wrap: refused by ForCopyOpts and kept on MySQL
// by VetoOpts, so the copy is never sent it unguarded.
func TestDateAddKeptOnMySQL_2800(t *testing.T) {
	for _, stmt := range []string{
		"SELECT a - dt + INTERVAL 1 DAY FROM t",        // the operand is a - dt
		"SELECT -dt + INTERVAL 1 DAY FROM t",           // the operand is -dt
		"SELECT 2 * dt + INTERVAL 1 DAY FROM t",        // the operand is 2 * dt
		"SELECT x || dt + INTERVAL 1 DAY FROM t",       // OR on MySQL, text on the copy
		"SELECT INTERVAL 1 DAY + dt FROM t",            // the INTERVAL first
		"SELECT (INTERVAL 1 DAY + dt) FROM t",          // the INTERVAL first
		"SELECT f(INTERVAL 1 DAY + dt) FROM t",         // the INTERVAL first
		"SELECT dt + INTERVAL 1 DAY * 2 FROM t",        // binds tighter than + on the copy
		"SELECT dt + INTERVAL 1 DAY / 2 FROM t",        // binds tighter than + on the copy
		"SELECT dt + INTERVAL -1 DAY FROM t",           // a sign the copy refuses
		"SELECT dt + INTERVAL (1) DAY FROM t",          // computed amount
		"SELECT dt + INTERVAL ? DAY FROM t",            // placeholder amount
		"SELECT ? + INTERVAL 1 DAY",                    // placeholder operand
		"SELECT (?) + INTERVAL 1 DAY",                  // placeholder in the operand
		"SELECT DATE(?) + INTERVAL 1 DAY",              // placeholder in the operand
		"SELECT CASE WHEN a THEN dt END + INTERVAL 1 DAY FROM t",
		"SELECT '2026-01-01' '10:00:00' + INTERVAL 1 DAY", // two literals, one string on MySQL
		"SELECT @v + INTERVAL 1 DAY",
		"SELECT DATE_ADD(?, INTERVAL 1 DAY)",
		"SELECT DATE_ADD(dt, INTERVAL ? DAY) FROM t",
		"SELECT ADDDATE(dt, 1) FROM t",
		"SELECT DATE_ADD(dt) FROM t",
		"SELECT db.date_add(dt, INTERVAL 1 DAY) FROM t",
	} {
		if v := VetoOpts(stmt, dateAddOpts); v == "" {
			t.Errorf("VetoOpts(%q) = \"\", want it kept on MySQL", stmt)
		}
		if got, refusal := ForCopyOpts(stmt, dateAddOpts); refusal == "" && unwrappedStep(got) {
			t.Errorf("ForCopyOpts(%q) = %q with an unguarded step and no refusal", stmt, got)
		}
	}
	// A prepared statement's piece after a placeholder: no operand in it.
	if _, refusal := ForCopyOpts(" + INTERVAL 1 DAY FROM t", dateAddOpts); refusal == "" {
		t.Error("a piece that starts with the step was not refused")
	}
}

// The seam the two must agree on: every statement VetoOpts lets through is
// sent by ForCopyOpts with no step left outside the macros.
func TestDateAddVetoAndRewriteAgree_2800(t *testing.T) {
	for _, stmt := range []string{
		"SELECT dt + INTERVAL 1 DAY FROM t", "SELECT dt - INTERVAL 1 MONTH FROM t WHERE dt > NOW() - INTERVAL 1 DAY",
		"SELECT DATE_ADD(dt, INTERVAL 1 DAY), ADDDATE(ts, INTERVAL 2 HOUR) FROM t",
		"SELECT (dt + INTERVAL 1 DAY) + INTERVAL 1 HOUR FROM t", "SELECT f(g(dt)) + INTERVAL 1 DAY FROM t",
		"SELECT dt + INTERVAL 1 DAY AS x FROM t ORDER BY x", "SELECT `dt` + INTERVAL 1 DAY x FROM `t`",
		"SELECT a FROM t WHERE k > ? AND dt + INTERVAL 1 DAY > ?",
		"SELECT a FROM t WHERE dt >= CURRENT_TIMESTAMP - INTERVAL 1 DAY",
	} {
		if v := VetoOpts(stmt, dateAddOpts); v != "" {
			continue
		}
		got, refusal := ForCopyOpts(stmt, dateAddOpts)
		if refusal != "" || unwrappedStep(got) && !clockStepOnly(got) {
			t.Errorf("VetoOpts(%q) passed, ForCopyOpts = %q, %q", stmt, got, refusal)
		}
	}
}

var (
	stepOutside   = regexp.MustCompile(`(?i)[+-]\s*interval\b|(^|[^\w.])(date_add|adddate)\s*\(`)
	clockStepText = regexp.MustCompile(`(?i)(now|curdate|utc_timestamp)\(\)\s*[+-]\s*interval\s+\S+\s+\w+`)
)

// unwrappedStep reports whether the copy's text holds a step outside the
// macros: a + or - before INTERVAL, or DATE_ADD or ADDDATE called.
func unwrappedStep(text string) bool { return stepOutside.MatchString(scan(text).blanked) }

// clockStepOnly reports whether every unwrapped step is of a clock call.
func clockStepOnly(text string) bool {
	return !stepOutside.MatchString(clockStepText.ReplaceAllString(scan(text).blanked, "x"))
}
