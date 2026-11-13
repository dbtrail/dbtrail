package readrouter

import (
	"strings"
	"testing"
)

// clockOpts is what the routed port passes: the copy's session has MySQL's
// NOW() and CURDATE() (sqlsandbox's macros), and the keywords that mean them
// are written as those calls.
var clockOpts = CopyOptions{MySQLClockKeywords: true}

// #2611: CURRENT_TIMESTAMP, LOCALTIME and LOCALTIMESTAMP are NOW() on MySQL
// and MariaDB, and CURRENT_DATE is CURDATE(), with or without (). The copy
// reads the keywords as its own (a TIMESTAMP WITH TIME ZONE with
// microseconds), so under routing they are written as the calls and then
// held to the rules the calls are: the same places, the same arithmetic
// vetoes, the same clock check and the same named-zone refusal.
func TestVeto_nowKeywords_2611(t *testing.T) {
	for _, stmt := range []string{
		"SELECT CURRENT_TIMESTAMP", "SELECT CURRENT_TIMESTAMP()", "SELECT current_timestamp ( )",
		"SELECT LOCALTIME", "SELECT LOCALTIME()", "SELECT LOCALTIMESTAMP", "SELECT LocalTimestamp()",
		"SELECT CURRENT_DATE", "SELECT CURRENT_DATE()",
		"SELECT * FROM t WHERE ts >= CURRENT_TIMESTAMP - INTERVAL 1 DAY",
		"SELECT * FROM t WHERE ts >= LOCALTIMESTAMP - INTERVAL 30 DAY AND ts < LOCALTIME",
		"SELECT * FROM t WHERE d >= CURRENT_DATE - INTERVAL 7 DAY",
		"SELECT * FROM t WHERE d BETWEEN CURRENT_DATE() - INTERVAL 7 DAY AND CURRENT_DATE",
		"SELECT * FROM t WHERE d = CURRENT_DATE AND ts < NOW()",
		"SELECT YEAR(CURRENT_DATE), MONTH(CURRENT_TIMESTAMP) FROM t",
		"SELECT * FROM t WHERE ts >= CURRENT_TIMESTAMP /* now */ - INTERVAL 1 DAY",
		"SELECT * FROM t WHERE d >= CURRENT_DATE - INTERVAL 7 DAY AND k > ?",
		// Inside a string or a comment.
		"SELECT * FROM t WHERE note = 'CURRENT_DATE - 1'", "SELECT 1 /* CURRENT_DATE - 1 */",
	} {
		if got := VetoOpts(stmt, clockOpts); got != "" {
			t.Errorf("VetoOpts(%q) = %q, want the copy to answer it", stmt, got)
		}
	}
	for _, stmt := range []string{
		// A fraction of a second.
		"SELECT CURRENT_TIMESTAMP(3)", "SELECT CURRENT_TIMESTAMP( 6 )", "SELECT LOCALTIME(6)", "SELECT LOCALTIMESTAMP(1)",
		"SELECT CURRENT_TIMESTAMP(0)",
		// A time of day, and the time each call runs at.
		"SELECT CURRENT_TIME", "SELECT CURRENT_TIME()", "SELECT CURTIME()", "SELECT UTC_TIME()", "SELECT SYSDATE()",
		// Arithmetic that makes them numbers on MySQL.
		"SELECT CURRENT_DATE - 1", "SELECT CURRENT_DATE + 1", "SELECT CURRENT_TIMESTAMP + 0", "SELECT 2 * LOCALTIME",
		"SELECT CURRENT_TIMESTAMP - ts FROM t", "SELECT SUM(CURRENT_DATE) FROM t", "SELECT AVG(LOCALTIMESTAMP) FROM t",
		"SELECT CURRENT_TIMESTAMP + INTERVAL 1 DAY * 2", "SELECT CURRENT_DATE() MOD 7",
		// Where CURDATE() stays MySQL's, CURRENT_DATE does too.
		"SELECT CURRENT_DATE - INTERVAL 7 DAY", "SELECT DATE_ADD(CURRENT_DATE, INTERVAL 1 DAY)",
		"SELECT * FROM t WHERE CURRENT_DATE = '2026-11-02 10:00:00'", "SELECT * FROM t WHERE CURRENT_DATE = ?",
		"SELECT COALESCE(s, CURRENT_DATE) FROM t", "SELECT * FROM (SELECT note AS d FROM ev) x WHERE d = CURRENT_DATE",
		"SELECT DATE(CURRENT_TIMESTAMP) - INTERVAL 1 DAY",
		// A name, not the keyword (quoted, or after a dot): kept on MySQL as
		// before, since the copy may still read it as its own function.
		"SELECT `current_date` FROM t", "SELECT t.current_date FROM t", "SELECT `localtime` FROM t",
		// An INTERVAL amount that is a placeholder is not read as a step, as
		// with CURDATE().
		"SELECT * FROM t WHERE d >= CURRENT_DATE - INTERVAL ? DAY",
	} {
		if got := VetoOpts(stmt, clockOpts); got == "" {
			t.Errorf("VetoOpts(%q) = none, want MySQL to answer it", stmt)
		}
	}
	// Without the option (the SQL comparison harness, Veto) the keywords stay
	// MySQL's: the copy would read them as its own.
	for _, stmt := range []string{
		"SELECT CURRENT_TIMESTAMP", "SELECT CURRENT_DATE()", "SELECT LOCALTIME",
		"SELECT * FROM t WHERE ts >= CURRENT_TIMESTAMP - INTERVAL 1 DAY",
	} {
		if got := Veto(stmt); got != vetoNowKeyword {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, vetoNowKeyword)
		}
	}
}

// The text the copy is sent: each keyword as the call it means, the client's
// parentheses kept, and nothing written where the word is a name, a string
// or a comment.
func TestForCopy_nowKeywords_2611(t *testing.T) {
	for stmt, want := range map[string]string{
		"SELECT CURRENT_TIMESTAMP":                                                "SELECT now()",
		"SELECT CURRENT_TIMESTAMP()":                                              "SELECT now()",
		"SELECT current_timestamp ( )":                                            "SELECT now ( )",
		"SELECT LOCALTIME, LOCALTIMESTAMP, LOCALTIME(), LOCALTIMESTAMP()":         "SELECT now(), now(), now(), now()",
		"SELECT CURRENT_DATE, CURRENT_DATE()":                                     "SELECT curdate(), curdate()",
		"SELECT * FROM `t` WHERE `d` >= CURRENT_DATE - INTERVAL 7 DAY":            `SELECT * FROM "t" WHERE "d" >= curdate() - INTERVAL 7 DAY`,
		"SELECT * FROM t WHERE ts >= CURRENT_TIMESTAMP/* x */() - INTERVAL 1 DAY": "SELECT * FROM t WHERE ts >= now/* x */() - INTERVAL 1 DAY",
		"SELECT `current_date`, t.current_date, t . current_date FROM t":          `SELECT "current_date", t.current_date, t . current_date FROM t`,
		"SELECT 'CURRENT_DATE', 1 /* LOCALTIME */":                                "SELECT 'CURRENT_DATE', 1 /* LOCALTIME */",
		"SELECT current_dates, localtimes FROM t":                                 "SELECT current_dates, localtimes FROM t",
	} {
		got, why := ForCopyOpts(stmt, clockOpts)
		if why != "" {
			t.Errorf("ForCopyOpts(%q) refused: %s", stmt, why)
			continue
		}
		if got != want {
			t.Errorf("ForCopyOpts(%q)\n got %q\nwant %q", stmt, got, want)
		}
		// Without the option nothing is written.
		if got, _ := ForCopy(stmt); strings.Contains(strings.ToLower(stmt), "current_date") && !strings.Contains(strings.ToLower(got), "current_date") {
			t.Errorf("ForCopy(%q) = %q: rewritten without the option", stmt, got)
		}
	}
	// A prepared statement is rewritten one piece at a time, between its
	// placeholders (shim copyParts): each piece reads the same.
	for part, want := range map[string]string{
		"SELECT * FROM t WHERE d >= CURRENT_DATE - INTERVAL ": "SELECT * FROM t WHERE d >= curdate() - INTERVAL ",
		" DAY AND ts < CURRENT_TIMESTAMP":                     " DAY AND ts < now()",
	} {
		if got, why := ForCopyOpts(part, clockOpts); why != "" || got != want {
			t.Errorf("ForCopyOpts(%q) = %q (%s), want %q", part, got, why, want)
		}
	}
}

// CallsNow and DateComparedColumns see the keywords as the calls, so the
// clock check (shim) and the named-zone and column-type refusals (console)
// apply to them as to NOW() and CURDATE().
func TestCallsNow_nowKeywords_2611(t *testing.T) {
	for stmt, want := range map[string]bool{
		"SELECT CURRENT_TIMESTAMP":                                 true,
		"SELECT * FROM t WHERE d >= CURRENT_DATE - INTERVAL 1 DAY": true,
		"SELECT LOCALTIME()":                                       true,
		"SELECT LOCALTIMESTAMP":                                    true,
		"SELECT `current_date` FROM t":                             false,
		"SELECT t.current_timestamp FROM t":                        false,
		"SELECT 'CURRENT_DATE'":                                    false,
	} {
		if got := CallsNow(stmt); got != want {
			t.Errorf("CallsNow(%q) = %v, want %v", stmt, got, want)
		}
		if got := ShapeOf(stmt).CallsNow(); got != want {
			t.Errorf("ShapeOf(%q).CallsNow() = %v, want %v", stmt, got, want)
		}
	}
	if got := strings.Join(DateComparedColumns("SELECT * FROM t WHERE d >= CURRENT_DATE - INTERVAL 7 DAY AND CURRENT_DATE() > e"), ","); got != "d,e" {
		t.Errorf("DateComparedColumns = %q, want d,e", got)
	}
}
