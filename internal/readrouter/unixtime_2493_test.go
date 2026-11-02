package readrouter

import "testing"

// The columns the #2493 tests read: dates the copy's unix_timestamp answers,
// whole numbers its from_unixtime answers, and the rest.
var testTimeColumns = TimeColumns{
	Dates:    []string{"o_entry_d", "created_on", "ts"},
	NotDates: []string{"amount", "dt3", "note", "secs", "ms", "f", "price"},
	Whole:    []string{"secs", "ms"},
	NotWhole: []string{"amount", "dt3", "note", "o_entry_d", "created_on", "ts", "f", "price"},
}

// #2493: the copy answers UNIX_TIMESTAMP(col) only over a column known to give
// MySQL's number (a DATE, or a DATETIME or TIMESTAMP without fractional
// seconds). Everything else stays on MySQL.
func TestUnixTimeVeto(t *testing.T) {
	for stmt, want := range map[string]string{
		// The issue's statement: the call is in the select list, before the
		// GROUP BY that names its alias.
		"SELECT FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(o_entry_d) / 3600) * 3600) AS p, COUNT(*) FROM orders GROUP BY p ORDER BY p": "",
		// After GROUP BY, HAVING or ORDER BY a name may be a select alias on
		// MySQL, which could hold anything.
		"SELECT dt3 AS o_entry_d FROM t ORDER BY UNIX_TIMESTAMP(o_entry_d)":                    VetoUnixTime,
		"SELECT COUNT(*) FROM orders GROUP BY FLOOR(UNIX_TIMESTAMP(o_entry_d) / 3600)":         VetoUnixTime,
		"SELECT UNIX_TIMESTAMP(o_entry_d) FROM orders":                                         "",
		"SELECT unix_timestamp( o_entry_d ) FROM orders":                                       "",
		"SELECT UNIX_TIMESTAMP(orders.o_entry_d) FROM orders":                                  "",
		"SELECT UNIX_TIMESTAMP(`O_ENTRY_D`) FROM orders":                                       "", // names compare without case
		"SELECT FLOOR(UNIX_TIMESTAMP(ts) / 86400) AS d FROM t WHERE created_on > '2024-01-01'": "",
		"SELECT UNIX_TIMESTAMP(created_on), UNIX_TIMESTAMP(ts) FROM t":                         "",
		"SELECT UNIX_TIMESTAMP(dt3) FROM t":                                                    VetoUnixTime, // fractional seconds: a decimal on MySQL
		"SELECT UNIX_TIMESTAMP(amount) FROM t":                                                 VetoUnixTime,
		"SELECT UNIX_TIMESTAMP(unknown_col) FROM t":                                            VetoUnixTime,
		"SELECT UNIX_TIMESTAMP('2024-01-01 10:00:00') FROM t":                                  VetoUnixTime, // a literal
		"SELECT UNIX_TIMESTAMP(o_entry_d + INTERVAL 1 DAY) FROM orders":                        VetoUnixTime,
		"SELECT UNIX_TIMESTAMP((o_entry_d)) FROM orders":                                       "",
		"SELECT UNIX_TIMESTAMP(o_entry_d, 1) FROM orders":                                      VetoUnixTime,
		"SELECT UNIX_TIMESTAMP(o_entry_d) FROM (SELECT note AS o_entry_d FROM t) x":            VetoUnixTime, // a derived table may rename
		"WITH x AS (SELECT 1) SELECT UNIX_TIMESTAMP(o_entry_d) FROM orders":                    VetoUnixTime,
		"SELECT unix_timestamp_col FROM t":                                                     "", // not the function
		"SELECT t.unix_timestamp FROM t":                                                       "",
		"SELECT 1 FROM t":                                                                      "",
		// A quoted name is still the function on MySQL, and the copy's macro
		// too.
		"SELECT `UNIX_TIMESTAMP`(`dt3`) FROM `t`":       VetoUnixTime,
		"SELECT `unix_timestamp` (dt3) FROM t":          VetoUnixTime,
		"SELECT `UNIX_TIMESTAMP`(`o_entry_d`) FROM `t`": "",
	} {
		shape := Shape(stmt)
		if got := UnixTimeVeto(string(shape), testTimeColumns); got != want {
			t.Errorf("%s:\n got %q\nwant %q", stmt, got, want)
		}
	}
	// A name both safe and not safe (two tables, two types) is not safe.
	both := TimeColumns{Dates: []string{"ts"}, NotDates: []string{"ts"}}
	if got := UnixTimeVeto(string(Shape("SELECT UNIX_TIMESTAMP(ts) FROM a JOIN b USING (id)")), both); got != VetoUnixTime {
		t.Errorf("a name in both lists: got %q", got)
	}
}

// #2493: MySQL prints FROM_UNIXTIME with as many fraction digits as its
// argument's TYPE has, whatever the value (measured on MySQL 8.0.46:
// FROM_UNIXTIME of a DECIMAL(12,2) holding 1700000000.00 prints .00, of a
// DOUBLE .000000, of ms/1000 .0000), and the copy prints none. So the
// argument must be an expression MySQL types as a whole number: whole-number
// columns, integer literals and UNIX_TIMESTAMP of a date column, joined by +,
// - and *, with / only inside FLOOR or CEIL (CEILING), which MySQL computes
// over the exact quotient and returns as a whole number. The copy divides as
// a DOUBLE, so a quotient holds only UNIX_TIMESTAMP calls and short literals:
// a column there may hold nanoseconds, past the 2^53 a DOUBLE holds exactly.
func TestFromUnixTimeVeto(t *testing.T) {
	for stmt, want := range map[string]string{
		"SELECT FROM_UNIXTIME(secs) FROM t":                                                "",
		"SELECT FROM_UNIXTIME(t.secs) FROM t":                                              "",
		"SELECT FROM_UNIXTIME(secs + 3600) FROM t":                                         "",
		"SELECT FROM_UNIXTIME(secs - 60 * 5) FROM t":                                       "",
		"SELECT FROM_UNIXTIME(-(-secs)) FROM t":                                            "",
		"SELECT FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(ts) / 86400) * 86400) FROM t":           "",
		"SELECT FROM_UNIXTIME(UNIX_TIMESTAMP(ts)) FROM t":                                  "",
		"SELECT FROM_UNIXTIME((secs)) FROM t":                                              "",
		"SELECT `FROM_UNIXTIME`(`secs`) FROM `t`":                                          "",
		"SELECT FROM_UNIXTIME(FLOOR(ms / 1000)) FROM t":                                    VetoFromUnixTime, // a column in a quotient
		"SELECT FROM_UNIXTIME(CEIL(ms / 1000)) FROM t":                                     VetoFromUnixTime,
		"SELECT FROM_UNIXTIME(CEILING(ms / 1000)) FROM t":                                  VetoFromUnixTime,
		"SELECT FROM_UNIXTIME(FLOOR(FLOOR(secs / 60) / 60) * 3600) FROM t":                 VetoFromUnixTime,
		"SELECT FROM_UNIXTIME(CEIL(UNIX_TIMESTAMP(ts) / 60) * 60) FROM t":                  "",
		"SELECT FROM_UNIXTIME(CEILING(UNIX_TIMESTAMP(ts) / 60) * 60) FROM t":               "",
		"SELECT FROM_UNIXTIME(FLOOR(FLOOR(UNIX_TIMESTAMP(ts) / 60) / 60) * 3600) FROM t":   "",
		"SELECT FROM_UNIXTIME(FLOOR((UNIX_TIMESTAMP(ts) + 18000) / 86400) * 86400) FROM t": "",
		"SELECT FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(ts) * 1000 / 7)) FROM t":                VetoFromUnixTime, // no * in a quotient
		"SELECT FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(ts) / 1000000)) FROM t":                 "",
		"SELECT FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(ts) / 10000000)) FROM t":                VetoFromUnixTime, // more than seven digits
		"SELECT FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(ts) DIV 60)) FROM t":                    VetoFromUnixTime,
		"SELECT FROM_UNIXTIME(ms / 1000) FROM t":                                           VetoFromUnixTime, // a decimal on MySQL
		"SELECT FROM_UNIXTIME(f) FROM t":                                                   VetoFromUnixTime, // a DOUBLE
		"SELECT FROM_UNIXTIME(price) FROM t":                                               VetoFromUnixTime, // a DECIMAL(12,2)
		"SELECT FROM_UNIXTIME(FLOOR(f / 3600) * 3600) FROM t":                              VetoFromUnixTime, // a DOUBLE times 3600
		"SELECT FROM_UNIXTIME(secs + 0.5) FROM t":                                          VetoFromUnixTime,
		"SELECT FROM_UNIXTIME(1700000000.0) FROM t":                                        VetoFromUnixTime,
		"SELECT FROM_UNIXTIME(1700000000) FROM t":                                          "",
		"SELECT FROM_UNIXTIME(secs, '%Y') FROM t":                                          VetoFromUnixTime, // the format form
		"SELECT FROM_UNIXTIME(UNIX_TIMESTAMP(dt3)) FROM t":                                 VetoUnixTime,
		"SELECT FROM_UNIXTIME(AVG(secs)) FROM t":                                           VetoFromUnixTime,
		"SELECT FROM_UNIXTIME(secs % 7) FROM t":                                            VetoFromUnixTime,
		"SELECT FROM_UNIXTIME(unknown_col) FROM t":                                         VetoFromUnixTime,
		"SELECT FROM_UNIXTIME(o_entry_d) FROM t":                                           VetoFromUnixTime,
		"SELECT FROM_UNIXTIME(?) FROM t":                                                   VetoFromUnixTime,
		"SELECT FROM_UNIXTIME(@v) FROM t":                                                  VetoFromUnixTime,
		"SELECT FROM_UNIXTIME('1700000000') FROM t":                                        VetoFromUnixTime,
		"SELECT FROM_UNIXTIME(FLOOR(ms / 1000)) FROM t ORDER BY FROM_UNIXTIME(ms)":         VetoFromUnixTime, // after ORDER BY ms may be an alias
		"SELECT FROM_UNIXTIME(secs) FROM (SELECT f AS secs FROM t) x":                      VetoFromUnixTime,
		"SELECT from_unixtime_col FROM t":                                                  "",
		"SELECT t.from_unixtime FROM t":                                                    "",
	} {
		shape := Shape(stmt)
		if got := UnixTimeVeto(string(shape), testTimeColumns); got != want {
			t.Errorf("%s:\n got %q\nwant %q", stmt, got, want)
		}
	}
}

// The no-argument form is "now", in the session's zone: still vetoed from the
// text. The one-argument form is no longer vetoed there; UnixTimeVeto decides
// it from the column types.
func TestVeto_unixTimestampWithoutAnArgumentOnly(t *testing.T) {
	for stmt, vetoed := range map[string]bool{
		"SELECT UNIX_TIMESTAMP()":                          true,
		"SELECT UNIX_TIMESTAMP( ) - 60":                    true,
		"SELECT * FROM t WHERE ts > UNIX_TIMESTAMP() - 60": true,
		"SELECT `UNIX_TIMESTAMP`() FROM `t`":               true,
		"SELECT `unix_timestamp` ( ) FROM t":               true,
		"SELECT UNIX_TIMESTAMP(o_entry_d) FROM orders":     false,
		"SELECT FROM_UNIXTIME(n) FROM t":                   false,
		"SELECT NOW()":                                     false, // #2611: the copy has it
		"SELECT CURTIME()":                                 true,
	} {
		if got := Veto(stmt) != ""; got != vetoed {
			t.Errorf("%s: vetoed = %v (%q), want %v", stmt, got, Veto(stmt), vetoed)
		}
	}
}
