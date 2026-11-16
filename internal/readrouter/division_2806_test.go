package readrouter

import (
	"strings"
	"testing"
)

// #2806: MySQL's / over exact numbers and AVG are a DECIMAL with
// div_precision_increment more digits (k / 7 is 0.1429), the copy's a DOUBLE
// (0.14285714285714285): measured through the port on MySQL 8.0.46. Every /
// stays on MySQL, but one inside FROM_UNIXTIME's argument, which UnixTimeVeto
// reads whole; so do AVG and MEDIAN, and a whole number written from 2^63 up.
func TestVeto_divisionAvgAndBigNumbers(t *testing.T) {
	want := map[string]string{
		"SELECT k / 7 FROM t":                       vetoDivision,
		"SELECT k/7 FROM t":                         vetoDivision,
		"SELECT -k / -7 FROM t":                     vetoDivision,
		"SELECT 1 / 2":                              vetoDivision,
		"SELECT k / 0 FROM t":                       vetoDivision,
		"SELECT a FROM t WHERE k / 3 > 1":           vetoDivision,
		"SELECT a FROM t ORDER BY k / 3":            vetoDivision,
		"SELECT a, k / 3 AS q FROM t GROUP BY a, q": vetoDivision,
		"SELECT FLOOR(k / 7), CEIL(k / 3) FROM t":   vetoDivision,
		"SELECT SUM(k) / COUNT(*) FROM t":           vetoDivision,
		"SELECT k / ? FROM t":                       vetoDivision,
		"SELECT YEAR(d) / 7 FROM t":                 vetoDivision,
		"SELECT dbl / 2 FROM t":                     vetoDivision,
		"SELECT k // 2 FROM t":                      vetoDivision,
		"SELECT k /\n7 FROM t":                      vetoDivision,
		"SELECT `a/b`, c / 2 FROM t":                vetoDivision,
		"SELECT 'x/y', c / 2 FROM t":                vetoDivision,
		"SELECT FROM_UNIXTIME(ts) / 2 FROM t":       vetoDivision,
		"SELECT (FROM_UNIXTIME(ts)) / 2 FROM t":     vetoDivision,
		"SELECT FROM_UNIXTIME(ts), k / 2 FROM t":    vetoDivision,
		"SELECT k / 2, FROM_UNIXTIME(ts) FROM t":    vetoDivision,
		"SELECT db.from_unixtime(k / 2) FROM t":     vetoDivision, // a stored function on MySQL
		"SELECT myfrom_unixtime(k / 2) FROM t":      vetoDivision,
		"SELECT FLOOR(UNIX_TIMESTAMP(ts) / 3600) AS h, COUNT(*) FROM t GROUP BY h": vetoDivision,
		"SELECT a FROM t WHERE k > 0) / (2":                                        vetoDivision, // a ) with none open ends nothing

		"SELECT AVG(k) FROM t":                          vetoAvg,
		"SELECT avg (k) FROM t":                         vetoAvg,
		"SELECT AVG\v(k) FROM t":                        vetoAvg,
		"SELECT Avg(\nk) FROM t":                        vetoAvg,
		"SELECT AVG(dbl) FROM t":                        vetoAvg,
		"SELECT a, AVG(k) OVER (PARTITION BY a) FROM t": vetoAvg,
		"SELECT a FROM t GROUP BY a HAVING AVG(k) > 1":  vetoAvg,
		"SELECT MEDIAN(k) OVER () FROM t":               vetoAvg,
		"SELECT t.avg(k) FROM t":                        vetoAvg,

		"SELECT 9223372036854775808 FROM t":                  vetoBigInteger,
		"SELECT -9223372036854775808 FROM t":                 vetoBigInteger,
		"SELECT 18446744073709551615 FROM t":                 vetoBigInteger,
		"SELECT 99999999999999999999 FROM t":                 vetoBigInteger,
		"SELECT a FROM t WHERE b = 09223372036854775808":     vetoBigInteger,
		"SELECT 1000000000000000000,9223372036854775808":     vetoBigInteger,
		"SELECT a FROM t WHERE b IN (1,9223372036854775808)": vetoBigInteger,
		"SELECT b+9223372036854775808 FROM t":                vetoBigInteger,
	}
	for stmt, w := range want {
		if got := Veto(stmt); got != w {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, w)
		}
	}
	clean := []string{
		"SELECT SUM(k), COUNT(*), MIN(k), MAX(k) FROM t",
		"SELECT SUM(k) * 1.5, SUM(k) + 1, -SUM(k) FROM t",
		"SELECT k * 2, k % 3, MOD(k, 3) FROM t",
		"SELECT `a/b` FROM t",
		"SELECT `a/b` AS `c/d` FROM t",
		"SELECT a FROM t WHERE s = 'a/b'",
		"SELECT a /* k / 2 */ FROM t",
		"SELECT a FROM t -- k / 2",
		"SELECT FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(ts) / 3600) * 3600) FROM t",
		"SELECT from_unixtime (floor(unix_timestamp(ts)/60)*60) AS m, COUNT(*) FROM t GROUP BY m",
		"SELECT FROM_UNIXTIME(CEIL((UNIX_TIMESTAMP(ts) + 1) / 86400) * 86400) FROM t",
		"SELECT FROM_UNIXTIME(FROM_UNIXTIME(1) / 2) FROM t", // inside the argument: UnixTimeVeto's
		"SELECT avg FROM t",
		"SELECT t.avg, avg_k FROM t",
		"SELECT average(k) FROM t",
		"SELECT myavg(k) FROM t",
		"SELECT 9223372036854775807, -9223372036854775807 FROM t",
		"SELECT 1000000000000000000 FROM t",
		"SELECT 9223372036854775808.5 FROM t",
		"SELECT 0.9223372036854775808 FROM t",
		"SELECT a9223372036854775808 FROM t",
		"SELECT `x9223372036854775808` FROM t",
		"SELECT a FROM t WHERE s = '9223372036854775808'",
	}
	for _, stmt := range clean {
		if got := Veto(stmt); got != "" {
			t.Errorf("Veto(%q) = %q, want none", stmt, got)
		}
	}
}

// The same under the options a routed server runs with, and through
// ForCopy's backticks: the rule reads the text the copy is sent.
func TestVetoOpts_divisionUnderRouting(t *testing.T) {
	o := CopyOptions{LikeIgnoresCase: true, MySQLClockKeywords: true, MySQLDateFunctions: true, MySQLTimestampDiff: true, MySQLDateAdd: true}
	if got := VetoOpts("SELECT `k` / 7 FROM `t` ORDER BY `k`", o); got != vetoDivision {
		t.Errorf("VetoOpts(k / 7) = %q, want %q", got, vetoDivision)
	}
	if got := VetoOpts("SELECT AVG(`k`) FROM `t`", o); got != vetoAvg {
		t.Errorf("VetoOpts(AVG) = %q, want %q", got, vetoAvg)
	}
	if got := VetoOpts("SELECT SUM(`k`) FROM `t`", o); got != "" {
		t.Errorf("VetoOpts(SUM) = %q, want none", got)
	}
}

// One pass: a statement of a million bytes of unclosed FROM_UNIXTIME calls
// and quoted names is read without growing faster than its length.
func TestVeto_divisionIsLinear(t *testing.T) {
	long := "SELECT " + strings.Repeat("FROM_UNIXTIME(", 50000) + strings.Repeat(`"a""b" `, 50000)
	if got := divisionVeto(long, false); got != "" {
		t.Errorf("divisionVeto(unclosed calls) = %q, want none", got)
	}
	if got := divisionVeto(long+" ) / 2", false); got != "" {
		t.Errorf("divisionVeto(a / still inside the calls) = %q, want none", got)
	}
	if got := divisionVeto(strings.Repeat("(", 100000)+"1/2", false); got != vetoDivision {
		t.Errorf("divisionVeto(deep) = %q, want %q", got, vetoDivision)
	}
	if got := divisionVeto(strings.Repeat("1,", 200000)+"9223372036854775808", false); got != vetoBigInteger {
		t.Errorf("divisionVeto(many numbers) = %q, want %q", got, vetoBigInteger)
	}
}
