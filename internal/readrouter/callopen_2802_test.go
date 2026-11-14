package readrouter

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// mysqlCallSpaces are the bytes MySQL takes between a function's name and its
// parenthesis, each named for a test's label (#2802).
var mysqlCallSpaces = []struct{ name, sep string }{
	{"none", ""}, {"space", " "}, {"tab", "\t"}, {"line feed", "\n"},
	{"vertical tab", "\v"}, {"form feed", "\f"}, {"carriage return", "\r"},
	{"two of them", "\v \f"},
}

// #2802: callOpen takes exactly the white space MySQL 8.0.46 takes between a
// name and its parenthesis: a space and 0x09 to 0x0D. Every other byte below
// 0x20 is a syntax error on MySQL, and so is any other character there.
func TestCallOpen_mysqlWhiteSpace_2802(t *testing.T) {
	re := regexp.MustCompile(`^f` + callOpen)
	for b := 0; b < 0x80; b++ {
		if b == '(' {
			continue
		}
		want := b == ' ' || (b >= 0x09 && b <= 0x0d)
		if got := re.MatchString("f" + string(rune(b)) + "(x)"); got != want {
			t.Errorf("callOpen over byte 0x%02x before the parenthesis: matched %v, want %v", b, got, want)
		}
	}
	for _, s := range []string{"f(x)", "f \v\t\n\f\r(x)"} {
		if !re.MatchString(s) {
			t.Errorf("callOpen does not match %q", s)
		}
	}
	for _, s := range []string{"f x(", "f", "f)"} {
		if re.MatchString(s) {
			t.Errorf("callOpen matches %q", s)
		}
	}
}

// #2802: under routing, DATEDIFF and DATE_FORMAT with any white space MySQL
// takes before the parenthesis are read as the calls they are. A vertical
// tab there hid the call from DateFunctionVeto while the rewrite, which reads
// the arguments with callArgs, still sent it to the copy as date_diff: the
// copy then cast a string with an offset to its own date, a day off
// MySQL's answer.
func TestDateFunctionVeto_whiteSpaceBeforeParenthesis_2802(t *testing.T) {
	dates := []string{"d", "dt"}
	other := []string{"s", "k"}
	vetoed := []string{
		"SELECT DATEDIFF{}('2026-01-15 02:00:00+05:00', d) FROM t",
		"SELECT DATEDIFF{}(d, '2026-01-15 22:00:00-05:00') FROM t",
		"SELECT DATEDIFF{}(s, d) FROM t",
		"SELECT id FROM t WHERE DATEDIFF{}(d, ?) > 3",
		"SELECT datediff{}(d, d), DATEDIFF{}(dt + INTERVAL 1 DAY, d) FROM t",
		"SELECT DATE_FORMAT{}('2026-01-15 02:00:00+05:00', '%Y-%m-%d') FROM t",
		"SELECT DATE_FORMAT{}(dt + INTERVAL 1 DAY, '%Y-%m-%d') FROM t",
		"SELECT Date_Format{}(s, '%Y') FROM t",
	}
	answered := []string{
		"SELECT DATEDIFF{}(dt, d) FROM t",
		"SELECT DATEDIFF{}(CURDATE(), d) FROM t",
		"SELECT DATE_FORMAT{}(dt, '%Y-%m') FROM t",
		"SELECT DATE_FORMAT{}(NOW(), '%Y') FROM t",
	}
	for _, sp := range mysqlCallSpaces {
		for _, tmpl := range vetoed {
			stmt := strings.ReplaceAll(tmpl, "{}", sp.sep)
			if got := DateFunctionVeto(Shape(stmt), dates, other); got != VetoDateFunction {
				t.Errorf("%s: DateFunctionVeto(%q) = %q, want it kept on MySQL", sp.name, stmt, got)
			}
		}
		for _, tmpl := range answered {
			stmt := strings.ReplaceAll(tmpl, "{}", sp.sep)
			if got := DateFunctionVeto(Shape(stmt), dates, other); got != "" {
				t.Errorf("%s: DateFunctionVeto(%q) = %q, want the copy to answer it", sp.name, stmt, got)
			}
		}
	}
}

// #2802: a DATEDIFF the rewrite leaves as written (not two arguments) stays
// on MySQL whatever white space stands before its parenthesis, and one it
// rewrites is sent as date_diff, the white space gone: the control and the
// rewrite read the same calls.
func TestVetoOpts_dateDiffWhiteSpace_2802(t *testing.T) {
	for _, sp := range mysqlCallSpaces {
		for _, stmt := range []string{
			"SELECT DATEDIFF" + sp.sep + "(d) FROM t",
			"SELECT DATEDIFF" + sp.sep + "('day', d, d2) FROM t",
			"SELECT DATEDIFF(d, d2), DATEDIFF" + sp.sep + "() FROM t",
		} {
			if got := VetoOpts(stmt, routedOpts); got != vetoDateDiff {
				t.Errorf("%s: VetoOpts(%q) = %q, want %q", sp.name, stmt, got, vetoDateDiff)
			}
		}
		stmt := "SELECT DATEDIFF" + sp.sep + "(d, d2) FROM t"
		if got := VetoOpts(stmt, routedOpts); got != "" {
			t.Errorf("%s: VetoOpts(%q) = %q, want the copy to answer it", sp.name, stmt, got)
		}
		text, refusal := ForCopyOpts(stmt, routedOpts)
		if want := "SELECT date_diff('day', CAST(d2 AS DATE), CAST(d AS DATE)) FROM t"; refusal != "" || text != want {
			t.Errorf("%s: ForCopyOpts(%q) = %q, %q; want %q", sp.name, stmt, text, refusal, want)
		}
	}
}

// #2802: the controls that already took a vertical tab, now through
// callOpen, find their call after each of MySQL's white space bytes.
func TestCallOpen_sharedByTheOtherControls_2802(t *testing.T) {
	for _, sp := range mysqlCallSpaces {
		for _, stmt := range []string{
			"SELECT LEFT{}(name, -1) FROM t",
			"SELECT RIGHT{}(name, -1) FROM t",
			"SELECT SUBSTRING{}(name, 0, 2) FROM t",
			"SELECT MID{}(name, 0, 2) FROM t",
			"SELECT CONCAT{}(a, b) FROM t",
			"SELECT GREATEST{}(a, b) FROM t",
		} {
			stmt = strings.ReplaceAll(stmt, "{}", sp.sep)
			if got := Veto(stmt); got == "" {
				t.Errorf("%s: Veto(%q) = \"\", want it kept on MySQL", sp.name, stmt)
			}
		}
		safe, other := []string{"total"}, []string{"price"}
		for stmt, want := range map[string]bool{
			"SELECT ROUND{}(price, 2) FROM t":      true,
			"SELECT ROUND{}(total, 2) FROM t":      false,
			"SELECT ROUND(SUM{}(total), 2) FROM t": false,
			"SELECT ROUND(SUM{}(price), 2) FROM t": true,
		} {
			stmt = strings.ReplaceAll(stmt, "{}", sp.sep)
			if got := RoundVeto(Shape(stmt), safe, other) != ""; got != want {
				t.Errorf("%s: RoundVeto(%q) vetoed %v, want %v", sp.name, stmt, got, want)
			}
		}
	}
	// A byte MySQL refuses there is no call to these controls either.
	for _, b := range []byte{0x01, 0x1c} {
		stmt := fmt.Sprintf("SELECT DATEDIFF%c('2026-01-15 02:00:00+05:00', d) FROM t", b)
		if got := DateFunctionVeto(Shape(stmt), []string{"d"}, nil); got != "" {
			t.Errorf("DateFunctionVeto(%q) = %q, want no call found", stmt, got)
		}
	}
}
