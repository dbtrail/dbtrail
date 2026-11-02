package readrouter

import (
	"regexp"
	"strings"
)

// The vetoes of #2611.
const (
	vetoNowKeyword    = "NOW(fsp), CURRENT_TIMESTAMP, CURRENT_DATE, LOCALTIME, CURTIME, SYSDATE or UTC_TIME (not given to the copy: a fraction of a second, a keyword the copy reads as its own, a time of day, the time each call runs at)"
	vetoNowArithmetic = "NOW(), CURDATE(), UTC_TIMESTAMP() or UTC_DATE() next to arithmetic other than INTERVAL (a number built from the digits on MySQL; a date or a date and time on the copy)"
	vetoDateStepped   = "CURDATE() or UTC_DATE() stepped by INTERVAL outside a comparison (a date on MySQL, a date and time on the copy)"
	vetoDateVsString  = "CURDATE() or UTC_DATE() compared with a string (MySQL reads a time in the string, the copy drops it)"
	vetoNowBare       = "UTC_DATE or UTC_TIMESTAMP without parentheses (the function on MySQL, a column name on the copy)"
)

// CallsNow reports whether a statement calls NOW(), CURDATE(),
// UTC_TIMESTAMP() or UTC_DATE() outside its string literals, comments and
// quoted names: the statements whose answer on the copy reads the copy's
// clock (#2611).
func CallsNow(stmt string) bool {
	return nowCall.MatchString(scan(stmt).blankedCopy)
}

var (
	dateCall = regexp.MustCompile(`(?i)\b(curdate|utc_date)\s*\(\s*\)`)
	// comparedBefore is a comparison, BETWEEN or AND right before a value.
	comparedBefore = regexp.MustCompile(`(?i)(<=>|<>|!=|<=|>=|=|<|>|\bbetween|\band)$`)
	// steppedThenCompared is an INTERVAL step followed by a comparison.
	steppedThenCompared = regexp.MustCompile(`(?i)^[+-]\s*interval\s+[\w.]+\s+\w+\s*(<=>|<>|!=|<=|>=|=|<|>)`)
	stepNext            = regexp.MustCompile(`(?i)^[+-]\s*interval\b`)
	stringAfter         = regexp.MustCompile(`(?i)^((<=>|<>|!=|<=|>=|=|<|>)\s*''|(not\s+)?in\s*\(\s*''|between\s+'')`)
	stringBefore        = regexp.MustCompile(`''\s*(<=>|<>|!=|<=|>=|=|<|>)$`)
	bareUTC             = regexp.MustCompile(`(?i)\butc_(date|timestamp)\b`)
)

// dateVeto says why the copy must not answer a CURDATE() or a UTC_DATE()
// for what surrounds it, or "" (#2611): a date stepped by INTERVAL is a date
// on MySQL and a date and time on the copy, which only a comparison does not
// show; a date compared with a string is compared as a date and time on
// MySQL when the string holds a time, and as a date on the copy, which drops
// the time; and UTC_DATE or UTC_TIMESTAMP with no parentheses is the
// function on MySQL and a column's name on the copy.
func dateVeto(blanked string) string {
	for _, m := range bareUTC.FindAllStringIndex(blanked, -1) {
		if rest := strings.TrimLeft(blanked[m[1]:], " \t\r\n"); !strings.HasPrefix(rest, "(") {
			return vetoNowBare
		}
	}
	for _, l := range dateCall.FindAllStringIndex(blanked, -1) {
		before := strings.TrimRight(blanked[:l[0]], " \t\r\n")
		after := strings.TrimLeft(blanked[l[1]:], " \t\r\n")
		if stringAfter.MatchString(after) || stringBefore.MatchString(before) {
			return vetoDateVsString
		}
		if stepNext.MatchString(after) && !comparedBefore.MatchString(before) && !steppedThenCompared.MatchString(after) {
			return vetoDateStepped
		}
	}
	return ""
}

// nowCall is a call the copy answers with MySQL's value: NOW(),
// CURDATE(), UTC_TIMESTAMP() or UTC_DATE(), with no argument.
var nowCall = regexp.MustCompile(`(?i)\b(now|curdate|utc_timestamp|utc_date)\s*\(\s*\)`)

// nowNames are those calls' names, for ColumnVeto.
var nowNames = []string{"now", "curdate", "utc_timestamp", "utc_date"}

// nowVeto says why the copy must not answer a statement because of what
// stands next to a nowCall, or "" (#2611). MySQL reads a date or a date and
// time next to + or - that INTERVAL does not follow, next to *, /, %, DIV or
// MOD, or under SUM, AVG and the other aggregates of numbers, as the number its digits spell (CURDATE() + 1 is
// 20261103); the copy adds a day, or refuses. The call's own parentheses end
// the date for the text: what follows them is read here. A call that a
// group holds ((NOW()) + 1), one under AVG, and one that reaches arithmetic
// under another name (a derived table's column) are ColumnVeto's, which
// reads them as it reads a date column called now.
func nowVeto(blanked string) string {
	if why := dateVeto(blanked); why != "" {
		return why
	}
	locs := nowCall.FindAllStringIndex(blanked, -1)
	if len(locs) == 0 {
		return ""
	}
	for _, l := range locs {
		before := strings.TrimRight(blanked[:l[0]], " \t\r\n")
		after := strings.TrimLeft(blanked[l[1]:], " \t\r\n")
		if before != "" && strings.ContainsRune("+-*/%", rune(before[len(before)-1])) || numericWord.MatchString(before) {
			return vetoNowArithmetic
		}
		if after != "" {
			switch c := after[0]; {
			case c == '+' || c == '-':
				if !intervalNext.MatchString(after[1:]) {
					return vetoNowArithmetic
				}
			case c == '*' || c == '/' || c == '%':
				return vetoNowArithmetic
			}
			if numericWordAfter.MatchString(after) {
				return vetoNowArithmetic
			}
		}
	}
	if numericAggregate.MatchString(blanked) {
		return vetoNowArithmetic
	}
	if why := ColumnVeto(blanked, nowNames, nil, false); why != "" {
		return vetoNowArithmetic
	}
	return ""
}

var (
	intervalNext     = regexp.MustCompile(`(?i)^\s*interval\b`)
	numericWord      = regexp.MustCompile(`(?i)\b(div|mod)$`)
	numericWordAfter = regexp.MustCompile(`(?i)^(div|mod)\b`)
	// numericAggregate is a call under an aggregate that sums or spreads
	// numbers, which MySQL feeds the digits.
	numericAggregate = regexp.MustCompile(`(?i)\b(sum|avg|std|stddev|stddev_pop|stddev_samp|variance|var_pop|var_samp|bit_and|bit_or|bit_xor)\s*\(\s*(distinct\s+)?(now|curdate|utc_timestamp|utc_date)\s*\(`)
)
