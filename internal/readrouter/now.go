package readrouter

import (
	"regexp"
	"strings"
)

// The vetoes of #2611.
const (
	vetoNowKeyword    = "NOW(fsp), CURRENT_TIMESTAMP, CURRENT_DATE, LOCALTIME, CURTIME, SYSDATE or UTC_TIME (not given to the copy: a fraction of a second, a keyword the copy reads as its own, a time of day, the time each call runs at)"
	vetoNowArithmetic = "NOW(), CURDATE(), UTC_TIMESTAMP() or UTC_DATE() next to arithmetic other than INTERVAL (a number built from the digits on MySQL; a date or a date and time on the copy)"
	vetoDatePlace     = "CURDATE() or UTC_DATE() other than compared with a column, inside YEAR, MONTH, DAY or QUARTER, or a column of its own (a date on MySQL that the copy may turn into a date and time, or compare as one)"
	vetoDateWrapped   = "NOW() or UTC_TIMESTAMP() made a date and then stepped (a date on MySQL, a date and time on the copy)"
	vetoNowBare       = "UTC_DATE or UTC_TIMESTAMP without parentheses (the function on MySQL, a column name on the copy)"
)

// CallsNow reports whether a statement calls NOW(), CURDATE(),
// UTC_TIMESTAMP() or UTC_DATE() outside its string literals, comments and
// quoted names: the statements whose answer on the copy reads the copy's
// clock (#2611).
func CallsNow(stmt string) bool {
	return nowCall.MatchString(scan(stmt).blankedCopy)
}

// DateComparedColumns names the columns a statement compares CURDATE() and
// UTC_DATE() with, lowercased, each once, without its table (#2611). The
// router lets the copy answer such a comparison whatever the column; the
// caller, which knows the columns' types, must refuse it unless each is a
// date: MySQL compares a date with a string holding a time as a date and
// time, where the copy drops the time.
func DateComparedColumns(stmt string) []string {
	cols, _ := dateUses(strings.ToLower(scan(stmt).blankedCopy))
	return cols
}

const (
	ident = `(?:[a-z_][\w$]*\.)?([a-z_][\w$]*)`
	cmp   = `(?:<=>|<>|!=|<=|>=|=|<|>)`
	// nowCallText is one of the four calls, written as nowCall reads it.
	nowCallText = `(?:now|curdate|utc_timestamp|utc_date)\s*\(\s*\)`
	// stepText is one INTERVAL step: a number or a string, and a unit.
	stepText = `\s*[+-]\s*interval\s+(?:\d+|'')\s+[a-z_]+\b`
)

var (
	dateCall = regexp.MustCompile(`(?i)\b(curdate|utc_date)\s*\(\s*\)`)
	bareUTC  = regexp.MustCompile(`(?i)\butc_(date|timestamp)\b`)
	steps    = regexp.MustCompile(`^(` + stepText + `)+`)
	// ended is what may follow a value the copy gives as MySQL does: the
	// end, or what ends a condition or a select item.
	ended = regexp.MustCompile(`^($|;|\)|,|(and|or|order|group|limit|having|then|window|union|from|as)\b)`)
	// arithmeticAfter is arithmetic right after a value: MySQL reads a date
	// there as a number.
	arithmeticAfter = regexp.MustCompile(`^([-+*/%]|(div|mod)\b)`)
	// startsCondition is what may stand before a call that a comparison
	// follows: no operand of its own on the left.
	startsCondition = regexp.MustCompile(`(^|\b(select|where|and|or|on|having|when|not)|\(|,)$`)
	plainBefore     = regexp.MustCompile(`([-+*/%]|\b(div|mod|interval))$`)
	columnThenCmp   = regexp.MustCompile(ident + `\s*` + cmp + `$`)
	cmpThenColumn   = regexp.MustCompile(`^` + cmp + `\s*` + ident)
	columnBetween   = regexp.MustCompile(ident + `\s+(?:not\s+)?between$`)
	betweenHigh     = regexp.MustCompile(ident + `\s+(?:not\s+)?between\s+` + nowCallText + `(?:` + stepText + `)*\s+and$`)
	highCall        = regexp.MustCompile(`^and\s+` + nowCallText)
	datePart        = regexp.MustCompile(`\b(year|month|day|dayofmonth|quarter|dayofyear)\s*\($`)
	selectItem      = regexp.MustCompile(`(\bselect|,)$`)
	setOperation    = regexp.MustCompile(`\b(union|intersect|except)\b`)
	// namesItsOwn is a statement that gives names a comparison may read
	// back as columns: a derived table, a subquery, a WITH, a HAVING over
	// select items.
	namesItsOwn = regexp.MustCompile(`\(\s*select\b|\bwith\b|\bhaving\b`)
	orderBy     = regexp.MustCompile(`\border\s+by\b`)
	// wrappedNow is a NOW() or a UTC_TIMESTAMP() made a date (DATE,
	// LAST_DAY, a CAST AS DATE) and then stepped or added to: a date on
	// MySQL, and so is the step from it, where the copy's is a date and time.
	wrappedNow = regexp.MustCompile(`\b((date|last_day)\s*\(\s*(now|utc_timestamp)\s*\(\s*\)|cast\s*\(\s*(now|utc_timestamp)\s*\(\s*\)\s+as\s+date)\s*\)\s*[-+*/%]`)
)

// operandBefore says the text before a column has an operand that ends
// there: x + d = CURDATE() compares x + d.
func operandBefore(before string, col []int) bool {
	return plainBefore.MatchString(strings.TrimRight(before[:col[0]], " \t\r\n"))
}

// dateUses reads every CURDATE() and UTC_DATE() of a blanked, lowercased
// statement, and says whether each stands where the copy gives what MySQL
// does (ok), and the columns they are compared with. A date stepped by
// INTERVAL is a date on MySQL and a date and time on the copy, which only a
// comparison does not show; a date picked among strings (GREATEST, COALESCE,
// CASE, UNION), compared with a string or a parameter, or stepped through
// another function is MySQL's. So the copy takes a date in four places only:
// compared with a column (d >= CURDATE() - INTERVAL 7 DAY, BETWEEN
// included), inside YEAR, MONTH, DAY or QUARTER, or a select item of its own
// outside any parentheses, in a statement with no set operation and no
// ORDER BY (which may read it back under its alias). A compared name must be
// a column: in a statement with a derived table, a subquery, a WITH or a
// HAVING, or one that gives the name to a select item itself, it may stand
// for text, which the caller's types cannot see.
func dateUses(b string) (cols []string, ok bool) {
	seen := map[string]bool{}
	add := func(c string) {
		if !seen[c] {
			seen[c] = true
			cols = append(cols, c)
		}
	}
	setOp := setOperation.MatchString(b)
	locs := dateCall.FindAllStringIndex(b, -1)
	if len(locs) > 0 && namesItsOwn.MatchString(b) {
		return nil, false
	}
	add = func(c string) {
		if aliased(b, c) {
			ok = false
		}
		if !seen[c] {
			seen[c] = true
			cols = append(cols, c)
		}
	}
	ok = true
	for _, l := range locs {
		before := strings.TrimRight(b[:l[0]], " \t\r\n")
		stepped := steps.FindString(b[l[1]:])
		after := strings.TrimLeft(b[l[1]+len(stepped):], " \t\r\n")
		switch {
		case stepped == "" && datePart.MatchString(before) && strings.HasPrefix(after, ")"):
		case stepped == "" && !setOp && !orderBy.MatchString(b) && depth(before) == 0 && selectItem.MatchString(before) && ended.MatchString(after):
		default:
			m := columnThenCmp.FindStringSubmatchIndex(before)
			if m != nil && m[1] == len(before) && !operandBefore(before, m) && ended.MatchString(after) {
				add(before[m[2]:m[3]])
				continue
			}
			if m := cmpThenColumn.FindStringSubmatchIndex(after); m != nil && startsCondition.MatchString(before) {
				rest := strings.TrimLeft(after[m[1]:], " \t\r\n")
				if !strings.HasPrefix(rest, "(") && !strings.HasPrefix(rest, ".") && ended.MatchString(rest) {
					add(after[m[2]:m[3]])
					continue
				}
			}
			if m := columnBetween.FindStringSubmatchIndex(before); m != nil && !operandBefore(before, m) && highCall.MatchString(after) {
				add(before[m[2]:m[3]])
				continue
			}
			if m := betweenHigh.FindStringSubmatchIndex(before); m != nil && m[1] == len(before) && !operandBefore(before, m) && ended.MatchString(after) {
				add(before[m[2]:m[3]])
				continue
			}
			return cols, false
		}
	}
	return cols, ok
}

// aliased says a statement gives name to one of its select items (AS name,
// or an expression and the name before a comma or FROM).
func aliased(b, name string) bool {
	q := regexp.QuoteMeta(name)
	if regexp.MustCompile(`\bas\s+` + q + `\b`).MatchString(b) {
		return true
	}
	for _, m := range regexp.MustCompile(`(\w+|[)'])\s+`+q+`\s*(,|\bfrom\b)`).FindAllStringSubmatch(b, -1) {
		if !selectWord[m[1]] {
			return true
		}
	}
	return false
}

// selectWord are the words a select item's name may follow without being
// its alias.
var selectWord = map[string]bool{"select": true, "distinct": true, "all": true, "by": true}

// depth is how many parentheses are open at the end of s.
func depth(s string) int {
	return strings.Count(s, "(") - strings.Count(s, ")")
}

// dateVeto says why the copy must not answer a statement for where its
// CURDATE() and UTC_DATE() stand (dateUses), for a NOW() made a date and
// then stepped, or for a UTC_DATE or a UTC_TIMESTAMP with no parentheses,
// the function on MySQL and a column's name on the copy; or "".
func dateVeto(blanked string) string {
	b := strings.ToLower(blanked)
	for _, m := range bareUTC.FindAllStringIndex(b, -1) {
		if rest := strings.TrimLeft(b[m[1]:], " \t\r\n"); !strings.HasPrefix(rest, "(") {
			return vetoNowBare
		}
	}
	if wrappedNow.MatchString(b) {
		return vetoDateWrapped
	}
	if _, ok := dateUses(b); !ok {
		return vetoDatePlace
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
		// Past its INTERVAL steps, a value ends: arithmetic there is on
		// MySQL's number (NOW() + INTERVAL 1 DAY * 2 doubles the date and
		// time's digits) and on the copy's INTERVAL. A step whose amount is
		// not a number is not read as one, so what follows it is arithmetic.
		after = strings.ToLower(after)
		after = strings.TrimLeft(after[len(steps.FindString(after)):], " \t\r\n")
		if arithmeticAfter.MatchString(after) {
			return vetoNowArithmetic
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
	numericWord = regexp.MustCompile(`(?i)\b(div|mod)$`)
	// numericAggregate is a call under an aggregate that sums or spreads
	// numbers, which MySQL feeds the digits.
	numericAggregate = regexp.MustCompile(`(?i)\b(sum|avg|std|stddev|stddev_pop|stddev_samp|variance|var_pop|var_samp|bit_and|bit_or|bit_xor)\s*\(\s*(distinct\s+)?(now|curdate|utc_timestamp|utc_date)\s*\(`)
)
