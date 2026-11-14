package readrouter

import (
	"regexp"
	"strconv"
	"strings"
)

// dateSubMacro is the copy's name for MySQL's DATE_SUB, a macro the SQL
// worker defines for a client that writes MySQL (sqlsandbox's dateSubMacro).
// DuckDB's own date_sub takes a unit and two dates and counts the units
// between them, so MySQL's two-argument call cannot be sent under its name.
const dateSubMacro = "_bintrail_date_sub"

// mysqlDateSubCall rewrites MySQL's DATE_SUB(d, INTERVAL n unit) or
// SUBDATE(d, INTERVAL n unit) for the copy (#2611): rest is the text right
// after the function's name, and the call returned is the copy's macro over
// the same two arguments, with how many bytes of rest it replaces. ok is false,
// and nothing is rewritten, unless the call has two arguments, neither empty,
// and the second opens with the word INTERVAL. SUBDATE(d, n), the days form,
// is not rewritten: the copy has no subdate and refuses it.
//
// Whether the second argument opens with INTERVAL is read in blRest, the
// same call in the blanked text, as DateSubVeto reads it: a comment there
// is a space (DATE_SUB(dt, /* c */ INTERVAL 1 DAY)), and so is a vertical
// tab, so the two never disagree about which calls are MySQL's.
func mysqlDateSubCall(rest, blRest string) (call string, n int, ok bool) {
	args, n, ok := callArgs(rest)
	if !ok || len(args) != 2 || strings.TrimSpace(args[0]) == "" {
		return "", 0, false
	}
	blArgs, _, ok := callArgs(blRest)
	if !ok || len(blArgs) != 2 || !intervalFirst.MatchString(strings.Join(strings.Fields(blArgs[1]), " ")+" ") {
		return "", 0, false
	}
	return dateSubMacro + "(" + argText(args[0]) + ", " + argText(args[1]) + ")", n, true
}

// intervalFirst is an argument that opens with the word INTERVAL.
var intervalFirst = regexp.MustCompile(`(?i)^interval\s`)

// VetoDateSub is why DateSubVeto keeps a statement on MySQL.
const VetoDateSub = "DATE_SUB or SUBDATE other than of a DATETIME or TIMESTAMP column without fractional seconds, NOW() or UTC_TIMESTAMP(), " +
	"minus INTERVAL and a whole number written in the statement with a unit of SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER or YEAR, " +
	"or of a DATE column minus SECOND, MINUTE or HOUR (MySQL keeps a date a date, reads text its own way and prints a fraction the copy does not)"

var (
	// dateSubCall opens a DATE_SUB or a SUBDATE call; callsOf keeps out a
	// longer name.
	// \v too: Go's \s leaves it out, and callArgs (the rewrite) skips it.
	dateSubCall = regexp.MustCompile(`(?i)(date_sub|subdate)[\s\v]*\(`)
	// dateSubStep is the second argument the copy may be given: INTERVAL, a
	// whole number written in the statement (a quoted one is blanked to ''
	// here, and scan has refused one that is not digits), and a unit of one
	// word whose arithmetic was measured to agree.
	// Nine digits at most: the copy reads a longer amount as a number too
	// large for an interval and refuses the statement.
	dateSubStep = regexp.MustCompile(`(?i)^interval\s+(\d{1,9}|'')\s+(second|minute|hour|day|week|month|quarter|year)$`)
	// clockTimeCall is an argument that is NOW() or UTC_TIMESTAMP() and
	// nothing else: a date and time with no fraction on both sides.
	clockTimeCall = regexp.MustCompile(`(?i)^(?:now|utc_timestamp)\s*\(\s*\)$`)
)

// DateSubVeto says why the copy must not answer a routed statement because of
// what its DATE_SUB and SUBDATE calls are given, or "" when it can (#2611).
// Under routing the copy is sent DATE_SUB(d, INTERVAL n unit) as its macro,
// d minus the interval, which errors outside the years 1 to 9999 (MySQL's
// NULL or zero date there). Measured on MySQL 8.0.46 and the copy, over month
// ends, February 29, the turn of a year and a session zone of +05:00, the two
// agree where:
//
//   - d is a DATETIME or a TIMESTAMP column declared without fractional
//     seconds, or NOW() or UTC_TIMESTAMP(), and the unit is SECOND, MINUTE,
//     HOUR, DAY, WEEK, MONTH, QUARTER or YEAR. A fraction prints as many
//     digits as MySQL's type has on MySQL and as the value needs on the copy
//     (.250000 and .25), so a DATETIME(6) and MICROSECOND stay on MySQL;
//   - d is a DATE column and the unit SECOND, MINUTE or HOUR: a date and time
//     on both. With a unit of a day or more MySQL answers a DATE and the copy
//     a date and time, so that stays on MySQL, and so do CURDATE() and
//     UTC_DATE();
//   - the amount is a whole number written in the statement. MySQL rounds
//     1.5, the copy refuses a sign before the number; a placeholder could be
//     either.
//
// times are the DATETIME and TIMESTAMP columns without fractional seconds
// (the caller leaves a TIMESTAMP out under a session zone with clock changes,
// where MySQL steps the wall clock and the copy the instant), dates the DATE
// columns, other all the rest. A name in times or dates and in other counts
// as other. As DateFunctionVeto keeps it, a name the statement may give itself
// stays on MySQL (a subquery, a WITH, an alias, a string alias), and so does a
// call next to a dot (a stored function).
//
// What stands around the call is the other checks': ColumnVeto reads the call
// as a group that holds its column (DATE_SUB(dt, INTERVAL 1 DAY) + 0, AVG of
// it), nowVeto as one that holds NOW().
func DateSubVeto(shape string, dates, times, other []string) string {
	calls := callsOf(dateSubCall, shape)
	if len(calls) == 0 {
		return ""
	}
	if subquery.MatchString(shape) || stringAliased(shape) {
		return VetoDateSub
	}
	timeCol, dateCol := knownNames(times, other), knownNames(dates, other)
	low := strings.ToLower(strings.ReplaceAll(shape, `"`, ""))
	for _, l := range calls {
		name := strings.ToLower(dateSubCall.FindStringSubmatch(shape[l[0]:l[1]])[1])
		if namedByDot(shape, l[0], len(name)) {
			return VetoDateSub
		}
		args, _, ok := callArgs(shape[l[0]+len(name):])
		if !ok || len(args) != 2 {
			return VetoDateSub
		}
		step := dateSubStep.FindStringSubmatch(strings.Join(strings.Fields(args[1]), " "))
		if step == nil {
			return VetoDateSub
		}
		unit := strings.ToLower(step[2])
		if n, err := strconv.Atoi(step[1]); err == nil && n > maxStep[unit] {
			// Past every date of the years 1 to 9999: NULL on MySQL, and on
			// the copy an overflow, which would count as its fault.
			return VetoDateSub
		}
		a := unwrap(strings.TrimSpace(args[0]))
		if clockTimeCall.MatchString(a) {
			continue
		}
		m := columnRef.FindStringSubmatch(a)
		if m == nil {
			return VetoDateSub
		}
		col := unquote(m[1])
		if aliased(low, strings.ToLower(col)) {
			return VetoDateSub
		}
		switch f := sameName(col); {
		case timeCol[f] && !dateCol[f]:
		case dateCol[f] && !timeCol[f] && subDay[unit]:
		default:
			return VetoDateSub
		}
	}
	return ""
}

// maxStep is, per unit, the largest amount that can still land inside the
// years 1 to 9999 (10,000 years of each unit, rounded up).
var maxStep = map[string]int{"year": 10000, "quarter": 40000, "month": 120000, "week": 521800,
	"day": 3652500, "hour": 87660000, "minute": 999999999, "second": 999999999}

// subDay are the units that step a DATE into a date and time on MySQL.
var subDay = map[string]bool{"second": true, "minute": true, "hour": true}

// DateSubVeto is DateSubVeto over this shape.
func (s StatementShape) DateSubVeto(dates, times, other []string) string {
	return DateSubVeto(string(s), dates, times, other)
}
