package readrouter

import (
	"regexp"
	"strings"
)

// The copy's names for MySQL's TIMESTAMPDIFF, macros the SQL worker defines
// for a client that writes MySQL (sqlsandbox's timestampDiffMacros): one for
// the units of a fixed length, one for those counted in months. DuckDB has no
// timestampdiff, and its date_diff counts the boundaries a unit crosses where
// MySQL counts whole units.
const (
	timestampDiffMacro       = "_bintrail_timestampdiff"
	timestampDiffMonthsMacro = "_bintrail_timestampdiff_months"
)

// timestampDiffUnits are the units the copy is sent TIMESTAMPDIFF with, each
// with its macro and its length: in microseconds, or in months. MySQL's other
// spellings (SQL_TSI_DAY and the rest) are not here, so a call with one is
// sent as written and stays on MySQL.
var timestampDiffUnits = map[string][2]string{
	"microsecond": {timestampDiffMacro, "1"},
	"second":      {timestampDiffMacro, "1000000"},
	"minute":      {timestampDiffMacro, "60000000"},
	"hour":        {timestampDiffMacro, "3600000000"},
	"day":         {timestampDiffMacro, "86400000000"},
	"week":        {timestampDiffMacro, "604800000000"},
	"month":       {timestampDiffMonthsMacro, "1"},
	"quarter":     {timestampDiffMonthsMacro, "3"},
	"year":        {timestampDiffMonthsMacro, "12"},
}

// timestampDiffUnit reads a TIMESTAMPDIFF call in blRest, the blanked text
// right after the function's name: ok when it has three arguments, the first
// one word of timestampDiffUnits and the other two not empty. The unit is
// read in the blanked text, where a comment beside it is a space.
func timestampDiffUnit(blRest string) (unit [2]string, ok bool) {
	args, _, ok := callArgs(blRest)
	if !ok || len(args) != 3 || strings.TrimSpace(args[1]) == "" || strings.TrimSpace(args[2]) == "" {
		return unit, false
	}
	unit, ok = timestampDiffUnits[strings.ToLower(strings.TrimSpace(args[0]))]
	return unit, ok
}

// mysqlTimestampDiffCall rewrites MySQL's TIMESTAMPDIFF(unit, a, b) for the
// copy (#2611): rest is the text right after the function's name, blRest the
// same in the blanked text, and the call returned is the unit's macro over a
// and b as written, with the unit's length, and how many bytes of rest it
// replaces. ok is false, and nothing is rewritten, unless timestampDiffUnit
// reads the call.
func mysqlTimestampDiffCall(rest, blRest string) (call string, n int, ok bool) {
	unit, ok := timestampDiffUnit(blRest)
	if !ok {
		return "", 0, false
	}
	args, n, ok := callArgs(rest)
	if !ok || len(args) != 3 {
		return "", 0, false
	}
	return unit[0] + "(" + argText(args[1]) + ", " + argText(args[2]) + ", " + unit[1] + ")", n, true
}

var (
	// timestampDiffCall opens a TIMESTAMPDIFF call; callsOf keeps out a
	// longer name. \v too: Go's \s leaves it out, and callArgs (the rewrite)
	// skips it.
	timestampDiffCall = regexp.MustCompile(`(?i)timestampdiff[\s\v]*\(`)
	// The DATEDIFF veto's text when only one of the two options lets its
	// function through, and when both do.
	timeDiffOrDateDiff = regexp.MustCompile(`(?i)\b(datediff|timediff)[\s\v]*\(`)
	timeDiffAlone      = regexp.MustCompile(`(?i)\btimediff[\s\v]*\(`)
)

// dateDiffVeto is how VetoOpts reads its TIMESTAMPDIFF/DATEDIFF entry under
// o: keep is true when a call stays on MySQL whatever else the statement
// holds, and otherwise re is what the entry still matches. With
// MySQLDateFunctions a DATEDIFF passes when every one is written as date_diff
// (dateDiffsRewritten); with MySQLTimestampDiff a TIMESTAMPDIFF passes when
// none is left in the text the copy is sent (#2611). That text is read again
// rather than guessed: a call the rewrite does not read (a comment before its
// parenthesis, a unit it does not have, db.timestampdiff) is sent as written,
// and the copy has no timestampdiff to answer it with.
func dateDiffVeto(sc scanned, o CopyOptions, re *regexp.Regexp) (_ *regexp.Regexp, keep bool) {
	if o.MySQLDateFunctions && !dateDiffsRewritten(sc.blankedCopy) {
		return nil, true
	}
	if o.MySQLTimestampDiff && len(callsOf(timestampDiffCall, sc.blankedCopy)) > 0 &&
		len(callsOf(timestampDiffCall, scan(sc.copyText(o)).blankedCopy)) > 0 {
		return nil, true
	}
	switch {
	case o.MySQLDateFunctions && o.MySQLTimestampDiff:
		return timeDiffAlone, false
	case o.MySQLDateFunctions:
		return timeDiffOnly, false
	case o.MySQLTimestampDiff:
		return timeDiffOrDateDiff, false
	}
	return re, false
}

// vetoCopyMacro keeps on MySQL a statement that calls one of the copy's own
// macros by name (_bintrail_date_sub, _bintrail_timestampdiff, ...): MySQL
// has no such function and refuses the statement, where the copy would
// answer it, over whatever it is given. After a dot too: db._bintrail_x(a)
// is a stored function on MySQL, and the copy may read col._bintrail_x(a) as
// the macro called on a column.
const vetoCopyMacro = "a function named _bintrail_... (one of the copy's own, which MySQL does not have)"

var copyMacroCall = regexp.MustCompile(`(?i)(^|[^\w$"\x60])_bintrail_\w*[\s\v]*\(`)

// copyMacroVeto is vetoCopyMacro over a blanked statement, or "".
func copyMacroVeto(blanked string) string {
	if copyMacroCall.MatchString(blanked) {
		return vetoCopyMacro
	}
	return ""
}

// VetoTimestampDiff is why TimestampDiffVeto keeps a statement on MySQL.
const VetoTimestampDiff = "TIMESTAMPDIFF of something other than a DATE, or a DATETIME or TIMESTAMP column without fractional seconds, NOW(), CURDATE(), UTC_TIMESTAMP() or UTC_DATE() " +
	"(MySQL reads a string, a number or an expression into a date its own way)"

// TimestampDiffVeto says why the copy must not answer a routed statement
// because of what its TIMESTAMPDIFF calls are given, or "" when it can
// (#2611). Under routing the copy is sent TIMESTAMPDIFF(unit, a, b) as its
// macro over a and b, which gives MySQL's whole units over two dates read on
// the session's wall clock (sqlsandbox's timestampDiffMacros). Measured on
// MySQL 8.0.46 and the copy for every unit both ways round, over month ends,
// February 29, the years 1 and 9999 and a session zone of +05:00, the two
// agree when each of a and b is a bare column in dates and in no table's
// other, or NOW(), CURDATE(), UTC_TIMESTAMP() or UTC_DATE().
//
// dates are the DATE columns and the DATETIME and TIMESTAMP columns without
// fractional seconds, none kept as text (the caller leaves a TIMESTAMP out
// under a session zone with clock changes, as for DATE_SUB), other all the
// rest. A string MySQL parses its own way (it takes 20260115 for a date), and
// a fraction is left out with the text it cannot be told apart from. As
// DateFunctionVeto keeps it, a name the statement may give itself stays on
// MySQL (a subquery, a WITH, an alias, a string alias), and so does a call next
// to a dot (a stored function).
//
// What stands around the call is the other checks': arithmetic on it reads as
// a group that holds its column (ColumnVeto) or NOW() (nowVeto), and stays on
// MySQL.
func TimestampDiffVeto(shape string, dates, other []string) string {
	calls := callsOf(timestampDiffCall, shape)
	if len(calls) == 0 {
		return ""
	}
	if subquery.MatchString(shape) || stringAliased(shape) {
		return VetoTimestampDiff
	}
	known := knownNames(dates, other)
	low := strings.ToLower(strings.ReplaceAll(shape, `"`, ""))
	for _, l := range calls {
		const name = len("timestampdiff")
		if namedByDot(shape, l[0], name) {
			return VetoTimestampDiff
		}
		if _, ok := timestampDiffUnit(shape[l[0]+name:]); !ok {
			return VetoTimestampDiff
		}
		args, _, _ := callArgs(shape[l[0]+name:])
		for _, a := range args[1:] {
			a = unwrap(strings.TrimSpace(a))
			if wholeClockCall.MatchString(a) {
				continue
			}
			m := columnRef.FindStringSubmatch(a)
			if m == nil {
				return VetoTimestampDiff
			}
			col := unquote(m[1])
			if !known[sameName(col)] || aliased(low, strings.ToLower(col)) {
				return VetoTimestampDiff
			}
		}
	}
	return ""
}

// TimestampDiffVeto is TimestampDiffVeto over this shape.
func (s StatementShape) TimestampDiffVeto(dates, other []string) string {
	return TimestampDiffVeto(string(s), dates, other)
}
