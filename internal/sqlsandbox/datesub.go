package sqlsandbox

// dateSubMacro is MySQL's DATE_SUB(d, INTERVAL n unit) for the copy (#2611),
// the routing port's spelling of DATE_SUB and SUBDATE with an INTERVAL
// (readrouter.CopyOptions.MySQLDateSub). DuckDB's own date_sub takes a unit
// and two dates and counts the units between them, so the call keeps a name
// of its own.
//
// It is x minus the interval: month and year steps clip to the month's last
// day on both sides (March 31 minus a month is February 28, February 29
// minus a year February 28 of the year before), and a TIMESTAMP steps the
// wall clock of the session's zone on both under UTC and a fixed offset.
// Outside the years 1 to 9999 the two part: MySQL answers NULL, or a zero
// date for a day step into the year 0 (0001-01-01 minus a day is 0000-00-00
// there), and the copy a year 0 or a negative year. There the macro errors
// with the declined-value mark, so read routing sends the statement to
// MySQL without counting a fault of the copy (DeclinedValue). The range is
// read on the wall clock, as MySQL's is: the cast to TIMESTAMP takes a
// TIMESTAMP WITH TIME ZONE (a DATETIME under a UTC session, a TIMESTAMP) to
// the session's wall clock. Not year(): over a TIMESTAMP WITH TIME ZONE it
// counts years by era, so the year before 1 is 1 again (1 BC) and passes.
//
// What it is given is the router's to decide (readrouter.DateSubVeto): a
// DATE stepped by a day or more is a DATE on MySQL and a date and time here,
// and is kept on MySQL. Installed with nowMacros, for a client that writes
// MySQL (Session.MySQLClock); TEMP, as they are.
var dateSubMacro = dateStepMacro("_bintrail_date_sub", "x - i", "DATE_SUB")

// dateAddMacro is the other direction (#2800): x plus the interval, the
// routing port's spelling of DATE_ADD and ADDDATE with an INTERVAL, and of
// x + INTERVAL n unit (readrouter.CopyOptions.MySQLDateAdd, which writes
// x - INTERVAL n unit as dateSubMacro). Past the year 9999 MySQL answers
// NULL and the copy a year of five digits, which reaches a client as a value
// MySQL never sends, or keeps a row MySQL's NULL leaves out of a WHERE or a
// COUNT. x + i is what the copy computed for each spelling before: its own
// date_add(x, i) is the same value of the same type, over a DATE, a TIMESTAMP
// and a TIMESTAMP WITH TIME ZONE, under UTC and a fixed offset (measured on
// DuckDB 1.5.6). Installed with dateSubMacro.
var dateAddMacro = dateStepMacro("_bintrail_date_add", "x + i", "DATE_ADD")

// dateStepMacro is the macro name(x, i) that is step, x stepped by the
// interval i, and errors with the declined-value mark where that is outside
// the years 1 to 9999 on the session's wall clock (see dateSubMacro for why
// the cast and not year()). what names the step in the error.
func dateStepMacro(name, step, what string) string {
	return `CREATE OR REPLACE TEMP MACRO ` + name + `(x, i) AS (
  CASE
    WHEN CAST(` + step + ` AS TIMESTAMP) NOT BETWEEN TIMESTAMP '0001-01-01 00:00:00' AND TIMESTAMP '9999-12-31 23:59:59.999999' THEN error('` + what + ` to a time before the year 1 or after the year 9999 (NULL or a zero date on MySQL) ` + valueDeclinedMark + `')
    ELSE ` + step + `
  END
);`
}
