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
// MySQL without counting a fault of the copy (DeclinedValue). year() reads a
// TIMESTAMP WITH TIME ZONE in the session's zone, the wall clock MySQL's
// range is about.
//
// What it is given is the router's to decide (readrouter.DateSubVeto): a
// DATE stepped by a day or more is a DATE on MySQL and a date and time here,
// and is kept on MySQL. Installed with nowMacros, for a client that writes
// MySQL (Session.MySQLClock); TEMP, as they are.
const dateSubMacro = `CREATE OR REPLACE TEMP MACRO _bintrail_date_sub(x, i) AS (
  CASE
    WHEN year(x - i) NOT BETWEEN 1 AND 9999 THEN error('DATE_SUB to a time before the year 1 or after the year 9999 (NULL or a zero date on MySQL) is not answered by the copy')
    ELSE x - i
  END
);`
