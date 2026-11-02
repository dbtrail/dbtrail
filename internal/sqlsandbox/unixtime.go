package sqlsandbox

import (
	"errors"
	"strings"
)

// unixTimeMacros are MySQL's UNIX_TIMESTAMP(x) and FROM_UNIXTIME(n) for the
// copy (#2493), which the reports that bucket time by hour or day are written
// with, and which DuckDB does not have.
//
// Each gives MySQL's value where it is exact and errors where it is not. Under
// read routing an error on the copy sends the statement to MySQL, which then
// answers; a value that only looks right would be answered by the copy. So:
//
//   - unix_timestamp(x) is the seconds of x read in the session's zone:
//     a TIMESTAMP column is an instant on the copy, a DATETIME a wall clock
//     under a non-UTC session (views.BaselineTable.WallClockDatetimes) and a
//     UTC instant under UTC, a DATE its midnight, and the cast to TIMESTAMPTZ
//     reads each the way MySQL does. Seconds by integer division of the
//     microseconds, since a double cast to BIGINT rounds. MySQL returns a
//     decimal for a value with fractional seconds, so one errors; the router
//     also answers only columns declared without them
//     (readrouter.UnixTimeVeto). A NULL errors too: the copy holds MySQL's
//     zero date ('0000-00-00') as a NULL, and MySQL's UNIX_TIMESTAMP of it is
//     0, so a NULL on the copy cannot be told from a value MySQL answers.
//   - from_unixtime(n) is the wall clock of epoch second n in the session's
//     zone. A fractional n errors. MySQL prints as many fraction digits as
//     the TYPE of n has, whatever its value (a DECIMAL(12,2) holding a whole
//     number prints .00), which the copy's types do not follow (its integer
//     division is a DOUBLE): the router answers only an argument MySQL types
//     as a whole number (readrouter.UnixTimeVeto). The two-argument form has
//     no macro, and errors.
//   - Both error outside 1970-01-01 00:00:01 to 2038-01-19 03:14:07 UTC,
//     where MySQL (0 before 1970) and MariaDB (a narrower range, by version)
//     differ from each other and from a plain conversion. Second 0 is out
//     too: MariaDB 11.8 and 12.3 answer FROM_UNIXTIME(0) with NULL, MySQL and
//     MariaDB 11.4 with 1970-01-01 00:00:00 (CI, 2026-11-01).
//
// TEMP and CREATE OR REPLACE: the worker reuses its connection, and the
// macros live in its temporary schema, not in the views or the files.
const unixTimeMacros = `CREATE OR REPLACE TEMP MACRO unix_timestamp(x) AS (
  CASE
    WHEN x IS NULL THEN error('UNIX_TIMESTAMP of a NULL (on the copy a NULL or a zero date, which MySQL reads as 0) is not answered by the copy')
    WHEN epoch_us(CAST(x AS TIMESTAMPTZ)) NOT BETWEEN 1000000 AND 2147483647999999 THEN error('UNIX_TIMESTAMP of a time before 1970-01-01 00:00:01 or after 2038-01-19 03:14:07 UTC is not answered by the copy')
    WHEN epoch_us(CAST(x AS TIMESTAMPTZ)) % 1000000 <> 0 THEN error('UNIX_TIMESTAMP of a time with fractional seconds (a decimal on MySQL) is not answered by the copy')
    ELSE epoch_us(CAST(x AS TIMESTAMPTZ)) // 1000000
  END
);
CREATE OR REPLACE TEMP MACRO from_unixtime(n) AS (
  CASE
    WHEN n IS NULL THEN NULL
    WHEN n >= 1 AND n <= 2147483647 AND n = floor(n) THEN CAST(to_timestamp(n) AS TIMESTAMP)
    ELSE error('FROM_UNIXTIME of zero, a negative, fractional or past-2038 number is not answered by the copy')
  END
);`

// valueDeclinedMark ends both macros' error messages: the copy declining a
// value it would not answer exactly, not failing.
const valueDeclinedMark = "is not answered by the copy"

// DeclinedValue reports whether err is one of the macros above declining a
// value (a fraction, a time outside 1970 to 2038): a decision about the
// value, like ColumnsDifferError, and not a fault of the copy, so read routing
// sends the statement to MySQL without counting a fault. Only a QueryError
// counts: the mark is DuckDB's text for the statement, never a refusal or a
// worker's failure.
func DeclinedValue(err error) bool {
	var q *QueryError
	return errors.As(err, &q) && strings.Contains(q.Message, valueDeclinedMark)
}

// declineMarker opens every DeclineSQL statement.
const declineMarker = "\n-- copy checks (#2533)\n"

// DeclineSQL is a statement for the end of a views script that declines the
// statement after it when cond holds over the views (#2533): what says what
// was found, and the copy declines it as the macros above decline a value
// (DeclinedValue), so read routing sends the statement to MySQL as the
// copy's decision, not its fault. cond is SQL the caller builds over the
// views it installs; the worker runs it in the statement's own session.
//
// Each one starts with declineMarker, where the worker splits the script: the
// views before it are installed as views, and what follows runs as the
// statement's own work, so any failure there (a decline, or memory run out
// reading the values) is the statement's error, not a failed session.
func DeclineSQL(cond, what string) string {
	return declineMarker + "SELECT CASE WHEN " + cond + " THEN error('" + strings.ReplaceAll(what+" "+valueDeclinedMark, "'", "''") + "') END;\n"
}
