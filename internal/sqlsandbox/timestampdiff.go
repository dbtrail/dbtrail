package sqlsandbox

// timestampDiffMacros are MySQL's TIMESTAMPDIFF(unit, a, b) for the copy
// (#2611), the routing port's spelling of it
// (readrouter.CopyOptions.MySQLTimestampDiff). DuckDB has no timestampdiff,
// and its date_diff counts the boundaries a unit crosses (one month from
// January 31 to February 1), where MySQL counts whole units and drops the
// rest, toward zero.
//
//   - _bintrail_timestampdiff(a, b, per) is for MICROSECOND to WEEK: the
//     microseconds from a to b divided by per, the unit's length in
//     microseconds, toward zero. Integers only: the microseconds from the year
//     1 to 9999 pass 2^53, where a division in floating point would round.
//   - _bintrail_timestampdiff_months(a, b, per) is for MONTH (per 1), QUARTER
//     (3) and YEAR (12): the whole months from the earlier of the two to the
//     later, divided by per, with b's sign. A month is whole when the later
//     one's day, then its time of day to the microsecond, has reached the
//     earlier one's: January 31 to February 28 is no month, as on MySQL,
//     which does not clip to a month's last day here.
//
// Both read a and b on the wall clock of the session's zone, as MySQL reads a
// TIMESTAMP: the cast to TIMESTAMP takes a TIMESTAMP WITH TIME ZONE (a
// DATETIME under a UTC session, a TIMESTAMP) there, and a DATE to its
// midnight. Not year() and the rest over the TIMESTAMP WITH TIME ZONE itself:
// it counts years by era (sqlsandbox's dateSubMacro). NULL on either side is
// NULL. The answer is a BIGINT, MySQL's type for it.
//
// What they are given is the router's to decide (readrouter.TimestampDiffVeto).
// Installed with nowMacros, for a client that writes MySQL
// (Session.MySQLClock); TEMP, as they are.
const timestampDiffMacros = `CREATE OR REPLACE TEMP MACRO _bintrail_us_between(a, b) AS (
  epoch_us(CAST(b AS TIMESTAMP)) - epoch_us(CAST(a AS TIMESTAMP))
);
CREATE OR REPLACE TEMP MACRO _bintrail_timestampdiff(a, b, per) AS (
  CASE
    WHEN _bintrail_us_between(a, b) < 0 THEN -(_bintrail_us_between(b, a) // CAST(per AS BIGINT))
    ELSE _bintrail_us_between(a, b) // CAST(per AS BIGINT)
  END
);
CREATE OR REPLACE TEMP MACRO _bintrail_whole_months(x, y) AS (
  (year(y) - year(x)) * 12 + month(y) - month(x)
  - CASE WHEN day(y) < day(x) OR (day(y) = day(x) AND CAST(y AS TIME) < CAST(x AS TIME)) THEN 1 ELSE 0 END
);
CREATE OR REPLACE TEMP MACRO _bintrail_timestampdiff_months(a, b, per) AS (
  CASE
    WHEN CAST(b AS TIMESTAMP) < CAST(a AS TIMESTAMP) THEN -(_bintrail_whole_months(CAST(b AS TIMESTAMP), CAST(a AS TIMESTAMP)) // CAST(per AS BIGINT))
    ELSE _bintrail_whole_months(CAST(a AS TIMESTAMP), CAST(b AS TIMESTAMP)) // CAST(per AS BIGINT)
  END
);`
