package sqlsandbox

// nowMacros are MySQL's NOW(), CURDATE(), UTC_TIMESTAMP() and UTC_DATE() for
// the copy (#2611), which reports write "the last 30 days" with. DuckDB's own
// now() is a TIMESTAMP WITH TIME ZONE with microseconds, and it has no
// curdate, utc_timestamp or utc_date; MySQL's are a date and time with no
// fraction (or a date), on the wall clock of the session's zone (or of UTC).
// So:
//
//   - now() is the statement's instant, cut to the second, as the wall clock
//     of the session's zone (the copy's session runs under the zone the port
//     read from the source, sqlsandbox.Session.TimeZone);
//   - curdate() is its date there; utc_timestamp() and utc_date() the same
//     in UTC.
//
// Compared with a DATETIME column (a wall clock on the copy) they compare as
// on MySQL. Compared with a TIMESTAMP column (an instant on the copy) the copy
// reads the wall clock back into an instant in the session's zone, where MySQL
// reads the column into a wall clock: the same answer under UTC and a fixed
// offset, and not always under a zone with clock changes, where a wall clock
// (NOW() - INTERVAL 30 DAY landing in the hour that repeats) can stand for
// two instants or none. So read routing gives them to the copy only under UTC
// or a fixed offset (console's sqlNowRefusalFor); under a named zone MySQL
// answers.
//
// The instant is the copy's: the copy's host clock, at the start of the
// statement on the copy, where MySQL's is MySQL's host clock at the start of
// the statement there. The two are as far apart as the two clocks and the
// moments the statement starts on each; the port checks the source's clock
// against this host's before each routed statement that calls one
// (shim's clockKeepsCopyFromAnswering), which also finds a session that froze
// its clock (SET timestamp).
//
// Installed only for a client that writes MySQL (Session.MySQLClock): they
// replace DuckDB's own now(), which the SQL card's reader may use as DuckDB
// has it. _bintrail_wall is now() over a given instant, for tests. TEMP, as
// the UNIX_TIMESTAMP macros: the worker reuses its connection. The router
// lets through only the calls with no argument and no arithmetic but INTERVAL
// next to them (readrouter.nowVeto); NOW(3), CURRENT_TIMESTAMP, CURRENT_DATE
// and LOCALTIME, which DuckDB reads as its own, stay MySQL's.
const nowMacros = `CREATE OR REPLACE TEMP MACRO _bintrail_wall(t) AS (CAST(date_trunc('second', t) AS TIMESTAMP));
CREATE OR REPLACE TEMP MACRO now() AS (_bintrail_wall(current_timestamp));
CREATE OR REPLACE TEMP MACRO curdate() AS (CAST(_bintrail_wall(current_timestamp) AS DATE));
CREATE OR REPLACE TEMP MACRO utc_timestamp() AS (CAST(date_trunc('second', current_timestamp AT TIME ZONE 'UTC') AS TIMESTAMP));
CREATE OR REPLACE TEMP MACRO utc_date() AS (CAST(current_timestamp AT TIME ZONE 'UTC' AS DATE));`
