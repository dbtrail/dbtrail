package sqlsandbox

// avgMacros are MySQL's AVG of whole numbers for the copy (#2920), the
// routing port's spelling of AVG(x) and AVG(DISTINCT x)
// (readrouter.CopyOptions.MySQLAvg). DuckDB's avg is a DOUBLE: AVG of 1, 2
// and 4 is 2.3333333333333335 there and the DECIMAL 2.3333 on MySQL, and
// DuckDB divides a DECIMAL as a DOUBLE too. So the average is computed with
// whole numbers only:
//
//   - the sum as a 128-bit integer (x cast to HUGEINT first, so a BIGINT
//     UNSIGNED and a BOOLEAN sum as MySQL sums them), and the count;
//   - the sum times 10^4 divided by the count, rounded half away from zero,
//     as MySQL rounds it (one 1 among 32 rows is 0.0313, one -1 -0.0313):
//     (2 * |sum| * 10^4 + count) // (2 * count), with the sum's sign;
//   - that whole number times 0.0001, a DECIMAL(38,0) by a DECIMAL(4,4),
//     which DuckDB multiplies exactly into a DECIMAL(38,4): MySQL's type and
//     scale for the AVG of a whole number under div_precision_increment 4,
//     the only value the routing port lets reach the copy.
//
// No row, or NULLs only, is NULL, as on MySQL. A sum too large for the
// multiplication (past about 8.5e33) or for the sum itself (past about
// 1.7e38) is an error on the copy, never a wrapped number
// (TestAvgMacros_overflow_2920); the router sends a statement the copy
// errored on to MySQL (shim RouteReasonCopyRefused). No BIGINT column reaches
// either in fewer than about 10^15 rows.
//
// What they are given is the router's to decide (readrouter.AvgVeto): a bare
// whole-number column. Over anything else the cast would round a fraction
// away. Installed with nowMacros, for a client that writes MySQL
// (Session.MySQLClock); TEMP, as they are. A macro takes no DISTINCT, so
// AVG(DISTINCT x) has one of its own.
const avgMacros = `CREATE OR REPLACE TEMP MACRO _bintrail_avg_of(s, n) AS (
  CASE WHEN n = 0 THEN NULL
  ELSE CAST(sign(s) * ((abs(s) * 20000 + n) // (2 * n)) AS DECIMAL(38,0)) * CAST(0.0001 AS DECIMAL(4,4))
  END
);
CREATE OR REPLACE TEMP MACRO _bintrail_avg(x) AS (
  _bintrail_avg_of(sum(CAST(x AS HUGEINT)), count(x))
);
CREATE OR REPLACE TEMP MACRO _bintrail_avg_distinct(x) AS (
  _bintrail_avg_of(sum(DISTINCT CAST(x AS HUGEINT)), count(DISTINCT x))
);`
