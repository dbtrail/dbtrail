package sqlsandbox

// pgRoundMacros are PostgreSQL's ROUND and FLOOR of a numeric division and
// ROUND of a numeric AVG for the copy of a PostgreSQL source, the PostgreSQL port's
// spelling of round(a / b, k) and round([10^j *] avg(x), k)
// (pgshim.RewriteForCopy). Installed with Session.PostgresRules.
//
// On PostgreSQL a division of numerics is exact up to a scale it picks for
// the quotient (select_div_scale, at least 16 significant digits), rounded
// there half away from zero, and round(x, k) then rounds that half away
// from zero to exactly k decimals: 50 is written 50.00. avg(x) of a whole
// number or a numeric is the same division, sum by count. The copy's
// engine divides a DECIMAL as a DOUBLE, and its round of a DOUBLE writes
// 50.0. So the quotient is worked out here with whole numbers only:
//
//   - each side as a 128-bit whole number and its scale, read from the
//     engine's type (DECIMAL(p,s)) and its text with the point taken out;
//   - the quotient times 10^k, rounded half away from zero: (2a + b) //
//     (2b) over the magnitudes, with the sign of the two;
//   - that whole number as a DECIMAL(38,0) times the unit the port writes
//     (0.01 for k = 2), which the engine multiplies exactly into a
//     DECIMAL(38,k): PostgreSQL's value and its scale.
//
// One rounding where PostgreSQL makes two (at its division's scale, then at
// k) gives another digit only when the quotient lies just under a half at k
// and PostgreSQL's first rounding reaches the half. Its division's scale is
// at least 16 less the number of digits of the quotient's whole part (from
// select_div_scale's weights: a quotient of weight w is at least 10000^w),
// so that band is known, and a quotient in it is refused here, never
// answered otherwise: through a forwarding account it then goes to
// PostgreSQL. So is a quotient too large to place (its scale would not
// reach past k).
//
// Refused too, because PostgreSQL would answer otherwise or not at all: a
// side that is not a whole number or a DECIMAL here (a float, which
// round(x, k) refuses on PostgreSQL, and 1e2, a numeric there and a DOUBLE
// here), and a division whose two sides are both whole numbers (PostgreSQL
// divides those as whole numbers, and the sum of a bigint, a numeric there,
// is a whole number here). A NULL side is NULL and a zero divisor fails, as
// on PostgreSQL; a number too large for 128 bits fails too. Every refusal is
// an error of the statement, which a forwarding account sends to the
// source.
//
// floor(a / b) is the same quotient, taken down to the whole number below it
// (toward minus infinity), a numeric with no decimals on PostgreSQL (95, not
// 95.0): _bintrail_pg_floor_div. PostgreSQL rounds the quotient at its
// division's scale first, so a quotient just under a whole number (just
// over one, when negative) may reach it there; the macro refuses that band,
// as the round macros refuse theirs.
//
// TEMP, as the other macros. A macro takes no DISTINCT, and the port leaves
// avg(DISTINCT x) alone.
const pgRoundMacros = `CREATE OR REPLACE TEMP MACRO _bintrail_pg_exact(x) AS (
  typeof(x) LIKE 'DECIMAL(%' OR typeof(x) IN ('TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'HUGEINT', 'UTINYINT', 'USMALLINT', 'UINTEGER', 'UBIGINT', 'UHUGEINT')
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_scale(x) AS (
  CASE WHEN typeof(x) LIKE 'DECIMAL(%' THEN CAST(regexp_extract(typeof(x), ',([0-9]+)[)]$', 1) AS INTEGER) ELSE 0 END
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_unscaled(x) AS (
  CAST(replace(CAST(x AS VARCHAR), '.', '') AS HUGEINT)
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_pow10(n) AS (
  CASE WHEN n BETWEEN 0 AND 38 THEN [CAST(1 AS HUGEINT), 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, 10000000000, 100000000000, 1000000000000, 10000000000000, 100000000000000, 1000000000000000, 10000000000000000, 100000000000000000, 1000000000000000000, 10000000000000000000, 100000000000000000000, 1000000000000000000000, 10000000000000000000000, 100000000000000000000000, 1000000000000000000000000, 10000000000000000000000000, 100000000000000000000000000, 1000000000000000000000000000, 10000000000000000000000000000, 100000000000000000000000000000, 1000000000000000000000000000000, 10000000000000000000000000000000, 100000000000000000000000000000000, 1000000000000000000000000000000000, 10000000000000000000000000000000000, 100000000000000000000000000000000000, 1000000000000000000000000000000000000, 10000000000000000000000000000000000000, 100000000000000000000000000000000000000][n + 1]
  ELSE error('the copy cannot round this as PostgreSQL does: a number with more than 38 digits') END
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_digits(v) AS (
  CASE WHEN v = 0 THEN 0 ELSE length(CAST(v AS VARCHAR)) END
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_qround(sgn, a, b, l, kk) AS (
  CASE WHEN l - kk < 1 OR (2 * (a % b) < b AND b - 2 * (a % b) <= b // _bintrail_pg_pow10(least(l - kk, 38)))
    THEN error('the copy cannot round this as PostgreSQL does: the quotient lies too close to a half, where PostgreSQL rounds it twice')
  ELSE sgn * ((2 * a + b) // (2 * b)) END
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_qcore(xn, sn, xd, sd, kk) AS (
  _bintrail_pg_qround(sign(xn) * sign(xd),
    abs(xn) * _bintrail_pg_pow10(sd + kk),
    abs(xd) * _bintrail_pg_pow10(sn),
    16 - _bintrail_pg_digits((abs(xn) * _bintrail_pg_pow10(sd)) // (abs(xd) * _bintrail_pg_pow10(sn))),
    kk)
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_round_q(n, d, kk, mode) AS (
  CASE
    WHEN NOT (_bintrail_pg_exact(n) AND _bintrail_pg_exact(d))
      THEN error('the copy cannot round this as PostgreSQL does: a side of the division is not a numeric or a whole number')
    WHEN mode = 'div' AND typeof(n) NOT LIKE 'DECIMAL(%' AND typeof(d) NOT LIKE 'DECIMAL(%'
      THEN error('the copy cannot round this as PostgreSQL does: both sides of the division are whole numbers')
    WHEN n IS NULL OR d IS NULL THEN NULL
    WHEN d = 0 THEN error('division by zero')
    ELSE _bintrail_pg_qcore(_bintrail_pg_unscaled(n), _bintrail_pg_scale(n), _bintrail_pg_unscaled(d), _bintrail_pg_scale(d), kk)
  END
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_fcore(sgn, a, b, l) AS (
  CASE WHEN l < 1 OR (a % b > 0 AND (CASE WHEN sgn >= 0 THEN 2 * (b - a % b) ELSE 2 * (a % b) END) <= b // _bintrail_pg_pow10(least(l, 38)))
    THEN error('the copy cannot take floor() of this as PostgreSQL does: the quotient lies too close to a whole number, where PostgreSQL rounds it first')
  WHEN sgn >= 0 THEN a // b
  ELSE -(a // b + CASE WHEN a % b > 0 THEN 1 ELSE 0 END) END
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_floor_core(xn, sn, xd, sd) AS (
  _bintrail_pg_fcore(sign(xn) * sign(xd),
    abs(xn) * _bintrail_pg_pow10(sd),
    abs(xd) * _bintrail_pg_pow10(sn),
    16 - _bintrail_pg_digits((abs(xn) * _bintrail_pg_pow10(sd)) // (abs(xd) * _bintrail_pg_pow10(sn))))
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_floor_div(n, d) AS (
  CAST(CASE
    WHEN NOT (_bintrail_pg_exact(n) AND _bintrail_pg_exact(d))
      THEN error('the copy cannot take floor() of this as PostgreSQL does: a side of the division is not a numeric or a whole number')
    WHEN typeof(n) NOT LIKE 'DECIMAL(%' AND typeof(d) NOT LIKE 'DECIMAL(%'
      THEN error('the copy cannot take floor() of this as PostgreSQL does: both sides of the division are whole numbers')
    WHEN n IS NULL OR d IS NULL THEN NULL
    WHEN d = 0 THEN error('division by zero')
    ELSE _bintrail_pg_floor_core(_bintrail_pg_unscaled(n), _bintrail_pg_scale(n), _bintrail_pg_unscaled(d), _bintrail_pg_scale(d))
  END AS DECIMAL(38,0))
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_round_div(n, d, k, unit) AS (
  CAST(_bintrail_pg_round_q(n, d, k, 'div') AS DECIMAL(38,0)) * unit
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_round_avg(x, kk, unit) AS (
  CAST(_bintrail_pg_round_q(sum(x), count(x), kk, 'avg') AS DECIMAL(38,0)) * unit
);`
