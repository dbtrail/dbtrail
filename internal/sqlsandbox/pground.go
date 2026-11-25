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
// extract(epoch from x) of a timestamp is a numeric with six decimals on
// PostgreSQL (1763460000.123456), a DOUBLE here: _bintrail_pg_epoch takes it
// from the engine's whole microseconds instead, exact, and refuses anything
// but a timestamp (a date or an interval PostgreSQL reads otherwise).
// to_timestamp(n) reads a double on PostgreSQL; _bintrail_pg_to_timestamp
// takes only a whole number of seconds, which a double holds exactly, so
// to_timestamp(floor(extract(epoch from x) / 600) * 600) answers as there.
//
// floor(extract(epoch from x) / N), N a whole number of seconds the port
// reads as written (pgshim epochOverSeconds), is _bintrail_pg_floor_epoch:
// the same answer and the same refusals as _bintrail_pg_floor_div of
// _bintrail_pg_epoch, worked out in 64-bit whole numbers straight from the
// engine's microseconds, a = epoch_us(x) over b = N * 1000000, where the
// general macros take each side through its text and 128 bits. It is the
// time bucket of a report, run on every row, and the general way made such
// a report three to five times slower on the copy. The quotient a / b has
// at most 13 whole digits (epoch_us stays under 2^63, b is at least a
// million), so PostgreSQL keeps at least 16 - 13 = 3 of its decimals and
// the band refused is at most b / 1000 wide: a gap to the whole number
// (twice the distance, in units of 1/b) with 1000 * gap > b is outside it
// and needs no count of digits. Only a quotient within b / 1000 of a whole
// number counts them, by comparisons, and is refused as the general macro
// refuses it. With N at most 999999999, 1000 * gap stays under 2 * 10^18.
// The engine's // and % of whole numbers truncate toward zero, so a
// negative a with a remainder is one below its quotient.
//
// round(x, k) of anything else, round(x) and avg(x) on their own are the
// port's too, on every connection (pgshim.RewriteForCopy):
//
//   - round(x, k), k from 0 to 15, is _bintrail_pg_round_k: a numeric of
//     exactly k decimals, as PostgreSQL's round(numeric, k) writes it (500 is
//     500.00, 12.50 to 3 is 12.500), where the engine keeps x's own type (500,
//     12.50). x is taken to a DECIMAL(38,s) first, a whole number to
//     DECIMAL(38,0): the engine's round of a BIGINT overflows in silence
//     (9223372036854775807 to tens comes out negative). The engine rounds a
//     DECIMAL half away from zero, as PostgreSQL does, and adding a zero of k
//     decimals gives the result k: the zero is DECIMAL(1,0) times the unit,
//     so the width stays within 38 and a value that does not fit fails.
//   - round(x, -k) is _bintrail_pg_round_neg: to tens, hundreds..., no
//     decimals. The engine's round of a narrow DECIMAL with decimals to a
//     negative k is wrong (-5.0, a DECIMAL(2,1), to tens is 0, PostgreSQL's
//     -10). Taken to DECIMAL(38,s) first it measured right, and the decimals
//     are cut too (trunc), which cannot change the answer: the half it
//     compares with, 5 * 10^(k-1), is a whole number, so cutting the
//     decimals never moves a value across it.
//   - round(x) is _bintrail_pg_round_whole: the engine's, which matches
//     PostgreSQL's round(numeric) for a DECIMAL. Refused for anything else:
//     PostgreSQL's round(double precision) rounds a half to even, and a whole
//     number is a double precision there (round(5) is a double, and
//     9007199254740993 is written 9.007199254740992e+15).
//   - avg(x) is _bintrail_pg_avg: the engine's for a float, which PostgreSQL
//     averages as a double precision too. Of a numeric or a whole number
//     PostgreSQL writes as many decimals as the quotient needs, from 16 to
//     20 and more (500.0000000000000000, 0.50000000000000000000), which is
//     not one type the copy can give a column: refused. It is the avg of a
//     connection the copy answers alone, and of a float everywhere.
//   - avg(x) of numerics or whole numbers on a connection that can reach
//     its source is _bintrail_pg_avgn (pgshim.retypeForCopy, #3219), and
//     n / d on its own is _bintrail_pg_div (pgshim.rewriteDivisions): the
//     exact quotient, of the exact sum over the count for the avg, rounded
//     once, half away from zero, to 16 decimals, a DECIMAL(38,16). It is
//     worked out in whole numbers of 128 bits, each side without its
//     decimal point (_bintrail_pg_uns), so a quotient that ends within 16
//     decimals is exact (0.3 / 3 is 0.1000000000000000). PostgreSQL writes
//     the same quotient with 16 significant digits or more: the same text
//     from 1 to 9999, 12 decimals above and 20 below. A divisor of zero is
//     PostgreSQL's error, where the engine answers NULL, and NULL over zero
//     is NULL, as on PostgreSQL. Two whole numbers, which PostgreSQL divides
//     as whole numbers, and a float are refused; the port's screen sends
//     both to the source before the copy is asked. A quotient or a side
//     past what 128 bits hold fails, and the source answers, and so does a
//     quotient under 0.000001 that is not zero: 16 decimals would keep too
//     few of its digits, or round it to zero and change which rows a
//     comparison finds.
//
// round(x, k) and round(x, -k) refuse a float: round(double precision, k)
// does not exist on PostgreSQL, and a float here may stand for a numeric
// there (1e2).
//
// A CASE or a COALESCE that holds one of these (pgshim packMixedScale) has
// one type here for all its values, where PostgreSQL keeps each value's own
// decimals: CASE WHEN c THEN round(x, 2) ELSE 0 END is 0 there and 0.00
// here. Each value is packed with the decimals of its own type
// (_bintrail_pg_pack; _bintrail_pg_packn for a COALESCE, which takes the
// first value that is not NULL), and _bintrail_pg_unpack refuses a value
// whose decimals are not the type's. A float on either side makes the CASE a
// float, as on PostgreSQL, and nothing is checked.
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
CREATE OR REPLACE TEMP MACRO _bintrail_pg_us_digits(w) AS (
  CASE WHEN w = 0 THEN 0 WHEN w < 10 THEN 1 WHEN w < 100 THEN 2 WHEN w < 1000 THEN 3
  WHEN w < 10000 THEN 4 WHEN w < 100000 THEN 5 WHEN w < 1000000 THEN 6 WHEN w < 10000000 THEN 7
  WHEN w < 100000000 THEN 8 WHEN w < 1000000000 THEN 9 WHEN w < 10000000000 THEN 10
  WHEN w < 100000000000 THEN 11 WHEN w < 1000000000000 THEN 12 WHEN w < 10000000000000 THEN 13
  ELSE error('the copy cannot take floor() of this as PostgreSQL does: a quotient past 13 digits') END
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_us_gap(a, b) AS (
  CASE WHEN a > 0 THEN 2 * (b - a % b) ELSE -2 * (a % b) END
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_us_floor(a, b) AS (
  CASE WHEN a % b = 0 THEN a // b
  WHEN 1000 * _bintrail_pg_us_gap(a, b) <= b
    AND _bintrail_pg_us_gap(a, b) <= b // _bintrail_pg_pow10(16 - _bintrail_pg_us_digits(abs(a) // b))
    THEN error('the copy cannot take floor() of this as PostgreSQL does: the quotient lies too close to a whole number, where PostgreSQL rounds it first')
  WHEN a > 0 THEN a // b
  ELSE a // b - 1 END
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_floor_epoch(x, n) AS (
  CAST(CASE WHEN typeof(x) IN ('TIMESTAMP', 'TIMESTAMP WITH TIME ZONE')
    THEN _bintrail_pg_us_floor(epoch_us(x), CAST(n AS BIGINT) * 1000000)
  ELSE error('the copy cannot take extract(epoch) of this as PostgreSQL does: it is not a timestamp') END AS DECIMAL(38,0))
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_epoch(x) AS (
  CASE WHEN typeof(x) IN ('TIMESTAMP', 'TIMESTAMP WITH TIME ZONE')
    THEN CAST(epoch_us(x) AS DECIMAL(38,0)) * CAST(0.000001 AS DECIMAL(7,6))
  ELSE error('the copy cannot take extract(epoch) of this as PostgreSQL does: it is not a timestamp') END
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_extract_whole(x) AS (
  CAST(x AS DECIMAL(38,0))
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_to_timestamp(x) AS (
  CASE WHEN NOT (typeof(x) LIKE 'DECIMAL(%,0)' OR typeof(x) IN ('TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'HUGEINT'))
    THEN error('the copy cannot take to_timestamp() of this as PostgreSQL does: it is not a whole number of seconds')
  WHEN abs(x) > 9007199254740992
    THEN error('the copy cannot take to_timestamp() of this as PostgreSQL does: too many seconds')
  ELSE to_timestamp(CAST(x AS DOUBLE)) END
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_round_div(n, d, k, unit) AS (
  CAST(_bintrail_pg_round_q(n, d, k, 'div') AS DECIMAL(38,0)) * unit
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_round_avg(x, kk, unit) AS (
  CAST(_bintrail_pg_round_q(sum(x), count(x), kk, 'avg') AS DECIMAL(38,0)) * unit
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_dec38(x) AS (
  CASE WHEN typeof(x) IN ('DOUBLE', 'FLOAT')
    THEN error('the copy cannot round this as PostgreSQL does: a double precision, which PostgreSQL rounds only to a whole number, half to even')
  WHEN NOT _bintrail_pg_exact(x)
    THEN error('the copy cannot round this as PostgreSQL does: it is not a numeric or a whole number')
  ELSE x + CAST(0 AS DECIMAL(38,0)) END
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_round_k(x, kk, unit) AS (
  round(_bintrail_pg_dec38(x), kk) + CAST(0 AS DECIMAL(1,0)) * unit
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_round_neg(x, kk) AS (
  round(trunc(_bintrail_pg_dec38(x)), kk)
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_round_whole(x) AS (
  CASE WHEN typeof(x) LIKE 'DECIMAL(%' THEN round(x)
  WHEN typeof(x) IN ('DOUBLE', 'FLOAT')
    THEN error('the copy cannot round this as PostgreSQL does: a double precision, which PostgreSQL rounds half to even')
  WHEN _bintrail_pg_exact(x)
    THEN error('the copy cannot round this as PostgreSQL does: a whole number, which PostgreSQL rounds as a double precision')
  ELSE error('the copy cannot round this as PostgreSQL does: it is not a numeric') END
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_avg(x) AS (
  CASE WHEN typeof(min(x)) IN ('DOUBLE', 'FLOAT') THEN avg(x)
  ELSE error('the copy cannot write avg() of this as PostgreSQL does: of a numeric or a whole number PostgreSQL writes as many decimals as each average needs; round(avg(x), k) the copy answers') END
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_uns(x) AS (
  CAST(x * _bintrail_pg_pow10(_bintrail_pg_scale(x)) AS HUGEINT)
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_q16(xn, sn, xd, sd) AS (
  CASE WHEN xn <> 0 AND abs(xn) * _bintrail_pg_pow10(sd + 6) < abs(xd) * _bintrail_pg_pow10(sn)
    THEN error('the copy cannot divide this as PostgreSQL does: a quotient under 0.000001, of which 16 decimals keep too few digits')
  ELSE sign(xn) * sign(xd) * ((2 * (abs(xn) * _bintrail_pg_pow10(sd + 16)) + abs(xd) * _bintrail_pg_pow10(sn)) // (2 * (abs(xd) * _bintrail_pg_pow10(sn)))) END
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_div(n, d) AS (
  CAST(CASE
    WHEN NOT (_bintrail_pg_exact(n) AND _bintrail_pg_exact(d))
      THEN error('the copy cannot divide this as PostgreSQL does: a side of the division is not a numeric or a whole number')
    WHEN typeof(n) NOT LIKE 'DECIMAL(%' AND typeof(d) NOT LIKE 'DECIMAL(%'
      THEN error('the copy cannot divide this as PostgreSQL does: both sides of the division are whole numbers')
    WHEN n IS NULL OR d IS NULL THEN NULL
    WHEN d = 0 THEN error('division by zero')
    ELSE _bintrail_pg_q16(_bintrail_pg_uns(n), _bintrail_pg_scale(n), _bintrail_pg_uns(d), _bintrail_pg_scale(d))
  END AS DECIMAL(38,0)) * CAST(0.0000000000000001 AS DECIMAL(16,16))
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_ascii(x) AS (
  CASE WHEN ascii(x) > 127
    THEN error('the copy cannot answer ascii() of this character: it depends on the source database''s encoding')
    ELSE ascii(x) END
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_avgn(x) AS (
  CAST(CASE
    WHEN NOT _bintrail_pg_exact(min(x))
      THEN error('the copy cannot write avg() of this as PostgreSQL does: it is not a numeric or a whole number')
    WHEN count(x) = 0 THEN NULL
    ELSE _bintrail_pg_q16(_bintrail_pg_uns(sum(x)), _bintrail_pg_scale(sum(x)), count(x), 0)
  END AS DECIMAL(38,0)) * CAST(0.0000000000000001 AS DECIMAL(16,16))
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_tscale(t) AS (
  CASE WHEN t LIKE 'DECIMAL(%' THEN CAST(regexp_extract(t, ',([0-9]+)[)]$', 1) AS INTEGER)
  WHEN t IN ('TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'HUGEINT', 'UTINYINT', 'USMALLINT', 'UINTEGER', 'UBIGINT', 'UHUGEINT') THEN 0
  ELSE -1 END
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_pack(x) AS (
  struct_pack(v := x, s := _bintrail_pg_tscale(typeof(x)))
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_packn(x) AS (
  CASE WHEN x IS NULL THEN NULL ELSE struct_pack(v := x, s := _bintrail_pg_tscale(typeof(x))) END
);
CREATE OR REPLACE TEMP MACRO _bintrail_pg_unpack(p) AS (
  CASE WHEN struct_extract(p, 'v') IS NOT NULL AND struct_extract(p, 's') >= 0
      AND _bintrail_pg_tscale(typeof(struct_extract(p, 'v'))) >= 0
      AND struct_extract(p, 's') <> _bintrail_pg_tscale(typeof(struct_extract(p, 'v')))
    THEN error('the copy cannot write this as PostgreSQL does: a CASE or COALESCE around a round() whose values have different numbers of decimals, which PostgreSQL keeps for each value')
  ELSE struct_extract(p, 'v') END
);`
