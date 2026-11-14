package readrouter

import (
	"strings"
	"testing"
)

// #2790: on a connection the copy answers alone, MySQL's two-argument
// DATEDIFF given a string with a UTC offset after its time is refused, since
// MySQL moves that time into the session's zone (or reads NULL for an offset
// it does not take) and the copy takes the date as written. Each case below
// was measured on MySQL 8.0.46 against the copy under the zones UTC, +05:00
// and -08:00: the refused ones answered differently in at least one, the
// answered ones alike in all.
func TestUnroutedDateDiffOffsetVeto(t *testing.T) {
	refused := []string{
		// The issue's case, and each side.
		"SELECT DATEDIFF('2026-01-15 02:00:00+05:00', d) FROM t",
		"SELECT DATEDIFF(d, '2026-01-15 02:00:00+05:00') FROM t",
		"SELECT DATEDIFF(d, '2026-01-15 22:00:00-05:00') FROM t",
		// Both arguments.
		"SELECT DATEDIFF('2026-01-15 02:00:00+05:00', '2026-01-16 02:00:00+05:00')",
		// Case, white space, a T, one-digit fields, a fraction.
		"select datediff ( ' 2026-01-15 02:00:00+05:00 ' , d ) from t",
		"SELECT DateDiff('2026-01-15T02:00:00+05:00', d) FROM t",
		"SELECT DATEDIFF('2026-01-15 2:0:0+05:00', d) FROM t",
		"SELECT DATEDIFF('2026-01-15 02:00:00.5+05:00', d) FROM t",
		"SELECT DATEDIFF('2026-01-15 02:00:00.+05:00', d) FROM t",
		// Offsets MySQL reads as NULL, which the copy answers with a number.
		"SELECT DATEDIFF('2026-01-15 02:00:00+0500', d) FROM t",
		"SELECT DATEDIFF('2026-01-15 02:00:00+05', d) FROM t",
		"SELECT DATEDIFF('2026-01-15 02:00:00-00:00', d) FROM t",
		"SELECT DATEDIFF('2026-01-15 02:00:00+14:01', d) FROM t",
		// An offset of zero: MySQL's answer then depends on the session's zone.
		"SELECT DATEDIFF('2026-01-15 02:00:00+00:00', d) FROM t",
		// Inside an argument.
		"SELECT DATEDIFF(COALESCE('2026-01-15 02:00:00+05:00', d), d) FROM t",
		"SELECT DATEDIFF(CAST('2026-01-15 02:00:00+05:00' AS DATETIME), d) FROM t",
		"SELECT DATEDIFF(TIMESTAMP '2026-01-15 02:00:00+05:00', d) FROM t",
		"SELECT DATEDIFF(d, (SELECT '2026-01-15 02:00:00+05:00')) FROM t",
		// In WHERE, nested in another DATEDIFF, beside a doubled quote.
		"SELECT id FROM t WHERE DATEDIFF(d, '2026-01-15 02:00:00+05:00') > 0",
		"SELECT DATEDIFF(d, d2) + DATEDIFF(d, '2026-01-15 02:00:00+05:00') FROM t",
		"SELECT DATEDIFF(DATEDIFF(d, '2026-01-15 02:00:00+05:00'), d) FROM t",
		"SELECT 'it''s', DATEDIFF(d, '2026-01-15 02:00:00+05:00') FROM t",
		// A comment beside the call.
		"SELECT /* x */ DATEDIFF(d, /* y */ '2026-01-15 02:00:00+05:00') FROM t",
		"SELECT DATEDIFF(d, -- y\n'2026-01-15 02:00:00+05:00') FROM t",
		// A prepared statement's argument, as the port writes it in.
		"SELECT DATEDIFF(d,  '2026-01-15 02:00:00+05:00' ) FROM t",
		// A backtick-quoted column beside it, one whose name holds a comma,
		// a parenthesis or a quote among them.
		"SELECT DATEDIFF(`d`, '2026-01-15 02:00:00+05:00') FROM `t`",
		"SELECT DATEDIFF(`a,b`, '2026-01-15 02:00:00+05:00') FROM t",
		"SELECT DATEDIFF(`a)b`, '2026-01-15 02:00:00+05:00') FROM t",
		"SELECT DATEDIFF(`a'b`, '2026-01-15 02:00:00+05:00') FROM t",
		"SELECT DATEDIFF(`a\"b`, '2026-01-15 02:00:00+05:00') FROM t",
		"SELECT DATEDIFF(`a``b`, '2026-01-15 02:00:00+05:00') FROM t",
		// Strings side by side, which MySQL reads as one.
		"SELECT DATEDIFF('2026-01-15 02:00:00' '+05:00', d) FROM t",
		"SELECT DATEDIFF('2026-01-15 02:00:00'\n'+05:00', d) FROM t",
		"SELECT DATEDIFF('2026-01-15 02:00:00' /* x */ '+05:00', d) FROM t",
	}
	for _, stmt := range refused {
		r, ok := UnroutedDateDiffOffsetVeto(stmt)
		if !ok {
			t.Errorf("%q: answered, want refused", stmt)
			continue
		}
		if r != dateDiffOffsetRefusal {
			t.Errorf("%q: refused with %+v", stmt, r)
		}
		if r2, ok2 := UnroutedVeto(stmt); !ok2 || r2 != r {
			t.Errorf("%q: UnroutedVeto gave %+v, %v, want the DATEDIFF refusal", stmt, r2, ok2)
		}
	}

	answered := []string{
		// Columns, NULL, a string with no offset.
		"SELECT DATEDIFF(d, dt) FROM t",
		"SELECT DATEDIFF(d, NULL) FROM t",
		"SELECT DATEDIFF(d, '2026-01-15 02:00:00') FROM t",
		"SELECT DATEDIFF(d, '2026-01-15') FROM t",
		"SELECT DATEDIFF(d, '2026-01-15 02:00:00.5') FROM t",
		// What MySQL does not read as an offset: both answer alike.
		"SELECT DATEDIFF('2026-01-15 02:00:00Z', d) FROM t",
		"SELECT DATEDIFF('2026-01-15 23:30:00z', d) FROM t",
		"SELECT DATEDIFF('2026-01-15 02:00:00 +05:00', d) FROM t",
		"SELECT DATEDIFF('2026-01-15+05:00', d) FROM t",
		"SELECT DATEDIFF('2026-01-15 02:00+05:00', d) FROM t",
		"SELECT DATEDIFF('2026-01-15 02:00:00 UTC', d) FROM t",
		// The string outside DATEDIFF.
		"SELECT DATEDIFF(d, dt), '2026-01-15 02:00:00+05:00' FROM t",
		"SELECT DATEDIFF(d, dt) FROM t WHERE s = '2026-01-15 02:00:00+05:00'",
		// Inside a string or a comment.
		"SELECT 'DATEDIFF(''2026-01-15 02:00:00+05:00'', d)' FROM t",
		"SELECT DATEDIFF(d, dt) /* DATEDIFF('2026-01-15 02:00:00+05:00', d) */ FROM t",
		"SELECT DATEDIFF(d, dt) FROM t -- DATEDIFF('2026-01-15 02:00:00+05:00', d)",
		"SELECT DATEDIFF(d, dt) FROM t # DATEDIFF('2026-01-15 02:00:00+05:00', d)",
		// A name, not MySQL's function: next to a dot, quoted.
		"SELECT db.datediff('2026-01-15 02:00:00+05:00', d) FROM t",
		"SELECT `datediff`('2026-01-15 02:00:00+05:00', d) FROM t",
		"SELECT \"datediff\"('2026-01-15 02:00:00+05:00', d) FROM t",
		"SELECT x_datediff('2026-01-15 02:00:00+05:00', d) FROM t",
		// The copy's own functions: a unit first, or date_diff.
		"SELECT datediff('day', '2026-01-15 02:00:00+05:00', d) FROM t",
		"SELECT date_diff('day', CAST(d AS DATE), CAST(CAST('2026-01-15 02:00:00+05:00' AS TIMESTAMPTZ) AS DATE)) FROM t",
		// One argument, which MySQL refuses by itself, and a call left open.
		"SELECT DATEDIFF('2026-01-15 02:00:00+05:00') FROM t",
		"SELECT DATEDIFF('2026-01-15 02:00:00+05:00', d FROM t",
		// Two strings that are not side by side.
		"SELECT DATEDIFF(CONCAT('2026-01-15 02:00:00', '+05:00'), d) FROM t",
		// No offset after a date that ends in a number.
		"SELECT DATEDIFF('2026-01-01', '2025-12-31') FROM t",
		"",
	}
	for _, stmt := range answered {
		if r, ok := UnroutedDateDiffOffsetVeto(stmt); ok {
			t.Errorf("%q: refused (%s), want answered", stmt, r.Message())
		}
	}

	msg := dateDiffOffsetRefusal.Message()
	for _, want := range []string{"DATEDIFF", "+05:00", "time zone", "NULL", "CAST(CAST(a AS TIMESTAMPTZ) AS DATE)", "Engine Routing"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the refusal %q does not name %q", msg, want)
		}
	}
}

// Under Engine Routing nothing is refused: a DATEDIFF of a string, with an
// offset or without, keeps the statement on MySQL (#2611), which answers it.
func TestDateFunctionVeto_offsetStringStaysOnMySQL(t *testing.T) {
	for _, stmt := range []string{
		"SELECT DATEDIFF('2026-01-15 02:00:00+05:00', d) FROM t",
		"SELECT DATEDIFF(d, '2026-01-15 02:00:00+05:00') FROM t",
		"SELECT DATEDIFF(COALESCE('2026-01-15 02:00:00+05:00', d), d) FROM t",
		"SELECT DATEDIFF(s, d) FROM t",
	} {
		if why := DateFunctionVeto(string(ShapeOf(stmt)), []string{"d"}, []string{"s"}); why != VetoDateFunction {
			t.Errorf("%q: veto %q, want %q", stmt, why, VetoDateFunction)
		}
	}
}
