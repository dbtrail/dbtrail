package pgshim

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
)

// Exactness of the PostgreSQL port against PostgreSQL 16 (#3024, #3025,
// #3026, #3027). What a test says PostgreSQL does was read from PostgreSQL
// 16.15 (UTF8, en_US.utf8); what it says the copy does, from the copy's
// engine through internal/pgcompare (zoneAndBetween).

// --- #3024: a time zone in a literal, and both bounds of a BETWEEN -----------

func TestZonedTyped(t *testing.T) {
	// PostgreSQL 16 reads each of these as the same value with the zone
	// taken out, in a session in UTC and in one in America/Bogota:
	// TIMESTAMP '2026-03-02 10:00:00+02' is 2026-03-02 10:00:00.
	zoned := map[string][]string{
		"timestamp": {
			"2026-03-02 10:00:00+02", "2026-03-02 10:00:00+02:00", "2026-03-02 10:00:00+0200",
			"2026-03-02 10:00:00 +02", "2026-03-02 10:00:00-05:30", "2026-03-02T10:00:00+02:00",
			"2026-03-02T10:00:00Z", "2026-03-02 10:00:00.123456+02", "2026-03-02 10:00:00 UTC",
			"2026-03-02 10:00:00 utc", "2026-03-02 10:00:00 GMT", "2026-03-02 10:00:00 Etc/UTC",
			"2026-03-02 10:00:00 PST", "2026-03-02 10:00:00 America/Bogota", "2026-03-02 10:00:00+02:30:15",
			"2026-03-02 10:00+02", "2026-03-02+02", "2026-03-02 10:00:00+16",
		},
		"date": {
			"2026-03-02 23:30:00-05", "2026-03-02 00:30:00+05", "2026-03-02T23:30:00-05:00",
			"2026-03-02 23:30:00 UTC", "2026-03-02+02", "2026-03-02 Z", "2026-03-02 -05:00",
		},
		"time": {"10:00:00+02", "23:30:00-05:00", "10:00:00 UTC", "10:00:00Z", "10:00:00 PST", "10:00+02"},
	}
	for typ, texts := range zoned {
		for _, text := range texts {
			if !zonedTyped(typ, text) {
				t.Errorf("%s '%s': not read as written with a time zone", typ, text)
			}
		}
	}
	plain := map[string][]string{
		"timestamp": {"2026-03-02 10:00:00", "2026-03-02 10:00:00.5", "2026-03-02 10:00", "2026-03-02", "2026-03-02T10:00:00", "1999-12-31 23:59:59.999999"},
		"date":      {"2026-03-02", "2026-12-31", "1999-01-01", "2026-3-2"},
		"time":      {"10:00:00", "10:00", "23:59:59.999999"},
		// An interval holds no zone: PostgreSQL reads 1:30 hours as 01:30:00
		// and refuses 10:00+02.
		"interval": {"1:30 hours", "1 day", "10:00+02", "2 hours 30 minutes"},
	}
	for typ, texts := range plain {
		for _, text := range texts {
			if zonedTyped(typ, text) {
				t.Errorf("%s '%s': read as written with a time zone", typ, text)
			}
		}
	}
}

func TestCopyScreen_zoneInALiteral(t *testing.T) {
	const o, c = "pgcmp_orders", "pgcmp_customers"
	toSource := map[string]string{
		`SELECT id FROM ` + o + ` WHERE placed_at = TIMESTAMP '2026-03-02 10:00:00+02'`:                                               zonedTypedVeto,
		`SELECT id FROM ` + o + ` WHERE placed_at = timestamp '2026-03-02 10:00:00 Europe/Paris'`:                                     zonedTypedVeto,
		`SELECT id FROM ` + o + ` WHERE placed_at >= TIMESTAMP '2026-03-02T10:00:00Z'`:                                                zonedTypedVeto,
		`SELECT id FROM ` + o + ` WHERE placed_at IN (TIMESTAMP '2026-03-02 10:00:00+02')`:                                            zonedTypedVeto,
		`SELECT TIMESTAMP '2026-03-02 10:00:00+02' AS t`:                                                                              zonedTypedVeto,
		`SELECT TIME '10:00:00+02' AS t`:                                                                                              zonedTypedVeto,
		`SELECT name FROM ` + c + ` WHERE signup = DATE '2026-01-15 23:30:00-05'`:                                                     zonedTypedVeto,
		`SELECT name FROM ` + c + ` WHERE signup = DATE '2026-01-15+02'`:                                                              zonedTypedVeto,
		`SELECT name FROM ` + c + ` WHERE signup = '2026-01-15 23:30:00-05'::date`:                                                    zonedTypedVeto,
		`SELECT id FROM ` + o + ` WHERE placed_at BETWEEN TIMESTAMP '2026-03-02 10:00:00' AND TIMESTAMP '2026-03-03 10:00:00+02'`:     zonedTypedVeto,
		`SELECT id FROM ` + o + ` WHERE placed_at NOT BETWEEN TIMESTAMP '2026-03-02 10:00:00+02' AND TIMESTAMP '2026-03-03 10:00:00'`: zonedTypedVeto,
		// A cast to a timestamp was the source's already, zone or none.
		`SELECT id FROM ` + o + ` WHERE placed_at = '2026-03-02 10:00:00+02'::timestamp`:         zonedTypedVeto,
		`SELECT id FROM ` + o + ` WHERE placed_at = '2026-03-02 10:00:00'::timestamp`:            "cast",
		`SELECT id FROM ` + o + ` WHERE placed_at = CAST('2026-03-02 10:00:00+02' AS timestamp)`: "CAST",
		`SELECT id FROM ` + o + ` WHERE placed_at = '2026-03-02 10:00:00+02'`:                    zonedCompareVeto,
		`SELECT id FROM ` + o + ` WHERE '2026-03-02 10:00:00+02' = placed_at`:                    zonedCompareVeto,
	}
	for q, why := range toSource {
		if got := CopyScreen(q, routeTypes); !strings.Contains(got, why) {
			t.Errorf("%s:\n  got %q, want the source for %q", q, got, why)
		}
	}
	copyOK := []string{
		`SELECT id FROM ` + o + ` WHERE placed_at = TIMESTAMP '2026-03-02 10:00:00'`,
		`SELECT id FROM ` + o + ` WHERE placed_at < TIMESTAMP '2026-02-01 00:00:00.5'`,
		`SELECT name FROM ` + c + ` WHERE signup >= DATE '2026-01-01'`,
		`SELECT name FROM ` + c + ` WHERE signup >= '2026-01-01'::date`,
		`SELECT id FROM ` + o + ` WHERE placed_at = '2026-03-02 10:00:00'`,
	}
	for _, q := range copyOK {
		if why := CopyScreen(q, routeTypes); why != "" {
			t.Errorf("%s: sent to the source (%s), want the copy", q, why)
		}
	}
}

func TestCopyScreen_bothBoundsOfBetween(t *testing.T) {
	const o, c = "pgcmp_orders", "pgcmp_customers"
	const text = "text compared with BETWEEN"
	toSource := map[string]string{
		// A zone in either bound, against a timestamp column.
		`SELECT id FROM ` + o + ` WHERE placed_at BETWEEN '2026-03-02 10:00:00' AND '2026-03-03 10:00:00+02'`:                             zonedCompareVeto,
		`SELECT id FROM ` + o + ` WHERE placed_at BETWEEN '2026-03-02 10:00:00' AND '2026-03-03 10:00:00 Europe/Paris'`:                   zonedCompareVeto,
		`SELECT id FROM ` + o + ` WHERE placed_at BETWEEN '2026-03-02 12:00:00+02' AND '2026-03-03 10:00:00'`:                             zonedCompareVeto,
		`SELECT id FROM ` + o + ` o WHERE o.placed_at BETWEEN '2026-03-02 10:00:00' AND '2026-03-03T10:00:00Z'`:                           zonedCompareVeto,
		`SELECT id FROM ` + o + ` WHERE qty BETWEEN 1 AND 5 AND placed_at BETWEEN '2026-03-02' AND '2026-03-03 10:00:00+02'`:              zonedCompareVeto,
		`SELECT id FROM ` + o + ` WHERE (placed_at BETWEEN '2026-03-02 10:00:00' AND '2026-03-03 10:00:00+02') AND qty BETWEEN 1 AND 100`: zonedCompareVeto,
		`SELECT id FROM ` + o + ` WHERE (qty BETWEEN 1 AND 5) = (placed_at BETWEEN '2026-03-02' AND '2026-03-03 10:00:00+02')`:            zonedCompareVeto,
		// With NOT the column is not the word before BETWEEN: a string in
		// either bound is the source's.
		`SELECT id FROM ` + o + ` WHERE placed_at NOT BETWEEN '2026-01-01 00:00:00' AND '2026-03-03 10:00:00+02'`: text,
		// A column with a time zone: a zone in a bound is compared as text
		// by the screen, as with < and >.
		`SELECT id FROM ` + o + ` WHERE seen_at BETWEEN '2026-03-02 10:00:00+00' AND '2026-03-03 12:00:00+02'`: text,
		`SELECT id FROM ` + o + ` WHERE seen_at BETWEEN '2026-03-02 10:00:00' AND '2026-03-03 12:00:00+02'`:    text,
		// Text in the upper bound.
		`SELECT id FROM ` + o + ` WHERE qty BETWEEN 1 AND note`:         text,
		`SELECT id FROM ` + o + ` WHERE 5 BETWEEN 1 AND note`:           text,
		`SELECT id FROM ` + o + ` o WHERE 5 BETWEEN 1 AND o.note`:       text,
		`SELECT name FROM ` + c + ` WHERE $1 BETWEEN $2 AND name`:       text,
		`SELECT name FROM ` + c + ` WHERE $1 BETWEEN 'a' AND name`:      text,
		`SELECT name FROM ` + c + ` WHERE $1 BETWEEN name AND $2`:       text,
		`SELECT name FROM ` + c + ` WHERE 'b' BETWEEN 'a' AND name`:     text,
		`SELECT name FROM ` + c + ` WHERE name BETWEEN 'a' AND 'c'`:     text,
		`SELECT name FROM ` + c + ` WHERE city BETWEEN name AND 'z'`:    text,
		`SELECT name FROM ` + c + ` WHERE name NOT BETWEEN 'a' AND 'c'`: text,
		// Nothing gives two parameters, or a parameter and a string, a type.
		`SELECT id FROM ` + o + ` WHERE $1 BETWEEN $2 AND $3`:          text,
		`SELECT id FROM ` + o + ` WHERE $1 BETWEEN 1 AND $2`:           text,
		`SELECT id FROM ` + o + ` WHERE $1 BETWEEN 1 AND 'x'`:          text,
		// The AND of a BETWEEN is not one inside a parenthesis of its lower bound.
		`SELECT id FROM ` + o + ` WHERE qty BETWEEN (CASE WHEN id > 1 AND id < 5 THEN 1 ELSE 2 END) AND note`: text,
		`SELECT id FROM ` + o + ` WHERE qty BETWEEN SYMMETRIC 9 AND 2`: "SYMMETRIC",
		`SELECT id FROM ` + o + ` WHERE qty BETWEEN 1`:                 "AND was not found",
		`SELECT id FROM ` + o + ` WHERE (qty BETWEEN 1) AND 2 = 2`:     "AND was not found",
	}
	for q, why := range toSource {
		if got := CopyScreen(q, routeTypes); !strings.Contains(got, why) {
			t.Errorf("%s:\n  got %q, want the source for %q", q, got, why)
		}
	}
	copyOK := []string{
		`SELECT id FROM ` + o + ` WHERE qty BETWEEN 2 AND 9`,
		`SELECT id FROM ` + o + ` WHERE qty NOT BETWEEN 3 AND 10`,
		`SELECT id FROM ` + o + ` WHERE amount BETWEEN 5 AND 50`,
		`SELECT id FROM ` + o + ` WHERE placed_at BETWEEN '2026-03-02 10:00:00' AND '2026-03-03 10:00:00'`,
		`SELECT id FROM ` + o + ` WHERE placed_at BETWEEN TIMESTAMP '2026-03-02 10:00:00' AND TIMESTAMP '2026-03-03 10:00:00'`,
		`SELECT id FROM ` + o + ` WHERE placed_at NOT BETWEEN TIMESTAMP '2026-01-01 00:00:00' AND TIMESTAMP '2026-03-03 10:00:00'`,
		`SELECT name FROM ` + c + ` WHERE signup BETWEEN DATE '2025-12-01' AND DATE '2026-01-15'`,
		`SELECT name FROM ` + c + ` WHERE signup BETWEEN '2025-12-01' AND '2026-01-15'`,
		// In the extended protocol each bound takes the column's type.
		`SELECT id FROM ` + o + ` WHERE qty BETWEEN $1 AND $2`,
		`SELECT id FROM ` + o + ` WHERE placed_at BETWEEN $1 AND $2`,
		`SELECT id FROM ` + o + ` WHERE qty BETWEEN 2 AND $1`,
		// One BETWEEN after another, inside one, and beside one.
		`SELECT id FROM ` + o + ` WHERE qty BETWEEN 1 AND 5 AND id BETWEEN 2 AND 3`,
		`SELECT id FROM ` + o + ` WHERE (qty BETWEEN 1 AND 5) AND amount BETWEEN 1 AND 2`,
		`SELECT id FROM ` + o + ` WHERE qty BETWEEN (1) AND (qty + 1)`,
		`SELECT id FROM ` + o + ` WHERE qty BETWEEN (CASE WHEN id > 1 AND id < 5 THEN 1 ELSE 2 END) AND 9`,
		`SELECT id FROM ` + o + ` o WHERE o.qty BETWEEN o.id AND o.customer_id`,
		`SELECT customer_id FROM ` + o + ` GROUP BY customer_id HAVING count(*) BETWEEN 1 AND 5`,
	}
	for _, q := range copyOK {
		if why := CopyScreen(q, routeTypes); why != "" {
			t.Errorf("%s: sent to the source (%s), want the copy", q, why)
		}
	}
}

// An operator of several characters does not end in + or -, unless it holds
// one of ~ ! @ # % ^ & | ` ?. PostgreSQL 16: 1 =-1 is false, 2+-3 is -1,
// 3*-2 is -6, 1 <>-1 is true, 5 >=-5 is true; 2 !=-2 and 1 @-1 fail with
// "operator does not exist: integer !=- integer".
func TestOperatorLen(t *testing.T) {
	for op, want := range map[string]int{
		"=": 1, "-": 1, "+": 1, "=-": 1, "+-": 1, "*-": 1, "/-": 1, "<-": 1, "<>-": 2, ">=-": 2, "<=+": 2,
		"=-+": 1, "+-+-": 1, "-+": 1, "--": 1,
		"!=-": 3, "@-": 2, "~-": 2, "#-": 2, "%-": 2, "^-": 2, "&-": 2, "|-": 2, "||-": 3, "?-": 2, "`-": 2,
		"<->": 3, "->": 2, "->>": 3, "<=": 2, "!=": 2, "-|": 2,
	} {
		if got := operatorLen(op); got != want {
			t.Errorf("operatorLen(%q) = %d, want %d", op, got, want)
		}
	}
	toks, spans, why := lexSQLSpans(`a =-1 OR b<>-2 OR c !=-3`)
	if why != "" {
		t.Fatal(why)
	}
	var got []string
	for i, tok := range toks {
		got = append(got, tok.text)
		if q := `a =-1 OR b<>-2 OR c !=-3`; tok.kind == rtOp && q[spans[i][0]:spans[i][1]] != tok.text {
			t.Errorf("the operator %q is placed at %q", tok.text, q[spans[i][0]:spans[i][1]])
		}
	}
	if want := "a = - 1 or b <> - 2 or c !=- 3"; strings.Join(got, " ") != want {
		t.Errorf("tokens %q, want %q", strings.Join(got, " "), want)
	}
	for _, q := range []string{
		`SELECT id FROM pgcmp_orders WHERE qty =-5`,
		`SELECT id, qty+-1, qty*-2 FROM pgcmp_orders WHERE qty <>-5 AND qty >=-5`,
	} {
		if why := CopyScreen(q, routeTypes); why != "" {
			t.Errorf("%s: sent to the source (%s), want the copy", q, why)
		}
	}
	if why := CopyScreen(`SELECT id FROM pgcmp_orders WHERE qty !=-5`, routeTypes); !strings.Contains(why, "the operator !=-") {
		t.Errorf("qty !=-5: %q, want the source for the operator !=-", why)
	}
}

// --- #3025: the fraction of a second ----------------------------------------

func TestTimestampText(t *testing.T) {
	at := func(y int, mo time.Month, d, h, mi, s, ns int) time.Time {
		return time.Date(y, mo, d, h, mi, s, ns, time.UTC)
	}
	for _, tt := range []struct {
		name string
		in   time.Time
		want string // PostgreSQL 16's text of the same timestamp
	}{
		{"no fraction", at(2026, 1, 2, 3, 4, 5, 0), "2026-01-02 03:04:05"},
		{"half a second", at(2026, 1, 2, 3, 4, 5, 500_000_000), "2026-01-02 03:04:05.5"},
		{"trailing zeros dropped", at(2026, 1, 2, 3, 4, 5, 120_000_000), "2026-01-02 03:04:05.12"},
		{"six digits", at(2026, 1, 2, 3, 4, 5, 123_456_000), "2026-01-02 03:04:05.123456"},
		{"one microsecond", at(2026, 1, 2, 3, 4, 5, 1_000), "2026-01-02 03:04:05.000001"},
		{"inner zeros kept", at(2026, 1, 2, 3, 4, 5, 100_200_000), "2026-01-02 03:04:05.1002"},
		// TIMESTAMP '2026-01-02 03:04:05.1234567' is .123457 on PostgreSQL,
		// and '...05.9999995' is 03:04:06.
		{"finer than a microsecond is rounded", at(2026, 1, 2, 3, 4, 5, 123_456_700), "2026-01-02 03:04:05.123457"},
		{"rounded into the next second", at(2026, 1, 2, 3, 4, 5, 999_999_600), "2026-01-02 03:04:06"},
		{"another zone is written in UTC", time.Date(2026, 1, 2, 3, 4, 5, 120_000_000, time.FixedZone("x", -5*3600)), "2026-01-02 08:04:05.12"},
		{"midnight", at(2026, 1, 2, 0, 0, 0, 0), "2026-01-02 00:00:00"},
		{"year 99", at(99, 1, 2, 3, 4, 5, 500_000_000), "0099-01-02 03:04:05.5"},
		{"year 1", at(1, 1, 1, 0, 0, 0, 0), "0001-01-01 00:00:00"},
		{"1 BC", at(0, 12, 31, 23, 59, 59, 250_000_000), "0001-12-31 23:59:59.25 BC"},
		{"44 BC", at(-43, 3, 15, 12, 0, 0, 500_000_000), "0044-03-15 12:00:00.5 BC"},
		{"4713 BC", at(-4712, 1, 1, 0, 0, 0, 0), "4713-01-01 00:00:00 BC"},
		{"five digits of year", at(10000, 1, 1, 0, 0, 0, 500_000_000), "10000-01-01 00:00:00.5"},
		{"the last timestamp", at(294276, 12, 31, 23, 59, 59, 999_999_000), "294276-12-31 23:59:59.999999"},
		// What the copy's engine hands over for TIMESTAMP 'infinity' and
		// '-infinity' (read from it: 294247-01-10 04:00:54.775807 and
		// -290308-12-21 19:59:05.224193).
		{"infinity", time.UnixMicro(math.MaxInt64), "infinity"},
		{"-infinity", time.UnixMicro(-math.MaxInt64), "-infinity"},
		{"a microsecond before infinity", time.UnixMicro(math.MaxInt64 - 1).UTC(), "294247-01-10 04:00:54.775806"},
	} {
		if got := string(timestampText(tt.in)); got != tt.want {
			t.Errorf("%s: %q, want %q", tt.name, got, tt.want)
		}
		cell, err := textCell("c", tt.in)
		if err != nil || string(cell) != tt.want {
			t.Errorf("%s: textCell gives %q, %v; want %q", tt.name, cell, err, tt.want)
		}
	}
}

// --- #3027: the not-captured marker ------------------------------------------

func TestTextCellNotCapturedMarkerRefused(t *testing.T) {
	marker := map[string]any{event.NotCapturedKey: true}
	cell, err := textCell("payload", marker)
	if err == nil {
		t.Fatalf("the not-captured marker was written as a value: %q", cell)
	}
	for _, want := range []string{"payload", "not-captured marker", "defect"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal %q does not say %q", err, want)
		}
	}
	if _, err := imageCells(map[string]any{"id": "7", "payload": marker}, []string{"id", "payload"}); err == nil {
		t.Error("a row with the not-captured marker in a column was written")
	}
	// The unchanged-TOAST marker keeps its own refusal.
	if _, err := textCell("payload", map[string]any{event.UnchangedToastKey: true}); err == nil || !strings.Contains(err.Error(), "unchanged-TOAST") {
		t.Errorf("the unchanged-TOAST marker: %v", err)
	}
	// Only the marker itself: a value that looks like it is a value.
	for _, v := range []any{
		map[string]any{event.NotCapturedKey: false},
		map[string]any{event.NotCapturedKey: true, "x": 1},
		event.NotCapturedKey,
	} {
		if _, err := textCell("c", v); err != nil {
			t.Errorf("%v was refused as the marker: %v", v, err)
		}
	}
}

// --- #3026: savepoint names ---------------------------------------------------

// PostgreSQL 16 (UTF8): after SAVEPOINT "A", ROLLBACK TO a fails (savepoint
// "a" does not exist) and ROLLBACK TO "A" works; after SAVEPOINT A, ROLLBACK
// TO "a" works; after SAVEPOINT É, ROLLBACK TO é fails and ROLLBACK TO "É"
// works; after SAVEPOINT ÀB, ROLLBACK TO Àb works and ROLLBACK TO àb fails;
// "a""b" is a"b; a name is cut to 63 bytes.
func TestSavepointStmt(t *testing.T) {
	long := "a12345678901234567890123456789012345678901234567890123456789012" // 63 bytes
	for _, tt := range []struct{ stmt, kind, name string }{
		{`SAVEPOINT a`, "savepoint", "a"},
		{`SAVEPOINT A`, "savepoint", "a"},
		{`savepoint AbC`, "savepoint", "abc"},
		{`SAVEPOINT "A"`, "savepoint", "A"},
		{`SAVEPOINT "a"`, "savepoint", "a"},
		{`SAVEPOINT "AbC"`, "savepoint", "AbC"},
		{`SAVEPOINT "a b"`, "savepoint", "a b"},
		{`SAVEPOINT "a  b"`, "savepoint", "a  b"},
		{`SAVEPOINT "a""b"`, "savepoint", `a"b`},
		{`SAVEPOINT "a;b"`, "savepoint", "a;b"},
		{`SAVEPOINT "a--b"`, "savepoint", "a--b"},
		{`SAVEPOINT "release"`, "savepoint", "release"},
		{`SAVEPOINT É`, "savepoint", "É"},
		{`SAVEPOINT é`, "savepoint", "é"},
		{`SAVEPOINT "É"`, "savepoint", "É"},
		{`SAVEPOINT ÀB`, "savepoint", "Àb"},
		{`SAVEPOINT İ`, "savepoint", "İ"},
		{`SAVEPOINT a$1`, "savepoint", "a$1"},
		{`SAVEPOINT A_1`, "savepoint", "a_1"},
		{`SAVEPOINT savepoint`, "savepoint", "savepoint"},
		{`SAVEPOINT ` + long + `x`, "savepoint", long},
		{`SAVEPOINT ` + long + `y`, "savepoint", long},
		{`SAVEPOINT "` + strings.ToUpper(long) + `Z"`, "savepoint", strings.ToUpper(long)},
		// Cut at 63 bytes, never inside a character: 62 bytes and é (2).
		{`SAVEPOINT ` + long[:62] + `éz`, "savepoint", long[:62]},
		{"  SAVEPOINT\n\ta ; ", "savepoint", "a"},
		{`SAVEPOINT a;;`, "savepoint", "a"},
		{`SAVEPOINT /* x /* y */ */ a -- z`, "savepoint", "a"},
		{`SAVEPOINT a; -- done`, "savepoint", "a"},
		{`RELEASE a`, "release", "a"},
		{`RELEASE A`, "release", "a"},
		{`RELEASE "A"`, "release", "A"},
		{`RELEASE SAVEPOINT a`, "release", "a"},
		{`release savepoint "A"`, "release", "A"},
		{`RELEASE savepoint`, "release", "savepoint"},
		{`RELEASE SAVEPOINT savepoint`, "release", "savepoint"},
		{`RELEASE/*x*/SAVEPOINT/* y */a`, "release", "a"},
		{`ROLLBACK TO a`, "rollback", "a"},
		{`ROLLBACK TO A`, "rollback", "a"},
		{`ROLLBACK TO "A"`, "rollback", "A"},
		{`ROLLBACK TO SAVEPOINT a`, "rollback", "a"},
		{`rollback to savepoint "A";`, "rollback", "A"},
		{`ROLLBACK WORK TO SAVEPOINT a`, "rollback", "a"},
		{`ROLLBACK TRANSACTION TO a`, "rollback", "a"},
		{`ROLLBACK TO savepoint`, "rollback", "savepoint"},
		{`ROLLBACK TO "a b"`, "rollback", "a b"},
		// Not one of the three.
		{`ROLLBACK`, "", ""},
		{`ROLLBACK WORK`, "", ""},
		{`ROLLBACK TO`, "", ""},
		{`ROLLBACK TO a b`, "", ""},
		{`ROLLBACK TO SAVEPOINT a b`, "", ""},
		{`ROLLBACK AND CHAIN`, "", ""},
		{`ABORT TO a`, "", ""},
		{`SAVEPOINT`, "", ""},
		{`SAVEPOINT a b`, "", ""},
		{`SAVEPOINT ""`, "", ""},
		{`SAVEPOINT "a`, "", ""},
		{`SAVEPOINT 'a'`, "", ""},
		{`SAVEPOINT 1`, "", ""},
		{`SAVEPOINT a.b`, "", ""},
		{`SAVEPOINT U&"\0041"`, "", ""},
		{`"SAVEPOINT" a`, "", ""},
		{`RELEASE "savepoint" a`, "", ""},
		{`ROLLBACK "to" a`, "", ""},
		{`RELEASE`, "", ""},
		{`RELEASE SAVEPOINT a b`, "", ""},
		{`SAVEPOINT a; SELECT 1`, "", ""},
		{`SAVEPOINT a; RELEASE a`, "", ""},
		{`SELECT 1`, "", ""},
		{``, "", ""},
	} {
		kind, name := savepointStmt(tt.stmt)
		if kind != tt.kind || name != tt.name {
			t.Errorf("%q: %q %q, want %q %q", tt.stmt, kind, name, tt.kind, tt.name)
		}
		if got := isRollbackTo(tt.stmt); got != (tt.kind == "rollback") {
			t.Errorf("isRollbackTo(%q) = %v", tt.stmt, got)
		}
	}
}

// The settings a ROLLBACK TO brings back are those of the savepoint
// PostgreSQL goes back to. Each step is a statement the source took, and the
// name the session's settings carry after it (appName stands for them all).
func TestTookSavepointStmt_namesAsPostgreSQL(t *testing.T) {
	type step struct {
		stmt string
		set  string // a setting made before the statement, "" for none
		want string // the settings after the statement
		left string // the savepoints kept, innermost last
	}
	for name, steps := range map[string][]step{
		// SAVEPOINT "A"; SAVEPOINT a; ROLLBACK TO "A": the outer one.
		"quoted capital and unquoted are two": {
			{stmt: `SAVEPOINT "A"`, set: "one", want: "one", left: "A"},
			{stmt: `SAVEPOINT a`, set: "two", want: "two", left: "A a"},
			{stmt: `ROLLBACK TO "A"`, set: "three", want: "one", left: "A"},
			// a went with the rollback: PostgreSQL fails ROLLBACK TO a here,
			// and a statement the source failed is never followed.
		},
		"the inner one of the two": {
			{stmt: `SAVEPOINT "A"`, set: "one", want: "one", left: "A"},
			{stmt: `SAVEPOINT a`, set: "two", want: "two", left: "A a"},
			{stmt: `ROLLBACK TO A`, set: "three", want: "two", left: "A a"},
			{stmt: `ROLLBACK TO SAVEPOINT "A"`, set: "four", want: "one", left: "A"},
		},
		"unquoted in any case is one": {
			{stmt: `SAVEPOINT Abc`, set: "one", want: "one", left: "abc"},
			{stmt: `ROLLBACK TO aBC`, set: "two", want: "one", left: "abc"},
			{stmt: `ROLLBACK TO "abc"`, set: "three", want: "one", left: "abc"},
			{stmt: `RELEASE SAVEPOINT ABC`, set: "four", want: "four", left: ""},
		},
		"released, then made again": {
			{stmt: `SAVEPOINT a`, set: "one", want: "one", left: "a"},
			{stmt: `RELEASE A`, set: "two", want: "two", left: ""},
			{stmt: `SAVEPOINT "a"`, set: "three", want: "three", left: "a"},
			{stmt: `ROLLBACK TO a`, set: "four", want: "three", left: "a"},
		},
		"two of one name: the later one": {
			{stmt: `SAVEPOINT a`, set: "one", want: "one", left: "a"},
			{stmt: `SAVEPOINT A`, set: "two", want: "two", left: "a a"},
			{stmt: `RELEASE "a"`, set: "three", want: "three", left: "a"},
			{stmt: `ROLLBACK TO a`, set: "four", want: "one", left: "a"},
		},
		"a release lets go the ones made after it": {
			{stmt: `SAVEPOINT "A"`, set: "one", want: "one", left: "A"},
			{stmt: `SAVEPOINT a`, set: "two", want: "two", left: "A a"},
			{stmt: `SAVEPOINT b`, set: "three", want: "three", left: "A a b"},
			{stmt: `RELEASE "A"`, set: "four", want: "four", left: ""},
		},
		"a release of the inner one keeps the outer": {
			{stmt: `SAVEPOINT "A"`, set: "one", want: "one", left: "A"},
			{stmt: `SAVEPOINT a`, set: "two", want: "two", left: "A a"},
			{stmt: `RELEASE a`, set: "three", want: "three", left: "A"},
			{stmt: `ROLLBACK TO "A"`, set: "four", want: "one", left: "A"},
		},
		"only A to Z are folded": {
			{stmt: `SAVEPOINT É`, set: "one", want: "one", left: "É"},
			{stmt: `SAVEPOINT é`, set: "two", want: "two", left: "É é"},
			{stmt: `ROLLBACK TO "É"`, set: "three", want: "one", left: "É"},
		},
		"names with a space in quotes": {
			{stmt: `SAVEPOINT "a b"`, set: "one", want: "one", left: "a b"},
			{stmt: `SAVEPOINT b`, set: "two", want: "two", left: "a b b"},
			{stmt: `ROLLBACK TO "a b"`, set: "three", want: "one", left: "a b"},
		},
	} {
		s := &session{up: &upstream{inTx: true}, tx: 'T'}
		for i, st := range steps {
			if st.set != "" {
				s.set.appName, s.sessionLevel.appName = st.set, st.set
			}
			s.up.sent = map[string]string{"x": "y"}
			s.tookSavepointStmt(st.stmt)
			var left []string
			for _, sp := range s.savepoints {
				left = append(left, sp.name)
			}
			if s.set.appName != st.want || s.sessionLevel.appName != st.want || strings.Join(left, " ") != st.left {
				t.Errorf("%s, step %d (%s): settings %q/%q and savepoints %q, want %q and %q",
					name, i+1, st.stmt, s.set.appName, s.sessionLevel.appName, strings.Join(left, " "), st.want, st.left)
			}
			if kind, _ := savepointStmt(st.stmt); kind == "rollback" && s.up.sent != nil {
				t.Errorf("%s, step %d (%s): the source's settings are not given again after the rollback", name, i+1, st.stmt)
			}
		}
	}
}
