package sqlsandbox

import (
	"context"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// #2614: INTERVAL, UUID and TIMETZ cells print as the engine prints them,
// and so does every value inside a LIST, ARRAY, STRUCT, MAP or UNION, whose
// own type is read out of the column's (a DATE in a LIST used to print as a
// timestamp). The engine's text is the reference: each expression is run
// beside its own ::VARCHAR, in the same statement.
func TestRun_cellsPrintAsTheEngine_2614(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	exprs := []string{
		// INTERVAL: zero, time only, negative, mixed signs, years, singular
		// and plural, fractions trimmed, hours past 24.
		"INTERVAL '0 seconds'",
		"INTERVAL '3 months 5 hours'",
		"INTERVAL 90 MINUTE",
		"INTERVAL '-3 hours'",
		"INTERVAL '-1 day' + INTERVAL '30 minutes'",
		"INTERVAL '1 day' - INTERVAL '30 minutes'",
		"INTERVAL '14 months 2 days'",
		"INTERVAL '-13 months'",
		"INTERVAL '1 year'",
		"INTERVAL '2 years 1 month 1 day'",
		"INTERVAL '-2 days'",
		"INTERVAL '1.5 seconds'",
		"INTERVAL '0.000001 seconds'",
		"INTERVAL '-0.25 seconds'",
		"INTERVAL '25 hours'",
		"INTERVAL '100000 hours 59 minutes 59.999999 seconds'",
		"TIMESTAMP '2026-01-02' - TIMESTAMP '2026-01-01 23:00'",
		// UUID: canonical form, also when its 16 bytes are valid UTF-8.
		"'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID",
		"'01010101-0101-0101-0101-010101010101'::UUID",
		"'00000000-0000-0000-0000-000000000000'::UUID",
		"'ffffffff-ffff-ffff-ffff-ffffffffffff'::UUID",
		// TIMETZ: the wall clock and its offset, as written.
		"'10:30:00+02'::TIMETZ",
		"'10:30:00.5+05:30'::TIMETZ",
		"'23:59:59.123456-08'::TIMETZ",
		"'00:00:00+00'::TIMETZ",
		"'12:00:00-03:30'::TIMETZ",
		"'24:00:00+00'::TIMETZ",
		"'24:00:00-05'::TIMETZ",
		"'10:30:00+02:30:15'::TIMETZ",
		// Unchanged, for contrast.
		"DATE '2026-01-02'",
		"'10:30:00'::TIME",
	}
	var sel []string
	for i, e := range exprs {
		sel = append(sel, e+" AS v"+strconv.Itoa(i), "("+e+")::VARCHAR AS t"+strconv.Itoa(i))
	}
	res, err := r.Run(context.Background(), f.job("SELECT "+strings.Join(sel, ", ")))
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range exprs {
		got, want := res.Rows[0][2*i], res.Rows[0][2*i+1]
		if got != want {
			t.Errorf("%s (%s) = %#v, want the engine's %#v", e, res.Columns[2*i].Type, got, want)
		}
	}

	// Inside nested values the element is rendered by its own type.
	res, err = r.Run(context.Background(), f.job(`SELECT
		[DATE '2026-01-02'] AS ld,
		[[DATE '2026-01-02']] AS lld,
		array_value(DATE '2026-01-02', DATE '2026-01-03') AS arr,
		{'d': DATE '2026-01-02', "x, y": 1.50::DECIMAL(10,2), 'u': 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID} AS s,
		{'say "hi"': DATE '2026-01-02'} AS sq,
		[{'a': DATE '2026-01-02', 'i': INTERVAL 90 MINUTE}] AS ls,
		MAP {'k': DATE '2026-01-02'} AS m,
		MAP {DATE '2026-01-02': 1} AS mk,
		MAP {'k': {'t': '10:30:00+02'::TIMETZ}} AS ms,
		union_value(d := DATE '2026-01-02') AS un,
		union_value(t := TIMESTAMP '2026-01-02 03:04:05')::UNION(d DATE, t TIMESTAMP) AS un2,
		union_value(d := DATE '2026-01-02')::UNION(t TIMESTAMP, d DATE) AS un3,
		['10:30:00+02'::TIMETZ] AS lt,
		['01010101-0101-0101-0101-010101010101'::UUID] AS lu,
		[INTERVAL '-1 day' + INTERVAL '30 minutes'] AS li,
		['POINT(0 0)'::GEOMETRY] AS lg,
		{'g': 'POINT(0 0)'::GEOMETRY, 'b': 'text'::BLOB} AS sgb`))
	if err != nil {
		t.Fatal(err)
	}
	const point = "0x010100000000000000000000000000000000000000"
	want := []any{
		[]any{"2026-01-02"},
		[]any{[]any{"2026-01-02"}},
		[]any{"2026-01-02", "2026-01-03"},
		map[string]any{"d": "2026-01-02", "x, y": "1.50", "u": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
		map[string]any{`say "hi"`: "2026-01-02"},
		[]any{map[string]any{"a": "2026-01-02", "i": "01:30:00"}},
		map[string]any{"k": "2026-01-02"},
		map[string]any{"2026-01-02": json.Number("1")},
		map[string]any{"k": map[string]any{"t": "10:30:00+02"}},
		"2026-01-02",
		// The member's own type, not the first one's.
		"2026-01-02T03:04:05Z",
		"2026-01-02",
		[]any{"10:30:00+02"},
		[]any{"01010101-0101-0101-0101-010101010101"},
		[]any{"-1 day 00:30:00"},
		[]any{point},
		// The BLOB beside a GEOMETRY keeps its own rule (text when valid UTF-8).
		map[string]any{"g": point, "b": "text"},
	}
	for i, w := range want {
		if !reflect.DeepEqual(res.Rows[0][i], w) {
			t.Errorf("%s (%s) = %#v, want %#v", res.Columns[i].Name, res.Columns[i].Type, res.Rows[0][i], w)
		}
	}
}

// The element types read out of a column's type: a LIST or ARRAY suffix,
// the fields of a STRUCT or UNION, the key and value of a MAP. Names and
// parameters may hold commas, parentheses and quotes. Anything not read
// gives "", which renders an element as before.
func TestNestedTypes_2614(t *testing.T) {
	for _, c := range []struct {
		typ      string
		elem     string
		fields   map[string]string
		key, val string
	}{
		{typ: "DATE[]", elem: "DATE"},
		{typ: "DATE[][]", elem: "DATE[]"},
		{typ: "DATE[2]", elem: "DATE"},
		{typ: "DECIMAL(10,2)[]", elem: "DECIMAL(10,2)"},
		{typ: `STRUCT("a" DATE)[]`, elem: `STRUCT("a" DATE)`},
		{typ: `STRUCT("d" DATE, "x, y" DECIMAL(10,2), "u" UUID)`, fields: map[string]string{"d": "DATE", "x, y": "DECIMAL(10,2)", "u": "UUID"}},
		{typ: `STRUCT("say ""hi""" DATE, "(" INTEGER)`, fields: map[string]string{`say "hi"`: "DATE", "(": "INTEGER"}},
		// The engine's typeof shapes (unquoted names, ENUM labels): the
		// driver sends quoted names and a bare ENUM, so these are read but
		// never arrive.
		{typ: `STRUCT("e" ENUM('a,b', 'c)'), "t" TIMETZ)`, fields: map[string]string{"e": "ENUM('a,b', 'c)')", "t": "TIMETZ"}},
		{typ: `STRUCT(a DATE, b INTEGER)`, fields: map[string]string{"a": "DATE", "b": "INTEGER"}},
		{typ: `UNION("d" DATE, "s" VARCHAR)`, fields: map[string]string{"d": "DATE", "s": "VARCHAR"}},
		{typ: "MAP(VARCHAR, DATE)", key: "VARCHAR", val: "DATE"},
		{typ: `MAP(VARCHAR, STRUCT("t" TIMETZ, "n" DECIMAL(4,1)))`, key: "VARCHAR", val: `STRUCT("t" TIMETZ, "n" DECIMAL(4,1))`},
		{typ: "MAP(DATE, INTEGER[])", key: "DATE", val: "INTEGER[]"},
		// Not read: none of the three.
		{typ: "DATE"},
		{typ: ""},
		{typ: "STRUCT("},
		{typ: `STRUCT("a DATE)`},
		{typ: "MAP(VARCHAR)"},
		{typ: "]"},
		{typ: "DATE["},
	} {
		if got := listElemType(c.typ); got != c.elem {
			t.Errorf("listElemType(%q) = %q, want %q", c.typ, got, c.elem)
		}
		if got := structFieldTypes(c.typ); !reflect.DeepEqual(got, c.fields) && !(len(got) == 0 && len(c.fields) == 0) {
			t.Errorf("structFieldTypes(%q) = %#v, want %#v", c.typ, got, c.fields)
		}
		k, v := mapTypes(c.typ)
		if k != c.key || v != c.val {
			t.Errorf("mapTypes(%q) = %q, %q, want %q, %q", c.typ, k, v, c.key, c.val)
		}
	}
}
