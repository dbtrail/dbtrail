package pgshim

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/dbtrail/dbtrail/internal/pgtext"
)

// #2768: each engine type is announced as the PostgreSQL type a client reads
// it as; a type with no equivalent this port is sure of is text.
func TestColumnType_2768(t *testing.T) {
	num := func(p, s int) int32 { return int32(p<<16|s) + 4 }
	for _, c := range []struct {
		duck string
		want colType
	}{
		{"BOOLEAN", colType{16, 1, -1}},
		{"SMALLINT", colType{21, 2, -1}},
		{"TINYINT", colType{21, 2, -1}},
		{"UTINYINT", colType{21, 2, -1}},
		{"INTEGER", colType{23, 4, -1}},
		{"USMALLINT", colType{23, 4, -1}},
		{"BIGINT", colType{20, 8, -1}},
		{"UINTEGER", colType{20, 8, -1}},
		{"UBIGINT", colType{1700, -1, -1}},
		{"HUGEINT", colType{1700, -1, -1}},
		{"FLOAT", colType{700, 4, -1}},
		{"DOUBLE", colType{701, 8, -1}},
		{"DECIMAL(10,2)", colType{1700, -1, num(10, 2)}},
		{"DECIMAL(38,18)", colType{1700, -1, num(38, 18)}},
		{"DECIMAL(4,0)", colType{1700, -1, num(4, 0)}},
		{"DATE", colType{1082, 4, -1}},
		{"TIME", colType{1083, 8, -1}},
		{"TIMETZ", colType{1266, 12, -1}},
		{"TIMESTAMP", colType{1114, 8, -1}},
		{"TIMESTAMPTZ", colType{1184, 8, -1}},
		{"TIMESTAMP WITH TIME ZONE", colType{1184, 8, -1}},
		{"INTERVAL", colType{1186, 16, -1}},
		{"UUID", colType{2950, 16, -1}},
		{"BLOB", colType{17, -1, -1}},
		{"VARCHAR", colType{25, -1, -1}},
		{"INTEGER[]", colType{1007, -1, -1}},
		{"VARCHAR[]", colType{1009, -1, -1}},
		{"DECIMAL(4,1)[]", colType{1231, -1, num(4, 1)}},
		{"TIMESTAMPTZ[]", colType{1185, -1, -1}},
		{"BOOLEAN[]", colType{1000, -1, -1}},
		// No equivalent this port is sure of: text.
		{"INTEGER[][]", textColumn},
		{"INTEGER[2]", textColumn},
		{"JSON", textColumn},
		{"JSON[]", textColumn},
		{`STRUCT("a" INTEGER)`, textColumn},
		{"MAP(VARCHAR, INTEGER)", textColumn},
		{"TIMESTAMP_NS", textColumn},
		{"BIT", textColumn},
		{"ENUM", textColumn},
		{"DECIMAL(x,2)", textColumn},
		{"DECIMAL(2,3)", textColumn},
		{"", textColumn},
		{"integer", textColumn},
	} {
		if got := columnType(c.duck); got != c.want {
			t.Errorf("%q: %+v, want %+v", c.duck, got, c.want)
		}
	}
}

// #2768: a cell's binary form, read back by pgx's own codec, is the value its
// text reads as, the very text the port sends in text format: the two
// formats cannot say different things. The texts are what pgtext.Cell writes
// for the copy's cells (and PostgreSQL writes for the source's).
func TestBinaryCell_readsAsItsText_2768(t *testing.T) {
	m := pgtype.NewMap()
	for _, c := range []struct {
		duck  string
		texts []string
	}{
		{"BOOLEAN", []string{"t", "f"}},
		{"SMALLINT", []string{"0", "-32768", "32767"}},
		{"INTEGER", []string{"0", "-2147483648", "2147483647", "42"}},
		{"BIGINT", []string{"-9223372036854775808", "9223372036854775807", "9007199254740993"}},
		{"FLOAT", []string{"1.1", "-0", "NaN", "Infinity", "-Infinity", "3.4028235e+38", "1e-05"}},
		{"DOUBLE", []string{"0.1", "-0", "NaN", "Infinity", "-Infinity", "1e+20", "2.5", "0.3333333333333333", "5e-324"}},
		{"DECIMAL(10,2)", []string{"10.50", "0.00", "-0.50", "12345678.90", "7.25"}},
		{"DECIMAL(38,18)", []string{"12345.678000000000000000", "-0.000000000000000001", "99999999999999999999.999999999999999999"}},
		{"DECIMAL(10,0)", []string{"0", "10000", "-12345", "100000000"}},
		{"HUGEINT", []string{"170141183460469231731687303715884105727", "-1", "0"}},
		{"DATE", []string{"2026-03-04", "2000-01-01", "1999-12-31", "0001-01-01", "9999-12-31"}},
		{"TIME", []string{"00:00:00", "12:00:00.5", "23:59:59.999999"}},
		{"TIMESTAMP", []string{"2026-03-04 05:06:07", "2026-03-04 05:06:07.123456", "1999-12-31 23:59:59.5", "0001-01-01 00:00:00", "9999-12-31 23:59:59.999999"}},
		{"TIMESTAMPTZ", []string{"2026-03-04 05:06:07+00", "2026-03-04 05:06:07.123456+00", "1970-01-01 00:00:00+00"}},
		{"INTERVAL", []string{"00:00:00", "1 year 2 mons -3 days +04:05:06.5", "-1 years", "1 day", "-00:00:01.5", "25:00:00", "1 mon", "-2 days +01:00:00", "100000:00:00"}},
		{"UUID", []string{"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "00000000-0000-0000-0000-000000000000"}},
		{"BLOB", []string{`\x`, `\xdead`, `\x00ff`}},
		{"VARCHAR", []string{"", "año", "a\\b"}},
		{"INTEGER[]", []string{"{}", "{1,2,NULL}", "{-1}"}},
		{"VARCHAR[]", []string{`{"a b",NULL,""}`, `{"NULL",x,"a\"b","c\\d"}`, `{}`}},
		{"DECIMAL(4,1)[]", []string{"{1.5,-0.5,NULL}"}},
		{"DATE[]", []string{"{2026-01-01,NULL}"}},
		{"TIMESTAMPTZ[]", []string{`{"2026-01-01 00:00:00+00"}`}},
		{"BOOLEAN[]", []string{"{t,f,NULL}"}},
		{"DOUBLE[]", []string{"{1.5,NaN,-Infinity}"}},
		{"BLOB[]", []string{`{"\\x01","\\x"}`}},
		{"INTERVAL[]", []string{`{"1 day","-1 years +00:00:01"}`}},
		{"UUID[]", []string{"{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}"}},
	} {
		ct := columnType(c.duck)
		typ, ok := m.TypeForOID(ct.oid)
		if !ok {
			t.Fatalf("%s: pgx has no codec for %d", c.duck, ct.oid)
		}
		for _, text := range c.texts {
			bin, err := binaryCell(ct, []byte(text))
			if err != nil {
				t.Errorf("%s %q: %v", c.duck, text, err)
				continue
			}
			fromText, err := typ.Codec.DecodeValue(m, ct.oid, pgtype.TextFormatCode, []byte(text))
			if err != nil {
				t.Errorf("%s %q: pgx does not read the text: %v", c.duck, text, err)
				continue
			}
			fromBinary, err := typ.Codec.DecodeValue(m, ct.oid, pgtype.BinaryFormatCode, bin)
			if err != nil {
				t.Errorf("%s %q: pgx does not read the binary %x: %v", c.duck, text, bin, err)
				continue
			}
			if a, b := normalize(t, m, ct.oid, fromText), normalize(t, m, ct.oid, fromBinary); !reflect.DeepEqual(a, b) {
				t.Errorf("%s %q: the binary %x reads as %v, the text as %v", c.duck, text, bin, b, a)
			}
		}
	}
}

// normalize makes two decoded values comparable: an instant by its UTC
// reading, a numeric by its text (which carries its scale), a float by its
// bits (NaN equals NaN, -0 is not 0).
func normalize(t *testing.T, m *pgtype.Map, oid uint32, v any) any {
	switch x := v.(type) {
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	case pgtype.Numeric:
		b, err := m.Encode(pgtype.NumericOID, pgtype.TextFormatCode, x, nil)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	case float64:
		return math.Float64bits(x)
	case float32:
		return math.Float32bits(x)
	case []any:
		elem := uint32(0)
		if e, ok := arrayElement[oid]; ok {
			elem = e
		}
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = normalize(t, m, elem, e)
		}
		return out
	}
	return v
}

// A few binary forms by their exact bytes, as PostgreSQL 16 sends them
// (SELECT ... with every result in binary): the ones pgx has no codec for,
// or whose bytes say more than the value (numeric's scale and weight).
func TestBinaryCell_exactBytes_2768(t *testing.T) {
	for _, c := range []struct {
		duck, text, hex string
	}{
		// numeric: ndigits, weight, sign, dscale, then base-10000 digits.
		{"DECIMAL(10,2)", "10.50", "0002000000000002000a1388"},
		{"DECIMAL(10,2)", "0.00", "0000000000000002"},
		{"DECIMAL(10,2)", "-0.50", "0001ffff400000021388"},
		{"DECIMAL(10,0)", "10000", "00010001000000000001"},
		{"DECIMAL(38,18)", "0.000000000000000001", "0001fffb000000120064"},
		{"HUGEINT", "NaN", "00000000c0000000"},
		// timetz: microseconds, then the zone in seconds WEST of UTC.
		{"TIMETZ", "12:00:00+02", "0000000a0eebb000ffffe3e0"},
		{"TIMETZ", "12:00:00.5-03:30", "0000000a0ef3512000003138"},
		{"TIMETZ", "00:00:00+00", "000000000000000000000000"},
		// date: days from 2000-01-01.
		{"DATE", "1999-12-31", "ffffffff"},
		// interval: microseconds, days, months.
		{"INTERVAL", "1 year 2 mons -3 days +04:05:06.5", "000000036c9361a0fffffffd0000000e"},
		{"INTEGER[]", "{}", "000000000000000000000017"},
		{"INTEGER[]", "{1,NULL}", "00000001000000010000001700000002000000010000000400000001ffffffff"},
	} {
		got, err := binaryCell(columnType(c.duck), []byte(c.text))
		if err != nil {
			t.Errorf("%s %q: %v", c.duck, c.text, err)
			continue
		}
		want, _ := hex.DecodeString(c.hex)
		if !bytes.Equal(got, want) {
			t.Errorf("%s %q: %x, want %x", c.duck, c.text, got, want)
		}
	}
}

// A text that does not read as its type has no binary form: an error, never
// bytes that mean another value.
func TestBinaryCell_refusesTextThatIsNotItsType_2768(t *testing.T) {
	for _, c := range []struct{ duck, text string }{
		{"BOOLEAN", "true"},
		{"BOOLEAN", ""},
		{"SMALLINT", "32768"},
		{"INTEGER", "1.5"},
		{"INTEGER", " 1"},
		{"BIGINT", "9223372036854775808"},
		{"DOUBLE", "inf"},
		{"DOUBLE", "0x1p-2"},
		{"DOUBLE", "1_000"},
		{"DOUBLE", ""},
		{"DECIMAL(10,2)", "1e5"},
		{"DECIMAL(10,2)", "1."},
		{"DECIMAL(10,2)", ".5"},
		{"DECIMAL(10,2)", "--1"},
		{"DECIMAL(10,2)", ""},
		// The engine's "infinity" date and timestamp, which are not the
		// instants PostgreSQL names infinity.
		{"DATE", "5881580-07-11"},
		{"DATE", "2026-02-30"},
		{"TIMESTAMP", "294247-01-10 04:00:54.775807"},
		{"TIMESTAMPTZ", "2026-01-01 00:00:00"},
		{"TIMESTAMPTZ", "2026-01-01 00:00:00+02"},
		{"TIME", "25:00:00"},
		{"TIME", "12:00"},
		{"TIME", "12:00:00.1234567"},
		{"TIMETZ", "12:00:00"},
		{"TIMETZ", "12:00:00+2"},
		{"INTERVAL", "1 week"},
		{"INTERVAL", "1 day 1 year"},
		{"INTERVAL", ""},
		{"INTERVAL", "00:00:00 1 day"},
		{"UUID", "a0eebc999c0b4ef8bb6d6bb9bd380a11"},
		{"BLOB", "dead"},
		{"BLOB", `\xdea`},
		{"INTEGER[]", "{1,{2}}"},
		{"INTEGER[]", "{1,x}"},
		{"INTEGER[]", "1,2"},
		{"VARCHAR[]", `{"a}`},
		{"INTEGER[]", "{1,}"},
	} {
		if got, err := binaryCell(columnType(c.duck), []byte(c.text)); err == nil {
			t.Errorf("%s %q: %x, want an error", c.duck, c.text, got)
		}
	}
}

// The texts the copy's cells come out as (pgtext.Cell over the sandbox's
// cell shapes, seen on the engine: #2768 explore run) all have a binary form
// under the type the column is announced with.
func TestBinaryCell_everyCellOfTheCopy_2768(t *testing.T) {
	for _, c := range []struct {
		duck string
		cell any
	}{
		{"BOOLEAN", true},
		{"INTEGER", jsonNumber("1")},
		{"UINTEGER", jsonNumber("4294967295")},
		{"UBIGINT", jsonNumber("18446744073709551615")},
		{"DOUBLE", jsonNumber("-0")},
		{"DOUBLE", "NaN"},
		{"FLOAT", "Infinity"},
		{"FLOAT", jsonNumber("1.100000023841858")},
		{"DOUBLE", jsonNumber("100000000000000000000")},
		{"DECIMAL(10,4)", "123.4500"},
		{"HUGEINT", "170141183460469231731687303715884105727"},
		{"TIMESTAMPTZ", "2026-01-02T03:04:05.123456Z"},
		{"TIMESTAMP", "2026-01-02T03:04:05Z"},
		{"TIMETZ", "12:00:00+02"},
		{"TIME", "12:00:00.5"},
		{"INTERVAL", "1 year 2 months -3 days 04:05:06.5"},
		{"UUID", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
		{"BLOB", "0x00FF"},
		{"BLOB", "ab"},
		{"DATE", "2026-01-02"},
		{"INTEGER[]", []any{jsonNumber("1"), nil}},
		{"VARCHAR[]", []any{"a b", nil, "", "NULL"}},
		{"DATE[]", []any{"2026-01-01"}},
		{"TIMESTAMPTZ[]", []any{"2026-01-01T00:00:00Z"}},
		{"DECIMAL(4,1)[]", []any{"1.5"}},
		{"BLOB[]", []any{"\x01"}},
		{"INTERVAL[]", []any{"1 day"}},
		{"BOOLEAN[]", []any{true, false}},
	} {
		text := pgtext.Cell(c.duck, c.cell)
		if text == nil {
			t.Fatalf("%s %v: no text", c.duck, c.cell)
		}
		if _, err := binaryCell(columnType(c.duck), []byte(*text)); err != nil {
			t.Errorf("%s %#v (text %q): %v", c.duck, c.cell, *text, err)
		}
	}
}

func jsonNumber(s string) any { return json.Number(s) }
