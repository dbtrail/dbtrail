package pgshim

import (
	"encoding/binary"
	"math"
	"strconv"
	"strings"
	"testing"
)

// #2767: where a $n placeholder is, read the way PostgreSQL's lexer reads the
// statement. A $1 inside a string, a quoted name, a comment or a dollar quote
// is text; a wrong answer here writes an argument inside a string.
func TestSplitParams(t *testing.T) {
	cases := []struct {
		q       string
		refs    []int
		highest int
	}{
		{"SELECT 1", nil, 0},
		{"", nil, 0},
		{"SELECT $1", []int{0}, 1},
		{"SELECT $1 + $1", []int{0, 0}, 1},
		{"SELECT $2, $1", []int{1, 0}, 2},
		{"SELECT $10", []int{9}, 10},
		{"SELECT $3", []int{2}, 3}, // $1 and $2 exist and are unused
		{"SELECT $1::int", []int{0}, 1},
		{"SELECT x FROM t WHERE a=$1AND b=$2", []int{0, 1}, 2},
		// Strings: a quote doubled, a backslash a character.
		{"SELECT '$1'", nil, 0},
		{"SELECT 'it''s $1', $2", []int{1}, 2},
		{`SELECT 'a\', $1`, []int{0}, 1}, // the backslash does not escape: the string ends at the second quote
		// Escape strings: a backslash escapes, a quote included.
		{`SELECT E'\' $1', $2`, []int{1}, 2},
		{`SELECT e'\\', $1`, []int{0}, 1},
		{`SELECT E'it''s $1', $2`, []int{1}, 2},
		// An E that ends a name does not start an escape string.
		{`SELECT namee'\', $1`, []int{0}, 1},
		// Quoted names.
		{`SELECT "$1", $2`, []int{1}, 2},
		{`SELECT "a""$1", $2`, []int{1}, 2},
		// A $ inside a name is part of it.
		{"SELECT col$1, $2", []int{1}, 2},
		{"SELECT a$b$c, $1", []int{0}, 1},
		// Comments, block comments nested.
		{"SELECT 1 -- $1\n, $2", []int{1}, 2},
		{"SELECT 1 -- $1", nil, 0},
		{"SELECT /* $1 */ $2", []int{1}, 2},
		{"SELECT /* a /* $1 */ $2 */ $3", []int{2}, 3},
		{"SELECT /* unclosed $1", nil, 0},
		// Dollar quotes.
		{"SELECT $$ $1 $$, $2", []int{1}, 2},
		{"SELECT $tag$ $1 $other$ $2 $tag$, $3", []int{2}, 3},
		{"SELECT $q$ ' $q$, $1", []int{0}, 1},
		{"SELECT $$ unclosed $1", nil, 0},
		// A lone $ is a character.
		{"SELECT $ + $1", []int{0}, 1},
		// A -- comment ends at a carriage return too.
		{"SELECT 1 -- x\r'\n $1 ', $2", []int{1}, 2},
		// What follows a number starts a token of its own: 1e'..' is 1 and
		// an escape string, 1$$..$$ is 1 and a dollar quote.
		{`SELECT 1e'\' $1', $2`, []int{1}, 2},
		{"SELECT 1$$ $1 $$, $2", []int{1}, 2},
		{`SELECT x1e'\', $1`, []int{0}, 1},
		{"SELECT col1$1, $2", []int{1}, 2},
		// Unclosed strings run to the end.
		{"SELECT 'open $1", nil, 0},
		{`SELECT "open $1`, nil, 0},
	}
	for _, c := range cases {
		tm, perr := splitParams(c.q)
		if perr != nil {
			t.Errorf("%q: %v", c.q, perr)
			continue
		}
		if len(tm.refs) != len(c.refs) || tm.highest != c.highest {
			t.Errorf("%q: refs %v highest %d, want %v %d", c.q, tm.refs, tm.highest, c.refs, c.highest)
			continue
		}
		for i := range c.refs {
			if tm.refs[i] != c.refs[i] {
				t.Errorf("%q: refs %v, want %v", c.q, tm.refs, c.refs)
				break
			}
		}
		if len(tm.parts) != len(tm.refs)+1 {
			t.Errorf("%q: %d parts for %d placeholders", c.q, len(tm.parts), len(tm.refs))
			continue
		}
		var rebuilt strings.Builder
		for i, p := range tm.parts {
			rebuilt.WriteString(p)
			if i < len(tm.refs) {
				rebuilt.WriteString("$" + strconv.Itoa(tm.refs[i]+1))
			}
		}
		if rebuilt.String() != c.q {
			t.Errorf("%q: parts %q do not rebuild the text", c.q, tm.parts)
		}
	}
	for _, q := range []string{"SELECT $0", "SELECT $65536", "SELECT $99999999999999999999"} {
		if _, perr := splitParams(q); perr == nil || perr.code != "42P02" {
			t.Errorf("%q: %v, want 42P02", q, perr)
		}
	}
}

func TestTemplateFill(t *testing.T) {
	tm, perr := splitParams("SELECT * FROM t WHERE a = $1 AND b = $2 OR c = $1 LIMIT 5-$3")
	if perr != nil {
		t.Fatal(perr)
	}
	args := []arg{
		{kind: argText, text: "x' OR '1'='1"},
		{null: true},
		{kind: argInt, text: "-3"},
	}
	got, perr := tm.fill(args, readCopy)
	if perr != nil {
		t.Fatal(perr)
	}
	want := "SELECT * FROM t WHERE a =  'x'' OR ''1''=''1'  AND b =  NULL  OR c =  'x'' OR ''1''=''1'  LIMIT 5- (-3) "
	if got != want {
		t.Fatalf("fill:\n got %q\nwant %q", got, want)
	}
}

func TestArgLiteral(t *testing.T) {
	cases := []struct {
		name     string
		oid      uint32
		format   int16
		raw      []byte
		copyLit  string
		ttLit    string
		ttRefuse bool
	}{
		{"NULL untyped", 0, 0, nil, "NULL", "NULL", false},
		{"NULL int4", oidInt4, 0, nil, "CAST(NULL AS INTEGER)", "NULL", false},
		{"text", oidText, 0, []byte("it's"), "'it''s'", "", true},
		{"unspecified backslash", 0, 0, []byte(`a\b`), `'a\b'`, `'a\\b'`, false},
		{"empty string", 0, 0, []byte{}, "''", "''", false},
		{"int4 text", oidInt4, 0, []byte(" 42 "), "42", "42", false},
		{"int8 negative", oidInt8, 0, []byte("-9223372036854775808"), "(-9223372036854775808)", "-9223372036854775808", false},
		{"int4 binary", oidInt4, 1, []byte{0xff, 0xff, 0xff, 0xfe}, "(-2)", "-2", false},
		{"int2 binary", oidInt2, 1, []byte{0, 7}, "7", "7", false},
		{"int8 binary", oidInt8, 1, be64(1 << 40), "1099511627776", "1099511627776", false},
		{"oid binary", oidOID, 1, []byte{0xff, 0xff, 0xff, 0xff}, "4294967295", "4294967295", false},
		{"float8 text", oidFloat8, 0, []byte("0.1"), "CAST('0.1' AS DOUBLE)", "'0.1'", false},
		{"float8 NaN", oidFloat8, 0, []byte("NaN"), "CAST('NaN' AS DOUBLE)", "'NaN'", false},
		{"float8 binary", oidFloat8, 1, be64(math.Float64bits(1.5)), "CAST('1.5' AS DOUBLE)", "'1.5'", false},
		{"float4 binary", oidFloat4, 1, be32(math.Float32bits(0.1)), "CAST('0.1' AS FLOAT)", "'0.1'", false},
		{"bool text", oidBool, 0, []byte("yes"), "TRUE", "'true'", false},
		{"bool binary", oidBool, 1, []byte{0}, "FALSE", "'false'", false},
		{"numeric text", oidNumeric, 0, []byte("-12.50"), "(-12.50)", "'-12.50'", false},
		{"numeric exponent", oidNumeric, 0, []byte("1e5"), "'1e5'", "'1e5'", false},
		{"numeric binary", oidNumeric, 1, numericBinary(2, 0, 0, 2, 12, 5000), "12.50", "'12.50'", false},
		{"bytea binary", oidBytea, 1, []byte{0, 1, 0xff}, "unhex('0001ff')", `'\\x0001ff'`, false},
		{"bytea hex text", oidBytea, 0, []byte(`\x00 01ff`), "unhex('0001ff')", `'\\x0001ff'`, false},
		{"bytea escape text", oidBytea, 0, []byte(`a\\b\001`), "unhex('615c6201')", `'\\x615c6201'`, false},
		{"date binary", oidDate, 1, be32(366), "CAST('2001-01-01' AS DATE)", "'2001-01-01'", false},
		{"date infinity", oidDate, 1, be32(math.MaxInt32), "CAST('infinity' AS DATE)", "'infinity'", false},
		{"timestamp binary", oidTimestamp, 1, be64(uint64(86400*1e6 + 1)), "CAST('2000-01-02 00:00:00.000001' AS TIMESTAMP)", "'2000-01-02 00:00:00.000001'", false},
		{"timestamptz binary", oidTimestamptz, 1, be64i(-1e6), "CAST('1999-12-31 23:59:59+00' AS TIMESTAMPTZ)", "'1999-12-31 23:59:59+00'", false},
		{"time binary", oidTime, 1, be64(uint64(int64(3661e6 + 500000))), "CAST('01:01:01.5' AS TIME)", "'01:01:01.5'", false},
		{"uuid binary", oidUUID, 1, []byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0},
			"CAST('12345678-9abc-def0-1234-56789abcdef0' AS UUID)", "'12345678-9abc-def0-1234-56789abcdef0'", false},
		{"date text", oidDate, 0, []byte("2026-01-02"), "CAST('2026-01-02' AS DATE)", "'2026-01-02'", false},
		{"jsonb binary", oidJSONB, 1, append([]byte{1}, `{"a":1}`...), `'{"a":1}'`, `'{"a":1}'`, false},
		{"varchar binary", oidVarchar, 1, []byte("año"), "'año'", "'año'", false},
	}
	for _, c := range cases {
		a, perr := decodeArg(0, c.oid, c.format, c.raw)
		if perr != nil {
			t.Errorf("%s: decode: %v", c.name, perr)
			continue
		}
		if got, perr := a.literal(readCopy); perr != nil || got != c.copyLit {
			t.Errorf("%s: copy literal %q (%v), want %q", c.name, got, perr, c.copyLit)
		}
		got, perr := a.literal(readTimeTravel)
		if c.ttRefuse {
			if perr == nil || perr.code != "22023" {
				t.Errorf("%s: time-travel literal %q (%v), want a 22023 refusal", c.name, got, perr)
			}
			continue
		}
		if perr != nil || got != c.ttLit {
			t.Errorf("%s: time-travel literal %q (%v), want %q", c.name, got, perr, c.ttLit)
		}
	}
}

func TestDecodeArgRefusals(t *testing.T) {
	cases := []struct {
		name   string
		oid    uint32
		format int16
		raw    []byte
		code   string
	}{
		{"NUL in text", 0, 0, []byte("a\x00b"), "22021"},
		{"bad UTF-8", oidText, 0, []byte{0xff, 0xfe}, "22021"},
		{"bad UTF-8 binary", oidVarchar, 1, []byte{0xc3}, "22021"},
		{"int4 syntax", oidInt4, 0, []byte("12a"), "22P02"},
		{"int4 range", oidInt4, 0, []byte("2147483648"), "22003"},
		{"int2 range", oidInt2, 0, []byte("-32769"), "22003"},
		{"int8 empty", oidInt8, 0, []byte(""), "22P02"},
		{"float syntax", oidFloat8, 0, []byte("1.2.3"), "22P02"},
		{"bool syntax", oidBool, 0, []byte("maybe"), "22P02"},
		{"bool empty", oidBool, 0, []byte(" "), "22P02"},
		{"numeric syntax", oidNumeric, 0, []byte("12,5"), "22P02"},
		{"bytea bad hex", oidBytea, 0, []byte(`\x0g`), "22P02"},
		{"bytea odd hex", oidBytea, 0, []byte(`\x012`), "22P02"},
		{"bytea bad escape", oidBytea, 0, []byte(`\9`), "22P02"},
		{"int4 binary short", oidInt4, 1, []byte{1, 2}, "22P03"},
		{"bool binary long", oidBool, 1, []byte{1, 1}, "22P03"},
		{"jsonb bad version", oidJSONB, 1, []byte{2, '{', '}'}, "22P03"},
		{"numeric binary short", oidNumeric, 1, []byte{0, 1}, "22P03"},
		{"numeric binary bad digit", oidNumeric, 1, numericBinary(1, 0, 0, 0, 10000), "22P03"},
		{"interval binary", oidInterval, 1, make([]byte, 16), "0A000"},
		{"array binary", 1007, 1, []byte{0}, "0A000"},
		{"unknown format", 0, 2, []byte("x"), "22023"},
		{"time binary past a day", oidTime, 1, be64(uint64(int64(86400e6 + 1))), "22P03"},
		{"date binary out of range", oidDate, 1, be32i(-800000), "22008"},
	}
	for _, c := range cases {
		_, perr := decodeArg(0, c.oid, c.format, c.raw)
		if perr == nil || perr.code != c.code {
			t.Errorf("%s: %v, want %s", c.name, perr, c.code)
		}
	}
}

func TestNumericBinaryText(t *testing.T) {
	cases := []struct {
		raw  []byte
		want string
	}{
		{numericBinary(0, 0, 0, 0), "0"},
		{numericBinary(0, 0, 0, 2), "0.00"},
		{numericBinary(1, 0, 0, 0, 7), "7"},
		{numericBinary(2, 1, 0, 0, 1, 0), "10000"},
		{numericBinary(1, 1, 0, 0, 1), "10000"}, // trailing zero groups are not sent
		{numericBinary(2, 0, 0x4000, 4, 3, 1415), "-3.1415"},
		{numericBinary(1, -1, 0, 4, 1), "0.0001"},
		{numericBinary(1, -2, 0, 8, 5), "0.00000005"},
		{numericBinary(1, -1, 0, 2, 5000), "0.50"},
		{numericBinary(0, 0, 0xC000, 0), "NaN"},
		{numericBinary(0, 0, 0xD000, 0), "Infinity"},
		{numericBinary(0, 0, 0xF000, 0), "-Infinity"},
	}
	for _, c := range cases {
		got, ok := numericBinaryText(c.raw)
		if !ok || got != c.want {
			t.Errorf("numeric %x: %q %v, want %q", c.raw, got, ok, c.want)
		}
	}
	if _, ok := numericBinaryText(numericBinary(0, 0, 0x1234, 0)); ok {
		t.Error("an unknown sign was read")
	}
}

func TestParseBool(t *testing.T) {
	for in, want := range map[string]bool{"t": true, "TRUE": true, "tr": true, " yes ": true, "y": true, "on": true, "1": true,
		"f": false, "False": false, "n": false, "no": false, "off": false, "of": false, "0": false} {
		if got, ok := parseBool(in); !ok || got != want {
			t.Errorf("parseBool(%q) = %v %v, want %v", in, got, ok, want)
		}
	}
	for _, in := range []string{"", "o", "truex", "2", "nope"} {
		if _, ok := parseBool(in); ok {
			t.Errorf("parseBool(%q) read a boolean", in)
		}
	}
}

func be32(v uint32) []byte { return binary.BigEndian.AppendUint32(nil, v) }
func be32i(v int32) []byte { return be32(uint32(v)) }
func be64i(v int64) []byte { return be64(uint64(v)) }
func be64(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }

// numericBinary builds numeric's binary form.
func numericBinary(ndigits, weight int16, sign, dscale uint16, digits ...uint16) []byte {
	b := binary.BigEndian.AppendUint16(nil, uint16(ndigits))
	b = binary.BigEndian.AppendUint16(b, uint16(weight))
	b = binary.BigEndian.AppendUint16(b, sign)
	b = binary.BigEndian.AppendUint16(b, dscale)
	for _, d := range digits {
		b = binary.BigEndian.AppendUint16(b, d)
	}
	return b
}

// A placeholder named many times repeats its argument each time: the finished
// text is refused past the longest statement the port takes, before it is
// written, and a template with too many placeholders is refused at Parse.
func TestTemplateFillIsCapped(t *testing.T) {
	tm, perr := splitParams("SELECT " + strings.Repeat("$1,", 20) + "1")
	if perr != nil {
		t.Fatal(perr)
	}
	big := arg{kind: argText, text: strings.Repeat("x", 1<<20)}
	if _, perr := tm.fill([]arg{big}, readCopy); perr == nil || perr.code != "54000" {
		t.Fatalf("20 copies of a 1 MB argument: %v, want 54000", perr)
	}
	small := arg{kind: argText, text: strings.Repeat("x", 1<<10)}
	if _, perr := tm.fill([]arg{small}, readCopy); perr != nil {
		t.Fatalf("20 copies of a 1 KB argument: %v", perr)
	}
	if _, perr := splitParams("SELECT " + strings.Repeat("$1,", maxPlaceholders) + "1"); perr != nil {
		t.Fatalf("%d placeholders: %v", maxPlaceholders, perr)
	}
	if _, perr := splitParams("SELECT " + strings.Repeat("$1,", maxPlaceholders+1) + "1"); perr == nil || perr.code != "54000" {
		t.Fatalf("%d placeholders: %v, want 54000", maxPlaceholders+1, perr)
	}
}
