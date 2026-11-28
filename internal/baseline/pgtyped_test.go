package baseline

import (
	"math"
	"strings"
	"testing"
)

func numericMod(p, s int) int32 { return int32((p<<16)|s) + 4 }

// Which columns a snapshot file stores typed (#3315): the whole numbers,
// oid, date, uuid and a numeric with a precision and a scale below it, and
// never a key column, an array, a domain or a table with no primary key.
func TestPGTypedColumns(t *testing.T) {
	rel := PGRelation{OID: 1, PrimaryKey: []string{"id", "k"}, Columns: []PGColumn{
		{Name: "id", TypeOID: pgOIDInt8, TypeMod: -1},
		{Name: "k", TypeOID: pgOIDText, TypeMod: -1},
		{Name: "a", TypeOID: pgOIDInt2, TypeMod: -1},
		{Name: "b", TypeOID: pgOIDInt4, TypeMod: -1},
		{Name: "c", TypeOID: pgOIDInt8, TypeMod: -1},
		{Name: "o", TypeOID: pgOIDOID, TypeMod: -1},
		{Name: "d", TypeOID: pgOIDDate, TypeMod: -1},
		{Name: "u", TypeOID: pgOIDUUID, TypeMod: -1},
		{Name: "n", TypeOID: pgOIDNumeric, TypeMod: numericMod(12, 2)},
		{Name: "n0", TypeOID: pgOIDNumeric, TypeMod: numericMod(15, 0)},
		{Name: "n38", TypeOID: pgOIDNumeric, TypeMod: numericMod(38, 37)},
		{Name: "nfull", TypeOID: pgOIDNumeric, TypeMod: numericMod(2, 2)},
		{Name: "nwide", TypeOID: pgOIDNumeric, TypeMod: numericMod(39, 2)},
		{Name: "nfree", TypeOID: pgOIDNumeric, TypeMod: -1},
		{Name: "nneg", TypeOID: pgOIDNumeric, TypeMod: int32((5<<16)|0x7fe) + 4},
		{Name: "ints", TypeOID: 1007, TypeMod: -1},
		{Name: "dom", TypeOID: 70000, TypeMod: -1},
		{Name: "t", TypeOID: pgOIDText, TypeMod: -1},
		{Name: "f", TypeOID: pgOIDFloat8, TypeMod: -1},
		{Name: "ts", TypeOID: pgOIDTimestamp, TypeMod: -1},
		{Name: "bo", TypeOID: pgOIDBool, TypeMod: -1},
		{Name: "select", TypeOID: pgOIDInt4, TypeMod: -1},
	}}
	got := PGTypedColumns(rel)
	want := map[string]string{
		"a": "SMALLINT", "b": "INTEGER", "c": "BIGINT", "o": "UINTEGER", "d": "DATE", "u": "UUID",
		"n": "DECIMAL(12,2)", "n0": "DECIMAL(15,0)", "n38": "DECIMAL(38,37)", "select": "INTEGER",
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: got %q, want %q", k, got[k], v)
		}
	}
	// Each one is the type the views read the column as.
	for _, c := range rel.Columns {
		if as := got[c.Name]; as != "" && as != pgCastOf(c) {
			t.Errorf("%s is stored as %s and read as %s", c.Name, as, pgCastOf(c))
		}
	}
	folded := rel
	folded.Columns = append(append([]PGColumn{}, rel.Columns...), PGColumn{Name: "B", TypeOID: pgOIDText, TypeMod: -1})
	if got := PGTypedColumns(folded); got != nil {
		t.Fatalf("a table with two names that differ only in case stores %v typed", got)
	}
	rel.PrimaryKey = nil
	if got := PGTypedColumns(rel); got != nil {
		t.Fatalf("a table with no primary key stores %v typed", got)
	}
}

// The footer value reads back, and one of another version or with a type
// this build does not store is refused.
func TestPGTypedColumnsFooter(t *testing.T) {
	in := map[string]string{"a": "INTEGER", "n": "DECIMAL(12,2)", "Mixed Case|x": "DATE"}
	enc, err := EncodePGTypedColumns(in)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParsePGTypedColumns(enc)
	if err != nil || len(got) != 3 || got["Mixed Case|x"] != "DATE" {
		t.Fatalf("got %v, %v", got, err)
	}
	if got, err := ParsePGTypedColumns(""); err != nil || got != nil {
		t.Fatalf("empty: %v, %v", got, err)
	}
	for _, bad := range []string{
		`{"v":2,"columns":{"a":"INTEGER"}}`,
		`{"v":1,"columns":{"a":"BOOLEAN"}}`,
		`{"v":1,"columns":{"a":"DECIMAL(2,2)"}}`,
		`{"v":1,"columns":{"a":"DECIMAL(39,2)"}}`,
		`{"v":1,"columns":{"a":"DECIMAL(12, 2)"}}`,
		`{"v":1,"columns":{"":"INTEGER"}}`,
		`not json`,
	} {
		if _, err := ParsePGTypedColumns(bad); err == nil {
			t.Errorf("%s was taken", bad)
		}
	}
	rel := PGRelation{OID: 1, PrimaryKey: []string{"id"}, Columns: []PGColumn{
		{Name: "id", TypeOID: pgOIDInt8, TypeMod: -1}, {Name: "a", TypeOID: pgOIDInt4, TypeMod: -1}, {Name: "t", TypeOID: pgOIDText, TypeMod: -1},
	}}
	if err := CheckPGTypedColumns(rel, map[string]string{"a": "INTEGER"}); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []map[string]string{{"a": "BIGINT"}, {"id": "BIGINT"}, {"t": "INTEGER"}, {"gone": "INTEGER"}} {
		if err := CheckPGTypedColumns(rel, bad); err == nil {
			t.Errorf("%v agrees with the shape", bad)
		}
	}
}

// pgTypedCase is one text PostgreSQL may hand over for a column stored as
// As, and whether the writer stores it typed.
type pgTypedCase struct {
	As, Raw string
	OK      bool
}

// PGTypedCorpus is every edge case the writer is held to: what it stores
// typed (OK), each time as the value that renders back to the same text,
// and what it leaves to a text file. The views' test reads the same corpus
// through the casts of the views (TestPGTypedAgreesWithTheViewsCasts).
var PGTypedCorpus = []pgTypedCase{
	{"SMALLINT", "0", true}, {"SMALLINT", "1", true}, {"SMALLINT", "-1", true},
	{"SMALLINT", "32767", true}, {"SMALLINT", "-32768", true},
	{"SMALLINT", "32768", false}, {"SMALLINT", "-32769", false},
	{"SMALLINT", "-0", false}, {"SMALLINT", "+1", false}, {"SMALLINT", "01", false},
	{"SMALLINT", " 1", false}, {"SMALLINT", "1 ", false}, {"SMALLINT", "", false},
	{"SMALLINT", "1.0", false}, {"SMALLINT", "1e2", false}, {"SMALLINT", "0x10", false},
	{"SMALLINT", "1_000", false}, {"SMALLINT", "abc", false},
	{"INTEGER", "2147483647", true}, {"INTEGER", "-2147483648", true}, {"INTEGER", "42", true},
	{"INTEGER", "2147483648", false}, {"INTEGER", "-2147483649", false}, {"INTEGER", "1.5", false},
	{"BIGINT", "9223372036854775807", true}, {"BIGINT", "-9223372036854775808", true},
	{"BIGINT", "9223372036854775808", false}, {"BIGINT", "00", false},
	{"UINTEGER", "0", true}, {"UINTEGER", "4294967295", true}, {"UINTEGER", "16384", true},
	{"UINTEGER", "4294967296", false}, {"UINTEGER", "-1", false}, {"UINTEGER", "+1", false}, {"UINTEGER", "01", false},
	{"DATE", "2024-02-29", true}, {"DATE", "1970-01-01", true}, {"DATE", "1969-12-31", true},
	{"DATE", "0001-01-01", true}, {"DATE", "1582-10-10", true}, {"DATE", "9999-12-31", true},
	{"DATE", "10000-01-01", true}, {"DATE", "294276-12-31", true}, {"DATE", "5874897-12-31", true},
	{"DATE", "infinity", true}, {"DATE", "-infinity", true},
	{"DATE", "2023-02-29", false}, {"DATE", "2024-13-01", false}, {"DATE", "2024-00-10", false},
	{"DATE", "2024-01-00", false}, {"DATE", "2024-04-31", false}, {"DATE", "0000-01-01", false},
	{"DATE", "0001-01-01 BC", false}, {"DATE", "2024-01-05 BC", false},
	{"DATE", "Infinity", false}, {"DATE", "2024-1-5", false}, {"DATE", " 2024-01-05", false},
	{"DATE", "2024-01-05 ", false}, {"DATE", "", false}, {"DATE", "20240105", false},
	{"DATE", "01000-01-01", false}, {"DATE", "2024/01/05", false},
	{"UUID", "12345678-9abc-def0-1234-56789abcdef0", true},
	{"UUID", "00000000-0000-0000-0000-000000000000", true},
	{"UUID", "ffffffff-ffff-ffff-ffff-ffffffffffff", true},
	{"UUID", "12345678-9ABC-DEF0-1234-56789ABCDEF0", false},
	{"UUID", "123456789abcdef0123456789abcdef0", false},
	{"UUID", "{12345678-9abc-def0-1234-56789abcdef0}", false},
	{"UUID", "12345678-9abc-def0-1234-56789abcdef", false},
	{"UUID", "12345678-9abc-def0-1234-56789abcdefg", false}, {"UUID", "", false},
	{"DECIMAL(12,2)", "0.00", true}, {"DECIMAL(12,2)", "1.50", true}, {"DECIMAL(12,2)", "-1.50", true},
	{"DECIMAL(12,2)", "0.05", true}, {"DECIMAL(12,2)", "-0.05", true},
	{"DECIMAL(12,2)", "9999999999.99", true}, {"DECIMAL(12,2)", "-9999999999.99", true},
	{"DECIMAL(12,2)", "10000000000.00", false}, {"DECIMAL(12,2)", "-0.00", false},
	{"DECIMAL(12,2)", "1.5", false}, {"DECIMAL(12,2)", "1.505", false}, {"DECIMAL(12,2)", "1", false},
	{"DECIMAL(12,2)", "NaN", false}, {"DECIMAL(12,2)", "Infinity", false}, {"DECIMAL(12,2)", "-Infinity", false},
	{"DECIMAL(12,2)", "1e2", false}, {"DECIMAL(12,2)", ".50", false}, {"DECIMAL(12,2)", "+1.50", false},
	{"DECIMAL(12,2)", "01.50", false}, {"DECIMAL(12,2)", " 1.50", false}, {"DECIMAL(12,2)", "", false},
	{"DECIMAL(12,2)", "1.-5", false}, {"DECIMAL(12,2)", "-", false},
	{"DECIMAL(5,2)", "999.99", true}, {"DECIMAL(5,2)", "-999.99", true}, {"DECIMAL(5,2)", "1000.00", false},
	{"DECIMAL(9,0)", "999999999", true}, {"DECIMAL(9,0)", "-999999999", true},
	{"DECIMAL(15,0)", "0", true}, {"DECIMAL(15,0)", "-15", true}, {"DECIMAL(15,0)", "999999999999999", true},
	{"DECIMAL(15,0)", "-0", false}, {"DECIMAL(15,0)", "1000000000000000", false}, {"DECIMAL(15,0)", "1.0", false},
	{"DECIMAL(18,4)", "99999999999999.9999", true}, {"DECIMAL(18,4)", "-99999999999999.9999", true},
	{"DECIMAL(19,2)", "99999999999999999.99", true}, {"DECIMAL(19,2)", "-99999999999999999.99", true},
	{"DECIMAL(19,2)", "0.01", true},
	{"DECIMAL(38,4)", "1234567890123456789012345678901234.5678", true},
	{"DECIMAL(38,4)", "-9999999999999999999999999999999999.9999", true},
	{"DECIMAL(38,4)", "0.0001", true},
	{"DECIMAL(38,4)", "99999999999999999999999999999999999.0000", false},
	{"DECIMAL(38,0)", "99999999999999999999999999999999999999", true},
	{"DECIMAL(38,0)", "-99999999999999999999999999999999999999", true},
	{"DECIMAL(38,0)", "100000000000000000000000000000000000000", false},
	{"DECIMAL(38,37)", "9.9999999999999999999999999999999999999", true},
	{"DECIMAL(38,37)", "-0.0000000000000000000000000000000000001", true},
}

// Every value the writer stores typed renders back as the text it was
// written from, and every other one is refused, never stored as another
// value.
func TestPGTypedValues(t *testing.T) {
	for _, c := range PGTypedCorpus {
		kind, err := pgTypedKindOf(c.As)
		if err != nil {
			t.Fatalf("%s: %v", c.As, err)
		}
		p, s, _ := pgDecimalOf(c.As)
		v, ok := pgTypedValueOf(kind, p, s, c.Raw)
		if ok != c.OK {
			t.Errorf("%s %q: stored typed %v, want %v", c.As, c.Raw, ok, c.OK)
			continue
		}
		if ok {
			if back := pgTypedText(kind, s, v); back != c.Raw {
				t.Errorf("%s %q renders back as %q", c.As, c.Raw, back)
			}
		}
	}
}

// The date arithmetic agrees with itself across the whole range a column
// can hold, and infinity is the engine's own.
func TestPGDateDays(t *testing.T) {
	for _, d := range []int64{-719162, -1, 0, 1, 19782, 2932896, 2932897, 2145031948} {
		y, m, dd := civilFromDays(d)
		if back := daysFromCivil(y, m, dd); back != d {
			t.Errorf("%d -> %d-%d-%d -> %d", d, y, m, dd, back)
		}
	}
	if days, _ := pgDateDays("1970-01-01"); days != 0 {
		t.Fatalf("1970-01-01 is day %d", days)
	}
	if days, _ := pgDateDays("infinity"); days != math.MaxInt32 {
		t.Fatalf("infinity is day %d", days)
	}
	if days, _ := pgDateDays("-infinity"); days != -math.MaxInt32 {
		t.Fatalf("-infinity is day %d", days)
	}
}

// A writer told to store a column the table does not have refuses.
func TestNewPGWriterUnknownColumn(t *testing.T) {
	_, err := NewPGWriter(t.TempDir()+"/t.parquet", []string{"id", "a"}, map[string]string{"b": "INTEGER"}, WriterConfig{Compression: "none", RowGroupSize: 10})
	if err == nil || !strings.Contains(err.Error(), "b") {
		t.Fatalf("got %v", err)
	}
}

// A list of stored columns that cannot be used is recorded as such, with the
// file kept: the views refuse the table rather than read it otherwise.
func TestApplyPGTypedUnusable(t *testing.T) {
	rel := PGRelation{OID: 1, PrimaryKey: []string{"id"}, Columns: []PGColumn{
		{Name: "id", TypeOID: pgOIDInt8, TypeMod: -1}, {Name: "a", TypeOID: pgOIDInt4, TypeMod: -1},
	}}
	for value, unusable := range map[string]bool{
		`{"v":1,"columns":{"a":"INTEGER"}}`: false,
		`{"v":2,"columns":{"a":"INTEGER"}}`: true,
		`{"v":1,"columns":{"a":"BIGINT"}}`:  true,
		`{"v":1,"columns":{"id":"BIGINT"}}`: true,
	} {
		st := footerScan{footers: map[string]TableFooter{"f": {PG: PGCasts(rel), PGRelation: &rel}}, unparsable: map[string]bool{}}
		st.collectPGTyped("f", value)
		st.applyPGTyped()
		f, ok := st.footers["f"]
		if !ok || (f.PGTypedUnusable != "") != unusable {
			t.Errorf("%s: footer kept %v, unusable %q", value, ok, f.PGTypedUnusable)
		}
		if !unusable && f.PGStored["a"] != "INTEGER" {
			t.Errorf("%s: stored %v", value, f.PGStored)
		}
	}
}

// A file that stores columns typed and lost its PostgreSQL entry keeps one
// that refuses, and is not reported as having no schema.
func TestRefuseUnreadPGTyped(t *testing.T) {
	st := footerScan{footers: map[string]TableFooter{}, unparsable: map[string]bool{"f": true}, failed: map[string]bool{}}
	st.collectPGTyped("f", `{"v":1,"columns":{"a":"INTEGER"}}`)
	st.refuseUnreadPGTyped()
	if f := st.footers["f"]; f.PGTypedUnusable == "" || f.PG != nil || st.unparsable["f"] {
		t.Fatalf("got %+v, unparsable %v", f, st.unparsable["f"])
	}
}
