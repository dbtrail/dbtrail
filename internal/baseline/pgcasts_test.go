package baseline

import (
	"reflect"
	"testing"
)

func TestPGCasts(t *testing.T) {
	numeric := func(precision, scale int32) int32 { return ((precision << 16) | (scale & 0x7ff)) + 4 }
	rel := PGRelation{OID: 1, Columns: []PGColumn{
		// Listed out of table order on purpose: Ordinal decides.
		{Name: "note", TypeOID: 25, TypeMod: -1, Ordinal: 3},
		{Name: "id", TypeOID: 20, TypeMod: -1, Ordinal: 1},
		{Name: "amount", TypeOID: 1700, TypeMod: numeric(12, 2), Ordinal: 2},
		{Name: "free", TypeOID: 1700, TypeMod: -1, Ordinal: 4},
		{Name: "wide", TypeOID: 1700, TypeMod: numeric(60, 2), Ordinal: 5},
		{Name: "neg_scale", TypeOID: 1700, TypeMod: numeric(5, -2), Ordinal: 6},
		{Name: "scale_over", TypeOID: 1700, TypeMod: numeric(3, 5), Ordinal: 7},
		{Name: "max", TypeOID: 1700, TypeMod: numeric(38, 38), Ordinal: 8},
		{Name: "flag", TypeOID: 16, Ordinal: 9},
		{Name: "small", TypeOID: 21, Ordinal: 10},
		{Name: "n", TypeOID: 23, Ordinal: 11},
		{Name: "r", TypeOID: 700, Ordinal: 12},
		{Name: "d", TypeOID: 701, Ordinal: 13},
		{Name: "day", TypeOID: 1082, Ordinal: 14},
		{Name: "ts", TypeOID: 1114, Ordinal: 15},
		{Name: "tstz", TypeOID: 1184, Ordinal: 16},
		{Name: "tag", TypeOID: 2950, Ordinal: 17},
		{Name: "payload", TypeOID: 17, Ordinal: 18},
		{Name: "code", TypeOID: 1043, TypeMod: 20, Ordinal: 19},
		{Name: "fixed", TypeOID: 1042, TypeMod: 8, Ordinal: 20},
		{Name: "attrs", TypeOID: 3802, Ordinal: 21},
		{Name: "labels", TypeOID: 1009, Ordinal: 22},
		{Name: "at", TypeOID: 1083, Ordinal: 23},
		{Name: "span", TypeOID: 1186, Ordinal: 24},
		{Name: "mood", TypeOID: 16390, Ordinal: 25},
		{Name: "ident", TypeOID: 19, Ordinal: 26},
		{Name: "ref", TypeOID: 26, Ordinal: 27},
		{Name: "ints", TypeOID: 1007, TypeMod: -1, Ordinal: 28},
		{Name: "bigs", TypeOID: 1016, TypeMod: -1, Ordinal: 29},
		{Name: "smalls", TypeOID: 1005, TypeMod: -1, Ordinal: 30},
		{Name: "flags", TypeOID: 1000, TypeMod: -1, Ordinal: 31},
		{Name: "codes", TypeOID: 1015, TypeMod: 20, Ordinal: 32},
		{Name: "fixeds", TypeOID: 1014, TypeMod: 8, Ordinal: 33},
		{Name: "names", TypeOID: 1003, TypeMod: -1, Ordinal: 34},
		{Name: "reals", TypeOID: 1021, TypeMod: -1, Ordinal: 35},
		{Name: "doubles", TypeOID: 1022, TypeMod: -1, Ordinal: 36},
		{Name: "refs", TypeOID: 1028, TypeMod: -1, Ordinal: 37},
		{Name: "days", TypeOID: 1182, TypeMod: -1, Ordinal: 38},
		{Name: "stamps", TypeOID: 1115, TypeMod: -1, Ordinal: 39},
		{Name: "stampz", TypeOID: 1185, TypeMod: -1, Ordinal: 40},
		{Name: "tags", TypeOID: 2951, TypeMod: -1, Ordinal: 41},
		{Name: "prices", TypeOID: 1231, TypeMod: numeric(10, 2), Ordinal: 42},
		{Name: "frees", TypeOID: 1231, TypeMod: -1, Ordinal: 43},
		{Name: "wides", TypeOID: 1231, TypeMod: numeric(60, 2), Ordinal: 44},
		{Name: "blobs", TypeOID: 1001, TypeMod: -1, Ordinal: 45},
		{Name: "docs", TypeOID: 3807, TypeMod: -1, Ordinal: 46},
		{Name: "spans", TypeOID: 1187, TypeMod: -1, Ordinal: 47},
		{Name: "moods", TypeOID: 16391, TypeMod: -1, Ordinal: 48},
	}}
	want := []PGCast{
		{"id", "BIGINT"}, {"amount", "DECIMAL(12,2)"}, {"note", PGAsText}, {"free", PGAsNumeric},
		{"wide", ""}, {"neg_scale", ""}, {"scale_over", ""}, {"max", "DECIMAL(38,38)"},
		{"flag", "BOOLEAN"}, {"small", "SMALLINT"}, {"n", "INTEGER"}, {"r", "FLOAT"}, {"d", "DOUBLE"},
		{"day", "DATE"}, {"ts", "TIMESTAMP"}, {"tstz", "TIMESTAMPTZ"}, {"tag", "UUID"}, {"payload", PGAsBytes},
		{"code", PGAsText}, {"fixed", PGAsText}, {"attrs", ""}, {"labels", "VARCHAR[]"}, {"at", ""}, {"span", ""}, {"mood", ""},
		{"ident", PGAsText}, {"ref", "UINTEGER"},
		{"ints", "INTEGER[]"}, {"bigs", "BIGINT[]"}, {"smalls", "SMALLINT[]"}, {"flags", "BOOLEAN[]"},
		{"codes", "VARCHAR[]"}, {"fixeds", "VARCHAR[]"}, {"names", "VARCHAR[]"}, {"reals", "FLOAT[]"},
		{"doubles", "DOUBLE[]"}, {"refs", "UINTEGER[]"}, {"days", "DATE[]"}, {"stamps", "TIMESTAMP[]"},
		{"stampz", "TIMESTAMPTZ[]"}, {"tags", "UUID[]"}, {"prices", "DECIMAL(10,2)[]"},
		// Left as text: a numeric with no precision or too wide, bytea,
		// jsonb, interval, and an array type made by a user.
		{"frees", ""}, {"wides", ""}, {"blobs", ""}, {"docs", ""}, {"spans", ""}, {"moods", ""},
	}
	if got := PGCasts(rel); !reflect.DeepEqual(got, want) {
		t.Fatalf("PGCasts:\n got %v\nwant %v", got, want)
	}
	if rel.Columns[0].Name != "note" {
		t.Fatal("PGCasts reordered the relation it was given")
	}
}

// A snapshot written before the ordinal was recorded keeps the order its
// columns were listed in, and one ordinal missing is the same as none.
func TestPGCastsWithoutOrdinals(t *testing.T) {
	rel := PGRelation{OID: 1, Columns: []PGColumn{
		{Name: "b", TypeOID: 23, Ordinal: 2},
		{Name: "a", TypeOID: 23},
	}}
	got := PGCasts(rel)
	if len(got) != 2 || got[0].Name != "b" || got[1].Name != "a" {
		t.Fatalf("got %v, want the listed order b, a", got)
	}
}

func TestPGListElement(t *testing.T) {
	for as, want := range map[string]string{
		"INTEGER[]": "INTEGER", "DECIMAL(10,2)[]": "DECIMAL(10,2)", "VARCHAR[]": "VARCHAR",
	} {
		if got, ok := PGListElement(as); !ok || got != want {
			t.Errorf("PGListElement(%q) = %q, %v, want %q", as, got, ok, want)
		}
	}
	for _, as := range []string{"", "[]", "INTEGER", PGAsText, PGAsBytes, PGAsNumeric, "DATE"} {
		if got, ok := PGListElement(as); ok {
			t.Errorf("PGListElement(%q) = %q, want not a list", as, got)
		}
	}
}
