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
	}}
	want := []PGCast{
		{"id", "BIGINT"}, {"amount", "DECIMAL(12,2)"}, {"note", PGAsText}, {"free", PGAsNumeric},
		{"wide", ""}, {"neg_scale", ""}, {"scale_over", ""}, {"max", "DECIMAL(38,38)"},
		{"flag", "BOOLEAN"}, {"small", "SMALLINT"}, {"n", "INTEGER"}, {"r", "FLOAT"}, {"d", "DOUBLE"},
		{"day", "DATE"}, {"ts", "TIMESTAMP"}, {"tstz", "TIMESTAMPTZ"}, {"tag", "UUID"}, {"payload", PGAsBytes},
		{"code", PGAsText}, {"fixed", PGAsText}, {"attrs", ""}, {"labels", ""}, {"at", ""}, {"span", ""}, {"mood", ""},
		{"ident", PGAsText}, {"ref", "UINTEGER"},
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
