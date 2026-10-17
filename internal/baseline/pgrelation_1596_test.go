package baseline

import "testing"

// The footer value round-trips, including the PostgreSQL shapes that are easy
// to get wrong: typmod -1 (no modifier), a quoted identifier with a space and
// a non-ASCII name, an array type, and the full uint32 OID range.
func TestPGRelation_roundTrip(t *testing.T) {
	in := PGRelation{
		OID: 4294967295,
		Columns: []PGColumn{
			{Name: "id", TypeOID: 23, TypeMod: -1, Type: "integer"},
			{Name: "Order Note", TypeOID: 1043, TypeMod: 44, Type: "character varying(40)"},
			{Name: "año", TypeOID: 1007, TypeMod: -1, Type: "integer[]"},
		},
	}
	s, err := in.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := ParsePGRelation(s)
	if err != nil || !ok {
		t.Fatalf("ParsePGRelation(%q) = ok %v, err %v", s, ok, err)
	}
	if got.OID != in.OID || len(got.Columns) != len(in.Columns) {
		t.Fatalf("got %+v, want %+v", got, in)
	}
	for i := range in.Columns {
		if got.Columns[i] != in.Columns[i] {
			t.Errorf("column %d: got %+v, want %+v", i, got.Columns[i], in.Columns[i])
		}
	}
}

// The JSON field names are a contract with #2441, which compares a pgoutput
// Relation message's columns against {name, type_oid, typmod}.
func TestPGRelation_fieldNames(t *testing.T) {
	s, err := PGRelation{OID: 7, Columns: []PGColumn{{Name: "a", TypeOID: 25, TypeMod: -1, Type: "text"}}}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	want := `{"oid":7,"columns":[{"name":"a","type_oid":25,"typmod":-1,"type":"text"}]}`
	if s != want {
		t.Fatalf("Encode = %s, want %s", s, want)
	}
}

func TestParsePGRelation_absentAndInvalid(t *testing.T) {
	for _, s := range []string{"", "   ", "\n"} {
		if _, ok, err := ParsePGRelation(s); ok || err != nil {
			t.Errorf("ParsePGRelation(%q) = ok %v, err %v; want absent, no error", s, ok, err)
		}
	}
	bad := map[string]string{
		"not json":       "{",
		"zero oid":       `{"oid":0,"columns":[{"name":"a","type_oid":25,"typmod":-1}]}`,
		"no columns":     `{"oid":7,"columns":[]}`,
		"empty name":     `{"oid":7,"columns":[{"name":"","type_oid":25,"typmod":-1}]}`,
		"zero type oid":  `{"oid":7,"columns":[{"name":"a","type_oid":0,"typmod":-1}]}`,
		"duplicate name": `{"oid":7,"columns":[{"name":"a","type_oid":25,"typmod":-1},{"name":"a","type_oid":23,"typmod":-1}]}`,
		"negative oid":   `{"oid":-1,"columns":[{"name":"a","type_oid":25,"typmod":-1}]}`,
	}
	for name, s := range bad {
		if _, ok, err := ParsePGRelation(s); err == nil {
			t.Errorf("%s: ParsePGRelation(%q) = ok %v, nil error; want an error", name, s, ok)
		}
	}
}

// Column names are case-sensitive in PostgreSQL ("A" and "a" are two
// columns), so they are not duplicates.
func TestParsePGRelation_caseDistinctNamesAreNotDuplicates(t *testing.T) {
	s := `{"oid":7,"columns":[{"name":"a","type_oid":25,"typmod":-1},{"name":"A","type_oid":25,"typmod":-1}]}`
	if _, ok, err := ParsePGRelation(s); err != nil || !ok {
		t.Fatalf("ParsePGRelation = ok %v, err %v", ok, err)
	}
}
