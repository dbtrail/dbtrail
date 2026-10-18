package baseline

import (
	"strings"
	"testing"
)

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

// What a CREATE TABLE needs beyond the column list (#1596 slice 2) round-trips:
// NOT NULL, a default with quotes and a newline, identity, an owned sequence,
// the position among the generated columns, a composite primary key in key
// order, and a STORED generated column with its expression.
func TestPGRelation_createTableFacts(t *testing.T) {
	in := PGRelation{
		OID: 16500,
		Columns: []PGColumn{
			{Name: "id", TypeOID: 23, TypeMod: -1, Type: "integer", NotNull: true, Default: "nextval('public.\"Odd seq\"'::regclass)", Sequence: `public."Odd seq"`, Ordinal: 1},
			{Name: "tenant", TypeOID: 23, TypeMod: -1, Type: "integer", NotNull: true, Identity: "d", Sequence: "public.t_tenant_seq", Ordinal: 2},
			{Name: "note", TypeOID: 25, TypeMod: -1, Type: "text", Default: "'it''s\nhere'::text", Ordinal: 4},
		},
		PrimaryKey: []string{"tenant", "id"},
		Generated:  []PGGeneratedColumn{{Name: "twice", Type: "integer", Expr: "(id * 2)", Ordinal: 3}},
	}
	s, err := in.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := ParsePGRelation(s)
	if err != nil || !ok {
		t.Fatalf("ParsePGRelation(%q) = ok %v, err %v", s, ok, err)
	}
	if len(got.Columns) != 3 || got.Columns[0] != in.Columns[0] || got.Columns[1] != in.Columns[1] || got.Columns[2] != in.Columns[2] {
		t.Fatalf("columns = %+v", got.Columns)
	}
	if strings.Join(got.PrimaryKey, ",") != "tenant,id" || len(got.Generated) != 1 || got.Generated[0] != in.Generated[0] {
		t.Fatalf("key %v generated %+v", got.PrimaryKey, got.Generated)
	}
	// The field names are a contract with every reader, old and new.
	if !strings.Contains(s, `"not_null":true`) || !strings.Contains(s, `"identity":"d"`) || !strings.Contains(s, `"sequence":`) ||
		!strings.Contains(s, `"ordinal":1`) || !strings.Contains(s, `"primary_key":["tenant","id"]`) || !strings.Contains(s, `"generated":[{"name":"twice","type":"integer","expr":"(id * 2)","ordinal":3}]`) {
		t.Fatalf("Encode = %s", s)
	}
}

// A footer that says something wrong about the table is refused, never used
// to write a CREATE TABLE that differs from it.
func TestParsePGRelation_badCreateTableFacts(t *testing.T) {
	col := `{"name":"a","type_oid":25,"typmod":-1}`
	for name, s := range map[string]string{
		"a key column that is not a column": `{"oid":7,"columns":[` + col + `],"primary_key":["b"]}`,
		"an empty key column":               `{"oid":7,"columns":[` + col + `],"primary_key":[""]}`,
		"a key column twice":                `{"oid":7,"columns":[` + col + `],"primary_key":["a","a"]}`,
		"a generated key column":            `{"oid":7,"columns":[` + col + `],"primary_key":["g"],"generated":[{"name":"g","type":"integer","expr":"1"}]}`,
		"an unknown identity":               `{"oid":7,"columns":[{"name":"a","type_oid":25,"typmod":-1,"identity":"x"}]}`,
		"a generated column named twice":    `{"oid":7,"columns":[` + col + `],"generated":[{"name":"a","type":"integer","expr":"1"}]}`,
		"a generated column with no type":   `{"oid":7,"columns":[` + col + `],"generated":[{"name":"g","expr":"1"}]}`,
		"a generated column with no expr":   `{"oid":7,"columns":[` + col + `],"generated":[{"name":"g","type":"integer"}]}`,
		"a generated column with no name":   `{"oid":7,"columns":[` + col + `],"generated":[{"type":"integer","expr":"1"}]}`,
	} {
		if _, _, err := ParsePGRelation(s); err == nil {
			t.Errorf("%s: ParsePGRelation(%q) accepted it", name, s)
		}
	}
}
