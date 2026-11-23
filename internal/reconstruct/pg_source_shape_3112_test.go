package reconstruct

import (
	"context"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// The source's columns now against the columns a snapshot's file records
// (#3112): what refuses, what does not, and which table is compared.
func TestCheckPGSourceShape_3112(t *testing.T) {
	const int4, int8, text, numeric = 23, 20, 25, 1700
	col := func(name string, oid uint32, mod int32, attnum int) baseline.PGColumn {
		return baseline.PGColumn{Name: name, TypeOID: oid, TypeMod: mod, Ordinal: attnum}
	}
	file := baseline.PGRelation{OID: 500, PrimaryKey: []string{"id"}, Columns: []baseline.PGColumn{
		col("id", int4, -1, 1), col("status", text, -1, 2), col("amount", numeric, 655366, 3),
	}}
	same := func() []baseline.PGColumn { return append([]baseline.PGColumn(nil), file.Columns...) }
	name := PGTableName{"public", "orders"}

	cases := []struct {
		what   string
		rel    baseline.PGRelation
		source map[PGTableName]PGSourceShape
		want   []string // every one in the refusal; nil = not refused
	}{
		{"the same columns", file, map[PGTableName]PGSourceShape{name: {500, same()}}, nil},
		{"a column dropped", file, map[PGTableName]PGSourceShape{name: {500, same()[:2]}},
			[]string{"public.orders", "dropped amount", "on the source"}},
		{"a column added", file, map[PGTableName]PGSourceShape{name: {500, append(same(), col("region", text, -1, 4))}},
			[]string{"added region"}},
		{"a column renamed", file, map[PGTableName]PGSourceShape{name: {500, []baseline.PGColumn{
			col("id", int4, -1, 1), col("state", text, -1, 2), col("amount", numeric, 655366, 3)}}},
			[]string{"added state", "dropped status"}},
		{"a type changed", file, map[PGTableName]PGSourceShape{name: {500, []baseline.PGColumn{
			col("id", int8, -1, 1), col("status", text, -1, 2), col("amount", numeric, 655366, 3)}}},
			[]string{"retyped id"}},
		{"only a type's modifier changed", file, map[PGTableName]PGSourceShape{name: {500, []baseline.PGColumn{
			col("id", int4, -1, 1), col("status", text, -1, 2), col("amount", numeric, 786442, 3)}}},
			[]string{"retyped amount"}},
		{"dropped and added again with the same name and type", file, map[PGTableName]PGSourceShape{name: {500, []baseline.PGColumn{
			col("id", int4, -1, 1), col("status", text, -1, 2), col("amount", numeric, 655366, 4)}}},
			[]string{"dropped and added again amount"}},
		{"several at once are all said", file, map[PGTableName]PGSourceShape{name: {500, []baseline.PGColumn{
			col("id", int8, -1, 1), col("amount", numeric, 655366, 5), col("region", text, -1, 4)}}},
			[]string{"added region", "dropped status", "retyped id", "dropped and added again amount"}},
		{"every column gone", file, map[PGTableName]PGSourceShape{name: {500, nil}},
			[]string{"dropped id, status, amount"}},
		{"another table under the name", file, map[PGTableName]PGSourceShape{name: {501, same()}},
			[]string{"OID 501", "OID 500", "dropped and created again"}},

		// A file written before attnum was recorded: its columns are still
		// compared by name and type, and nothing is made of a number it does
		// not have.
		{"a file with no attnum, same columns", baseline.PGRelation{OID: 500, Columns: []baseline.PGColumn{
			col("id", int4, -1, 0), col("status", text, -1, 0), col("amount", numeric, 655366, 0)}},
			map[PGTableName]PGSourceShape{name: {500, []baseline.PGColumn{
				col("id", int4, -1, 1), col("status", text, -1, 2), col("amount", numeric, 655366, 7)}}}, nil},
		{"a file with no attnum, a column dropped", baseline.PGRelation{OID: 500, Columns: []baseline.PGColumn{
			col("id", int4, -1, 0), col("status", text, -1, 0)}},
			map[PGTableName]PGSourceShape{name: {500, []baseline.PGColumn{col("id", int4, -1, 1)}}},
			[]string{"dropped status"}},

		// What a file records beyond the list of columns is not compared: a
		// default, NOT NULL, the key. The source's shape never carries them.
		{"a default and NOT NULL on the file only", baseline.PGRelation{OID: 500, Columns: []baseline.PGColumn{
			{Name: "id", TypeOID: int4, TypeMod: -1, Ordinal: 1, NotNull: true, Default: "7", Type: "integer"}}},
			map[PGTableName]PGSourceShape{name: {500, []baseline.PGColumn{col("id", int4, -1, 1)}}}, nil},

		// Which table: the two parts of the name, exactly.
		{"nothing was read from the source", file, nil, nil},
		{"the table is not among the source's", file, map[PGTableName]PGSourceShape{}, nil},
		{"a name that differs by case is another table", file,
			map[PGTableName]PGSourceShape{{"public", "Orders"}: {501, nil}, {"Public", "orders"}: {502, nil}}, nil},
		{"the same table name in another schema is another table", file,
			map[PGTableName]PGSourceShape{{"other", "orders"}: {501, nil}, name: {500, same()}}, nil},
		{"a dot inside a name does not move it to another schema", file,
			map[PGTableName]PGSourceShape{{"public.orders", ""}: {501, nil}, {"", "public.orders"}: {502, nil}}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.what, func(t *testing.T) {
			err := checkPGSourceShape(WithPGSourceShapes(context.Background(), tc.source), "public", "orders", tc.rel)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("not refused")
			}
			for _, w := range append(tc.want, "take a new snapshot") {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("the refusal does not say %q: %v", w, err)
				}
			}
		})
	}
}

// A name with a dot in it is compared under its own two parts.
func TestCheckPGSourceShape_dottedName_3112(t *testing.T) {
	rel := baseline.PGRelation{OID: 500, Columns: []baseline.PGColumn{{Name: "id", TypeOID: 23, TypeMod: -1, Ordinal: 1}}}
	shapes := map[PGTableName]PGSourceShape{
		{"a.b", "c"}: {500, rel.Columns},
		{"a", "b.c"}: {500, nil}, // this one changed
	}
	ctx := WithPGSourceShapes(context.Background(), shapes)
	if err := checkPGSourceShape(ctx, "a.b", "c", rel); err != nil {
		t.Fatalf(`"a.b"."c" was compared with another table's columns: %v`, err)
	}
	if err := checkPGSourceShape(ctx, "a", "b.c", rel); err == nil {
		t.Fatal(`"a"."b.c" changed and was not refused`)
	}
}

// Nothing read from the source puts nothing on the context: a fold that is
// not a scheduled update compares nothing.
func TestWithPGSourceShapes_nilCarriesNothing_3112(t *testing.T) {
	ctx := context.Background()
	if got := WithPGSourceShapes(ctx, nil); got != ctx {
		t.Fatal("a nil map changed the context")
	}
	if PGSourceShapesIn(ctx) != nil {
		t.Fatal("a plain context carries shapes")
	}
	empty := map[PGTableName]PGSourceShape{}
	if got := PGSourceShapesIn(WithPGSourceShapes(ctx, empty)); got == nil {
		t.Fatal("an answer with no table reads as no answer")
	}
}
