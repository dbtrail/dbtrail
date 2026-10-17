package metadata

import (
	"reflect"
	"testing"
)

func shape(id int, oid uint32, schema, table string) PGShape {
	return PGShape{SnapshotID: id, Relation: PGRelationSchema{OID: oid, Schema: schema, Table: table,
		Columns: []PGRelationColumn{{Name: "id", Ordinal: 1, TypeOID: 23, TypeMod: -1}}}}
}

func ids(s []PGShape) []int {
	out := make([]int, len(s))
	for i, x := range s {
		out[i] = x.SnapshotID
	}
	return out
}

// The rules for which stored shapes are still "the last one seen".
func TestLatestPGShapes(t *testing.T) {
	cases := []struct {
		name string
		in   []PGShape
		want []int
	}{
		{"empty", nil, []int{}},
		{"one table", []PGShape{shape(1, 10, "s", "a")}, []int{1}},
		// Older rows of the index carry no OID: kept, keyed by name.
		{"legacy alone", []PGShape{shape(1, 0, "s", "a")}, []int{1}},
		// Every restart wrote a snapshot: the newest per OID / per legacy
		// name wins.
		{"newest per oid", []PGShape{shape(1, 10, "s", "a"), shape(2, 10, "s", "a"), shape(3, 10, "s", "b")}, []int{3}},
		{"newest legacy per name", []PGShape{shape(1, 0, "s", "a"), shape(2, 0, "s", "a")}, []int{2}},
		// Identified under the name, then renamed away: the legacy shape of
		// the old name stays superseded.
		{"legacy superseded by a since-renamed table", []PGShape{shape(1, 0, "s", "a"), shape(2, 10, "s", "a"), shape(3, 10, "s", "b")}, []int{3}},
		// Trailing space is a different PostgreSQL name.
		{"trailing space is another name", []PGShape{shape(1, 0, "s", "a"), shape(2, 10, "s", "a ")}, []int{1, 2}},
		// After the upgrade the same table is written again with its OID:
		// the newer, identified shape replaces the legacy one.
		{"legacy superseded by identified", []PGShape{shape(1, 0, "s", "a"), shape(2, 10, "s", "a")}, []int{2}},
		// A legacy shape newer than an identified one of the same name (a
		// downgrade wrote it) is the last one seen; both are kept and the
		// caller takes the newest by name.
		{"legacy newer than identified", []PGShape{shape(1, 10, "s", "a"), shape(2, 0, "s", "a")}, []int{1, 2}},
		// Two OIDs under one name: a drop and a recreate. Both are kept; the
		// old OID is what tells the stream the name was reused.
		{"name reused by a new oid", []PGShape{shape(1, 10, "s", "a"), shape(2, 11, "s", "a")}, []int{1, 2}},
		// Same name in two schemas is two tables.
		{"same table name, two schemas", []PGShape{shape(1, 0, "s", "a"), shape(2, 10, "t", "a")}, []int{1, 2}},
		// Identifiers are case-sensitive in PostgreSQL.
		{"case-distinct names", []PGShape{shape(1, 0, "s", "a"), shape(2, 10, "s", "A")}, []int{1, 2}},
		{"case-distinct legacy names", []PGShape{shape(1, 0, "s", "a"), shape(2, 0, "s", "A")}, []int{1, 2}},
		// Input order does not matter; output is by snapshot_id.
		{"unsorted input", []PGShape{shape(3, 11, "s", "b"), shape(1, 0, "s", "a"), shape(2, 10, "s", "a")}, []int{2, 3}},
	}
	for _, c := range cases {
		if got := ids(latestPGShapes(c.in)); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}
